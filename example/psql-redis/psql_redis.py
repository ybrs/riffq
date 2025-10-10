"""
This is to demonstrate using postgresql protocol to connect to redis.

It is for illustration purposes. We use hashsets as tables.

"""
import redis
from sqlglot import parse_one, exp
import logging, pyarrow as pa, riffq
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

# Note: We could put connection in handle_auth and 
#   put the connection as a property to Connection class
#   but sometimes you'd want to prepare connections before client connects
#   (eg: for creating a pool etc. and sharing connections between clients)
#   this is to illustrate that case so connections is outside of Connection class 
redis_connections = defaultdict(
    lambda: redis.Redis(host="localhost", port=6379, db=0, password=None, decode_responses=True)
)

class Connection(riffq.BaseConnection):
    def handle_auth(self, user, password, host, database=None, callback=callable):
        callback(user == "user" and password == "secret")

    def handle_insert(self, ast, callback):
        """INSERT INTO <table> (id, value) VALUES (...)

        Supports only columns [id, value]. Multiple VALUES tuples allowed.
        """
        table = next(ast.find_all(exp.Table)).name
        cols_exp = ast.args.get("columns") or []
        cols = []
        for c in cols_exp:
            if isinstance(c, exp.Identifier):
                cols.append(c.name)
            elif isinstance(c, exp.Column):
                cols.append(c.name)
            else:
                cols.append(str(c))
        if cols and cols != ["id", "value"]:
            return callback(("ERROR", "42601", "only columns (id, value) are supported"), is_error=True)

        rows = []
        values_node = ast.args.get("expression")
        if isinstance(values_node, exp.Values):
            tuples = values_node.expressions or []
        else:
            tuples = []
        for row in tuples:
            values = []
            # row is Tuple of expressions
            for v in getattr(row, "expressions", []):
                if isinstance(v, exp.Literal):
                    values.append(v.this)
                elif isinstance(v, exp.Identifier):
                    values.append(v.name)
                else:
                    values.append(v.sql())
            rows.append(values)

        if not rows:
            return callback(("ERROR", "42601", "INSERT requires VALUES"), is_error=True)

        r = redis_connections[self.conn_id]
        affected = 0
        key = table

        for values in rows:
            if cols:
                try:
                    idx_id = cols.index("id")
                    idx_val = cols.index("value")
                except ValueError:
                    return callback(("ERROR", "42601", "(id, value) must be provided"), is_error=True)
                row_id = str(values[idx_id])
                row_val = str(values[idx_val])
            else:
                if len(values) != 2:
                    return callback(("ERROR", "42601", "expected 2 values: (id, value)"), is_error=True)
                row_id, row_val = map(str, values)
            affected += int(r.hset(key, row_id, row_val))

        batch = self.arrow_batch([pa.array([affected])], ["rows_affected"])
        return self.send_reader(batch, callback)

    def handle_update(self, ast, callback):
        """UPDATE <table> SET value = <expr> WHERE id = <literal>"""
        table = next(ast.find_all(exp.Table)).name
        set_expr = ast.args.get("expressions") or []
        if not set_expr:
            return callback(("ERROR", "42601", "UPDATE requires SET"), is_error=True)

        # Only support single assignment to column `value`
        assignment = set_expr[0]
        if not isinstance(assignment, exp.EQ):
            return callback(("ERROR", "42601", "unsupported SET expression"), is_error=True)
        if not isinstance(assignment.left, exp.Column) or assignment.left.name != "value":
            return callback(("ERROR", "42601", "only SET value = ... is supported"), is_error=True)

        if isinstance(assignment.right, exp.Literal):
            new_value = assignment.right.this
        else:
            new_value = assignment.right.sql()

        # WHERE id = <literal>
        where = ast.args.get("where")
        if not where or not isinstance(where.this, exp.EQ):
            return callback(("ERROR", "42601", "UPDATE requires WHERE id = ..."), is_error=True)
        cond = where.this
        if not isinstance(cond.left, exp.Column) or cond.left.name != "id":
            return callback(("ERROR", "42601", "only WHERE id = ... is supported"), is_error=True)
        row_id = cond.right.this if isinstance(cond.right, exp.Literal) else cond.right.sql()

        r = redis_connections[self.conn_id]
        key = table
        # Update only if field exists
        exists = r.hexists(key, row_id)
        if exists:
            r.hset(key, row_id, str(new_value))
            affected = 1
        else:
            affected = 0
        batch = self.arrow_batch([pa.array([affected])], ["rows_affected"])
        return self.send_reader(batch, callback)

    def handle_delete(self, ast, callback):
        """DELETE FROM <table> WHERE id = <literal>"""
        table = next(ast.find_all(exp.Table)).name
        where = ast.args.get("where")
        if not where or not isinstance(where.this, exp.EQ):
            return callback(("ERROR", "42601", "DELETE requires WHERE id = ..."), is_error=True)
        cond = where.this
        if not isinstance(cond.left, exp.Column) or cond.left.name != "id":
            return callback(("ERROR", "42601", "only WHERE id = ... is supported"), is_error=True)
        row_id = cond.right.this if isinstance(cond.right, exp.Literal) else cond.right.sql()

        r = redis_connections[self.conn_id]
        key = table
        affected = int(r.hdel(key, row_id))
        batch = self.arrow_batch([pa.array([affected])], ["rows_affected"])
        return self.send_reader(batch, callback)

    def handle_select(self, ast, callback):
        """SELECT [id, value | *] FROM <table> [WHERE id = <literal>]"""
        table = next(ast.find_all(exp.Table)).name

        # Determine selected columns
        selects = ast.args.get("expressions") or []
        want_cols = []
        for s in selects:
            if isinstance(s, exp.Star):
                want_cols = ["id", "value"]
                break
            if isinstance(s, exp.Column):
                want_cols.append(s.name)
            elif isinstance(s, exp.Alias) and isinstance(s.this, exp.Column):
                want_cols.append(s.this.name)
        if not want_cols:
            want_cols = ["id", "value"]

        # WHERE id = ... (optional)
        where = ast.args.get("where")
        only_id = None
        if where and isinstance(where.this, exp.EQ):
            cond = where.this
            if isinstance(cond.left, exp.Column) and cond.left.name == "id":
                only_id = cond.right.this if isinstance(cond.right, exp.Literal) else cond.right.sql()

        r = redis_connections[self.conn_id]
        key = table
        ids = []
        values = []
        if only_id is not None:
            val = r.hget(key, str(only_id))
            if val is not None:
                ids.append(str(only_id))
                values.append(str(val))
        else:
            for k, v in r.hgetall(key).items():
                ids.append(str(k))
                values.append(str(v))

        cols = []
        names = []
        if "id" in want_cols:
            cols.append(pa.array(ids))
            names.append("id")
            
        if "value" in want_cols:
            cols.append(pa.array(values))
            names.append("value")

        batch = self.arrow_batch(cols, names)
        return self.send_reader(batch, callback)

    def handle_switch_database(self, ast, callback):
        """USE <db_index>

        Switch Redis logical database by numeric index.
        """
        ident = ast.this
        db_token = None
        if isinstance(ident, exp.Identifier):
            db_token = ident.name
        else:
            db_token = str(ident)

        try:
            db_index = int(db_token)
        except ValueError:
            return callback(("ERROR", "3D000", "USE expects numeric db index"), is_error=True)

        # Recreate client with new DB index for this connection
        redis_connections[self.conn_id] = redis.Redis(
            host="localhost", port=6379, db=db_index, password=None, decode_responses=True
        )
        # Acknowledge with a small result set
        batch = self.arrow_batch([pa.array([db_index])], ["db"])
        return self.send_reader(batch, callback)

    def _handle_query(self, sql, callback=callable, **kwargs):
        logging.info("received query %s", sql)
        ast = parse_one(sql)

        # Handle commands without tables early
        if isinstance(ast, exp.Use):
            return self.handle_switch_database(ast, callback)

        tables = list(ast.find_all(exp.Table))
        if len(tables) != 1:
            return callback(("ERROR", "42601", "you can only use one table"), is_error=True)

        if isinstance(ast, exp.Select):
            return self.handle_select(ast, callback)
        
        if isinstance(ast, exp.Insert):
            return self.handle_insert(ast, callback)

        if isinstance(ast, exp.Update):
            return self.handle_update(ast, callback)

        if isinstance(ast, exp.Delete):
            return self.handle_delete(ast, callback)

        return callback(("ERROR", "0A000", "unsupported statement"), is_error=True)

    def handle_query(self, sql, callback=..., **kwargs):
        try:
            return self._handle_query(sql, callback, **kwargs)
        except Exception as exc:
            logging.exception("execution error")
            callback(("ERROR", "XX000", str(exc)), is_error=True)

    def handle_connect(self, ip, port, callback=...):
        logging.info("connection from %s %s", ip, port)
        callback(True)

    def handle_disconnect(self, ip, port, callback=...):
        if self.conn_id in redis_connections:
            del redis_connections[self.conn_id]
        callback(True)

server = riffq.RiffqServer("127.0.0.1:5444", connection_cls=Connection)
server.start()
