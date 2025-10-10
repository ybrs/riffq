"""
This is to demonstrate using postgresql protocol to connect to redis.

It is for illustration purposes. We use hashsets as tables.

"""
import redis
from sqlglot import parse_one, exp
import logging, duckdb, pyarrow as pa, riffq
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

# Note: We could put connection in handle_auth and 
#   put the connection as a property to Connection class
#   but sometimes you'd want to prepare connections before client connects
#   (eg: for creating a pool etc. and sharing connections between clients)
#   this is to illustrate that case so connections is outside of Connection class 
redis_connections = defaultdict(lambda: redis.Redis(host="localhost", port=6379, db=0, password=None))

class Connection(riffq.BaseConnection):
    def handle_auth(self, user, password, host, database=None, callback=callable):
        callback(user == "user" and password == "secret")

    def handle_insert(self, ast, callback):
        # TODO:
        pass

    def handle_update(self, ast, callback):
        # TODO:
        pass

    def handle_delete(self, ast, callback):
        # TODO:
        pass

    def handle_select(self, ast, callback):
        # TODO:
        pass

    def handle_switch_database(self, ast, callback):
        # TODO:
        pass

    def _handle_query(self, sql, callback=callable, **kwargs):
        logging.info("received query", sql)
        ast = parse_one(sql)
        tables = list(ast.find_all(exp.Table))

        if len(tables) != 1:
            callback(("ERROR", "42601", "you can only use one table"), is_error=True)

        if isinstance(ast, exp.Use):
            return self.handle_switch_database(ast, callback)

        if isinstance(ast, exp.Select):
            return self.handle_select(ast, callback)
        
        if isinstance(exp.Insert):
            return self.handle_insert(ast, callback)

        if isinstance(exp.Update):
            return self.handle_update(ast, callback)

        if isinstance(exp.Delete):
            return self.handle_delete(ast, callback)

    def handle_query(self, sql, callback=..., **kwargs):
        try:
            return self._handle_query(sql, callback, **kwargs)
        except Exception as e:
            logging.exception("execution error")
            callback(("ERROR", "XX000", str(exc)), is_error=True)

    def handle_connect(self, ip, port, callback=...):
        logging.info("connection from {} {}", ip, port)
        callback(True)

    def handle_disconnect(self, ip, port, callback=...):
        del redis_connections[self.conn_id]
        callback()

server = riffq.RiffqServer("127.0.0.1:5444", connection_cls=Connection)
server.start()