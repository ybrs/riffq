import duckdb
import pyarrow as pa
import riffq
from riffq.helpers import to_arrow
import logging

def map_type(data_type: str) -> str:
    dt = data_type.upper()
    if "INT" in dt:
        return "int"
    if any(x in dt for x in ["DOUBLE", "DECIMAL", "REAL", "FLOAT"]):
        return "float"
    if "BOOL" in dt:
        return "bool"
    if dt == "DATE":
        return "date"
    if "TIMESTAMP" in dt or "DATETIME" in dt:
        return "datetime"
    return "str"

class Connection(riffq.BaseConnection):
    def _handle_query(self, sql, callback, **kwargs):
        cur = duckdb_con.cursor()
        text = sql.strip().lower().split(';')[0]

        if text == "select pg_catalog.version()":
            batch = self.arrow_batch(
                [pa.array(["PostgreSQL 14.13"])],
                ["version"],
            )
            return self.send_reader(batch, callback)

        if text == "show transaction isolation level":
            batch = self.arrow_batch(
                [pa.array(["read committed"])],
                ["transaction_isolation"],
            )
            return self.send_reader(batch, callback)

        if text == "show standard_conforming_strings":
            batch = self.arrow_batch(
                [pa.array(["read committed"])],
                ["transaction_isolation"],
            )
            return self.send_reader(batch, callback)
        
        if text == "select current_schema()":
            batch = self.arrow_batch(
                [pa.array(["public"])],
                ["current_schema"],
            )
            return self.send_reader(batch, callback)

        try:
            reader = cur.execute(sql).fetch_record_batch()
            self.send_reader(reader, callback)
        except Exception as exc:
            logging.exception("error on executing query")
            batch = self.arrow_batch(
                [pa.array(["ERROR"]), pa.array([str(exc)])],
                ["error", "message"],
            )
            self.send_reader(batch, callback)

    def handle_query(self, sql, callback=callable, **kwargs):
        self.executor.submit(self._handle_query, sql, callback, **kwargs)

    def handle_auth(self, user, password, host, database=None, callback=callable):
        callback(True)
        # return callback(user == "user" and password == "secret")


def _handle_query(sql, callback, **kwargs):
    sql = sql.strip().lower()
    if sql.startswith("set"):
        return callback("SET", is_tag=True)

    if sql.startswith("begin"):
        return callback("BEGIN", is_tag=True)
    
    if sql.startswith("commit"):
        return callback("COMMIT", is_tag=True)
    
    if sql.startswith("rollback"):
        return callback("ROLLBACK", is_tag=True)
    
    if sql.startswith("discard all"):
        return callback("DISCARD ALL", is_tag=True)


    callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))


def run_server(port: int):
    global duckdb_con
    duckdb_con = duckdb.connect()

    duckdb_con.execute("CREATE TABLE users(id INTEGER, name VARCHAR)")
    duckdb_con.execute("CREATE TABLE projects(id INTEGER, name VARCHAR)")
    duckdb_con.execute(
        "CREATE TABLE tasks(id INTEGER, project_id INTEGER, description VARCHAR)"
    )

    server = riffq.RiffqServer(f"127.0.0.1:{port}", connection_cls=Connection)
    server.set_tls("certs/server.crt", "certs/server.key")

    server._server.register_database("duckdb")

    tbls = duckdb_con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog','information_schema')"
    ).fetchall()

    for schema_name, table_name in tbls:
        server._server.register_schema("duckdb", schema_name)
        cols_info = duckdb_con.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema=? AND table_name=?",
            (schema_name, table_name),
        ).fetchall()
        columns = []
        for col_name, data_type, is_nullable in cols_info:
            columns.append(
                {
                    col_name: {
                        "type": map_type(data_type),
                        "nullable": is_nullable.upper() == "YES",
                    }
                }
            )
        server._server.register_table("duckdb", schema_name, table_name, columns)

    # server.on_query(_handle_query)
    server.start(catalog_emulation=True)

if __name__ == "__main__":
    run_server(port=5433)
