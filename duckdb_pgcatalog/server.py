import duckdb
import pyarrow as pa
import riffq
from riffq.helpers import to_arrow
import logging
import duckdb
import pyarrow as pa
import riffq
logging.basicConfig(level=logging.DEBUG)

class Connection(riffq.BaseConnection):
    def _handle_query(self, sql, callback, **kwargs):
        print(">>> sql", sql)
        cur = duckdb_con.cursor()
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

    def handle_query(self, sql, callback, **kwargs):
        self.executor.submit(self._handle_query, sql, callback, **kwargs)

    def handle_auth(self, user, password, host, database=None, callback=callable):
        return callback(True)
        # 
        return callback(user == "user" and password == "secret")

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

def run_server(port: int):
    global duckdb_con
    duckdb_con = duckdb.connect()
    server = riffq.Server(f"127.0.0.1:{port}")
    conn_mgr = Connection(port)
    server.on_query(conn_mgr.handle_query)
    # server.set_tls("certs/server.crt", "certs/server.key")

    # duckdb_con.execute("CREATE TABLE users(id INTEGER, name VARCHAR)")
    # duckdb_con.execute("CREATE TABLE projects(id INTEGER, name VARCHAR)")
    # duckdb_con.execute(
    #     "CREATE TABLE tasks(id INTEGER, project_id INTEGER, description VARCHAR)"
    # )

    tbls = duckdb_con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog','information_schema')"
    ).fetchall()

    for schema_name, table_name in tbls:
        server.register_schema("duckdb", schema_name)
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
        server.register_table("duckdb", schema_name, table_name, columns)

    server.start(catalog_emulation=True)

if __name__ == "__main__":
    run_server(port=5433)
