import duckdb
import pyarrow as pa
import riffq
from riffq.helpers import to_arrow


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


def _handle_query(sql, callback, **kwargs):
    callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))


def run_server(port: int):
    con = duckdb.connect()
    con.execute("CREATE TABLE users(id INTEGER, name VARCHAR)")
    con.execute("CREATE TABLE projects(id INTEGER, name VARCHAR)")
    con.execute(
        "CREATE TABLE tasks(id INTEGER, project_id INTEGER, description VARCHAR)"
    )

    server = riffq.Server(f"127.0.0.1:{port}")
    server.register_database("duckdb")

    tbls = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog','information_schema')"
    ).fetchall()

    for schema_name, table_name in tbls:
        server.register_schema("duckdb", schema_name)
        cols_info = con.execute(
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

    server.on_query(_handle_query)
    server.start(catalog_emulation=True)
