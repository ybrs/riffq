import duckdb
import pyarrow as pa
import riffq
import logging
logging.basicConfig(level=logging.DEBUG)


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
    # server.set_tls("certs/server.crt", "certs/server.key")

    duckdb_con.execute("CREATE TABLE IF NOT EXISTS users(id INTEGER, name VARCHAR)")
    duckdb_con.execute("CREATE TABLE IF NOT EXISTS projects(id INTEGER, name VARCHAR)")
    duckdb_con.execute(
        "CREATE TABLE IF NOT EXISTS tasks(id INTEGER, project_id INTEGER, description VARCHAR)"
    )

    server.register_database("duckdb")

    tbls = duckdb_con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog','information_schema')"
    ).fetchall()

    registered_schemas = set()
    for schema_name, table_name in tbls:
        if schema_name not in registered_schemas:
            server.register_schema("duckdb", schema_name)
            registered_schemas.add(schema_name)
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

    def handle_query(sql, callback, **kwargs):
        cur = duckdb_con.cursor()
        try:
            rb = cur.execute(sql).fetch_record_batch()
        except Exception as exc:
            logging.exception("error on executing query")
            rb = pa.record_batch(
                [pa.array(["ERROR"]), pa.array([str(exc)])],
                names=["error", "message"],
            )
        reader = pa.RecordBatchReader.from_batches(rb.schema, [rb])
        if hasattr(reader, "__arrow_c_stream__"):
            capsule = reader.__arrow_c_stream__()
        else:
            from pyarrow.cffi import export_stream
            capsule = export_stream(reader)
        callback(capsule)

    server.on_query(handle_query)
    server.start(catalog_emulation=True)

if __name__ == "__main__":
    run_server(port=5433)
