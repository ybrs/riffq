import duckdb
import pyarrow as pa
import riffq
from concurrent.futures import ThreadPoolExecutor


def _send_reader(reader: pa.RecordBatchReader, callback) -> None:
    if hasattr(reader, "__arrow_c_stream__"):
        capsule = reader.__arrow_c_stream__()
    else:
        from pyarrow.cffi import export_stream
        capsule = export_stream(reader)
    callback(capsule)


def _handle_query(sql: str, callback, **kwargs) -> None:
    cur = duckdb_con.cursor()
    try:
        batch = cur.execute(sql).fetch_record_batch()
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        _send_reader(reader, callback)
    except Exception:
        empty = pa.record_batch([pa.array([1], pa.int64())], names=["val"])
        reader = pa.RecordBatchReader.from_batches(empty.schema, [empty])
        _send_reader(reader, callback)


def main(port: int) -> None:
    global duckdb_con, executor
    duckdb_con = duckdb.connect()
    duckdb_con.execute("CREATE TABLE users(id INTEGER, name TEXT)")
    duckdb_con.execute("CREATE TABLE projects(id INTEGER, name TEXT)")
    duckdb_con.execute("CREATE TABLE tasks(id INTEGER, project_id INTEGER, name TEXT)")

    executor = ThreadPoolExecutor(max_workers=4)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(_handle_query)

    server.register_database("duckdb")

    schemas = [row[0] for row in duckdb_con.execute(
        "SELECT DISTINCT table_schema FROM information_schema.tables"
    ).fetchall()]
    for schema in schemas:
        server.register_schema("duckdb", schema)

    tables = duckdb_con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables"
    ).fetchall()
    for schema, table in tables:
        server.register_table("duckdb", schema, table, [])

    server.start(catalog_emulation=True)


if __name__ == "__main__":
    main(5433)
