import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import duckdb
import pyarrow as pa
import riffq

def send_reader(reader, callback):
    if hasattr(reader, "__arrow_c_stream__"):
        capsule = reader.__arrow_c_stream__()
    else:
        from pyarrow.cffi import export_stream # type: ignore
        capsule = export_stream(reader)
    callback(capsule)

def arrow_batch(values, names):
    return pa.RecordBatchReader.from_batches(
        pa.schema(list(zip(names, [v.type for v in values]))),
        [pa.record_batch(values, names=names)],
    )

def _handle_query(sql, callback, **kwargs):
    cur = duckdb_con.cursor()
    query = sql.strip().lower().split(';')[0]

    if query.startswith("begin"):
        return callback("BEGIN", is_tag=True)
    if query.startswith("commit"):
        return callback("COMMIT", is_tag=True)
    if query.startswith("rollback"):
        return callback("ROLLBACK", is_tag=True)
    if query.startswith("discard all"):
        return callback("DISCARD ALL", is_tag=True)

    if query.startswith("select t.oid") and "from pg_type" in query and "hstore" in query:
        empty = [pa.array([], pa.int32()), pa.array([], pa.int32())]
        batch = arrow_batch(empty, ["oid", "typarray"])
        return send_reader(batch, callback)

    if query == "select pg_catalog.version()":
        batch = arrow_batch(
            [pa.array(["PostgreSQL 14.13"])],
            ["version"],
        )
        return send_reader(batch, callback)

    if query == "show transaction isolation level":
        batch = arrow_batch(
            [pa.array(["read committed"])],
            ["transaction_isolation"],
        )
        return send_reader(batch, callback)

    if query == "show standard_conforming_strings":
        batch = arrow_batch(
            [pa.array(["on"])],
            ["standard_conforming_strings"],
        )
        return send_reader(batch, callback)
    
    if query == "select current_schema()":
        batch = arrow_batch(
            [pa.array(["public"])],
            ["current_schema"],
        )
        return send_reader(batch, callback)

    if query.startswith("begin"):
        return callback("BEGIN", is_tag=True)
    if query.startswith("commit"):
        return callback("COMMIT", is_tag=True)
    if query.startswith("rollback"):
        return callback("ROLLBACK", is_tag=True)
    if query.startswith("discard all"):
        return callback("DISCARD ALL", is_tag=True)

    try:
        reader = cur.execute(sql).fetch_record_batch()
        send_reader(reader, callback)
    except Exception:
        logging.exception("error on executing query")
        batch = arrow_batch(
            [pa.array(["ERROR"]), pa.array(["unknown query"])],
            ["error", "message"],
        )
        send_reader(batch, callback)

def handle_query(sql, callback, **kwargs):
    executor.submit(_handle_query, sql, callback, **kwargs)

def main():
    global duckdb_con, executor
    duckdb_con = duckdb.connect("duckdata.db")
    duckdb_con.execute(
        """
        CREATE TABLE IF NOT EXISTS test_concurrency (
            id INTEGER,
            created_at TIMESTAMP,
            key TEXT,
            value TEXT
        )
    """
    )
    duckdb_con.execute("truncate table test_concurrency;")
    duckdb_con.execute(
        """
        INSERT INTO test_concurrency (id, created_at, key, value)
        VALUES
            (1, ?, 'alpha', 'value1'),
            (2, ?, 'beta', 'value2'),
            (3, ?, 'gamma', 'value3')
    """,
        [datetime.now(), datetime.now(), datetime.now()],
    )

    executor = ThreadPoolExecutor(max_workers=4)

    server = riffq.Server("127.0.0.1:5433")
    server.on_query(handle_query)
    server.start()

if __name__ == "__main__":
    main()
