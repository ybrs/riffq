import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import duckdb
import pyarrow as pa
from riffq import riffq

def send_reader(reader, callback):
    if hasattr(reader, "__arrow_c_stream__"):
        capsule = reader.__arrow_c_stream__()
    else:
        from pyarrow.cffi import export_stream
        capsule = export_stream(reader)
    callback(capsule)

def arrow_batch(values, names):
    return pa.RecordBatchReader.from_batches(
        pa.schema(list(zip(names, [v.type for v in values]))),
        [pa.record_batch(values, names=names)],
    )

def _handle_query(sql, callback, **kwargs):
    cur = duckdb_con.cursor()
    text = sql.strip().lower().split(';')[0]

    if text == "select pg_catalog.version()":
        batch = arrow_batch(
            [pa.array(["PostgreSQL 14.13"])],
            ["version"],
        )
        return send_reader(batch, callback)

    if text == "show transaction isolation level":
        batch = arrow_batch(
            [pa.array(["read committed"])],
            ["transaction_isolation"],
        )
        return send_reader(batch, callback)

    if text == "show standard_conforming_strings":
        batch = arrow_batch(
            [pa.array(["read committed"])],
            ["transaction_isolation"],
        )
        return send_reader(batch, callback)
    
    if text == "select current_schema()":
        batch = arrow_batch(
            [pa.array(["public"])],
            ["current_schema"],
        )
        return send_reader(batch, callback)

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

def handle_query(sql, callback, conn_id=None, **kwargs):
    print("conn_id", conn_id)
    executor.submit(_handle_query, sql, callback, **kwargs)

def handle_auth(conn_id, user, password, host, database=None):
    print("conn_id onauth:", conn_id)
    return user == "user" and password == "secret"

def handle_connect(conn_id, ip, port):
    print("conn_id onconnect:", conn_id)
    return True


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
    server.set_tls("certs/server.crt", "certs/server.key")
    server.on_authentication(handle_auth)
    server.on_query(handle_query)
    server.on_connect(handle_connect)
    server.start()

if __name__ == "__main__":
    main()
