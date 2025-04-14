import logging
import threading

from riffq import riffq
import duckdb
import pandas
from concurrent.futures import ThreadPoolExecutor

def get_schema_from_duckdb(columns, types):
    duckdb_to_simple = {
        "BOOLEAN": "bool",
        "TINYINT": "int",
        "SMALLINT": "int",
        "INTEGER": "int",
        "BIGINT": "int",
        "HUGEINT": "int",
        "UTINYINT": "int",
        "USMALLINT": "int",
        "UINTEGER": "int",
        "UBIGINT": "int",
        "FLOAT": "float",
        "DOUBLE": "float",
        "DECIMAL": "float",
        "REAL": "float",
        "VARCHAR": "str",
        "BLOB": "str",
        "UUID": "str",
        "DATE": "date",
        "TIMESTAMP": "datetime",
        "TIMESTAMP_S": "datetime",
        "TIMESTAMP_MS": "datetime",
        "TIMESTAMP_NS": "datetime",
        "TIME": "str",  # optional: could be treated as 'time'
        "INTERVAL": "str",
    }
    print("columns", columns)
    schema = []
    for col, dtype in zip(columns, types):
        duck_type = str(dtype).upper()
        mapped = duckdb_to_simple.get(duck_type, "str")
        schema.append({"name": col, "type": mapped})
    return schema


def _handle_query(sql, callback):
    local_con = duckdb_con.cursor()
    print("< received (python):", sql)
    if sql.strip().lower() == "select pg_catalog.version()":
        result = (
            [
                {"name": "version", "type": "string"},
            ],
            [
                ["PostgreSQL 14.13 (Homebrew) on aarch64-apple-darwin23.4.0, compiled by Apple clang version 15.0.0 (clang-1500.3.9.4), 64-bit",]
            ]
        )

        callback(result)
        return

    try:
        res = local_con.sql(sql)
        schema = get_schema_from_duckdb(res.columns, res.types)
        callback((schema, res.fetchall()))
    except Exception as e:
        logging.exception("error on executing query")
        result = (
            [ {"name": "error", "type": "str"}, {"name": "message", "type": "str"} ],
            [ ["ERROR", "unknown query"] ]
        )
        callback(result)

def handle_query(sql, callback):
    def task():
        try:
            _handle_query(sql, callback)
        except Exception:
            logging.exception("exception on executing query")
    executor.submit(task)

def main():
    global duckdb_con, executor
    from datetime import datetime
    duckdb_con = duckdb.connect("duckdata.db", read_only=False)

    duckdb_con.execute("""
    CREATE TABLE IF NOT EXISTS test_concurrency (
        id INTEGER,
        created_at TIMESTAMP,
        key TEXT,
        value TEXT
    )
    """)
    duckdb_con.execute("truncate table test_concurrency;")
    duckdb_con.execute("""
    INSERT INTO test_concurrency (id, created_at, key, value)
    VALUES 
        (1, ?, 'alpha', 'value1'),
        (2, ?, 'beta', 'value2'),
        (3, ?, 'gamma', 'value3')
    """, [datetime.now(), datetime.now(), datetime.now()])

    executor = ThreadPoolExecutor(max_workers=4)

    server = riffq.Server("127.0.0.1:5433")
    server.set_callback(handle_query)
    server.start()


if __name__ == "__main__":
    main()