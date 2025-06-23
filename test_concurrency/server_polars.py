import logging
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas
import polars as pl
import riffq

executor = ThreadPoolExecutor(max_workers=4)

def get_schema_from_polars(df: pl.DataFrame):
    polars_to_simple = {
        pl.Boolean: "bool",
        pl.Int8: "int",
        pl.Int16: "int",
        pl.Int32: "int",
        pl.Int64: "int",
        pl.UInt8: "int",
        pl.UInt16: "int",
        pl.UInt32: "int",
        pl.UInt64: "int",
        pl.Float32: "float",
        pl.Float64: "float",
        pl.String: "str",
        pl.Binary: "str",
        pl.Date: "date",
        pl.Datetime: "datetime",
        pl.Time: "str",
        pl.Duration: "str",
    }

    schema = []
    for col in df.schema:
        dtype = df.schema[col]
        mapped = polars_to_simple.get(dtype, "str")
        schema.append({"name": col, "type": mapped})
    return schema

def run_heavy_polars_query():
    df = heavy_df
    # This forces heavy CPU math in native Polars (Rust)
    return df.lazy().with_columns([
        (pl.col("x") ** 0.5).log().sin().cos().tan().exp().alias("result")
    ]).select([
        pl.col("result").sum()
    ]).collect()

def _handle_query(sql, callback, **kwargs):
    print("< received (python):", sql)
    query = sql.strip().lower()

    if query.startswith("begin"):
        return callback("BEGIN", is_tag=True)
    if query.startswith("commit"):
        return callback("COMMIT", is_tag=True)
    if query.startswith("rollback"):
        return callback("ROLLBACK", is_tag=True)
    if query.startswith("discard all"):
        return callback("DISCARD ALL", is_tag=True)

    if query.startswith("select t.oid") and "from pg_type" in query and "hstore" in query:
        return callback(([{"name": "oid", "type": "int"}, {"name": "typarray", "type": "int"}], []))

    if query == "select pg_catalog.version()":
        return callback(([{"name": "version", "type": "string"}],
                         [["PostgreSQL 14.13"]]))

    if query == "show transaction isolation level":
        return callback(([{"name": "transaction_isolation", "type": "string"}],
                         [["read committed"]]))

    if query == "show standard_conforming_strings":
        return callback(([{"name": "standard_conforming_strings", "type": "string"}],
                         [["on"]]))

    if query == "select current_schema()":
        return callback(([{"name": "current_schema", "type": "string"}],
                         [["public"]]))


    if query.startswith("begin"):
        return callback("BEGIN", is_tag=True)
    if query.startswith("commit"):
        return callback("COMMIT", is_tag=True)
    if query.startswith("rollback"):
        return callback("ROLLBACK", is_tag=True)
    if query.startswith("discard all"):
        return callback("DISCARD ALL", is_tag=True)


    if query.strip().lower().replace(';', '') == "select heavy_query":
        df = run_heavy_polars_query()
        schema = get_schema_from_polars(df)
        rows = df.to_numpy().tolist()
        return callback((schema, rows))

    try:
        sql_ctx = pl.SQLContext()
        sql_ctx.register("test_concurrency", test_concurrency_df)
        df = sql_ctx.execute(sql).collect()
        schema = get_schema_from_polars(df)
        rows = df.to_numpy().tolist()
        callback((schema, rows))
    except Exception:
        logging.exception("error on executing query")
        result = (
            [{"name": "error", "type": "str"}, {"name": "message", "type": "str"}],
            [["ERROR", "unknown query"]],
        )
        callback(result)

def handle_query(sql, callback, **kwargs):
    def task():
        try:
            _handle_query(sql, callback)
        except Exception:
            logging.exception("exception on executing query")
    executor.submit(task)

def main():
    global test_concurrency_df, heavy_df

    heavy_df = pl.DataFrame({
        # reduced range to keep memory usage reasonable while
        # still providing a noticeably heavy query
        "x": list(range(10_000_000))
    })

    test_concurrency_df = pl.DataFrame({
        "id": [1, 2, 3],
        "created_at": [datetime.now()] * 3,
        "key": ["alpha", "beta", "gamma"],
        "value": ["value1", "value2", "value3"]
    })
    server = riffq.Server("127.0.0.1:5434")
    server.on_query(handle_query)
    server.start()

if __name__ == "__main__":
    main()
