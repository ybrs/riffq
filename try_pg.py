import logging
import threading

from riffq import riffq

def _handle_query(sql, callback, **kwargs):
    print("< received (python):", sql, kwargs)

    print("sql", sql.strip().lower())
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

    result = (
        [ {"name": "error", "type": "str"}, {"name": "message", "type": "str"} ],
        [ ["ERROR", "unknown query"] ]
    )
    # result = (
    #     [ {"name": "col1", "type": "str"} ],
    #     [ ["hello"] ]
    # )

    callback(result)

def handle_query(sql, callback, **kwargs):
    try:
        _handle_query(sql, callback, **kwargs)
    except:
        logging.exception("exception on executing query")


if __name__ == "__main__":
    server = riffq.Server("127.0.0.1:5433")
    server.on_query(handle_query)
    server.start()
