"""Find where each duplicate `public` namespace row comes from.

pg_namespace holds two rows named public: oid 2200 and a generated one. Fixing
that needs to know which databases exist in the flattened catalog, which
database each public row belongs to, and whether anything else is doubled the
same way -- a flattened catalog covering several databases legitimately needs
one public per database, so the fix depends on whether these two rows describe
one database or two.

Run from the riffq project root:

    venv/bin/python -m claude-scripts.trace_public_namespace_origin
"""
import os
import sys
import tempfile

import psycopg

INTEGRATION_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests",
    "integration",
)
sys.path.insert(0, INTEGRATION_DIR)

import fixture_dataset  # noqa: E402
from fixture_server import start_process  # noqa: E402
from riffq.testing import stop_server  # noqa: E402

PORT = 55594

PROBES = [
    (
        "databases in the flattened catalog",
        "SELECT oid, datname FROM pg_catalog.pg_database ORDER BY datname",
    ),
    (
        "every namespace with its owner",
        "SELECT oid, nspname, nspowner FROM pg_catalog.pg_namespace ORDER BY nspname, oid",
    ),
    (
        "any other duplicated namespace names",
        "SELECT nspname, count(*) AS n FROM pg_catalog.pg_namespace "
        "GROUP BY nspname HAVING count(*) > 1",
    ),
    (
        "relations grouped by their namespace oid",
        "SELECT relnamespace, count(*) AS relations FROM pg_catalog.pg_class "
        "GROUP BY relnamespace ORDER BY relnamespace",
    ),
    (
        "information_schema.schemata with its catalog name",
        "SELECT catalog_name, schema_name FROM information_schema.schemata "
        "ORDER BY schema_name, catalog_name",
    ),
    (
        "information_schema.tables catalog/schema for the fixture tables",
        "SELECT table_catalog, table_schema, table_name FROM information_schema.tables "
        "WHERE table_name IN ('customers','orders','products') ORDER BY table_name",
    ),
    (
        "current database and search_path",
        "SELECT current_database(), current_schema()",
    ),
]


def main():
    """Run each probe and print its rows."""
    log_path = os.path.join(tempfile.gettempdir(), f"riffq_integration_{PORT}.log")
    server = start_process(PORT, log_path)
    try:
        with psycopg.connect(
            host="127.0.0.1",
            port=PORT,
            user="user",
            password="secret",
            dbname=fixture_dataset.DATABASE_NAME,
        ) as connection:
            for label, sql in PROBES:
                with connection.cursor() as cursor:
                    try:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        print(f"\n{label}  ({len(rows)} rows)")
                        for row in rows:
                            print("   ", row)
                    except Exception as error:
                        connection.rollback()
                        print(f"\n{label}\n    ERR {str(error).splitlines()[0]}")
    finally:
        stop_server(server)


if __name__ == "__main__":
    main()
