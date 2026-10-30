"""Check whether riffq reports duplicate schemas, and how far the duplication goes.

DBeaver shows "public" twice in its schema tree. That was filed as cosmetic,
but if pg_namespace itself carries two rows for one schema then every query
joining against it would double its results, which would be a data problem
rather than a display one.

This asks the question at several levels: the raw pg_namespace rows, the
distinct count, information_schema.schemata, and a join of pg_class against
pg_namespace (the shape catalog queries actually use, where a duplicate
namespace row would double every table).

Run from the riffq project root:

    venv/bin/python -m claude-scripts.inspect_pg_namespace_duplicates
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

PORT = 55595

PROBES = [
    (
        "all pg_namespace rows (oid, nspname)",
        "SELECT oid, nspname FROM pg_catalog.pg_namespace ORDER BY nspname",
    ),
    (
        "nspname values appearing more than once",
        "SELECT nspname, count(*) AS n FROM pg_catalog.pg_namespace "
        "GROUP BY nspname HAVING count(*) > 1 ORDER BY nspname",
    ),
    (
        "row count vs distinct nspname count",
        "SELECT count(*) AS rows, count(DISTINCT nspname) AS distinct_names "
        "FROM pg_catalog.pg_namespace",
    ),
    (
        "rows for public specifically",
        "SELECT oid, nspname FROM pg_catalog.pg_namespace WHERE nspname = 'public'",
    ),
    (
        "information_schema.schemata",
        "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
    ),
    (
        "fixture tables joined to pg_namespace (would double if ns duplicates)",
        "SELECT c.relname, n.nspname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relname IN ('customers', 'orders', 'products') "
        "ORDER BY c.relname",
    ),
    (
        "pg_class rows for the fixture tables (no join)",
        "SELECT relname, relnamespace FROM pg_catalog.pg_class "
        "WHERE relname IN ('customers', 'orders', 'products') ORDER BY relname",
    ),
    # A client that resolves 'public' to an OID and then enumerates tables by
    # that OID is the normal pattern (it is what DBeaver does). With two public
    # rows it can pick the wrong one, so these show what each choice yields.
    (
        "tables in namespace 2200 (PostgreSQL's canonical public oid)",
        "SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 2200 "
        "AND relkind = 'r' ORDER BY relname",
    ),
    (
        "tables in the generated public oid",
        "SELECT c.relname FROM pg_catalog.pg_class c "
        "WHERE c.relnamespace = (SELECT max(oid) FROM pg_catalog.pg_namespace "
        "WHERE nspname = 'public') AND c.relkind = 'r' ORDER BY c.relname",
    ),
    (
        "DBeaver-style enumeration: resolve public, then list its relations",
        "SELECT c.relname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY c.relname",
    ),
    (
        "which public oid owns the fixture types (pg_type.typnamespace)",
        "SELECT DISTINCT typnamespace FROM pg_catalog.pg_type "
        "WHERE typname IN ('customers', 'orders', 'products')",
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
