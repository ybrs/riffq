"""Isolate which clause makes Npgsql's GetSchema("Tables") return no rows.

Npgsql's Tables collection issues one query against information_schema.tables
filtering on ``table_type IN ('BASE TABLE', 'FOREIGN', 'FOREIGN TABLE')``, a
``table_schema NOT IN`` exclusion, and a bound ``table_schema = $1``. Against a
riffq fixture server it comes back empty even though the same three tables are
present and typed BASE TABLE.

This runs Npgsql's exact query and then drops one clause at a time, so the
clause responsible is identified rather than guessed at.

Run from the riffq project root:

    venv/bin/python -m claude-scripts.isolate_npgsql_get_tables_query
"""
import os
import sys
import tempfile

import psycopg

# The integration fixture modules live in a non-package directory, so make them
# importable by path rather than by package name.
INTEGRATION_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests",
    "integration",
)
sys.path.insert(0, INTEGRATION_DIR)

import fixture_dataset  # noqa: E402
from fixture_server import start_process  # noqa: E402
from riffq.testing import stop_server  # noqa: E402

PORT = 55598

# Npgsql's own Tables query, reproduced from NpgsqlSchema.GetTables with the
# schema restriction applied the way BuildCommand applies it.
NPGSQL_TABLES_QUERY = """
SELECT table_catalog, table_schema, table_name, table_type
FROM information_schema.tables
WHERE
    table_type IN ('BASE TABLE', 'FOREIGN', 'FOREIGN TABLE') AND
    table_schema NOT IN ('pg_catalog', 'information_schema')
 AND table_schema = %s
"""

# Each probe drops or changes exactly one thing from the query above.
PROBES = [
    (
        "npgsql exact (bound schema param)",
        NPGSQL_TABLES_QUERY,
        (fixture_dataset.SCHEMA_NAME,),
    ),
    (
        "schema as a literal instead of a parameter",
        NPGSQL_TABLES_QUERY.replace("%s", "'public'"),
        (),
    ),
    (
        "without the table_type IN filter",
        """
        SELECT table_catalog, table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_schema = %s
        """,
        (fixture_dataset.SCHEMA_NAME,),
    ),
    (
        "table_type IN with only BASE TABLE",
        """
        SELECT table_catalog, table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_type IN ('BASE TABLE') AND table_schema = %s
        """,
        (fixture_dataset.SCHEMA_NAME,),
    ),
    (
        "table_type = 'BASE TABLE' (equality, not IN)",
        """
        SELECT table_catalog, table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' AND table_schema = %s
        """,
        (fixture_dataset.SCHEMA_NAME,),
    ),
    (
        "without the NOT IN exclusion",
        """
        SELECT table_catalog, table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_type IN ('BASE TABLE', 'FOREIGN', 'FOREIGN TABLE')
          AND table_schema = %s
        """,
        (fixture_dataset.SCHEMA_NAME,),
    ),
]

# Does merely HAVING a parameter break the table_type predicate, or does the
# parameter have to be involved in it? If an unrelated parameter is enough,
# the fault is in the extended-protocol planning path rather than in how any
# particular value is bound.
PARAMETER_PROBES = [
    (
        "table_type predicate, NO parameter at all",
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_type = 'BASE TABLE' AND table_schema = 'public'",
        (),
    ),
    (
        "table_type predicate + an UNRELATED parameter",
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_type = 'BASE TABLE' AND table_schema = 'public' AND 1 = %s",
        (1,),
    ),
    (
        "table_type compared TO a parameter",
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_type = %s AND table_schema = 'public'",
        ("BASE TABLE",),
    ),
    (
        "table_schema parameter only, no table_type predicate",
        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
        ("public",),
    ),
    (
        "a different text column compared to a parameter",
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name = %s AND table_schema = 'public'",
        ("customers",),
    ),
    (
        "table_type predicate on a parameterized query over pg_class instead",
        "SELECT c.relname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relkind = 'r' AND n.nspname = %s",
        ("public",),
    ),
    (
        "table_type literal + parameter, with no other literal predicate",
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_type = 'BASE TABLE' AND 1 = %s",
        (1,),
    ),
    (
        "a DIFFERENT column's literal predicate + an unrelated parameter",
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name = 'customers' AND 1 = %s",
        (1,),
    ),
    (
        "table_type literal cast to text + an unrelated parameter",
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_type::text = 'BASE TABLE' AND table_schema = 'public' AND 1 = %s",
        (1,),
    ),
]

# Probes whose returned values matter, not just their row count: if table_type
# comes back NULL only on the parameterized path, that explains why every
# table_type predicate above filters everything out.
VALUE_PROBES = [
    (
        "values with a bound schema param",
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = %s ORDER BY table_name",
        (fixture_dataset.SCHEMA_NAME,),
    ),
    (
        "values with a literal schema",
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = 'public' ORDER BY table_name",
        (),
    ),
]


def main():
    """Run each probe against a fixture server and print its row count."""
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
            for label, sql, parameters in PROBES:
                with connection.cursor() as cursor:
                    try:
                        cursor.execute(sql, parameters)
                        rows = cursor.fetchall()
                        print(f"{len(rows):>3} rows  {label}")
                    except Exception as error:
                        connection.rollback()
                        print(f"  ERR    {label}: {error}")

            print()
            for label, sql, parameters in PARAMETER_PROBES:
                with connection.cursor() as cursor:
                    try:
                        cursor.execute(sql, parameters)
                        print(f"{len(cursor.fetchall()):>3} rows  {label}")
                    except Exception as error:
                        connection.rollback()
                        print(f"  ERR    {label}: {str(error).splitlines()[0]}")

            print()
            for label, sql, parameters in VALUE_PROBES:
                with connection.cursor() as cursor:
                    try:
                        cursor.execute(sql, parameters)
                        print(f"{label}: {cursor.fetchall()}")
                    except Exception as error:
                        connection.rollback()
                        print(f"{label}: ERR {error}")
    finally:
        stop_server(server)


if __name__ == "__main__":
    main()
