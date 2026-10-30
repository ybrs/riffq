"""Find which correlated scalar subquery shapes riffq can plan.

Npgsql's type-loading query fails to plan with "Correlated scalar subquery must
be aggregated to return at most one row". DataFusion's invariant check
(datafusion-expr/src/logical_plan/invariants.rs) accepts a correlated scalar
subquery only when its plan is an Aggregate, a Filter over an Aggregate, or has
max_rows() <= 1; Npgsql's is a plain Filter over a table scan, so it is
rejected.

That suggests two candidate rewrites -- wrap the projection in an aggregate, or
add LIMIT 1 -- but passing the invariant is not enough on its own: the subquery
must also survive decorrelation and physical planning. This runs each shape to
find out which actually execute, and checks their results against an equivalent
LEFT JOIN so a shape that plans but answers wrongly is not mistaken for a fix.

Run from the riffq project root:

    venv/bin/python -m claude-scripts.isolate_correlated_scalar_subquery
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

PORT = 55597

# Each probe is one shape of the same lookup: for each pg_type row, find its
# namespace name. The LEFT JOIN is the reference answer.
PROBES = [
    (
        "LEFT JOIN (reference)",
        """
        SELECT t.typname, n.nspname
        FROM pg_catalog.pg_type t
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    (
        "bare correlated scalar subquery in projection",
        """
        SELECT t.typname,
               (SELECT n.nspname FROM pg_catalog.pg_namespace n
                 WHERE n.oid = t.typnamespace) AS nspname
        FROM pg_catalog.pg_type t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    (
        "correlated scalar subquery inside CASE (the Npgsql shape)",
        """
        SELECT t.typname,
               CASE WHEN t.typtype = 'b'
                    THEN (SELECT n.nspname FROM pg_catalog.pg_namespace n
                           WHERE n.oid = t.typnamespace)
               END AS nspname
        FROM pg_catalog.pg_type t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    (
        "aggregated with max() in projection",
        """
        SELECT t.typname,
               (SELECT max(n.nspname) FROM pg_catalog.pg_namespace n
                 WHERE n.oid = t.typnamespace) AS nspname
        FROM pg_catalog.pg_type t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    (
        "aggregated with max() inside CASE",
        """
        SELECT t.typname,
               CASE WHEN t.typtype = 'b'
                    THEN (SELECT max(n.nspname) FROM pg_catalog.pg_namespace n
                           WHERE n.oid = t.typnamespace)
               END AS nspname
        FROM pg_catalog.pg_type t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    (
        "LIMIT 1 inside the subquery",
        """
        SELECT t.typname,
               (SELECT n.nspname FROM pg_catalog.pg_namespace n
                 WHERE n.oid = t.typnamespace LIMIT 1) AS nspname
        FROM pg_catalog.pg_type t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    (
        "LIMIT 1 inside the subquery, inside CASE",
        """
        SELECT t.typname,
               CASE WHEN t.typtype = 'b'
                    THEN (SELECT n.nspname FROM pg_catalog.pg_namespace n
                           WHERE n.oid = t.typnamespace LIMIT 1)
               END AS nspname
        FROM pg_catalog.pg_type t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    # The shapes above all plan, so the plain pattern is not what breaks
    # Npgsql. Its subquery differs in two ways: it correlates to a DERIVED
    # table's alias rather than a base table, and it reads pg_range. The
    # probes below vary one of those at a time.
    (
        "correlated to a derived table alias",
        """
        SELECT t.typname,
               (SELECT n.nspname FROM pg_catalog.pg_namespace n
                 WHERE n.oid = t.typnamespace) AS nspname
        FROM (SELECT typname, typnamespace FROM pg_catalog.pg_type) AS t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    (
        "correlated to a derived table alias, inside CASE",
        """
        SELECT t.typname,
               CASE WHEN t.typtype = 'b'
                    THEN (SELECT n.nspname FROM pg_catalog.pg_namespace n
                           WHERE n.oid = t.typnamespace)
               END AS nspname
        FROM (SELECT typname, typtype, typnamespace FROM pg_catalog.pg_type) AS t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    (
        "subquery over pg_range, the exact Npgsql lookup",
        """
        SELECT t.typname,
               (SELECT rngtypid FROM pg_catalog.pg_range
                 WHERE rngmultitypid = t.oid) AS elemtypoid
        FROM pg_catalog.pg_type t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    (
        "pg_range lookup inside CASE, correlated to a derived table alias",
        """
        SELECT t.typname,
               CASE WHEN t.typtype = 'm'
                    THEN (SELECT rngtypid FROM pg_catalog.pg_range
                           WHERE rngmultitypid = t.oid)
               END AS elemtypoid
        FROM (SELECT oid, typname, typtype FROM pg_catalog.pg_type) AS t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
    (
        "pg_range ALSO left-joined in the outer FROM (same name in scope twice)",
        """
        SELECT typ.typname,
               CASE WHEN typ.typtype = 'm'
                    THEN (SELECT rngtypid FROM pg_catalog.pg_range
                           WHERE rngmultitypid = typ.oid)
               END AS elemtypoid
        FROM pg_catalog.pg_type AS typ
        LEFT JOIN pg_catalog.pg_range ON (pg_range.rngtypid = typ.oid)
        ORDER BY typ.typname
        LIMIT 5
        """,
    ),
    (
        "same, but the subquery's columns are qualified by its own alias",
        """
        SELECT typ.typname,
               CASE WHEN typ.typtype = 'm'
                    THEN (SELECT r.rngtypid FROM pg_catalog.pg_range r
                           WHERE r.rngmultitypid = typ.oid)
               END AS elemtypoid
        FROM pg_catalog.pg_type AS typ
        LEFT JOIN pg_catalog.pg_range ON (pg_range.rngtypid = typ.oid)
        ORDER BY typ.typname
        LIMIT 5
        """,
    ),
    # Bisecting the real query (bisect_npgsql_type_query.py) showed the trigger
    # is not the subquery's shape but its POSITION: the same SELECT plans at
    # top level and fails once it becomes a derived table. These are the
    # minimal form of that, and the test of whether an aggregate rescues it.
    (
        "MINIMAL: correlated scalar subquery inside a derived table",
        """
        SELECT s.typname, s.nspname
        FROM (
            SELECT t.typname,
                   (SELECT n.nspname FROM pg_catalog.pg_namespace n
                     WHERE n.oid = t.typnamespace) AS nspname
            FROM pg_catalog.pg_type t
        ) AS s
        ORDER BY s.typname
        LIMIT 5
        """,
    ),
    (
        "MINIMAL, but aggregated with max(): does an aggregate rescue it?",
        """
        SELECT s.typname, s.nspname
        FROM (
            SELECT t.typname,
                   (SELECT max(n.nspname) FROM pg_catalog.pg_namespace n
                     WHERE n.oid = t.typnamespace) AS nspname
            FROM pg_catalog.pg_type t
        ) AS s
        ORDER BY s.typname
        LIMIT 5
        """,
    ),
    (
        "Npgsql's own CASE, with its sibling arms present",
        """
        SELECT t.typname,
               CASE
                 WHEN t.typtype = 'r' THEN t.typelem
                 WHEN t.typtype = 'm' THEN (SELECT rngtypid FROM pg_catalog.pg_range
                                             WHERE rngmultitypid = t.oid)
                 WHEN t.typtype = 'd' THEN t.typbasetype
               END AS elemtypoid
        FROM (SELECT oid, typname, typtype, typelem, typbasetype
                FROM pg_catalog.pg_type) AS t
        ORDER BY t.typname
        LIMIT 5
        """,
    ),
]


def main():
    """Run every probe and print whether it planned, and what it returned."""
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
                        print(f"OK   {label}")
                        print(f"       {rows}")
                    except Exception as error:
                        connection.rollback()
                        message = str(error).strip().splitlines()[0]
                        print(f"FAIL {label}")
                        print(f"       {message}")
    finally:
        stop_server(server)


if __name__ == "__main__":
    main()
