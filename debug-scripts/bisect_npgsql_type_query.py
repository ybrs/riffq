"""Cut Npgsql's type-loading query down until it plans, to find what breaks it.

Synthetic reductions of the correlated subquery all plan fine against riffq
(see isolate_correlated_scalar_subquery.py), so the failure depends on
something in the real query rather than on the subquery pattern alone. This
starts from Npgsql's actual query -- the text riffq logged when the connection
failed -- and removes one construct at a time.

Run from the riffq project root:

    venv/bin/python -m claude-scripts.bisect_npgsql_type_query
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

PORT = 55596

# Npgsql 9.0.5's type-loading query, as riffq logged it. Kept verbatim so the
# bisection starts from something known to fail.
FULL = """
SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
FROM (
    SELECT typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid,
           typ.typnotnull, typ.relkind, elemtyp.oid AS elemtypoid,
           elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,
           CASE WHEN elemproc.proname = 'array_recv' THEN 'a'
                ELSE elemtyp.typtype END AS elemtyptype,
           typ.typcategory
    FROM (
        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind,
               typelem AS elemoid,
               CASE WHEN proc.proname = 'array_recv' THEN 'a'
                    ELSE typ.typtype END AS typtype,
               CASE WHEN proc.proname = 'array_recv' THEN typ.typelem
                    WHEN typ.typtype = 'r' THEN rngsubtype
                    WHEN typ.typtype = 'm' THEN (SELECT rngtypid FROM pg_range
                                                  WHERE rngmultitypid = typ.oid)
                    WHEN typ.typtype = 'd' THEN typ.typbasetype
               END AS elemtypoid,
               typ.typcategory
        FROM pg_catalog.pg_type AS typ
        LEFT JOIN pg_catalog.pg_class AS cls ON (cls.oid = typ.typrelid)
        LEFT JOIN pg_catalog.pg_proc AS proc ON proc.oid = typ.typreceive
        LEFT JOIN pg_catalog.pg_range ON (pg_range.rngtypid = typ.oid)
    ) AS typ
    LEFT JOIN pg_catalog.pg_type AS elemtyp ON elemtyp.oid = elemtypoid
    LEFT JOIN pg_catalog.pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)
    LEFT JOIN pg_catalog.pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
) AS t
JOIN pg_catalog.pg_namespace AS ns ON (ns.oid = typnamespace)
WHERE (typtype IN ('b', 'r', 'm', 'e', 'd')
   OR (typtype = 'c' AND relkind = 'c')
   OR (typtype = 'p' AND typname IN ('record', 'void', 'unknown'))
   OR (typtype = 'a' AND (elemtyptype IN ('b', 'r', 'm', 'e', 'd')
       OR (elemtyptype = 'p' AND elemtypname IN ('record', 'void'))
       OR (elemtyptype = 'c' AND elemrelkind = 'c'))))
"""

# The innermost SELECT on its own: the one that contains the subquery.
INNER_ONLY = """
SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind,
       typelem AS elemoid,
       CASE WHEN proc.proname = 'array_recv' THEN 'a'
            ELSE typ.typtype END AS typtype,
       CASE WHEN proc.proname = 'array_recv' THEN typ.typelem
            WHEN typ.typtype = 'r' THEN rngsubtype
            WHEN typ.typtype = 'm' THEN (SELECT rngtypid FROM pg_range
                                          WHERE rngmultitypid = typ.oid)
            WHEN typ.typtype = 'd' THEN typ.typbasetype
       END AS elemtypoid,
       typ.typcategory
FROM pg_catalog.pg_type AS typ
LEFT JOIN pg_catalog.pg_class AS cls ON (cls.oid = typ.typrelid)
LEFT JOIN pg_catalog.pg_proc AS proc ON proc.oid = typ.typreceive
LEFT JOIN pg_catalog.pg_range ON (pg_range.rngtypid = typ.oid)
"""

# The same innermost SELECT with the multirange arm (and only that) removed.
INNER_WITHOUT_SUBQUERY = INNER_ONLY.replace(
    """            WHEN typ.typtype = 'm' THEN (SELECT rngtypid FROM pg_range
                                          WHERE rngmultitypid = typ.oid)\n""",
    "",
)

# The full query with only the multirange arm removed, to confirm the subquery
# is what fails rather than something else in the query.
FULL_WITHOUT_SUBQUERY = FULL.replace(
    """                    WHEN typ.typtype = 'm' THEN (SELECT rngtypid FROM pg_range
                                                  WHERE rngmultitypid = typ.oid)\n""",
    "",
)

# The innermost SELECT plans on its own but not inside the full query, so the
# outer layers are what flip it. These add one layer at a time on top of it.
WRAPPED_ONCE = f"""
SELECT typ.oid, typ.typname, typ.typtype, typ.elemtypoid
FROM ({INNER_ONLY}) AS typ
"""

WRAPPED_WITH_ELEM_JOINS = f"""
SELECT typ.oid, typ.typname, typ.typtype, elemtyp.oid AS elemtypoid
FROM ({INNER_ONLY}) AS typ
LEFT JOIN pg_catalog.pg_type AS elemtyp ON elemtyp.oid = typ.elemtypoid
"""

WRAPPED_TWICE_WITH_NAMESPACE_JOIN = f"""
SELECT ns.nspname, t.oid, t.typname
FROM (
    SELECT typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.elemtypoid
    FROM ({INNER_ONLY}) AS typ
) AS t
JOIN pg_catalog.pg_namespace AS ns ON (ns.oid = t.typnamespace)
"""

PROBES = [
    ("full Npgsql type query", FULL),
    ("full query, multirange subquery arm removed", FULL_WITHOUT_SUBQUERY),
    ("innermost SELECT only", INNER_ONLY),
    ("innermost SELECT, multirange subquery arm removed", INNER_WITHOUT_SUBQUERY),
    ("innermost wrapped in one derived table", WRAPPED_ONCE),
    ("wrapped, plus the elemtyp LEFT JOIN", WRAPPED_WITH_ELEM_JOINS),
    ("wrapped twice, plus the pg_namespace JOIN", WRAPPED_TWICE_WITH_NAMESPACE_JOIN),
]


def main():
    """Run each probe and report whether it planned."""
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
                        print(f"OK   {label} ({len(cursor.fetchall())} rows)")
                    except Exception as error:
                        connection.rollback()
                        message = " | ".join(str(error).strip().splitlines()[:2])
                        print(f"FAIL {label}")
                        print(f"       {message}")
    finally:
        stop_server(server)


if __name__ == "__main__":
    main()
