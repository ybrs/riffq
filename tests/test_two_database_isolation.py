"""The two registration paths must present the same catalog, and each database
must be an island.

riffq learns about databases in two independent ways -- the eager
`register_database`/`register_schema`/`register_table` calls, and a lazy source
consulted on every scan -- and they used to disagree: eager built an isolated
context per database, lazy flattened every database into one, so a connection to
db1 could see db2's tables and `information_schema` reported a catalog name that
`pg_database` had never heard of. This registers db1.s1.t1 and db2.s2.t2 through
each path and requires the same answers from both.

OIDs are deliberately not compared between the paths. Eager derives them from a
counter and lazy from a hash of the names, and unifying that is a separate piece
of work; what matters here is that they RESOLVE -- every probe joins through
`relnamespace` rather than reading it -- and that each path is internally
consistent. Comparing oids directly becomes possible once both paths share one
oid scheme.

Grown from claude-scripts/show_two_database_catalog_view.py, which printed these
same probes for a human to eyeball.
"""
import multiprocessing
import socket
import time
import unittest

import psycopg
from helpers import stop_server

# Each database gets its own schema and its own table, so anything visible
# across the boundary is unambiguous about which side it came from.
DATABASES = [
    ("db1", "s1", "t1"),
    ("db2", "s2", "t2"),
]

EAGER_PORT = 55484
LAZY_PORT = 55485

# Probes are phrased to avoid raw oids: relationships are resolved by joining,
# so a path whose oids do not line up fails the join and produces different
# output rather than silently comparing two arbitrary numbers.
PROBES = [
    ("current_database", "SELECT current_database()"),
    (
        "databases",
        "SELECT datname FROM pg_catalog.pg_database "
        "WHERE datname IN ('db1', 'db2') ORDER BY datname",
    ),
    (
        "user namespaces",
        "SELECT nspname FROM pg_catalog.pg_namespace "
        "WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
        "ORDER BY nspname",
    ),
    (
        "relations resolved through relnamespace",
        "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
        "ORDER BY n.nspname, c.relname",
    ),
    (
        "information_schema.tables",
        "SELECT table_catalog, table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
        "ORDER BY table_schema, table_name",
    ),
    (
        "information_schema.columns",
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
        "ORDER BY table_name, column_name",
    ),
    # schemata is an INNER JOIN from pg_namespace to pg_authid on nspowner. A
    # schema owned by a role that does not exist fails the join and is simply
    # absent - so this probe is about whether the schema is listed at all, not
    # just about the owner's name.
    (
        "information_schema.schemata",
        "SELECT schema_name, schema_owner FROM information_schema.schemata "
        "WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
        "ORDER BY schema_name",
    ),
    (
        "schema owners",
        "SELECT n.nspname, pg_catalog.pg_get_userbyid(n.nspowner) "
        "FROM pg_catalog.pg_namespace n "
        "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
        "ORDER BY n.nspname",
    ),
    (
        "pg_database metadata",
        "SELECT datname, pg_catalog.pg_get_userbyid(datdba), datallowconn, datistemplate "
        "FROM pg_catalog.pg_database WHERE datname IN ('db1', 'db2') ORDER BY datname",
    ),
]


def _stable_oid(*parts):
    """Derive a stable oid from names, the way teleduck's source does."""
    import zlib

    return 50_000_000 + (zlib.crc32("/".join(parts).encode()) % 10_000_000)


class TwoDatabaseSource:
    """A lazy catalog source exposing the same two databases as the eager run.

    Mirrors teleduck's DuckdbCatalogSource one method per level, but serves
    fixed rows so the two registration paths are compared on identical input.
    """

    def databases(self, callback):
        """Report both databases."""
        callback([{"oid": _stable_oid("db", name), "name": name} for name, _, _ in DATABASES])

    def schemas(self, database, callback):
        """Report the one schema belonging to `database`."""
        callback(
            [
                {"oid": _stable_oid("ns", database, schema), "name": schema}
                for name, schema, _ in DATABASES
                if name == database
            ]
        )

    def relations(self, database, schema, callback):
        """Report the one table in `database`.`schema`."""
        callback(
            [
                {
                    "oid": _stable_oid("rel", database, schema, table),
                    "reltype_oid": _stable_oid("type", database, schema, table),
                    "name": table,
                    "kind": "table",
                    "has_index": False,
                }
                for name, registered_schema, table in DATABASES
                if name == database and registered_schema == schema
            ]
        )

    def columns(self, database, schema, relation, callback):
        """Report the single id column every fixture table has."""
        callback([{"name": "id", "type_oid": 23, "nullable": False}])


def _run_server(port, lazy):
    """Serve two databases, each with one schema holding one table."""
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    if lazy:
        server.set_lazy_catalog(TwoDatabaseSource())
    else:
        for database, schema, table in DATABASES:
            server.register_database(database)
            server.register_schema(database, schema)
            server.register_table(
                database, schema, table, [{"id": {"type": "int", "nullable": False}}]
            )
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


def _start_server(port, lazy):
    """Launch a server and wait for it to bind, returning the process."""
    process = multiprocessing.Process(target=_run_server, args=(port, lazy), daemon=True)
    process.start()
    started = time.monotonic()
    while time.monotonic() - started < 120:
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return process
        time.sleep(0.1)
    stop_server(process)
    raise RuntimeError(f"server on port {port} did not start")


def _catalog_view(port, database):
    """Run every probe against `database` and return {label: rows}."""
    view = {}
    with psycopg.connect(
        host="127.0.0.1", port=port, user="user", dbname=database
    ) as connection:
        for label, sql in PROBES:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                view[label] = cursor.fetchall()
    return view


class TwoDatabaseIsolationTest(unittest.TestCase):
    """Both paths isolate each database, and both describe it the same way."""

    @classmethod
    def setUpClass(cls):
        cls.eager = _start_server(EAGER_PORT, lazy=False)
        cls.lazy = _start_server(LAZY_PORT, lazy=True)
        # Collected once: each connection builds that database's context, which
        # is the expensive part, and every test reads the same four views.
        cls.views = {
            ("eager", database): _catalog_view(EAGER_PORT, database)
            for database, _, _ in DATABASES
        }
        cls.views.update(
            {
                ("lazy", database): _catalog_view(LAZY_PORT, database)
                for database, _, _ in DATABASES
            }
        )

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.eager)
        stop_server(cls.lazy)

    def test_the_two_paths_describe_each_database_identically(self):
        # The spec for collapsing the two mechanisms onto one: whatever a client
        # sees must not depend on how the host registered its catalog.
        for database, _, _ in DATABASES:
            for label, _sql in PROBES:
                self.assertEqual(
                    self.views[("eager", database)][label],
                    self.views[("lazy", database)][label],
                    f"{label} differs between the eager and lazy paths on {database}",
                )

    def test_a_connection_sees_only_its_own_database_objects(self):
        for path in ("eager", "lazy"):
            for database, schema, table in DATABASES:
                view = self.views[(path, database)]
                namespaces = [row[0] for row in view["user namespaces"]]
                relations = view["relations resolved through relnamespace"]

                self.assertIn(schema, namespaces, f"{path}/{database} must see its own schema")
                self.assertEqual(
                    relations,
                    [(schema, table)],
                    f"{path}/{database} must see exactly its own table",
                )

                for other, other_schema, other_table in DATABASES:
                    if other == database:
                        continue
                    self.assertNotIn(
                        other_schema,
                        namespaces,
                        f"{path}/{database} must not see {other}'s schema",
                    )
                    self.assertNotIn(
                        (other_schema, other_table),
                        relations,
                        f"{path}/{database} must not see {other}'s table",
                    )

    def test_pg_database_lists_every_database_from_any_of_them(self):
        # The one catalog table that is deliberately not scoped: PostgreSQL
        # shows every database from every database, which is how "\l" works.
        for path in ("eager", "lazy"):
            for database, _, _ in DATABASES:
                self.assertEqual(
                    self.views[(path, database)]["databases"],
                    [("db1",), ("db2",)],
                    f"{path}/{database} must list both databases",
                )

    def test_information_schema_agrees_with_current_database(self):
        # These disagreed before: current_database() answered per connection
        # while the view bodies carried one catalog name baked in at startup.
        for path in ("eager", "lazy"):
            for database, _, _ in DATABASES:
                view = self.views[(path, database)]
                self.assertEqual(view["current_database"], [(database,)])
                catalogs = {row[0] for row in view["information_schema.tables"]}
                self.assertEqual(
                    catalogs,
                    {database},
                    f"{path}/{database}: information_schema must report the connected database",
                )


if __name__ == "__main__":
    unittest.main()
