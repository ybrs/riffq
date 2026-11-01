"""A declared base database and a live lazy source served together.

The arrangement a host wants when part of its catalog is fixed and part is
not: register_database("auth") + register_table(...) for the base that always
exists, and set_lazy_catalog(source) for what appears at runtime -- user
created databases, adhoc mounts. Before this, installing a source silently
discarded every declaration; now the catalog serves their union.

The rules under test, over the wire because that is where a host observes
them:
- both databases are connectable and both appear in pg_database (what \\l
  reads), from either connection;
- each database serves its own tables;
- a database BOTH sides contribute to merges: the source can mount a table
  into the declared base's public schema, and it lands beside the declared
  tables under the one canonical schema row;
- a database name BOTH sides claim is refused loudly, so a runtime mount can
  never silently shadow the base -- served by a second fixture server whose
  source does exactly that.
"""
import multiprocessing
import socket
import time
import unittest

import psycopg
from helpers import stop_server

PORT = 55489
COLLIDING_PORT = 55490

# The canonical pg_namespace oid of public, which the declared base's schema
# must keep even while the live source also claims a public of its own.
PUBLIC_SCHEMA_OID = 2200


class AdhocMountSource:
    """The live half: one runtime database, plus a table mounted into the
    declared base database's public schema."""

    def databases(self, callback):
        """One adhoc database of this source's own."""
        callback([{"oid": 70001, "name": "adhoc"}])

    def schemas(self, database, callback):
        """The adhoc database's schema, plus a public claimed under auth."""
        if database == "adhoc":
            callback([{"oid": 70100, "name": "main"}])
        elif database == "auth":
            callback([{"oid": 70101, "name": "public"}])

    def relations(self, database, schema, callback):
        """The adhoc table, and one mounted into auth.public."""
        if database == "adhoc" and schema == "main":
            callback(
                [
                    {
                        "oid": 70200,
                        "reltype_oid": 70201,
                        "name": "events",
                        "kind": "table",
                    }
                ]
            )
        elif database == "auth" and schema == "public":
            callback(
                [
                    {
                        "oid": 70300,
                        "reltype_oid": 70301,
                        "name": "sessions",
                        "kind": "table",
                    }
                ]
            )

    def columns(self, database, schema, relation, callback):
        """One column each, named after its table so answers are unambiguous."""
        if relation == "events":
            callback([{"name": "ts", "type_oid": 25, "nullable": True}])
        elif relation == "sessions":
            callback([{"name": "token", "type_oid": 25, "nullable": False}])


def _run_server(port):
    """A server declaring the auth base AND installing the live source."""
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        """Answer every forwarded statement with one dummy row."""
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.register_database("auth")
    server.register_schema("auth", "public")
    server.register_table(
        "auth", "public", "users", [{"id": {"type": "int", "nullable": False}}]
    )
    server.set_lazy_catalog(AdhocMountSource())
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class DeclaredBaseWithLazyMountsTest(unittest.TestCase):
    """The union of a declared base and a live source, observed by a client."""

    @classmethod
    def setUpClass(cls):
        """Start the merged-catalog fixture server and wait for its port."""
        cls.proc = multiprocessing.Process(target=_run_server, args=(PORT,), daemon=True)
        cls.proc.start()
        start = time.time()
        while time.time() - start < 60:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", PORT)) == 0:
                    return
            time.sleep(0.1)
        stop_server(cls.proc)
        raise RuntimeError("Server did not start")

    @classmethod
    def tearDownClass(cls):
        """Stop the fixture server."""
        stop_server(cls.proc)

    def _connect(self, database):
        """A psycopg connection to `database` on the fixture server."""
        return psycopg.connect(
            host="127.0.0.1", port=PORT, user="user", dbname=database, autocommit=True
        )

    def _rows(self, database, sql):
        """Every row `sql` returns over a fresh connection to `database`."""
        with self._connect(database) as conn, conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def test_both_databases_are_connectable_and_listed(self):
        """\\l lists the declared base and the live mount, from either side."""
        for database in ("auth", "adhoc"):
            rows = self._rows(
                database,
                "SELECT datname FROM pg_catalog.pg_database "
                "WHERE datname IN ('auth', 'adhoc') ORDER BY datname",
            )
            self.assertEqual([row[0] for row in rows], ["adhoc", "auth"], database)

    def test_the_live_database_serves_its_own_tables(self):
        """The runtime-mounted database answers with its own schema and table."""
        rows = self._rows(
            "adhoc",
            "SELECT schemaname, tablename FROM pg_catalog.pg_tables "
            "WHERE schemaname = 'main'",
        )
        self.assertEqual(rows, [("main", "events")])

    def test_mounted_tables_merge_into_the_declared_base(self):
        """auth.public holds the declared users AND the mounted sessions.

        Under ONE schema row -- the canonical public at 2200 that the declared
        base pinned -- although the live source claimed a public of its own.
        This is the merge the union exists for.
        """
        rows = self._rows(
            "auth",
            "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public'",
        )
        self.assertEqual(rows, [(PUBLIC_SCHEMA_OID,)], "one public, the canonical one")

        rows = self._rows(
            "auth",
            f"SELECT relname FROM pg_catalog.pg_class "
            f"WHERE relnamespace = {PUBLIC_SCHEMA_OID} "
            f"AND relname IN ('users', 'sessions') ORDER BY relname",
        )
        self.assertEqual([row[0] for row in rows], ["sessions", "users"])

    def test_a_name_collision_keeps_the_declared_identity(self):
        """Exactly one auth row, carrying the declared base's identity."""
        rows = self._rows(
            "auth",
            "SELECT oid FROM pg_catalog.pg_database WHERE datname = 'auth'",
        )
        self.assertEqual(len(rows), 1, "one auth row")
        self.assertNotEqual(rows[0][0], 70001, "not the live source's numbering")


class ImpostorDatabaseSource(AdhocMountSource):
    """A live source that also claims the declared base's own name."""

    def databases(self, callback):
        """The adhoc database plus an impostor reusing the base's name."""
        callback(
            [
                {"oid": 70001, "name": "adhoc"},
                {"oid": 99999, "name": "auth"},
            ]
        )


def _run_colliding_server(port):
    """A server whose declared base and live source both claim "auth"."""
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        """Answer every forwarded statement with one dummy row."""
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.register_database("auth")
    server.set_lazy_catalog(ImpostorDatabaseSource())
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class CollidingDatabaseNameIsRefusedTest(unittest.TestCase):
    """A database name claimed by the declaration AND the live source fails
    loudly instead of one side silently shadowing the other.

    PostgreSQL's uniqueness applies: two objects claiming one name means one
    of them is lying, and serving either quietly would hand clients rows from
    an object they did not pick. The refusal surfaces on connection, since
    admission itself has to list the databases.
    """

    @classmethod
    def setUpClass(cls):
        """Start the fixture whose two sources both claim the auth name."""
        cls.proc = multiprocessing.Process(
            target=_run_colliding_server, args=(COLLIDING_PORT,), daemon=True
        )
        cls.proc.start()
        start = time.time()
        while time.time() - start < 60:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", COLLIDING_PORT)) == 0:
                    return
            time.sleep(0.1)
        stop_server(cls.proc)
        raise RuntimeError("Server did not start")

    @classmethod
    def tearDownClass(cls):
        """Stop the colliding fixture server."""
        stop_server(cls.proc)

    def test_connecting_fails_naming_the_collision(self):
        """Admission fails loudly, and the error says what collided."""
        with self.assertRaises(psycopg.Error) as refused:
            psycopg.connect(
                host="127.0.0.1",
                port=COLLIDING_PORT,
                user="user",
                dbname="auth",
                autocommit=True,
                connect_timeout=10,
            )
        self.assertIn("more than one catalog source", str(refused.exception))


if __name__ == "__main__":
    unittest.main()
