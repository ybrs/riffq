import multiprocessing
import socket
import time
import psycopg
import unittest
from helpers import stop_server


def _run_server(port: int):
    import riffq
    from riffq.helpers import to_arrow

    # In-process catalog state. The lazy source reads it on every scan, so a
    # table appended here (by handle_query, below) appears in pg_class on the
    # very next catalog query -- without any re-registration.
    state = {"tables": ["users"]}

    def rel_oid(name):
        # Stable OID per table name so pg_class.oid and pg_attribute.attrelid
        # agree across scans and the joins resolve.
        return 20000 + (abs(hash(name)) % 5000)

    class FakeSource:
        """A lazy catalog source over the in-process `state` dict."""

        def databases(self, callback):
            callback([{"oid": 16384, "name": "appdb"}])

        def schemas(self, database, callback):
            if database == "appdb":
                callback([{"oid": 16385, "name": "public"}])

        def relations(self, database, schema, callback):
            if database == "appdb" and schema == "public":
                callback(
                    [
                        {
                            "oid": rel_oid(name),
                            "reltype_oid": rel_oid(name) + 100000,
                            "name": name,
                            "kind": "table",
                        }
                        for name in state["tables"]
                    ]
                )

        def columns(self, database, schema, relation, callback):
            callback(
                [
                    {"name": "id", "type_oid": 23, "nullable": False},
                    {"name": "name", "type_oid": 25, "nullable": True},
                ]
            )

    def handle_query(sql, callback, **kwargs):
        s = sql.strip().lower()
        # Emulate a data backend: a CREATE TABLE mutates the live catalog state.
        if s.startswith("create table "):
            rest = s[len("create table "):].strip()
            name = rest.split("(")[0].split()[0]
            if name not in state["tables"]:
                state["tables"].append(name)
            callback(to_arrow([{"name": "status", "type": "str"}], [["OK"]]))
            return
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.set_lazy_catalog(FakeSource())
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class LazyCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55461
        cls.proc = multiprocessing.Process(
            target=_run_server, args=(cls.port,), daemon=True
        )
        cls.proc.start()
        start = time.time()
        while time.time() - start < 10:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", cls.port)) == 0:
                    break
            time.sleep(0.1)
        else:
            stop_server(cls.proc)
            raise RuntimeError("Server did not start")

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def _conn(self):
        return psycopg.connect(
            f"postgresql://user@127.0.0.1:{self.port}/db", autocommit=True
        )

    def test_lazy_objects_and_builtins(self):
        with self._conn() as conn, conn.cursor() as cur:
            # The lazy database and its built-in neighbours both show up.
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='appdb'")
            self.assertEqual(cur.fetchone()[0], "appdb")
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='postgres'")
            self.assertEqual(cur.fetchone()[0], "postgres")

            cur.execute("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname='public'")
            self.assertIsNotNone(cur.fetchone())

            cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname='users'")
            self.assertEqual(cur.fetchone()[0], "users")

    def test_lazy_join_resolves(self):
        # pg_class join pg_attribute over the lazy 'users' relation.
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT a.attname FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
                "WHERE c.relname = 'users' ORDER BY a.attnum"
            )
            self.assertEqual([r[0] for r in cur.fetchall()], ["id", "name"])

    def test_lazy_reflects_table_created_after_startup(self):
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_catalog.pg_class WHERE relname='orders'")
            self.assertEqual(cur.fetchone()[0], 0)

            # Create the table through the data path; the source now reports it.
            cur.execute("CREATE TABLE orders(id int)")

            cur.execute("SELECT count(*) FROM pg_catalog.pg_class WHERE relname='orders'")
            self.assertEqual(
                cur.fetchone()[0], 1, "lazy catalog must reflect the new table"
            )


if __name__ == "__main__":
    unittest.main()
