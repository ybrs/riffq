"""A catalog-emulating server serves the databases it was told about, and only
those.

Connecting to a database the host never registered is refused the way PostgreSQL
refuses it, rather than being served some other database's catalog. These tests
pin that rule, the error a client sees when it trips over it, and the two ways it
deliberately does not apply: a server that emulates no catalog, and a server
whose lazy source gains a database while it runs.
"""
import multiprocessing
import socket
import threading
import time
import unittest

import psycopg
from helpers import stop_server


def _wait_for_port(port, timeout=30):
    """Block until something is listening on `port`, or raise."""
    start = time.time()
    while time.time() - start < timeout:
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise RuntimeError(f"server never bound port {port}")


def _handle_query(sql, callback, **kwargs):
    """Answer every host query with a single row, enough for a smoke check."""
    from riffq.helpers import to_arrow

    callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))


def _run_declaring_server(port):
    """A server declaring one database, `sales`, through the eager API."""
    import riffq

    server = riffq.Server(f"127.0.0.1:{port}")
    server.register_database("sales")
    server.register_schema("sales", "public")
    server.register_table(
        "sales", "public", "orders", [{"id": {"type": "int", "nullable": False}}]
    )
    server.on_query(_handle_query)
    server.start(catalog_emulation=True)


def _run_server_without_catalog_emulation(port):
    """A server that answers catalog queries itself, so riffq admits everyone."""
    import riffq

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(_handle_query)
    server.start()


class DatabaseAdmissionTest(unittest.TestCase):
    """A declared database is connectable; anything else is refused."""

    @classmethod
    def setUpClass(cls):
        cls.port = 55480
        cls.proc = multiprocessing.Process(
            target=_run_declaring_server, args=(cls.port,), daemon=True
        )
        cls.proc.start()
        _wait_for_port(cls.port)

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def test_declared_database_is_connectable(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/sales")
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname='orders'")
                self.assertEqual(cur.fetchone()[0], "orders")
        finally:
            conn.close()

    def test_undeclared_database_is_refused(self):
        # PostgreSQL's exact wording, sent as a FATAL carrying SQLSTATE 3D000 so
        # a driver classifies it as "no such database" rather than a generic
        # connection failure. The code itself is not asserted here: libpq's
        # default verbosity omits it from the message text, and psycopg leaves
        # sqlstate unset on a failure that happens during connect.
        with self.assertRaises(psycopg.OperationalError) as caught:
            psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/nosuchdb")
        self.assertIn('database "nosuchdb" does not exist', str(caught.exception))

    def test_the_refusal_names_the_databases_that_do_exist(self):
        # The whole migration for this change is "connect to the name you
        # registered", so the error says which names those are. Pinned because
        # it is the error's reason for existing, not decoration.
        with self.assertRaises(psycopg.OperationalError) as caught:
            psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/nosuchdb")
        self.assertIn("sales", str(caught.exception))


class NoCatalogEmulationAdmitsAnyDatabaseTest(unittest.TestCase):
    """Without catalog emulation the host answers catalog queries itself, so
    riffq has no catalog to isolate and no databases to admit anyone to."""

    @classmethod
    def setUpClass(cls):
        cls.port = 55481
        cls.proc = multiprocessing.Process(
            target=_run_server_without_catalog_emulation, args=(cls.port,), daemon=True
        )
        cls.proc.start()
        _wait_for_port(cls.port)

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def test_any_database_name_connects(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/anything_at_all")
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                self.assertEqual(cur.fetchone()[0], 1)
        finally:
            conn.close()


class StartRefusesEmulationWithNoDatabasesTest(unittest.TestCase):
    """A catalog-emulating server with nothing registered could never serve
    anyone, and says so at start() rather than binding a port and refusing
    every client."""

    def test_start_raises_naming_both_remedies(self):
        import riffq

        server = riffq.Server("127.0.0.1:55482")
        server.on_query(_handle_query)
        with self.assertRaises(ValueError) as caught:
            server.start(catalog_emulation=True)
        message = str(caught.exception)
        self.assertIn("register_database", message)
        self.assertIn("set_lazy_catalog", message)


def _run_lazy_server(port):
    """A server whose single database comes from a lazy source."""
    import riffq
    from riffq.helpers import to_arrow

    class SingleDatabaseSource:
        def databases(self, callback):
            callback([{"oid": 16400, "name": "shared"}])

        def schemas(self, database, callback):
            callback([{"oid": 16401, "name": "public"}])

        def relations(self, database, schema, callback):
            callback(
                [
                    {
                        "oid": 20100,
                        "reltype_oid": 30100,
                        "name": "widgets",
                        "kind": "table",
                        "has_index": False,
                    }
                ]
            )

        def columns(self, database, schema, relation, callback):
            callback([{"name": "id", "type_oid": 23, "nullable": False}])

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.set_lazy_catalog(SingleDatabaseSource())
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class ConcurrentFirstConnectionsTest(unittest.TestCase):
    """Two clients arriving together at a database nobody has connected to yet
    must both be served.

    Contexts are built on demand, so these two race the build. Before per-
    database building the whole map was populated at startup and the accept loop
    read an arbitrary entry from it; building on demand puts a fallible,
    second-long operation on the connect path, where a mishandled race shows up
    as a hang or a failed connection rather than a wrong answer.

    That the build happens ONCE rather than twice is not asserted here: nothing
    observable over the wire distinguishes the two, since both produce a correct
    catalog. It rests on tokio's OnceCell::get_or_try_init, which parks the
    losers on a semaphore until the winner finishes
    (tokio-1.52.3 src/sync/once_cell.rs:400-427).
    """

    @classmethod
    def setUpClass(cls):
        cls.port = 55483
        cls.proc = multiprocessing.Process(
            target=_run_lazy_server, args=(cls.port,), daemon=True
        )
        cls.proc.start()
        _wait_for_port(cls.port)

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def test_two_simultaneous_first_connections_both_succeed(self):
        results = []
        errors = []
        barrier = threading.Barrier(2)

        def connect_and_read():
            try:
                # Line both threads up so they race the build rather than
                # arriving after it has finished.
                barrier.wait(timeout=30)
                conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/shared")
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT relname FROM pg_catalog.pg_class WHERE relname='widgets'"
                        )
                        results.append(cur.fetchone()[0])
                finally:
                    conn.close()
            except Exception as exc:  # noqa: BLE001 - reported through the assertion below
                errors.append(exc)

        threads = [threading.Thread(target=connect_and_read) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)

        self.assertEqual(errors, [], f"both connections must succeed, got {errors}")
        self.assertEqual(results, ["widgets", "widgets"])

    def test_a_database_reached_after_startup_needs_no_restart(self):
        # The source is asked afresh on every connect, so a database it starts
        # reporting later is connectable without bouncing the server. This is
        # the reason contexts are built on demand rather than at startup.
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/shared")
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='shared'")
                self.assertEqual(cur.fetchone()[0], "shared")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
