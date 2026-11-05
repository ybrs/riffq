import psycopg
import unittest
from helpers import launch_server_process, stop_server, wait_for_catalog


def _run_server(port: int):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    print("starting server")
    server.register_database("db1")
    server.register_database("db2")
    server.register_schema("db1", "s1")
    server.register_schema("db2", "s2")
    server.register_table(
        "db1",
        "s1",
        "t1",
        [{"id": {"type": "int", "nullable": False}}],
    )
    server.register_table(
        "db2",
        "s2",
        "t2",
        [{"id": {"type": "int", "nullable": False}}],
    )
    print("registered databases and tables")
    server.on_query(handle_query)
    server.start(catalog_emulation=True)
    print("server started")


class MultipleDatabaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55441
        cls.proc = launch_server_process(_run_server, cls.port)

        # the TCP port accepts connections before catalog emulation finishes
        # loading, so the first client would race a half-ready server. poll the
        # registered database in pg_database until it answers instead of sleeping.
        wait_for_catalog(
            cls.port,
            "db1",
            "SELECT datname FROM pg_catalog.pg_database WHERE datname='db2'",
            "db2",
        )

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def test_isolation(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db1")
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname='t1'")
            self.assertEqual(cur.fetchone()[0], "t1")
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='db2'")
            self.assertEqual(cur.fetchone()[0], "db2")
        conn.close()

        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db2")
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname='t2'")
            self.assertEqual(cur.fetchone()[0], "t2")
        conn.close()


if __name__ == "__main__":
    unittest.main()
