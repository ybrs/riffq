import multiprocessing
import socket
import time
import psycopg
import unittest
from helpers import _ensure_riffq_built


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
        _ensure_riffq_built()
        cls.port = 55441
        cls.proc = multiprocessing.Process(target=_run_server, args=(cls.port,), daemon=True)
        cls.proc.start()
        start = time.time()
        # Having multiple databases can take sometime. This is a known issue. 
        while time.time() - start < 30:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", cls.port)) == 0:
                    break
            time.sleep(0.1)
        else:
            cls.proc.terminate()
            cls.proc.join()
            raise RuntimeError("Server did not start")

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.join()

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
