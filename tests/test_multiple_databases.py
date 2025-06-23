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
    server.register_database("db1")
    server.register_schema("db1", "public")
    server.register_table(
        "db1",
        "public",
        "tbl",
        [{"id": {"type": "int", "nullable": False}}],
    )
    server.register_database("db2")
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class MultipleDatabaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55441
        cls.proc = multiprocessing.Process(target=_run_server, args=(cls.port,), daemon=True)
        cls.proc.start()
        start = time.time()
        while time.time() - start < 10:
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

    def test_database_isolation(self):
        conn1 = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db1")
        with conn1.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname='tbl'")
            self.assertEqual(cur.fetchone()[0], "tbl")
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='db2'")
            self.assertEqual(cur.fetchone()[0], "db2")
        conn1.close()

        conn2 = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db2")
        with conn2.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='db1'")
            self.assertEqual(cur.fetchone()[0], "db1")
        conn2.close()


if __name__ == "__main__":
    unittest.main()
