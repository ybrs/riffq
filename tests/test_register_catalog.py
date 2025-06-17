import multiprocessing
import socket
import time
import psycopg
import unittest
from helpers import _ensure_riffq_built
import pyarrow as pa


def _run_server(port: int):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.register_database("mydb")
    server.register_schema("mydb", "myschema")
    server.register_table(
        "mydb",
        "myschema",
        "users",
        [{"id": {"type": "int", "nullable": False}}],
    )
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class RegisterCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55440
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

    def test_registered_objects(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='mydb'")
            self.assertEqual(cur.fetchone()[0], "mydb")
            cur.execute("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname='myschema'")
            self.assertEqual(cur.fetchone()[0], "myschema")
            cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname='users'")
            self.assertEqual(cur.fetchone()[0], "users")
        conn.close()


if __name__ == "__main__":
    unittest.main()
