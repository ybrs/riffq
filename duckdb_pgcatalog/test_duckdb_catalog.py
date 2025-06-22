import multiprocessing
import socket
import time
import psycopg
import unittest


def _run_server(port: int):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from duckdb_pgcatalog.server import run_server
    run_server(port)


class DuckDbCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
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

    def test_catalog_entries(self):
        conn = psycopg.connect(f"postgresql://user:123@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='duckdb'")
            self.assertEqual(cur.fetchone()[0], "duckdb")
            cur.execute("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname='main'")
            self.assertEqual(cur.fetchone()[0], "main")
            for table in ("users", "projects", "tasks"):
                cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname=%s", (table,))
                self.assertEqual(cur.fetchone()[0], table)
        conn.close()


if __name__ == "__main__":
    unittest.main()
