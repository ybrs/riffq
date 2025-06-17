import multiprocessing
import socket
import time
import unittest
from pathlib import Path
import importlib.util

import psycopg
from helpers import _ensure_riffq_built


def _run_server(port: int):
    spec = importlib.util.spec_from_file_location(
        "duckdb_server",
        Path(__file__).resolve().parent.parent / "duckdb-pgcatalog" / "server.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main(port)


class DuckdbCatalogTest(unittest.TestCase):
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

    def test_catalog_objects(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='duckdb'")
            self.assertEqual(cur.fetchone()[0], "duckdb")

            cur.execute("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname='main'")
            self.assertEqual(cur.fetchone()[0], "main")

            cur.execute(
                "SELECT relname FROM pg_catalog.pg_class WHERE relname IN ('users','projects','tasks')"
            )
            names = {row[0] for row in cur.fetchall()}
            self.assertEqual(names, {"users", "projects", "tasks"})
        conn.close()


if __name__ == "__main__":
    unittest.main()
