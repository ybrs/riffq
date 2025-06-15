import multiprocessing
import socket
import time
import unittest
import psycopg
from helpers import _ensure_riffq_built

from test_server import _run_server

class PgCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55434
        cls.proc = multiprocessing.Process(target=_run_server, args=(cls.port, True), daemon=True)
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

    def test_catalog_query(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database LIMIT 1")
            row = cur.fetchone()
            self.assertIsInstance(row[0], str)
        conn.close()

    def test_catalog_pg_class_query(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class LIMIT 1")
            row = cur.fetchone()
            self.assertIsInstance(row[0], str)
        conn.close()

    def test_non_catalog_query(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT 42")
            self.assertEqual(cur.fetchone()[0], 42)
        conn.close()


class PgCatalogDisabledTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55439
        cls.proc = multiprocessing.Process(target=_run_server, args=(cls.port, False), daemon=True)
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

    def test_catalog_query_fails(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class LIMIT 1")
            row = cur.fetchone()
            self.assertIsInstance(row[0], int)
        conn.close()

if __name__ == "__main__":
    unittest.main()
