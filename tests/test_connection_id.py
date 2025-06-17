import multiprocessing
import socket
import subprocess
import sys
import time
import threading
from pathlib import Path

import psycopg
import unittest
from helpers import _ensure_riffq_built

import pyarrow as pa

def _run_server(port: int):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kw):
        if sql.strip().lower() == "select conn_id":
            capsule = to_arrow(
                [{"name": "id",  "type": "int"}],
                [[int(kw["connection_id"])]],
            )
        else:
            capsule = to_arrow(
                [{"name": "val", "type": "int"}],
                [[1]],
            )
        callback(capsule)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.start()


class ConnectionIdTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55435
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

    def _fetch_id(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT conn_id")
            result = cur.fetchone()[0]
        conn.close()
        return result

    def test_connection_ids_increment(self):
        first = self._fetch_id()
        second = self._fetch_id()
        self.assertNotEqual(first, second)

    def test_parallel_connections_unique(self):
        results = []
        def worker():
            results.append(self._fetch_id())
        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start(); t2.start()
        t1.join(); t2.join()
        self.assertEqual(len(results), 2)
        self.assertEqual(len(set(results)), 2)


if __name__ == "__main__":
    unittest.main()
