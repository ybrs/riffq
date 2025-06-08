import multiprocessing
import socket
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import unittest
from helpers import _ensure_riffq_built

def _run_server(port: int):
    import riffq

    def handle_query(sql, callback, **kwargs):
        callback(([{"name": "val", "type": "int"}], [[1]]))

    def handle_connect(conn_id, ip, port, *, callback):
        callback(False)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_connect(handle_connect)
    server.start()


class OnConnectTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55436
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

    def test_reject(self):
        with self.assertRaises(psycopg.OperationalError):
            psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")


if __name__ == "__main__":
    unittest.main()
