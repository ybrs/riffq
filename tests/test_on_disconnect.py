import socket
import subprocess
import sys
import time
from pathlib import Path
import multiprocessing

import psycopg
import unittest
from helpers import _ensure_riffq_built


def _run_server(port: int, q):
    import riffq

    def handle_query(sql, callback, **kwargs):
        callback(([{"name": "val", "type": "int"}], [[1]]))

    def handle_disconnect(conn_id, ip, port):
        q.put(conn_id)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_disconnect(handle_disconnect)
    server.start()


class OnDisconnectTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55437
        cls.queue = multiprocessing.Queue()
        cls.proc = multiprocessing.Process(target=_run_server, args=(cls.port, cls.queue), daemon=True)
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

    def test_disconnect_called(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        conn.close()
        start = time.time()
        conn_id = None
        while time.time() - start < 5:
            if not self.queue.empty():
                conn_id = self.queue.get_nowait()
                break
            time.sleep(0.1)
        self.assertIsNotNone(conn_id, "disconnect callback not called")
        self.assertIsInstance(conn_id, int)


if __name__ == "__main__":
    unittest.main()
