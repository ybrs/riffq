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
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    def handle_auth(conn_id, user, password, host, *, callback, database=None):
        callback(user == "user" and password == "secret")

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_authentication(handle_auth)
    server.start()


def _run_server_custom(port: int):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    def handle_auth(conn_id, user, password, host, *, callback, database=None):
        if user == "user" and password == "secret":
            callback(True)
        else:
            callback(False, "FATAL", "28P01", "bad password")

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_authentication(handle_auth)
    server.start()


class AuthenticationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55438
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

    def test_auth_reject(self):
        with self.assertRaises(psycopg.OperationalError):
            psycopg.connect(f"postgresql://user:wrong@127.0.0.1:{self.port}/db")

    def test_auth_accept(self):
        conn = psycopg.connect(f"postgresql://user:secret@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone()[0], 1)
        conn.close()


class AuthenticationCustomErrorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55448
        cls.proc = multiprocessing.Process(target=_run_server_custom, args=(cls.port,), daemon=True)
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

    def test_auth_custom_error(self):
        with self.assertRaises(psycopg.OperationalError) as ctx:
            psycopg.connect(f"postgresql://user:wrong@127.0.0.1:{self.port}/db")
        self.assertEqual(ctx.exception.pgcode, "28P01")


if __name__ == "__main__":
    unittest.main()
