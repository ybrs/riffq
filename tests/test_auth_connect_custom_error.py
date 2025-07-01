import multiprocessing
import socket
import time
import psycopg
import unittest
from helpers import _ensure_riffq_built


def _run_connect_error_server(port: int):
    import riffq

    def handle_query(sql, callback, **kwargs):
        callback(([{"name": "val", "type": "int"}], [[1]]))

    def handle_connect(conn_id, ip, port, *, callback):
        callback(False, message="nope", sqlstate="57P03", severity="FATAL")

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_connect(handle_connect)
    server.start()


def _run_auth_error_server(port: int):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    def handle_auth(conn_id, user, password, host, *, callback, database=None):
        if password != "secret":
            callback(False, message="bad pass", sqlstate="28P02", severity="FATAL")
        else:
            callback(True)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_authentication(handle_auth)
    server.start()


class ConnectAuthErrorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()

    def _wait(self, port):
        start = time.time()
        while time.time() - start < 10:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    return
            time.sleep(0.1)
        raise RuntimeError("Server did not start")

    def test_connect_custom_error(self):
        port = 55441
        proc = multiprocessing.Process(target=_run_connect_error_server, args=(port,), daemon=True)
        proc.start()
        try:
            self._wait(port)
            with self.assertRaises(psycopg.OperationalError) as ctx:
                psycopg.connect(f"postgresql://user@127.0.0.1:{port}/db")
            self.assertIn("nope", str(ctx.exception))
        finally:
            proc.terminate()
            proc.join()

    def test_auth_custom_error(self):
        port = 55442
        proc = multiprocessing.Process(target=_run_auth_error_server, args=(port,), daemon=True)
        proc.start()
        try:
            self._wait(port)
            with self.assertRaises(psycopg.OperationalError) as ctx:
                psycopg.connect(f"postgresql://user:wrong@127.0.0.1:{port}/db")
            self.assertIn("bad pass", str(ctx.exception))
        finally:
            proc.terminate()
            proc.join()


if __name__ == "__main__":
    unittest.main()
