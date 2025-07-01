import multiprocessing
import socket
import time
import unittest
import psycopg
from helpers import _ensure_riffq_built


def _run_server(port: int):
    import riffq

    def handle_query(sql, callback, **kwargs):
        callback(([{"name": "val", "type": "int"}], [[1]]))

    def handle_connect(conn_id, ip, port, *, callback):
        callback(False, "nope", "FATAL", "28000")

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_connect(handle_connect)
    server.start()


class OnConnectErrorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55450
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

    def test_reject_custom_error(self):
        with self.assertRaises(psycopg.OperationalError) as ctx:
            psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        self.assertIn("nope", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
