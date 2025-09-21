import multiprocessing
import socket
import time
import unittest
import psycopg
from helpers import _ensure_riffq_built


def _run_server(port: int, version: str):
    import riffq

    def handle_query(sql, callback, **kwargs):
        # simple default for non-show
        callback(([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.start(server_version=version)


class ServerVersionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55441
        cls.version = "99.42-test"
        cls.proc = multiprocessing.Process(
            target=_run_server, args=(cls.port, cls.version), daemon=True
        )
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

    def test_show_server_version(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SHOW server_version")
            row = cur.fetchone()
            self.assertEqual(row[0], self.version)
        conn.close()


if __name__ == "__main__":
    unittest.main()

