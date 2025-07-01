import multiprocessing
import socket
import time
import unittest
import psycopg
from helpers import _ensure_riffq_built


def _run_server(port: int):
    import riffq

    def handle_query(sql, callback, **kwargs):
        if sql.strip().lower() == "select err":
            callback(("ERROR", "42846", "bad type"), is_error=True)
            return
        callback(([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.start()


class QueryErrorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55440
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

    def test_user_error(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            with self.assertRaises(psycopg.errors.CannotCoerce) as ctx:
                cur.execute("SELECT err")
            self.assertEqual(ctx.exception.sqlstate, "42846")
        conn.close()


if __name__ == "__main__":
    unittest.main()
