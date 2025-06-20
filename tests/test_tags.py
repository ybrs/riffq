import multiprocessing
import socket
import time
import psycopg
import unittest
from helpers import _ensure_riffq_built


def _run_server(port: int):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        sql_clean = sql.strip().lower()
        if sql_clean == "begin":
            callback("BEGIN", is_tag=True)
        elif sql_clean == "commit":
            callback("COMMIT", is_tag=True)
        else:
            callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.start()


class TagTest(unittest.TestCase):
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

    def test_tags(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            self.assertEqual(cur.statusmessage, "BEGIN")
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone()[0], 1)
            cur.execute("COMMIT")
            self.assertEqual(cur.statusmessage, "COMMIT")
        conn.close()


if __name__ == "__main__":
    unittest.main()
