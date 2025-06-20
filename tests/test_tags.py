import multiprocessing
import socket
import time
import psycopg
import unittest
from helpers import _ensure_riffq_built


def _run_server(port: int):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, *, tag_callback=None, **kwargs):
        sql_clean = sql.strip().lower()
        if sql_clean in ("begin", "commit", "rollback", "end"):
            if tag_callback:
                tag_callback("COMMIT" if sql_clean == "end" else sql_clean.upper())
            return
        if sql_clean.startswith("set "):
            if tag_callback:
                tag_callback("SET")
            return
        if sql_clean.startswith("show "):
            var = sql_clean.split()[1]
            callback(to_arrow([{"name": var, "type": "str"}], [["1"]]))
            return
        callback(([{"name": "val", "type": "int"}], [[1]]))

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

    def test_transaction_tags(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        cur = conn.cursor()
        cur.execute("BEGIN")
        self.assertEqual(cur.statusmessage, "BEGIN")
        cur.execute("COMMIT")
        self.assertEqual(cur.statusmessage, "COMMIT")
        conn.close()

    def test_set_show(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        cur = conn.cursor()
        cur.execute("SET myvar TO 1")
        self.assertEqual(cur.statusmessage, "SET")
        cur.execute("SHOW myvar")
        self.assertEqual(cur.fetchone()[0], "1")
        conn.close()


if __name__ == "__main__":
    unittest.main()
