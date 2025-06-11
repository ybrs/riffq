import multiprocessing
import socket
import time

import psycopg
import unittest
from helpers import _ensure_riffq_built


def _run_server(port: int):
    import riffq
    import duckdb
    import pyarrow as pa

    duckdb_con = duckdb.connect()
    duckdb_con.execute("CREATE TABLE users(id INTEGER, name TEXT)")
    duckdb_con.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    def send_reader(reader, callback):
        if hasattr(reader, "__arrow_c_stream__"):
            capsule = reader.__arrow_c_stream__()
        else:
            from pyarrow.cffi import export_stream
            capsule = export_stream(reader)
        callback(capsule)

    def arrow_batch(values, names):
        return pa.RecordBatchReader.from_batches(
            pa.schema(list(zip(names, [v.type for v in values]))),
            [pa.record_batch(values, names=names)],
        )

    def handle_query(sql, callback, **kwargs):
        text = sql.strip().lower().split(';')[0]
        if "pg_class" in text:
            batch = arrow_batch([pa.array(["users"])], ["relname"])
            return send_reader(batch, callback)
        try:
            reader = duckdb_con.execute(sql).fetch_record_batch()
            send_reader(reader, callback)
        except Exception:
            batch = arrow_batch([pa.array(["ERROR"]), pa.array(["unknown query"])], ["error", "message"])
            send_reader(batch, callback)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.start()


class PgCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55439
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

    def test_pg_class_shows_users(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_class")
            names = [row[0] for row in cur.fetchall()]
            self.assertIn("users", names)
            cur.execute("SELECT * FROM users ORDER BY id")
            rows = cur.fetchall()
            self.assertEqual(rows, [(1, "Alice"), (2, "Bob")])
        conn.close()


if __name__ == "__main__":
    unittest.main()
