import multiprocessing
import socket
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import unittest
import pyarrow as pa
from helpers import _ensure_riffq_built

def _run_server(port: int):
    import riffq
    import pyarrow as pa

    def send_batch(batch: pa.RecordBatch, callback):
        rdr = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        # modern pyarrow
        if hasattr(rdr, "__arrow_c_stream__"):
            capsule = rdr.__arrow_c_stream__()
        else:            
            from pyarrow.cffi import export_stream  # type: ignore
            capsule = export_stream(rdr)
        callback(capsule)

    def handle_query(sql, callback, **kwargs):
        args = kwargs.get("query_args")
        sql_clean = sql.strip().lower()
        if sql_clean.startswith("begin"):
            return callback("BEGIN", is_tag=True)
        if sql_clean.startswith("commit"):
            return callback("COMMIT", is_tag=True)
        if sql_clean.startswith("rollback"):
            return callback("ROLLBACK", is_tag=True)
        if sql_clean.startswith("discard all"):
            return callback("DISCARD ALL", is_tag=True)
        if sql_clean == "select multi":
            batch = pa.record_batch(
                [
                    pa.array([1], pa.int64()),
                    pa.array(["foo"], pa.string()),
                    pa.array([3.14], pa.float64()),
                    pa.array([None], pa.int64()),
                ],
                names=["a", "b", "c", "d"],
            )
            return send_batch(batch, callback)
                    
        if sql_clean == "select bool":
            batch = pa.record_batch([pa.array([True], pa.bool_())], names=["flag"])
            return send_batch(batch, callback)

        if args:
            value = int(args[0])
        else:
            if sql_clean.startswith("select ") and sql_clean[7:].isdigit():
                value = int(sql_clean[7:])
            else:
                value = 1

        batch = pa.record_batch([pa.array([value], pa.int64())], names=["val"])
        send_batch(batch, callback)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.start()

class ServerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55433
        cls.proc = multiprocessing.Process(
            target=_run_server, args=(cls.port,), daemon=True
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

    def test_simple_query(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone()[0], 1)
        conn.close()

    def test_extended_query(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT %s", (42,))
            self.assertEqual(cur.fetchone()[0], 42)
        conn.close()

    def test_multiple_columns(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT multi")
            row = cur.fetchone()
            self.assertEqual(row, (1, "foo", 3.14, None))
            self.assertIsInstance(row[0], int)
            self.assertIsInstance(row[1], str)
            self.assertIsInstance(row[2], float)
            self.assertIsNone(row[3])

            names = [desc.name for desc in cur.description]
            self.assertEqual(names, ["a", "b", "c", "d"])
        conn.close()

    def test_bool_column(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT bool")
            row = cur.fetchone()
            self.assertEqual(row, (True,))
            self.assertIsInstance(row[0], bool)
            names = [desc.name for desc in cur.description]
            self.assertEqual(names, ["flag"])
        conn.close()

    def test_transaction_tags(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            self.assertEqual(cur.statusmessage, "BEGIN")
            cur.execute("ROLLBACK")
            self.assertEqual(cur.statusmessage, "ROLLBACK")
            cur.execute("BEGIN")
            cur.execute("COMMIT")
            self.assertEqual(cur.statusmessage, "COMMIT")
        conn.close()

if __name__ == "__main__":
    unittest.main()
