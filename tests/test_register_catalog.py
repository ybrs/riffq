import multiprocessing
import socket
import time
import unittest
import psycopg
from helpers import _ensure_riffq_built


def _run_server(port: int):
    import riffq
    import pyarrow as pa

    def send_batch(batch: pa.RecordBatch, callback):
        rdr = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        if hasattr(rdr, "__arrow_c_stream__"):
            capsule = rdr.__arrow_c_stream__()
        else:
            from pyarrow.cffi import export_stream
            capsule = export_stream(rdr)
        callback(capsule)

    def handle_query(sql, callback, **kwargs):
        batch = pa.record_batch([pa.array([1], pa.int64())], names=["val"])
        send_batch(batch, callback)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.register_database("crm")
    server.register_schema("crm", "public")
    server.register_table(
        "crm",
        "public",
        "users",
        [{"id": {"type": "int", "nullable": False}}],
    )
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class RegisterCatalogTest(unittest.TestCase):
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

    def test_registered_database(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='crm'")
            self.assertEqual(cur.fetchone()[0], "crm")
        conn.close()

    def test_registered_table(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname='users'")
            self.assertEqual(cur.fetchone()[0], "users")
        conn.close()


if __name__ == "__main__":
    unittest.main()
