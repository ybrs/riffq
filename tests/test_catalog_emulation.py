import multiprocessing
import socket
import time
import psycopg
import unittest
from helpers import _ensure_riffq_built


def _run_server(port: int, catalog_emulation: bool):
    import riffq
    import pyarrow as pa

    def send_batch(batch: pa.RecordBatch, callback):
        rdr = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        if hasattr(rdr, "__arrow_c_stream__"):
            capsule = rdr.__arrow_c_stream__()
        else:
            from pyarrow.cffi import export_stream  # type: ignore
            capsule = export_stream(rdr)
        callback(capsule)

    def handle_query(sql, callback, **kwargs):
        batch = pa.record_batch([pa.array([1], pa.int64())], names=["val"])
        send_batch(batch, callback)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.start(catalog_emulation=catalog_emulation)


class CatalogEmulationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()

    def _start_server(self, catalog_emulation):
        port = 55440 if catalog_emulation else 55441
        proc = multiprocessing.Process(target=_run_server, args=(port, catalog_emulation), daemon=True)
        proc.start()
        start = time.time()
        while time.time() - start < 10:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(0.1)
        else:
            proc.terminate()
            proc.join()
            raise RuntimeError("Server did not start")
        return proc, port

    def test_catalog_enabled(self):
        proc, port = self._start_server(True)
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class LIMIT 1")
            row = cur.fetchone()[0]
        conn.close()
        proc.terminate(); proc.join()
        self.assertEqual(row, "pg_class")

    def test_catalog_disabled(self):
        proc, port = self._start_server(False)
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class LIMIT 1")
            row = cur.fetchone()[0]
        conn.close()
        proc.terminate(); proc.join()
        self.assertEqual(row, 1)


if __name__ == "__main__":
    unittest.main()
