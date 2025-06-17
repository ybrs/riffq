import multiprocessing
import socket
import time
import unittest
import psycopg
from helpers import _ensure_riffq_built

import pyarrow as pa


def _run_server_register(port: int):
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
        args = kwargs.get("query_args")
        sql_clean = sql.strip().lower()
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
    server.register_database("crm")
    server.register_schema("crm", "public")
    server.register_table("crm", "public", "users", {"id": ("int", False)})
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class RegisterCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55450
        cls.proc = multiprocessing.Process(target=_run_server_register, args=(cls.port,), daemon=True)
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

    def test_registered_metadata(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='crm'")
            self.assertEqual(cur.fetchone()[0], "crm")
            cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname='users'")
            self.assertEqual(cur.fetchone()[0], "users")
            cur.execute(
                "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname='users')"
            )
            self.assertEqual(cur.fetchone()[0], "id")
        conn.close()


if __name__ == "__main__":
    unittest.main()
