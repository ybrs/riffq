import unittest
from helpers import launch_server_process, stop_server
import psycopg


def _run_server_catalog(port: int, enabled: bool):
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
            batch = pa.record_batch([
                pa.array([1], pa.int64()),
                pa.array(["foo"], pa.string()),
                pa.array([3.14], pa.float64()),
                pa.array([None], pa.int64()),
            ], names=["a", "b", "c", "d"])
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
    # Catalog emulation serves one context per registered database and refuses
    # any other, so the database these tests connect to has to be declared.
    if enabled:
        server.register_database("db")
    server.on_query(handle_query)
    server.start(catalog_emulation=enabled)


class PgCatalogEnabledTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55434
        cls.proc = launch_server_process(_run_server_catalog, cls.port, args=(True,))

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def test_catalog_query(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database LIMIT 1")
            row = cur.fetchone()
            self.assertIsInstance(row[0], str)
        conn.close()

    def test_catalog_pg_class_query(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class LIMIT 1")
            row = cur.fetchone()
            self.assertIsInstance(row[0], str)
        conn.close()

    def test_non_catalog_query(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT 42")
            self.assertEqual(cur.fetchone()[0], 42)
        conn.close()

    def test_current_schemas(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT current_schemas(true)")
            row = cur.fetchone()
            self.assertIsInstance(row[0], list)
            self.assertGreater(len(row[0]), 0)
        conn.close()


class PgCatalogDisabledTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55439
        cls.proc = launch_server_process(_run_server_catalog, cls.port, args=(False,))

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def test_pg_class_disabled(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class LIMIT 1")
            row = cur.fetchone()
            self.assertEqual(row[0], 1)
        conn.close()


if __name__ == "__main__":
    unittest.main()
