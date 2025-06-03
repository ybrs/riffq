import multiprocessing
import socket
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import unittest
import pyarrow as pa


def _ensure_riffq_built():
    try:
        import riffq  # noqa: F401
        return
    except ImportError:
        pass

    # Attempt to build the extension from source
    try:
        subprocess.check_call(["maturin", "build", "--release", "-q"])
        wheel = next(Path("target/wheels").glob("riffq-*.whl"))
        subprocess.check_call([sys.executable, "-m", "pip", "install", str(wheel)])
    except Exception as exc:
        raise RuntimeError(f"riffq build failed: {exc}")


def _run_server(port: int):
    import riffq
    import pyarrow as pa
    import pyarrow.ipc as ipc

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
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, batch.schema) as writer:
                writer.write_batch(batch)
            callback(sink.getvalue().to_pybytes())
            return

        if args:
            value = int(args[0])
        else:
            if sql_clean.startswith("select ") and sql_clean[7:].isdigit():
                value = int(sql_clean[7:])
            else:
                value = 1

        batch = pa.record_batch([
            pa.array([value], pa.int64())
        ], names=["val"])
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        callback(sink.getvalue().to_pybytes())

    server = riffq.Server(f"127.0.0.1:{port}")
    server.set_callback(handle_query)
    server.start()


class ServerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55433
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
