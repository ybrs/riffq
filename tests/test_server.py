import multiprocessing
import socket
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import unittest


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

    def handle_query(sql, callback, **kwargs):
        args = kwargs.get("query_args")
        if args:
            value = int(args[0])
        else:
            sql = sql.strip().lower()
            if sql.startswith("select ") and sql[7:].isdigit():
                value = int(sql[7:])
            else:
                value = 1
        result = ([{"name": "val", "type": "int"}], [[value]])
        callback(result)

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
