import multiprocessing
import socket
import subprocess
import sys
import time
from pathlib import Path
import tempfile

import psycopg
import unittest
from helpers import _ensure_riffq_built

def _run_server_tls(port: int, cert: str, key: str):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        args = kwargs.get("query_args")

        sql_clean = sql.strip().lower()
        if sql_clean == "select 1":
            callback(to_arrow([{"name": "val", "type": "int"}], [[1]]) )
            return
        if args:
            value = int(args[0])
        elif sql_clean.startswith("select ") and sql_clean[7:].isdigit():
            value = int(sql_clean[7:])
        else:
            value = 1
        callback(to_arrow([{"name": "val", "type": "int"}], [[value]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.set_tls(cert, key)
    server.start(tls=True)


class ServerTLSTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55434
        cls.tmp = tempfile.TemporaryDirectory()
        cert = Path(cls.tmp.name) / "server.crt"
        key = Path(cls.tmp.name) / "server.key"
        subprocess.check_call([
            "openssl", "req", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-x509", "-days", "1",
            "-out", str(cert), "-subj", "/CN=localhost"
        ])
        cls.proc = multiprocessing.Process(target=_run_server_tls, args=(cls.port, str(cert), str(key)), daemon=True)
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
        cls.tmp.cleanup()

    def test_tls_query(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db", sslmode="require")
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone()[0], 1)
        conn.close()


if __name__ == "__main__":
    unittest.main()
