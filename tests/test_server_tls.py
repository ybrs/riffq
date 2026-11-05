import subprocess
from pathlib import Path
import tempfile

import psycopg
import unittest
from helpers import launch_server_process, stop_server


def _run_server_tls(port: int, cert: str, key: str):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        args = kwargs.get("query_args")

        sql_clean = sql.strip().lower()
        if sql_clean == "select 1":
            callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))
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
        cls.port = 55443
        cls.tmp = tempfile.TemporaryDirectory()
        cert = Path(cls.tmp.name) / "server.crt"
        key = Path(cls.tmp.name) / "server.key"
        subprocess.check_call([
            "openssl", "req", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-x509", "-days", "1",
            "-out", str(cert), "-subj", "/CN=localhost"
        ])
        cls.proc = launch_server_process(_run_server_tls, cls.port, args=(str(cert), str(key)))

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)
        cls.tmp.cleanup()

    def test_tls_query(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db", sslmode="require")
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone()[0], 1)
        conn.close()


if __name__ == "__main__":
    unittest.main()
