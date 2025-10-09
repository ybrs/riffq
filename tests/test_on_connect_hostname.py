import multiprocessing
import socket
import time
import unittest

import psycopg
from helpers import _ensure_riffq_built


def _run_server(port: int):
    import riffq

    seen = {"hostname": None}

    def handle_query(sql, callback, **kwargs):
        callback(([{"name": "val", "type": "int"}], [[1]]))

    def handle_connect(conn_id, ip, port, *, callback, hostname=None):
        # Reject with a message that includes the parsed hostname
        msg = f"sni_hostname={hostname}" if hostname is not None else "sni_hostname=None"
        callback(False, msg, "FATAL", "28000")

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_connect(handle_connect)
    server.start()


class OnConnectHostnameTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_riffq_built()
        cls.port = 55460
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

    def test_hostname_from_options(self):
        # psycopg/libpq will send the 'options' startup parameter as provided
        # We encode a custom key via -c sni_hostname=...
        with self.assertRaises(psycopg.OperationalError) as ctx:
            psycopg.connect(
                host="127.0.0.1",
                port=self.port,
                user="user",
                dbname="db",
                options='-c sni_hostname=my.example.test',
                application_name='sni_hostname=my.example.test',
            )
        self.assertIn("sni_hostname=my.example.test", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
