import unittest
from helpers import launch_server_process, stop_server
import psycopg


def _run_server(port: int):
    import riffq

    def handle_query(sql, callback, **kwargs):
        callback(([{"name": "val", "type": "int"}], [[1]]))

    def handle_connect(conn_id, ip, port, *, callback, server_name=None, metadata=None):
        callback(False, "nope", "FATAL", "28000")

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_connect(handle_connect)
    server.start()


class OnConnectErrorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55450
        cls.proc = launch_server_process(_run_server, cls.port)

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def test_reject_custom_error(self):
        with self.assertRaises(psycopg.OperationalError) as ctx:
            psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")
        self.assertIn("nope", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
