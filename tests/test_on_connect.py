
import psycopg
import unittest
from helpers import launch_server_process, stop_server


def _run_server(port: int):
    import riffq

    def handle_query(sql, callback, **kwargs):
        callback(([{"name": "val", "type": "int"}], [[1]]))

    def handle_connect(conn_id, ip, port, *, callback, server_name=None, metadata=None):
        assert metadata is not None
        assert metadata["user"] == "user"
        callback(False)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_connect(handle_connect)
    server.start()


class OnConnectTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55436
        cls.proc = launch_server_process(_run_server, cls.port)

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def test_reject(self):
        with self.assertRaises(psycopg.OperationalError):
            psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")


if __name__ == "__main__":
    unittest.main()
