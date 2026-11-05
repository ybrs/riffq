
import psycopg
import unittest
from helpers import launch_server_process, stop_server


def _run_server(port: int):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    def handle_auth(conn_id, user, password, host, *, callback, database=None):
        callback(user == "user" and password == "secret")

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_authentication(handle_auth)
    server.start()


class AuthenticationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55438
        cls.proc = launch_server_process(_run_server, cls.port)

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def test_auth_reject(self):
        with self.assertRaises(psycopg.OperationalError):
            psycopg.connect(f"postgresql://user:wrong@127.0.0.1:{self.port}/db")

    def test_auth_accept(self):
        conn = psycopg.connect(f"postgresql://user:secret@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone()[0], 1)
        conn.close()


if __name__ == "__main__":
    unittest.main()
