"""Regression test: a client that connects and never sends a byte must not
block other connections.

detect_gssencmode() used to be awaited inline in the accept loop, so its
peek() on a silent socket wedged ALL accepts: the kernel kept completing TCP
handshakes into the listen backlog, but no connection was ever served (no
SSLRequest reply, CLOSE_WAIT pile-up) until the process was restarted. This
caused a multi-day production outage for a riffq user; see the PR that
introduced this test.
"""
import multiprocessing
import socket
import time

import psycopg
import unittest
from helpers import stop_server


def _run_server(port: int):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.start()


class SilentClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55447
        cls.proc = multiprocessing.Process(target=_run_server, args=(cls.port,), daemon=True)
        cls.proc.start()

        start = time.time()
        while time.time() - start < 10:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", cls.port)) == 0:
                    break
            time.sleep(0.1)
        else:
            stop_server(cls.proc)
            raise RuntimeError("server did not start")

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def _query_ok(self, timeout: int = 5) -> bool:
        with psycopg.connect(
            f"host=127.0.0.1 port={self.port} user=user password=secret dbname=db",
            connect_timeout=timeout,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")
                return cur.fetchone()[0] == 1

    def test_silent_connection_does_not_block_other_clients(self):
        # Baseline: the server answers a normal client.
        self.assertTrue(self._query_ok())

        # Open a connection and send NOTHING. Before the fix this parked the
        # accept loop inside detect_gssencmode()'s unbounded peek().
        silent = socket.create_connection(("127.0.0.1", self.port))
        try:
            time.sleep(0.5)
            # A normal client must still be served while the silent
            # connection is held open.
            self.assertTrue(self._query_ok())

            # And several times in a row, to be sure we aren't racing.
            for _ in range(3):
                self.assertTrue(self._query_ok())
        finally:
            silent.close()

        # Still healthy after the silent client goes away.
        self.assertTrue(self._query_ok())


if __name__ == "__main__":
    unittest.main()
