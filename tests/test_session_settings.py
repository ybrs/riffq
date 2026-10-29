"""Live session settings: `SET` values read back through the server.

A client shapes its session with `SET` (`SET application_name = 'x'`);
riffq applies those to the connection's session without invoking the Python
query handler. `server.get_client_settings(connection_id)` exposes that live
session state to the host - unlike `get_client_metadata`, which is the
connect-time handshake snapshot and never changes.

The handler below answers sentinel statements by reading the calling
connection's own session, so every assertion runs over the wire against a real
connection. Unknown or disconnected ids must raise `KeyError`; a live
connection on a server without catalog emulation has no session and raises
`RuntimeError`.
"""
import multiprocessing
import socket
import time
import unittest

import psycopg
from helpers import stop_server

import pyarrow as pa


def _run_server(port: int):
    """Serve session-setting answers for sentinel statements."""
    import riffq

    def handle_query(sql, callback, **kwargs):
        connection_id = kwargs["connection_id"]
        if sql == "SELECT 'WHAT_IS_MY_APP_SETTING'":
            settings = server.get_client_settings(connection_id)
            value = settings["application_name"]
            batch = pa.record_batch(
                [pa.array([value], pa.string())], names=["val"]
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())
        elif sql == "SELECT 'WHAT_IS_MY_TIMEZONE'":
            settings = server.get_client_settings(connection_id)
            value = settings["timezone"]
            batch = pa.record_batch(
                [pa.array([value], pa.string())], names=["val"]
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())
        elif sql == "SELECT 'AM_I_KNOWN'":
            try:
                server.get_client_settings(connection_id)
            except KeyError:
                outcome = "UNKNOWN"
            else:
                outcome = "KNOWN"
            batch = pa.record_batch(
                [pa.array([outcome], pa.string())], names=["val"]
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())
        else:
            batch = pa.record_batch(
                [pa.array(["unreachable"], pa.string())], names=["val"]
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())

    server = riffq.Server(f"127.0.0.1:{port}")
    server.register_database("db")
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class SessionSettingsTest(unittest.TestCase):
    """`get_client_settings` reports the session the client shaped with SET."""

    @classmethod
    def setUpClass(cls):
        """Start the catalog-emulating server sessions live on."""
        cls.port = 55494
        cls.proc = multiprocessing.Process(
            target=_run_server, args=(cls.port,), daemon=True
        )
        cls.proc.start()
        start = time.time()
        while time.time() - start < 10:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", cls.port)) == 0:
                    break
            time.sleep(0.1)
        else:
            stop_server(cls.proc)
            raise RuntimeError("Server did not start")

    @classmethod
    def tearDownClass(cls):
        """Stop the server."""
        stop_server(cls.proc)

    def query_text(self, sql):
        """Run one sentinel statement and return its single text value."""
        with psycopg.connect(
            f"postgresql://user@127.0.0.1:{self.port}/db"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchone()[0]

    def test_set_values_read_back_live(self):
        """A `SET` the client sent is visible through the host-side reader.

        The handler never sees these statements - riffq applies them - so
        this is the only way the host learns the session's values.
        """
        with psycopg.connect(
            f"postgresql://user@127.0.0.1:{self.port}/db"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SET application_name = 'reporting'")
                cur.execute("SET TIME ZONE 'Europe/Istanbul'")
                cur.execute("SELECT 'WHAT_IS_MY_APP_SETTING'")
                self.assertEqual(cur.fetchone()[0], "reporting")
                cur.execute("SELECT 'WHAT_IS_MY_TIMEZONE'")
                self.assertEqual(cur.fetchone()[0], "Europe/Istanbul")

    def test_live_connection_is_known(self):
        """A connected id answers instead of raising."""
        self.assertEqual(self.query_text("SELECT 'AM_I_KNOWN'"), "KNOWN")


def _run_plain_server(port: int):
    """Serve without catalog emulation, so connections have no session."""
    import riffq

    def handle_query(sql, callback, **kwargs):
        if sql == "SELECT 'SETTINGS_OR_ERROR'":
            try:
                server.get_client_settings(kwargs["connection_id"])
            except RuntimeError:
                outcome = "RUNTIME_ERROR"
            except KeyError:
                outcome = "KEYERROR"
            else:
                outcome = "ANSWERED"
            batch = pa.record_batch(
                [pa.array([outcome], pa.string())], names=["val"]
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())
        else:
            batch = pa.record_batch(
                [pa.array(["unreachable"], pa.string())], names=["val"]
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.start()


class NoSessionTest(unittest.TestCase):
    """Without catalog emulation there is no session to report."""

    @classmethod
    def setUpClass(cls):
        """Start the plain server sessions never live on."""
        cls.port = 55495
        cls.proc = multiprocessing.Process(
            target=_run_plain_server, args=(cls.port,), daemon=True
        )
        cls.proc.start()
        start = time.time()
        while time.time() - start < 10:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", cls.port)) == 0:
                    break
            time.sleep(0.1)
        else:
            stop_server(cls.proc)
            raise RuntimeError("Server did not start")

    @classmethod
    def tearDownClass(cls):
        """Stop the plain server."""
        stop_server(cls.proc)

    def test_connection_without_session_reports_no_session(self):
        """A live id with no session is a `RuntimeError`, not a `KeyError`."""
        with psycopg.connect(
            f"postgresql://user@127.0.0.1:{self.port}/db"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 'SETTINGS_OR_ERROR'")
                self.assertEqual(cur.fetchone()[0], "RUNTIME_ERROR")


if __name__ == "__main__":
    unittest.main()
