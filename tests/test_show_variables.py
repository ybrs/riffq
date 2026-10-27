"""SHOW answers riffq handles itself, without reaching the query handler.

Clients read session variables while connecting, before any user query runs, so
riffq answers a few SHOW statements directly. psqlodbc asks for the isolation
level this way as part of its connect sequence, and it spells it
``SHOW transaction_isolation`` -- one variable name -- rather than the
three-keyword ``SHOW TRANSACTION ISOLATION LEVEL``. Both spellings must answer,
and with the same value.

The query handler here returns a sentinel, so a SHOW that was wrongly delegated
to it fails visibly instead of coincidentally looking right.
"""
import multiprocessing
import socket
import time
import unittest

import psycopg
from helpers import stop_server

import pyarrow as pa

# What the handler answers with. Any SHOW that reaches it returns this, which
# no SHOW test expects.
DELEGATED_SENTINEL = "reached-the-query-handler"


def _run_server(port: int):
    """Serve a sentinel string for every query, so delegation is detectable."""
    import riffq

    def handle_query(sql, callback, **kwargs):
        batch = pa.record_batch(
            [pa.array([DELEGATED_SENTINEL], pa.string())], names=["val"]
        )
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        callback(reader.__arrow_c_stream__())

    server = riffq.Server(f"127.0.0.1:{port}")
    # Catalog emulation serves one context per registered database and refuses
    # any other, so the database these tests connect to has to be declared.
    server.register_database("db")
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class ShowVariablesTest(unittest.TestCase):
    """riffq answers the SHOW statements clients issue while connecting."""

    @classmethod
    def setUpClass(cls):
        """Start the sentinel server and wait for it to accept connections."""
        cls.port = 55447
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

    def show(self, statement):
        """Run one SHOW statement and return its single value."""
        with psycopg.connect(
            f"postgresql://user@127.0.0.1:{self.port}/db"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(statement)
                return cur.fetchone()[0]

    def test_transaction_isolation_variable_is_answered(self):
        """SHOW transaction_isolation reports the isolation level.

        This is the spelling psqlodbc uses while connecting. Without it the
        driver cannot open a connection at all.
        """
        self.assertEqual(self.show("SHOW transaction_isolation"), "read committed")

    def test_transaction_isolation_level_phrase_is_answered(self):
        """SHOW TRANSACTION ISOLATION LEVEL reports the isolation level."""
        self.assertEqual(
            self.show("SHOW TRANSACTION ISOLATION LEVEL"), "read committed"
        )

    def test_both_isolation_spellings_agree(self):
        """The two spellings report the same value.

        They are matched by separate code paths -- one is a variable name, the
        other three keywords -- so they could drift apart without this.
        """
        self.assertEqual(
            self.show("SHOW transaction_isolation"),
            self.show("SHOW TRANSACTION ISOLATION LEVEL"),
        )

    def test_server_version_is_answered_by_riffq(self):
        """SHOW server_version is answered by riffq, not the query handler."""
        self.assertNotEqual(self.show("SHOW server_version"), DELEGATED_SENTINEL)

    def test_session_user_is_answered_by_riffq(self):
        """SHOW session_user reports the authenticated user.

        The connections here authenticate as ``user``. This used to fall
        through to the query handler, so it could disagree with
        ``SELECT session_user`` on the same connection.
        """
        self.assertEqual(self.show("SHOW session_user"), "user")

    def test_session_authorization_agrees_with_session_user(self):
        """SHOW session_authorization is the GUC spelling of the same role."""
        self.assertEqual(
            self.show("SHOW session_authorization"),
            self.show("SHOW session_user"),
        )

    def test_show_all_lists_the_tracked_variables(self):
        """SHOW ALL answers with one row per tracked variable.

        psycopg and GUI tools issue SHOW ALL while connecting; it used to
        reach the query handler because the SHOW parser only accepted a
        single variable name.
        """
        with psycopg.connect(
            f"postgresql://user@127.0.0.1:{self.port}/db"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW ALL")
                rows = cur.fetchall()
        by_name = {row[0]: row[1] for row in rows}
        self.assertNotIn(DELEGATED_SENTINEL, by_name.values())
        self.assertEqual(by_name["session_user"], "user")
        self.assertIn("search_path", by_name)
        self.assertIn("server_version", by_name)
        self.assertIn("timezone", by_name)

    def test_unknown_variable_falls_through_to_the_query_handler(self):
        """A SHOW riffq does not answer is still passed to the handler.

        The special cases must not swallow every SHOW, or an application that
        implements its own would stop receiving them.
        """
        self.assertEqual(self.show("SHOW some_application_variable"), DELEGATED_SENTINEL)


if __name__ == "__main__":
    unittest.main()
