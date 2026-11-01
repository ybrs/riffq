"""Multi-statement simple-query batches run every statement, not just the first.

The PostgreSQL simple query protocol lets one Query message carry several
statements separated by semicolons, and the server runs each in turn, replying
with one result set per statement. Clients that build their own SQL depend on
it: Npgsql sends its whole startup type-loading batch that way, so a server
that stopped at the first statement could not be connected to at all.

psycopg uses the simple query protocol when a query has no parameters, so these
tests reach that path by passing none, and step through the results with
``nextset()``.
"""
import multiprocessing
import socket
import time
import unittest

import psycopg
from helpers import stop_server

import pyarrow as pa


def _run_server(port: int):
    """Serve a single-column result whose value echoes the number selected.

    Keeping the handler this simple makes each statement in a batch
    distinguishable by its value, so the tests can tell which statement
    produced which result set.
    """
    import riffq

    def handle_query(sql, callback, **kwargs):
        cleaned = sql.strip().lower().rstrip(";").strip()
        if cleaned.startswith("select ") and cleaned[7:].isdigit():
            value = int(cleaned[7:])
        elif cleaned.startswith("select '"):
            # A quoted literal: return its length so a semicolon inside the
            # string is observable in the result.
            value = len(cleaned[8:].rstrip("'"))
        else:
            value = 0
        batch = pa.record_batch([pa.array([value], pa.int64())], names=["val"])
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        callback(reader.__arrow_c_stream__())

    server = riffq.Server(f"127.0.0.1:{port}")
    # Catalog emulation serves one context per registered database and refuses
    # any other, so the database these tests connect to has to be declared.
    server.register_database("db")
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class MultiStatementQueryTest(unittest.TestCase):
    """A batch of statements in one Query message yields one result per statement."""

    @classmethod
    def setUpClass(cls):
        """Start the echo server and wait for it to accept connections."""
        cls.port = 55446
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

    def connect(self):
        """Open a connection to the test server."""
        return psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")

    def collect_result_sets(self, cursor):
        """Return the first row of every result set the cursor holds.

        Args:
            cursor: A cursor that has just executed a batch.

        Returns:
            A list with one row per statement that returned rows.
        """
        rows = [cursor.fetchone()]
        while cursor.nextset():
            rows.append(cursor.fetchone())
        return rows

    def test_two_statements_both_run(self):
        """Both statements in a batch execute and each returns its own result.

        Before multi-statement support the server parsed only the first
        statement and failed on the second, so this is the regression guard for
        the whole feature.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1; SELECT 2")
                self.assertEqual(self.collect_result_sets(cur), [(1,), (2,)])

    def test_four_statements_run_in_order(self):
        """A longer batch runs every statement, in the order given."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1; SELECT 2; SELECT 3; SELECT 4")
                self.assertEqual(
                    self.collect_result_sets(cur), [(1,), (2,), (3,), (4,)]
                )

    def test_trailing_semicolon_does_not_add_a_statement(self):
        """A trailing semicolon does not produce an extra empty statement."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 7;")
                self.assertEqual(self.collect_result_sets(cur), [(7,)])

    def test_semicolon_inside_a_string_does_not_split(self):
        """A semicolon inside a string literal stays part of that statement.

        The handler returns the literal's length, so a batch split at the
        semicolon inside the string would report the wrong length or fail
        outright rather than quietly passing.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 'a;b'")
                self.assertEqual(self.collect_result_sets(cur), [(3,)])

    def test_single_statement_still_returns_one_result(self):
        """A batch of one behaves exactly as before this feature existed."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 5")
                self.assertEqual(cur.fetchone(), (5,))
                self.assertFalse(cur.nextset())


if __name__ == "__main__":
    unittest.main()
