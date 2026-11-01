"""SET statements on session variables are riffq's to answer (task 27).

A client's ``SET search_path`` used to be forwarded to the query handler like
any other statement: the catalog's own session state never changed, and the
client got whatever the handler happened to reply -- for a bare embedding,
no command tag at all. riffq now answers ``SET`` itself for the variables the
catalog stores (application_name, datestyle, search_path, timezone,
extra_float_digits), applies them to the connection's session, and replies
with the ``SET`` command tag, the way PostgreSQL does.

A ``SET`` on any other variable still reaches the query handler, which may be
its real owner -- a DuckDB host owns ``memory_limit`` -- so the handler here
returns a sentinel that makes wrongly swallowed statements visible.

Every asserted behavior was measured against PostgreSQL 17.4 on 2026-08-11:
the ``SET`` tag, ``SET TIME ZONE`` as an alias for ``SET timezone``,
case-insensitive variable names, and ``SET LOCAL`` outside a transaction
changing nothing while still answering ``SET``.
"""
import multiprocessing
import socket
import time
import unittest

import psycopg
from helpers import stop_server

import pyarrow as pa

# What the query handler answers with. Any statement that reaches it returns
# this one-row result, which no riffq-answered SET can produce.
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


class SessionSetTest(unittest.TestCase):
    """riffq answers SET for the session variables the catalog stores."""

    @classmethod
    def setUpClass(cls):
        """Start the sentinel server and wait for it to accept connections."""
        cls.port = 55491
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
        """Open one connection; SET effects are asserted within it."""
        return psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/db")

    def test_set_answers_the_set_command_tag(self):
        """SET on a session variable replies with PostgreSQL's SET tag."""
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SET search_path = myschema")
            self.assertEqual(cur.statusmessage, "SET")

    def test_set_search_path_changes_what_show_reports(self):
        """The applied value reads back on the same connection.

        This is the half a forwarded SET silently lost: the tag came from the
        host while the catalog session kept the old value.
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SET search_path = myschema")
            cur.execute("SHOW search_path")
            self.assertEqual(cur.fetchone()[0], "myschema")

    def test_set_time_zone_is_an_alias_for_set_timezone(self):
        """SET TIME ZONE lands in the same variable SHOW timezone reads."""
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'Europe/Istanbul'")
            self.assertEqual(cur.statusmessage, "SET")
            cur.execute("SHOW timezone")
            self.assertEqual(cur.fetchone()[0], "Europe/Istanbul")

    def test_current_setting_reads_the_session_not_the_host(self):
        """current_setting() reports the catalog session's value.

        Before task 27 the call was forwarded to the host, whose notion of
        search_path (DuckDB's, for teleduck) had nothing to do with the
        session the client had been shaping with SET.
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SET search_path = reporting")
            cur.execute("SELECT current_setting('search_path')")
            self.assertEqual(cur.fetchone()[0], "reporting")

    def test_set_local_acknowledges_without_applying(self):
        """SET LOCAL answers SET but changes nothing outside a transaction.

        Measured: PostgreSQL warns 'SET LOCAL can only be used in transaction
        blocks' and leaves the value alone, still replying SET.
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SET search_path = kept")
            cur.execute("SET LOCAL search_path = discarded")
            self.assertEqual(cur.statusmessage, "SET")
            cur.execute("SHOW search_path")
            self.assertEqual(cur.fetchone()[0], "kept")

    def test_a_variable_the_session_does_not_own_reaches_the_handler(self):
        """SET on an unowned variable is still the host's to answer.

        The interception must not swallow every SET, or a host with session
        variables of its own (DuckDB's memory_limit) would stop receiving
        them.
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SET memory_limit = '2GB'")
            self.assertEqual(cur.fetchone()[0], DELEGATED_SENTINEL)

    def test_multi_value_search_path_set_applies_the_list(self):
        """SET search_path TO a, b stores the list, as PostgreSQL does.

        The catalog planner accepts a single SET value, so this used to be
        classified as riffq's to apply and then fail to plan - a regression
        over forwarding it to the host, which accepted it.
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SET search_path TO a, b")
            self.assertEqual(cur.statusmessage, "SET")
            cur.execute("SHOW search_path")
            self.assertEqual(cur.fetchone()[0], "a, b")

    def test_parameterized_set_applies_the_bound_value(self):
        """A SET prepared with a bound parameter applies that parameter.

        psycopg sends the value over the extended protocol as $1; the SET
        used to reach the planner with the placeholder unbound and fail.
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SET application_name = %s", ("prepared-app",))
            self.assertEqual(cur.statusmessage, "SET")
            cur.execute("SHOW application_name")
            self.assertEqual(cur.fetchone()[0], "prepared-app")

    def test_set_session_user_is_not_riffqs_to_apply(self):
        """SET session_user reaches the handler, never the session state.

        session_user records who authenticated; letting a client SET it
        would impersonate another role to every identity function.
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SET session_user = 'admin'")
            self.assertEqual(cur.fetchone()[0], DELEGATED_SENTINEL)
            cur.execute("SELECT session_user")
            self.assertEqual(cur.fetchone()[0], "user")

    def test_extra_float_digits_is_accepted(self):
        """Drivers set extra_float_digits while connecting; it must succeed."""
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SET extra_float_digits = 3")
            self.assertEqual(cur.statusmessage, "SET")


if __name__ == "__main__":
    unittest.main()
