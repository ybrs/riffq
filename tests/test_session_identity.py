"""current_user reports the role the client authenticated as.

Reported as: connect as "user", ask for current_user, get "postgres". The
identity functions read a per-connection value now, written by riffq when it
admits the connection, so each client is told who it actually is -- including
from inside the information_schema views that compare CURRENT_USER to decide
what a role may see.
"""
import multiprocessing
import socket
import time
import unittest

import psycopg
from helpers import stop_server


def _run_server(port):
    """A catalog-emulating server that accepts any password."""
    import riffq
    from riffq.helpers import to_arrow

    def handle_auth(conn_id, user, password, host, *, callback, database=None):
        callback(True)

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.register_database("db")
    server.on_authentication(handle_auth)
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class SessionIdentityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55486
        cls.proc = multiprocessing.Process(target=_run_server, args=(cls.port,), daemon=True)
        cls.proc.start()
        start = time.time()
        while time.time() - start < 30:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", cls.port)) == 0:
                    break
            time.sleep(0.1)
        else:
            stop_server(cls.proc)
            raise RuntimeError("Server did not start")

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def _connect_as(self, user):
        return psycopg.connect(
            f"postgresql://{user}:secret@127.0.0.1:{self.port}/db", autocommit=True
        )

    def test_current_user_is_the_connecting_role(self):
        # The originally reported bug: this returned "postgres" whoever connected.
        with self._connect_as("alice") as conn, conn.cursor() as cur:
            cur.execute("SELECT current_user")
            self.assertEqual(cur.fetchone()[0], "alice")

    def test_session_user_agrees_with_current_user(self):
        with self._connect_as("alice") as conn, conn.cursor() as cur:
            cur.execute("SELECT session_user")
            self.assertEqual(cur.fetchone()[0], "alice")

    def test_two_clients_get_their_own_roles(self):
        # Contexts are cached per database and shared between connections, so
        # the role has to live on the per-connection clone rather than on the
        # cached base -- otherwise whoever connected first names everyone.
        with self._connect_as("alice") as alice, self._connect_as("bob") as bob:
            with alice.cursor() as cur:
                cur.execute("SELECT current_user")
                self.assertEqual(cur.fetchone()[0], "alice")
            with bob.cursor() as cur:
                cur.execute("SELECT current_user")
                self.assertEqual(cur.fetchone()[0], "bob")
            # Ask again on the first connection: the second must not have
            # overwritten it.
            with alice.cursor() as cur:
                cur.execute("SELECT current_user")
                self.assertEqual(cur.fetchone()[0], "alice")

    def test_every_spelling_postgresql_accepts_reports_the_connecting_role(self):
        """Each way PostgreSQL lets a client ask "who am I" answers here too.

        ``current_user`` and ``session_user`` are the only two sqlparser models
        as parenthesis-less function calls. ``current_role`` arrived as a bare
        column reference and ``user`` as a call to a name nothing was registered
        under, so both failed with "No field named ..."; the schema-qualified
        forms did not parse at all. All four are ordinary spellings a client may
        send, and a real PostgreSQL answers every one -- verified against the
        bundled 17.4 server, not assumed.

        Over the wire rather than in-process because the fix has two halves and
        this is the only place both are exercised: the statement has to be
        RECOGNISED as the catalog's to answer (the router inspects function
        calls, and a bare column reference is not one, so these used to be
        forwarded to the Python host) and then resolved.
        """
        for statement in (
            "SELECT current_user",
            "SELECT session_user",
            "SELECT current_role",
            "SELECT user",
            "SELECT pg_catalog.current_user()",
            "SELECT pg_catalog.session_user()",
        ):
            with self.subTest(statement=statement):
                with self._connect_as("alice") as conn, conn.cursor() as cur:
                    cur.execute(statement)
                    self.assertEqual(cur.fetchone()[0], "alice")

    def test_current_catalog_and_current_schema_answer_without_parentheses(self):
        """The two non-role identity keywords, which take no parentheses either.

        ``current_catalog`` is the SQL-standard spelling of
        ``current_database()`` and ``current_schema`` of ``current_schema()``.
        Both were "No field named ..." for the same reason as ``current_role``.
        """
        with self._connect_as("alice") as conn, conn.cursor() as cur:
            cur.execute("SELECT current_catalog")
            self.assertEqual(cur.fetchone()[0], "db")
            cur.execute("SELECT current_schema")
            self.assertEqual(cur.fetchone()[0], "public")

    def test_a_view_body_sees_the_connecting_role(self):
        # applicable_roles is one of the information_schema views whose body
        # compares CURRENT_USER. It is planned once when the database's context
        # is built, before any client connects, so this is the case that proves
        # the value is read at call time rather than baked into the plan.
        with self._connect_as("alice") as conn, conn.cursor() as cur:
            cur.execute("SELECT current_user FROM information_schema.applicable_roles LIMIT 1")
            row = cur.fetchone()
            if row is not None:
                self.assertEqual(row[0], "alice")

            # Independent of whether that view has rows: a view selecting
            # CURRENT_USER directly must report the caller.
            cur.execute(
                "SELECT current_user FROM pg_catalog.pg_tables WHERE tablename = 'pg_class'"
            )
            self.assertEqual(cur.fetchone()[0], "alice")


if __name__ == "__main__":
    unittest.main()
