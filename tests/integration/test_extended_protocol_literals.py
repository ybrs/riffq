"""A prepared statement's SQL must reach the server as the client wrote it.

The extended-query path lowercased the whole statement before running it, which
rewrote every string LITERAL inside it as well as the keywords. The effect was
silent and data-shaped rather than an error:

    SELECT 'MiXeD CaSe'                 simple protocol -> 'MiXeD CaSe'
    SELECT 'MiXeD CaSe' ... $1          extended        -> 'mixed case'

Any predicate comparing against a literal with an upper-case letter therefore
matched nothing once a parameter was bound. That is what made Npgsql's
GetSchema("Tables") come back empty (task 189): its query filters
``table_type IN ('BASE TABLE', ...)`` and binds the schema as a parameter, so
'BASE TABLE' became 'base table' and matched no row.

Everything else about the query worked, which is why it took so long to find -
the value stored was right, the parameter bound fine, and selecting the column
returned 'BASE TABLE'. Only the comparison failed.

These tests drive psycopg directly rather than a driver, because the defect is
in the protocol path and not in anything a particular driver does.
"""
import psycopg

from server_case import FixtureServerCase


class ExtendedProtocolLiteralsTest(FixtureServerCase):
    """String literals survive a parameterised statement unchanged."""

    PORT = 55549

    def connect(self):
        """Open a psycopg connection to this class's fixture server."""
        return psycopg.connect(
            f"host=127.0.0.1 port={self.PORT} dbname=memory "
            f"user=u password=p sslmode=disable"
        )

    def test_a_literal_keeps_its_case_without_a_parameter(self):
        """The simple-query path was always correct; this pins that it stays so."""
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT 'MiXeD CaSe'")
            self.assertEqual(cur.fetchone()[0], "MiXeD CaSe")

    def test_a_literal_keeps_its_case_with_a_parameter(self):
        """Binding a parameter switches psycopg to the extended protocol.

        This is the regression itself: the same literal came back lower-cased.
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT 'MiXeD CaSe' WHERE %s = %s", ("x", "x"))
            self.assertEqual(cur.fetchone()[0], "MiXeD CaSe")

    def test_an_upper_case_predicate_still_matches_with_a_parameter(self):
        """The shape Npgsql sends: an upper-case literal compared to a column.

        Asserted through the catalog rather than a scalar, because the catalog
        is where the mixed-case values live that clients filter on.
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE' AND %s = %s",
                ("x", "x"),
            )
            with_parameter = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE'"
            )
            without_parameter = cur.fetchone()[0]

        self.assertGreater(
            without_parameter,
            0,
            "the fixture catalog should report some BASE TABLE relations",
        )
        self.assertEqual(
            with_parameter,
            without_parameter,
            "binding an unrelated parameter must not change which rows match",
        )
