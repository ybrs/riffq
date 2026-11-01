"""Validate the fixture server with plain psycopg, before any driver is involved.

If the fixture itself is wrong -- tables missing from the catalog, wrong types,
wrong rows -- every ODBC and JDBC test would fail and it would be unclear whether
the driver or the fixture was at fault. This module pins the fixture with the
same psycopg path the existing riffq tests use, so a failure here means the
fixture is broken and a failure in the driver suites means the driver path is.

It uses no toolchain, so it runs even on a bare machine (psycopg is already a
test dependency).
"""
import datetime
import unittest

import psycopg

import fixture_dataset
from fixture_server import FIXTURE_PASSWORD, start_process
from riffq.testing import stop_server


class FixtureSmokeTest(unittest.TestCase):
    """Catalog visibility and data-path correctness over plain psycopg."""

    @classmethod
    def setUpClass(cls):
        """Start the fixture server and record where its query log went."""
        cls.port = 55531
        cls.log_path = "/tmp/riffq_integration_smoke.log"
        open(cls.log_path, "w", encoding="utf-8").close()
        cls.proc = start_process(cls.port, cls.log_path)

    @classmethod
    def tearDownClass(cls):
        """Stop the fixture server."""
        stop_server(cls.proc)

    def _connect(self):
        """Open an autocommit psycopg connection to the fixture server."""
        return psycopg.connect(
            f"postgresql://user:{FIXTURE_PASSWORD}@127.0.0.1:{self.port}"
            f"/{fixture_dataset.DATABASE_NAME}",
            autocommit=True,
        )

    def test_catalog_lists_the_three_tables(self):
        """pg_class reports exactly the three fixture tables in public."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT relname FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relkind = 'r' ORDER BY relname",
                (fixture_dataset.SCHEMA_NAME,),
            )
            self.assertEqual([r[0] for r in cur.fetchall()], fixture_dataset.TABLE_NAMES)

    def test_information_schema_columns_match_the_dataset(self):
        """information_schema.columns matches the dataset's names and order."""
        with self._connect() as conn, conn.cursor() as cur:
            for table, columns in fixture_dataset.TABLE_COLUMNS.items():
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s ORDER BY ordinal_position",
                    (table,),
                )
                self.assertEqual(
                    [r[0] for r in cur.fetchall()],
                    [column.name for column in columns],
                    table,
                )

    def test_select_returns_typed_rows(self):
        """A data-path SELECT returns the fixed rows with the expected Python types."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id, name, email, created_at FROM customers ORDER BY id")
            rows = cur.fetchall()
        self.assertEqual(rows, fixture_dataset.TABLE_ROWS["customers"])
        # NULL round-trips as None, and the types are what the drivers will read.
        self.assertIsNone(rows[1][2])
        self.assertIsInstance(rows[0][0], int)
        self.assertIsInstance(rows[0][3], datetime.datetime)

    def test_boolean_and_double_round_trip(self):
        """BOOLEAN stays bool and DOUBLE stays float through the wire."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT price, in_stock FROM products ORDER BY id")
            rows = cur.fetchall()
        self.assertIsInstance(rows[0][1], bool)
        self.assertIsInstance(rows[0][0], float)
        self.assertIsNone(rows[2][0])

    def test_parameterized_query_binds_value(self):
        """An extended-protocol parameter selects the matching row.

        This exercises the fixture's query_args binding on the data path, so a
        driver parameterized-query failure later is unambiguously the driver's.
        """
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT name FROM customers WHERE id = %s", (2,))
            self.assertEqual([r[0] for r in cur.fetchall()], ["Bo"])

    def test_bad_statement_raises_and_connection_survives(self):
        """A bad statement raises, and the connection still answers afterwards."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                with self.assertRaises(psycopg.Error):
                    cur.execute("SELECT * FROM table_that_does_not_exist")
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM customers")
                self.assertEqual(cur.fetchone()[0], len(fixture_dataset.TABLE_ROWS["customers"]))


if __name__ == "__main__":
    unittest.main()
