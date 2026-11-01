"""ODBC data-path tests: result values, types, and parameter binding.

These drive ordinary SQL through the Unicode psqlodbc driver -- exact row values
with their Python types, the result-set description, parameterized queries (which
go through ODBC's own extended-protocol parameter binding), batched fetches, and
error recovery. They complement the catalog tests, which cover driver-issued
metadata SQL.

The ANSI driver is not used here: it cannot decode riffq's varchar results (see
the gap documented in test_pyodbc_catalog), and every query below returns a text
column.
"""
import datetime
import unittest

import fixture_dataset
from server_case import FixtureServerCase
from toolchain import ODBC_UNICODE_DRIVER, connect_odbc, require_odbc

try:
    import pyodbc
except ImportError:
    pyodbc = None


@require_odbc()
class PyodbcQueriesTest(FixtureServerCase):
    """Value fidelity, types, parameters, and error handling over ODBC."""

    PORT = 55542

    def _connect(self):
        """Open an autocommit Unicode-driver connection to the fixture server."""
        return connect_odbc(self.PORT, driver=ODBC_UNICODE_DRIVER)

    def test_select_returns_exact_rows_with_python_types(self):
        """A full SELECT returns the fixed rows, each value with its Python type."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, name, email, created_at FROM customers ORDER BY id")
            rows = [tuple(row) for row in cursor.fetchall()]
        finally:
            conn.close()
        self.assertEqual(rows, fixture_dataset.TABLE_ROWS["customers"])
        self.assertIsNone(rows[1][2])
        self.assertIsInstance(rows[0][0], int)
        self.assertIsInstance(rows[0][1], str)
        self.assertIsInstance(rows[0][3], datetime.datetime)

    def test_boolean_and_double_types(self):
        """BOOLEAN comes back as bool, DOUBLE as float, and a NULL double as None."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT price, in_stock FROM products ORDER BY id")
            rows = [tuple(row) for row in cursor.fetchall()]
        finally:
            conn.close()
        self.assertIsInstance(rows[0][1], bool)
        self.assertIsInstance(rows[0][0], float)
        self.assertIsNone(rows[2][0])

    def test_description_matches_column_names(self):
        """cursor.description reports the selected columns in order."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, name, email, created_at FROM customers")
            names = [column[0] for column in cursor.description]
        finally:
            conn.close()
        self.assertEqual(names, ["id", "name", "email", "created_at"])

    def test_parameterized_query_by_integer(self):
        """A ? parameter on an integer column selects the matching row.

        This drives psqlodbc's extended-protocol parameter binding: it prepares
        and describes the statement, then binds the value. riffq decodes the
        untyped parameter as text and the backend coerces it to the column type.
        """
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM customers WHERE id = ?", 2)
            self.assertEqual([tuple(r) for r in cursor.fetchall()], [("Bo",)])
        finally:
            conn.close()

    def test_parameterized_query_by_string(self):
        """A ? parameter on a text column selects the matching rows."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM orders WHERE status = ?", "shipped")
            self.assertEqual([tuple(r) for r in cursor.fetchall()], [(1,)])
        finally:
            conn.close()

    def test_fetchmany_then_fetchall(self):
        """fetchmany returns a bounded batch and fetchall returns the rest."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM customers ORDER BY id")
            first = [tuple(r) for r in cursor.fetchmany(2)]
            rest = [tuple(r) for r in cursor.fetchall()]
        finally:
            conn.close()
        self.assertEqual(first, [(1,), (2,)])
        self.assertEqual(rest, [(3,)])

    def test_bad_statement_raises_and_connection_survives(self):
        """A bad statement raises pyodbc.Error, and the connection stays usable."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            with self.assertRaises(pyodbc.Error):
                cursor.execute("SELECT * FROM table_that_does_not_exist")
            # The same connection still answers a valid query afterwards.
            recovery = conn.cursor()
            recovery.execute("SELECT count(*) FROM customers")
            self.assertEqual(recovery.fetchone()[0], len(fixture_dataset.TABLE_ROWS["customers"]))
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
