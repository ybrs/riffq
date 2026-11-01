"""JDBC extended-protocol tests, asserting on RiffqExtendedHarness's JSON output.

A separate harness from the metadata one so a failure here is clearly an
extended-protocol problem, not a catalog one. It covers typed PreparedStatement
binding, ResultSetMetaData, a server-side cursor (fetch size inside a
transaction), server-side named statements (reuse past pgjdbc's prepareThreshold),
and error recovery after a failed executeUpdate.

Two parameter types are gaps, recorded as expectedFailure and in
tests/integration/README.md: riffq's extended-protocol parameter decoder yields
NULL for boolean and timestamp bind values (int, string, double, and a NULL bind
all decode correctly), so a query filtering on a bound boolean or timestamp
matches nothing. The same decoder limitation is why ODBC parameter binding fails
wholesale (see test_pyodbc_queries).
"""
import unittest

import fixture_dataset
from jdbc import run_harness
from server_case import FixtureServerCase
from toolchain import require_jdbc


@require_jdbc()
class JdbcExtendedTest(FixtureServerCase):
    """pgjdbc extended-protocol behaviour: working features and parameter gaps."""

    PORT = 55552

    @classmethod
    def setUpClass(cls):
        """Start the fixture server and run the extended harness once."""
        super().setUpClass()
        cls.result = run_harness("RiffqExtendedHarness", cls.PORT)

    def test_prepared_integer_parameter(self):
        """setInt binds a matching value through the extended protocol."""
        self.assertEqual(self.result["preparedInt"]["rows"], ["Bo"])

    def test_prepared_string_parameter(self):
        """setString binds a matching value."""
        self.assertEqual(self.result["preparedString"]["rows"], ["1"])

    def test_prepared_double_parameter(self):
        """setDouble binds a matching value."""
        self.assertEqual(self.result["preparedDouble"]["rows"], ["SKU-1"])

    def test_prepared_null_bind(self):
        """A NULL bind is honoured: WHERE ? IS NULL matches every row."""
        self.assertEqual(self.result["preparedNull"]["rows"], ["1", "2", "3"])

    def test_result_set_metadata(self):
        """ResultSetMetaData reports the selected columns and their JDBC types."""
        columns = self.result["resultSetMetaData"]
        self.assertEqual(
            [c["name"] for c in columns], ["id", "name", "email", "created_at"]
        )
        expected_types = [
            column.jdbc_type for column in fixture_dataset.TABLE_COLUMNS["customers"]
        ]
        self.assertEqual([c["type"] for c in columns], expected_types)

    def test_fetch_size_uses_cursor(self):
        """A fetch size inside a transaction (server-side cursor) returns all rows in order."""
        self.assertEqual(self.result["fetchSizeRows"], [1, 2, 3])

    def test_statement_reuse_past_prepare_threshold(self):
        """Reusing one PreparedStatement past prepareThreshold keeps returning correct rows.

        pgjdbc switches to a server-side named statement after five executions;
        the loop runs seven times, so this covers both the pre- and post-switch
        paths.
        """
        self.assertEqual(
            self.result["statementReuse"],
            ["Ada", "Bo", "Cy", "Ada", "Bo", "Cy", "Ada"],
        )

    def test_execute_update_error_is_clean(self):
        """A failed executeUpdate raises and leaves the connection usable."""
        recovery = self.result["executeUpdateError"]
        self.assertTrue(recovery["threw"])
        self.assertTrue(recovery["recovered"])

    def test_prepared_boolean_parameter(self):
        """A bound boolean selects the matching row (the one out-of-stock product).

        pgjdbc sends this parameter untyped; riffq decodes it as text and the
        backend coerces it to boolean.
        """
        self.assertEqual(self.result["preparedBoolean"]["rows"], ["SKU-2"])

    def test_prepared_timestamp_parameter(self):
        """A bound timestamp selects the matching row."""
        self.assertEqual(self.result["preparedTimestamp"]["rows"], ["Ada"])


if __name__ == "__main__":
    unittest.main()
