import sys
import unittest
from pathlib import Path

# Make `teleduck` importable regardless of how the suite is launched.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from teleduck.server import duckdb_type_to_oid


class DuckdbTypeToOidTest(unittest.TestCase):
    """`duckdb_type_to_oid` maps DuckDB's information_schema type strings to the
    right pg_type OID. The data_type strings below are exactly what DuckDB
    reports (e.g. 'TIMESTAMP WITH TIME ZONE', not 'TIMESTAMPTZ')."""

    def test_integer_family(self):
        self.assertEqual(duckdb_type_to_oid("INTEGER"), 23)
        self.assertEqual(duckdb_type_to_oid("BIGINT"), 20)   # contains 'INT'
        self.assertEqual(duckdb_type_to_oid("SMALLINT"), 21)
        self.assertEqual(duckdb_type_to_oid("TINYINT"), 21)
        self.assertEqual(duckdb_type_to_oid("HUGEINT"), 20)

    def test_interval_not_int(self):
        # Regression: "INTERVAL" contains "INT" and used to map to int4 (23).
        self.assertEqual(duckdb_type_to_oid("INTERVAL"), 1186)

    def test_timestamp_with_and_without_tz(self):
        # Regression: the DuckDB string has no "TZ" substring, so timestamptz
        # used to be misreported as timestamp (1114).
        self.assertEqual(duckdb_type_to_oid("TIMESTAMP"), 1114)
        self.assertEqual(duckdb_type_to_oid("TIMESTAMP WITH TIME ZONE"), 1184)

    def test_time_with_and_without_tz(self):
        self.assertEqual(duckdb_type_to_oid("TIME"), 1083)
        self.assertEqual(duckdb_type_to_oid("TIME WITH TIME ZONE"), 1266)

    def test_scalars_and_fallback(self):
        self.assertEqual(duckdb_type_to_oid("BOOLEAN"), 16)
        self.assertEqual(duckdb_type_to_oid("DOUBLE"), 701)
        self.assertEqual(duckdb_type_to_oid("FLOAT"), 700)
        self.assertEqual(duckdb_type_to_oid("DECIMAL(10,2)"), 1700)
        self.assertEqual(duckdb_type_to_oid("DATE"), 1082)
        self.assertEqual(duckdb_type_to_oid("VARCHAR"), 25)
        self.assertEqual(duckdb_type_to_oid("SOME_UNKNOWN_TYPE"), 25)  # fallback


if __name__ == "__main__":
    unittest.main()
