""".NET/Npgsql tier: the fixture as seen by a from-scratch protocol client.

Npgsql does not wrap libpq and does not use pgjdbc's metadata code, so this tier
covers a third client stack independently of the ODBC and JDBC ones. It is the
strictest protocol client the suite drives: it loads its type catalogue from
pg_type when the connection opens, and GetSchema issues its own catalog SQL.

The assertions come from fixture_dataset, the same source of truth the other two
tiers use, so all three agree on what the server should report rather than each
carrying its own copy.

The harness connects with Npgsql's default settings, so simply opening the
connection exercises its startup type loading: a multi-statement batch carrying
"SELECT version();" and its pg_type, composite and enum queries. That path
covers far more of the catalog than any single assertion here, and it was
broken in two separate ways before -- see tests/integration/README.md.
"""
import unittest

import fixture_dataset
from dotnet_harness import run_npgsql_harness
from server_case import FixtureServerCase
from toolchain import require_dotnet


@require_dotnet()
class NpgsqlTest(FixtureServerCase):
    """What Npgsql reads back from a fixture server."""

    PORT = 55561

    @classmethod
    def setUpClass(cls):
        """Start the fixture server and run the Npgsql harness once."""
        super().setUpClass()
        cls.meta = run_npgsql_harness(cls.PORT)

    def test_no_section_of_the_dump_failed(self):
        """Every harness section succeeded.

        The harness captures per-section failures instead of throwing, so
        without this a broken section would leave a null in the document and
        the other tests would fail with a confusing TypeError rather than
        naming the call that broke.
        """
        self.assertEqual(self.meta["errors"], {})

    def test_server_version_is_reported(self):
        """Npgsql parses the server version riffq reports at startup.

        Npgsql refuses to connect at all if it cannot parse this, so a failure
        here means the startup handshake, not the catalog.
        """
        self.assertTrue(self.meta["serverVersion"])

    def test_driver_is_npgsql(self):
        """The harness ran against the Npgsql version the project pins."""
        self.assertTrue(self.meta["npgsqlVersion"].startswith("9.0."))

    def test_tables_are_listed(self):
        """GetSchema("Tables") returns the three fixture tables.

        This was the tier's one expected failure. Npgsql's Tables query filters
        ``table_type IN ('BASE TABLE', 'FOREIGN', 'FOREIGN TABLE')`` and binds
        the schema as a parameter, and binding a parameter used to lower-case
        the whole statement - so 'BASE TABLE' became 'base table' and matched no
        row. Fixed in the extended-query path; see
        test_extended_protocol_literals.py for the mechanism.
        """
        listed = [t for t in self.meta["tables"] if t in fixture_dataset.TABLE_NAMES]
        self.assertEqual(sorted(listed), sorted(fixture_dataset.TABLE_NAMES))

    def test_columns_match_the_dataset_in_order(self):
        """GetSchema("Columns") reports each table's columns in ordinal order."""
        for table, columns in fixture_dataset.TABLE_COLUMNS.items():
            self.assertEqual(
                self.meta["columns"][table], [c.name for c in columns], table
            )

    def test_rows_match_the_dataset(self):
        """Every fixture row round-trips, including the NULLs."""
        customers = self.meta["rows"]["customers"]
        self.assertEqual(len(customers), 3)
        self.assertEqual([row[0] for row in customers], ["1", "2", "3"])
        self.assertEqual([row[1] for row in customers], ["Ada", "Bo", "Cy"])
        self.assertIsNone(customers[1][2])  # Bo's email is NULL

        products = self.meta["rows"]["products"]
        self.assertIsNone(products[2][2])  # SKU-3 price is NULL

    def test_column_clr_types_match_the_dataset(self):
        """Npgsql maps each pg_type OID the catalog reports to the right CLR type.

        This is the type-mapping equivalent of the JDBC tier's type-code test:
        it fails if the catalog reports a column as the wrong pg_type.
        """
        self.assertEqual(
            self.meta["columnTypes"], ["Int32", "String", "String", "DateTime"]
        )

    def test_parameterized_query_binds_over_the_extended_protocol(self):
        """A bound parameter selects the matching row, exercising Parse/Bind."""
        self.assertEqual(self.meta["parameterized"], ["Bo"])

    def test_default_startup_with_type_loading_connects(self):
        """Npgsql connects with its default settings, loading types from pg_type.

        Kept as its own test even though setUpClass already connects this way,
        because two distinct server bugs each broke exactly this and nothing
        else: riffq running only the first statement of a multi-statement
        simple-query batch, and the planner refusing a correlated scalar
        subquery inside a derived table. A failure here names the startup path
        directly instead of surfacing as every other test erroring in setup.
        """
        self.assertTrue(run_npgsql_harness(self.PORT)["serverVersion"])


if __name__ == "__main__":
    unittest.main()
