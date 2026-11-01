"""JDBC catalog-metadata tests, asserting on the Java harness's JSON dump.

RiffqMetadataHarness opens one pgjdbc connection and prints the results of the
DatabaseMetaData calls a JDBC application relies on. This module runs it once and
asserts every result against fixture_dataset. Each test's docstring names the
pg_catalog feature its driver query depends on, so a regression in the catalog
emulation fails a test that says why.

Every gap this suite once guarded is fixed, so no expectedFailure tests remain;
see the "Known gaps" section of tests/integration/README.md. Should a future
driver version surface a new one, guard it with an expectedFailure naming its
cause -- unittest reports an expectedFailure that starts passing as an
unexpected success, the signal to promote it back to a plain assertion.
"""
import unittest

import fixture_dataset
from jdbc import run_harness
from server_case import FixtureServerCase
from toolchain import require_jdbc, selected_pgjdbc_version


@require_jdbc()
class JdbcMetadataTest(FixtureServerCase):
    """DatabaseMetaData results from pgjdbc, working assertions and known gaps."""

    PORT = 55551

    @classmethod
    def setUpClass(cls):
        """Start the fixture server and run the metadata harness once."""
        super().setUpClass()
        cls.meta = run_harness("RiffqMetadataHarness", cls.PORT)

    def test_product_and_driver_identity(self):
        """The driver reports PostgreSQL, the pinned server version, and pgjdbc."""
        self.assertEqual(self.meta["productName"], "PostgreSQL")
        self.assertEqual(self.meta["productVersion"], "17.0")
        self.assertTrue(self.meta["driverVersion"])

    def test_running_driver_is_the_selected_version(self):
        """The connected driver is the pgjdbc release this run selected.

        Without this a matrix run would pass identically whether or not the jar
        swap took effect, reporting coverage of four releases while exercising
        one. pgjdbc reports its version as "42.2.14" or "42.2.14 (build ...)",
        so match on the prefix.
        """
        self.assertTrue(
            self.meta["driverVersion"].startswith(selected_pgjdbc_version()),
            f"selected pgjdbc {selected_pgjdbc_version()} but the driver "
            f"reports {self.meta['driverVersion']}",
        )

    def test_catalogs_include_the_database(self):
        """getCatalogs lists the fixture database."""
        self.assertIn(fixture_dataset.DATABASE_NAME, self.meta["catalogs"])

    def test_schemas_include_public(self):
        """getSchemas lists the fixture schema."""
        self.assertIn(fixture_dataset.SCHEMA_NAME, self.meta["schemas"])

    def test_columns_match_dataset(self):
        """getColumns matches the dataset's names, order, JDBC types, nullability."""
        for table, columns in fixture_dataset.TABLE_COLUMNS.items():
            reported = self.meta["columns"][table]
            self.assertEqual(
                [c["name"] for c in reported], [c.name for c in columns], table
            )
            self.assertEqual(
                [c["ordinal"] for c in reported],
                list(range(1, len(columns) + 1)),
                table,
            )
            self.assertEqual(
                [c["dataType"] for c in reported],
                [c.jdbc_type for c in columns],
                table,
            )
            self.assertEqual(
                [c["nullable"] for c in reported],
                [c.jdbc_nullable for c in columns],
                table,
            )

    def test_row_data_matches(self):
        """SELECT per table returns the fixed rows, including NULLs and booleans."""
        customers = self.meta["rows"]["customers"]
        self.assertEqual(len(customers), 3)
        self.assertEqual([row[0] for row in customers], ["1", "2", "3"])
        self.assertEqual([row[1] for row in customers], ["Ada", "Bo", "Cy"])
        self.assertIsNone(customers[1][2])  # Bo's email is NULL

        products = self.meta["rows"]["products"]
        self.assertEqual([row[3] for row in products], ["true", "false", "true"])
        self.assertIsNone(products[2][2])  # SKU-3 price is NULL

    def test_tables_list_is_returned(self):
        """getTables lists exactly the three fixture tables.

        pgjdbc's table query casts the literal 'pg_class' to regclass; pg_catalog
        resolves that to the relation's OID so the comparison against an oid
        column plans and executes.
        """
        names = sorted(table["name"] for table in self.meta["tables"])
        self.assertEqual(names, sorted(fixture_dataset.TABLE_NAMES))

    def test_primary_keys_are_empty_not_error(self):
        """getPrimaryKeys returns empty (the fixture has no primary keys).

        Its query both aliases a set-returning function (_pg_expandarray(indkey)
        AS keys) and accesses its fields inline ((_pg_expandarray(indkey)).n);
        pg_catalog's SRF-to-unnest rewrite now routes both through one unnested
        column. The harness records -1 for a failed call and 0 for an empty one.
        """
        self.assertEqual(
            [self.meta["primaryKeys"][table] for table in fixture_dataset.TABLE_NAMES],
            [0, 0, 0],
        )

    def test_imported_keys_are_empty_not_error(self):
        """getImportedKeys returns empty (the fixture has no foreign keys).

        Its query uses pg_catalog.generate_series as a table function, which
        pg_catalog now strips to the registered unqualified name.
        """
        self.assertEqual(
            [self.meta["importedKeys"][table] for table in fixture_dataset.TABLE_NAMES],
            [0, 0, 0],
        )

    def test_index_info_is_empty_not_error(self):
        """getIndexInfo returns empty (the fixture has no indexes).

        Its query calls the three-argument pg_get_indexdef(oid, column, pretty),
        for which pg_catalog now has a matching signature.
        """
        self.assertEqual(
            [self.meta["indexInfo"][table] for table in fixture_dataset.TABLE_NAMES],
            [0, 0, 0],
        )

    def test_type_info_is_non_empty(self):
        """getTypeInfo returns the server's type catalogue.

        Its query filters with a correlated boolean scalar subquery (rewritten to
        an EXISTS/count DataFusion can plan) and uses array_upper (mapped to
        array_length); both are now handled.
        """
        self.assertGreater(len(self.meta["typeInfo"]), 0)


if __name__ == "__main__":
    unittest.main()
