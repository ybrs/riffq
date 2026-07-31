"""The psql tier: every backslash command run against the fixture server.

psql's describe commands are not a driver's metadata API -- they are
handwritten catalog SQL, and they are the queries this project exists to
answer. ``\\dt`` reads pg_class and pg_namespace, ``\\l`` reads pg_database
through array_length and array_to_string, ``\\dT`` reads pg_type through
format_type and obj_description, ``\\du`` reads pg_roles, and the verbose forms
add pg_table_size, pg_database_size and pg_size_pretty on top. Running them and
comparing the rows is the most direct test of the emulation there is.

The assertions compare parsed records, not psql's aligned box output: column
widths and title lines move between releases, and asserting on them would test
psql's renderer rather than the catalog. The psql version is pinned for the
same reason the pgjdbc matrix pins its jars -- each major release rewrites some
of these queries -- and ``test_psql_is_the_pinned_version`` fails loudly if the
toolchain holds a different one.

Commands that do not work yet are covered by ``expectedFailure`` tests naming
their cause rather than left out, so the day the catalog can answer them
unittest reports an unexpected success. See the "Known gaps" section of
README.md.
"""
import unittest

import fixture_dataset
import psql_client
from server_case import FixtureServerCase
from toolchain import PSQL_VERSION

# Column positions in the records psql returns, one constant per command shape
# so the assertions below read as names rather than as indexes. psql prints
# these left to right in the order its describe query selects them.
RELATION_SCHEMA, RELATION_NAME, RELATION_TYPE = 0, 1, 2

# \dt+ keeps the four \dt columns and appends persistence, access method, size
# and description.
VERBOSE_RELATION_PERSISTENCE = 4
VERBOSE_RELATION_ACCESS_METHOD = 5
VERBOSE_RELATION_SIZE = 6

DATABASE_NAME_COLUMN = 0
DATABASE_ENCODING_COLUMN = 2

SCHEMA_NAME_COLUMN = 0
ROLE_NAME_COLUMN = 0


class PsqlBackslashTest(FixtureServerCase):
    """psql's describe commands answer from the fixture's catalog."""

    PORT = 55547

    def run_command(self, command):
        """Run one psql command against this class's fixture server.

        Args:
            command: The psql input, e.g. ``\\dt``.

        Returns:
            The parsed rows, as ``psql_client.run`` returns them.
        """
        return psql_client.run(self.PORT, command)

    def test_psql_is_the_pinned_version(self):
        """The toolchain's psql is the version these expectations were written for."""
        reported = psql_client.psql_version()
        self.assertEqual(
            reported,
            PSQL_VERSION,
            psql_client.describe_version_mismatch(reported),
        )

    def test_list_tables_reports_the_three_fixture_tables(self):
        """\\dt names the fixture's tables, in the public schema, typed table."""
        rows = self.run_command(r"\dt")
        self.assertEqual(
            sorted(row[RELATION_NAME] for row in rows),
            sorted(fixture_dataset.TABLE_NAMES),
        )
        for row in rows:
            self.assertEqual(row[RELATION_SCHEMA], fixture_dataset.SCHEMA_NAME)
            self.assertEqual(row[RELATION_TYPE], "table", row[RELATION_NAME])

    def test_list_relations_matches_list_tables(self):
        """\\d lists the same relations as \\dt: the fixture has only tables."""
        self.assertEqual(self.run_command(r"\d"), self.run_command(r"\dt"))

    def test_list_tables_verbose_reports_persistence_and_size(self):
        """\\dt+ adds persistence, access method and a formatted size per table.

        The size is what pg_size_pretty makes of pg_table_size, which the
        emulation reports as zero because the relations are views over DuckDB
        rather than heap files.
        """
        rows = self.run_command(r"\dt+")
        self.assertEqual(
            sorted(row[RELATION_NAME] for row in rows),
            sorted(fixture_dataset.TABLE_NAMES),
        )
        for row in rows:
            self.assertEqual(row[VERBOSE_RELATION_PERSISTENCE], "permanent")
            self.assertEqual(row[VERBOSE_RELATION_ACCESS_METHOD], "heap")
            self.assertEqual(row[VERBOSE_RELATION_SIZE], "0 bytes")

    def test_list_schemas_reports_the_fixture_schema(self):
        """\\dn lists the schema the fixture's tables live in."""
        rows = self.run_command(r"\dn")
        self.assertIn(
            fixture_dataset.SCHEMA_NAME,
            [row[SCHEMA_NAME_COLUMN] for row in rows],
        )

    def test_list_schemas_verbose_runs(self):
        """\\dn+ adds access privileges and description without failing."""
        rows = self.run_command(r"\dn+")
        self.assertIn(
            fixture_dataset.SCHEMA_NAME,
            [row[SCHEMA_NAME_COLUMN] for row in rows],
        )

    def test_list_roles_reports_a_role_with_attributes(self):
        """\\du reads pg_roles and reports at least the session's own role."""
        rows = self.run_command(r"\du")
        self.assertTrue(rows, "pg_roles reported no roles at all")
        for row in rows:
            self.assertTrue(row[ROLE_NAME_COLUMN], "a role with no name")

    def test_list_databases_reports_the_fixture_database(self):
        """\\l reads pg_database and names the database the fixture serves.

        This is the command that reaches array_length and array_to_string over
        an access-privileges array, which is why an ACL value spanning several
        lines has to survive the round trip intact.
        """
        rows = self.run_command(r"\l")
        names = [row[DATABASE_NAME_COLUMN] for row in rows]
        self.assertIn(fixture_dataset.DATABASE_NAME, names)
        for row in rows:
            self.assertEqual(row[DATABASE_ENCODING_COLUMN], "UTF8", row[0])

    def test_list_databases_verbose_reports_a_formatted_size(self):
        """\\l+ adds pg_size_pretty over pg_database_size for every database."""
        rows = self.run_command(r"\l+")
        self.assertIn(
            fixture_dataset.DATABASE_NAME,
            [row[DATABASE_NAME_COLUMN] for row in rows],
        )
        for row in rows:
            self.assertIn("0 bytes", row, f"no size column in {row}")

    def test_list_views_reports_none(self):
        """\\dv returns no rows: the fixture defines tables only."""
        self.assertEqual(self.run_command(r"\dv"), [])

    def test_list_indexes_reports_none(self):
        """\\di returns no rows: the fixture defines no indexes."""
        self.assertEqual(self.run_command(r"\di"), [])

    def test_list_sequences_reports_none(self):
        """\\ds returns no rows: the fixture defines no sequences."""
        self.assertEqual(self.run_command(r"\ds"), [])

    def test_list_materialized_views_reports_none(self):
        """\\dm returns no rows: the fixture defines no materialized views."""
        self.assertEqual(self.run_command(r"\dm"), [])

    def test_list_functions_runs(self):
        """\\df reads pg_proc and pg_namespace without error.

        No rows are expected: the emulated pg_proc holds the built-in functions,
        all of which live in pg_catalog, and \\df excludes that schema.
        """
        self.assertEqual(self.run_command(r"\df"), [])

    def test_list_types_runs(self):
        """\\dT reads pg_type through format_type and obj_description.

        No rows are expected for the same reason as \\df: every emulated type
        is a built-in, and \\dT excludes pg_catalog and information_schema.
        """
        self.assertEqual(self.run_command(r"\dT"), [])

    def test_list_languages_reports_plpgsql(self):
        """\\dL reads pg_language, which carries the procedural languages."""
        rows = self.run_command(r"\dL")
        self.assertIn("plpgsql", [row[0] for row in rows])

    def test_list_casts_reports_the_builtin_casts(self):
        """\\dC reads pg_cast joined to pg_type twice and pg_proc."""
        rows = self.run_command(r"\dC")
        self.assertTrue(rows, "pg_cast reported no casts at all")
        # Every cast row is source type, target type, function, implicitness.
        for row in rows:
            self.assertEqual(len(row), 4, row)

    def test_list_conversions_runs(self):
        """\\dc reads pg_conversion through pg_conversion_is_visible."""
        self.run_command(r"\dc")

    def test_list_operators_runs(self):
        """\\do reads pg_operator through pg_operator_is_visible."""
        self.run_command(r"\do")

    def test_list_event_triggers_runs(self):
        """\\dy renders an event trigger's tags from an array column.

        Its Tags column is ``array_to_string(array(select x from
        unnest(evttags) as t(x)), ', ')`` -- an ARRAY over a table function
        reading an outer column, written unqualified. That correlation has no
        physical plan, so this command worked only once the ARRAY-over-unnest
        rewrite turned the sub-query into an expression over the array itself.
        """
        self.run_command(r"\dy")

    def test_select_returns_the_fixture_rows(self):
        """A plain SELECT through psql returns the fixture's rows in order.

        The describe commands all go to the catalog emulation; this is the one
        assertion here that exercises the data path, so a tier that passed
        every backslash command over a broken server would still fail.
        """
        statement = (
            f"SELECT id, name FROM {fixture_dataset.SCHEMA_NAME}.customers ORDER BY id"
        )
        rows = self.run_command(statement)
        expected = [
            [str(row[0]), row[1]] for row in fixture_dataset.TABLE_ROWS["customers"]
        ]
        self.assertEqual(rows, expected)
        # Named rather than "nothing errored": the log is per fixture server and
        # test_a_rejected_statement_fails_the_run deliberately puts a failure in
        # it, so only this statement's own outcome is this test's business.
        self.assertNotIn(statement, self.errored_statements(), self.log_tail())

    def test_a_rejected_statement_fails_the_run(self):
        """psql exits non-zero when the server rejects a statement.

        Guards the tier itself: every assertion above trusts that a failed
        catalog query surfaces as a PsqlError rather than as an empty result.
        """
        with self.assertRaises(psql_client.PsqlError):
            self.run_command("SELECT * FROM public.no_such_table")

    def test_describe_table_reports_the_fixture_columns(self):
        """\\d <table> describes the table, columns and all.

        The command that took the longest to get working, and the one this
        tier was written for: it needed a correlated string_agg to be grouped
        by its join key, string_agg to answer to its pg_catalog name, and
        psql's ``generate_series(0, array_upper(prattrs, 1)) ... prattrs[s]``
        publication lookup to be rewritten as a membership test, because
        DataFusion's generate_series will not take a per-row bound.
        """
        rows = self.run_command(rf"\d {fixture_dataset.SCHEMA_NAME}.customers")
        self.assertEqual(
            [column.name for column in fixture_dataset.TABLE_COLUMNS["customers"]],
            [row[0] for row in rows],
            "\\d should list the fixture table's own columns, in ordinal order",
        )

    def test_describe_table_verbose_reports_storage(self):
        """\\d+ <table> adds the per-column storage the plain form omits.

        Asserting on a column \\d does not report keeps this from passing on a
        server that answered \\d+ with \\d's narrower query.
        """
        rows = self.run_command(rf"\d+ {fixture_dataset.SCHEMA_NAME}.customers")
        self.assertEqual(
            [column.name for column in fixture_dataset.TABLE_COLUMNS["customers"]],
            [row[0] for row in rows],
        )
        # Column, Type, Collation, Nullable, Default, Storage, ...
        self.assertTrue(
            all(row[5] for row in rows),
            f"the verbose form reports a storage mode per column: {rows}",
        )

    @unittest.expectedFailure
    def test_list_domains(self):
        """\\dD fails: pg_get_constraintdef is not implemented.

        The same gap the .NET tier reported; \\dD calls it to render a domain's
        CHECK constraints.
        """
        self.run_command(r"\dD")

    @unittest.expectedFailure
    def test_list_extensions(self):
        """\\dx fails on a schema-qualified regclass literal.

        It joins pg_description on
        ``'pg_catalog.pg_extension'::pg_catalog.regclass``. The regclass cast
        resolves a bare relation name to an oid, so the qualified spelling
        reaches simplify_expressions as a string being cast to int32 and the
        rule fails with "Cannot cast string 'pg_catalog.pg_extension'".
        """
        self.run_command(r"\dx")

    @unittest.expectedFailure
    def test_list_privileges(self):
        """\\dp fails: "Correlated scalar subquery must be aggregated".

        Its column-privileges and policies columns are both
        ``array_to_string(ARRAY(SELECT ... WHERE attrelid = c.oid), ...)``, a
        correlated subquery returning many rows. The ARRAY rewrite leaves it as
        a scalar subquery, which the analyzer then rejects for returning more
        than one row.
        """
        self.run_command(r"\dp")


if __name__ == "__main__":
    unittest.main()
