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

``make psql-matrix`` runs this file once per release in PSQL_MATRIX_VERSIONS.
That matters because the rewrites are real: psql 18's ``\\d`` calls
pg_get_constraintdef and pg_partition_ancestors, neither of which 17.4's does,
and its ``\\dx`` joins pg_available_extensions() as a function where 17.4
selects from the relation. Every command these tests cover currently behaves
the same under both releases; when one does not, mark it expected-to-fail for
that release alone, since unittest reports a passing expectedFailure as an
unexpected success and an unconditional marker would turn the working release
red.

Commands that do not work yet are covered by ``expectedFailure`` tests naming
their cause rather than left out, so the day the catalog can answer them
unittest reports an unexpected success. See the "Known gaps" section of
README.md.
"""
import unittest

import fixture_dataset
import psql_client
from server_case import FixtureServerCase
from toolchain import PSQL_VERSION, selected_psql_version


def expected_failure_under_psql(*versions):
    """Mark a test as an expected failure only under the psql versions named.

    A plain ``expectedFailure`` says "this never works". Some commands work
    under one psql release and not another, because psql rewrote the query
    between them, and marking those unconditionally would hide the release
    where they DO work -- unittest reports a passing expected failure as an
    unexpected success, so the run would go red on the good version.

    Args:
        *versions: The versions the test is expected to fail under, as they
            appear in PSQL_MATRIX_VERSIONS.

    Returns:
        A decorator that marks the test expected-to-fail when this run's psql
        is one of `versions`, and leaves it a plain assertion otherwise.
    """

    def decorate(test):
        if selected_psql_version() in versions:
            return unittest.expectedFailure(test)
        return test

    return decorate


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

    def test_list_role_grants_runs(self):
        """\\dg+ ... \\drg renders a role's grant options with concat_ws.

        psql writes it schema-qualified as ``pg_catalog.concat_ws``, which
        DataFusion knew only under its bare name, so the command failed with
        "function pg_catalog.concat_ws() does not exist" while the function was
        registered all along. The fixture grants no roles to other roles, so
        the assertion is that the command completes.
        """
        self.run_command(r"\drg")

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
        """\\d <table> describes the table, columns and all, on both releases.

        The command that took the longest to get working, and it needed a
        different pair of fixes per release: 17.4 wanted a correlated
        string_agg grouped by its join key and psql's index-walk publication
        lookup rewritten, and 18.4 additionally wanted pg_get_constraintdef
        (task 245) and pg_partition_ancestors (task 274).

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

        Runs on both releases, for the same reasons the plain form does.
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

    def test_list_domains_runs(self):
        """\\dD renders a domain's CHECK constraints through pg_get_constraintdef.

        That function is a resolver slot rather than a deparser: with nothing
        installed it answers NULL, so the Check column comes back empty instead
        of the command failing. The fixture defines no domains, so the useful
        assertion is that the command completes -- the emptiness is the
        fixture's, not the catalog's.
        """
        self.run_command(r"\dD")

    def test_list_extensions_reports_plpgsql(self):
        """\\dx lists the snapshot's installed extension, on both releases.

        Two generations of fixes meet here. 17.4's query joins pg_description
        on ``'pg_catalog.pg_extension'::pg_catalog.regclass``, a
        schema-qualified regclass literal that used to be flattened to text
        (task 249). 18.4 additionally joins ``pg_available_extensions()`` as a
        FUNCTION - which is how PostgreSQL itself defines the view - where
        17.4 selects from the relation of that name; the function form now
        exists (task 269), so the last version-scoped expected failure in this
        file is gone.
        """
        rows = self.run_command(r"\dx")
        self.assertIn("plpgsql", [row[0] for row in rows])

    def test_list_extension_contents_names_each_member_object(self):
        """\\dx+ describes every object the extension owns, per release.

        The output is pg_describe_object over the extension's pg_depend rows
        (task 261), and the four strings are byte-for-byte what a real
        PostgreSQL 17.4 prints for plpgsql - measured, not assumed. Asserted
        as exact rows so a rendering drift (a lost argument list, a wrong
        relkind label) fails loudly rather than passing as "some rows came
        back".
        """
        rows = self.run_command(r"\dx+ plpgsql")
        self.assertEqual(
            [row[0] for row in rows],
            [
                "function plpgsql_call_handler()",
                "function plpgsql_inline_handler(internal)",
                "function plpgsql_validator(oid)",
                "language plpgsql",
            ],
        )

    def test_show_function_definition_prints_the_real_source(self):
        """\\sf prints the function's CREATE OR REPLACE statement (task 262).

        psql resolves the name through a regproc cast and then calls
        pg_get_functiondef(oid), which did not exist - so \\sf failed outright.
        The definition asserted here is the REAL one: the snapshot's pg_proc
        row carries prosrc and every flag captured from a live server, and
        these five lines are byte-for-byte what a real PostgreSQL 17.4 prints
        for \\sf now. Exact lines, not "some output": a drifted flag order or
        a lost argument list must fail loudly.
        """
        rows = self.run_command(r"\sf now")
        lines = [row[0] for row in rows] if rows and len(rows[0]) == 1 else rows
        self.assertEqual(
            "\n".join(line if isinstance(line, str) else line[0] for line in lines),
            "CREATE OR REPLACE FUNCTION pg_catalog.now()\n"
            " RETURNS timestamp with time zone\n"
            " LANGUAGE internal\n"
            " STABLE PARALLEL SAFE STRICT\n"
            "AS $function$now$function$",
        )

    def test_list_collations_runs(self):
        """\\dO filters on pg_char_to_encoding(getdatabaseencoding()).

        The name-to-id direction of the encoding table did not exist (task
        260), so the whole command failed before any collation could be
        considered. Zero data rows is the CORRECT answer - the bundled
        PostgreSQL 17.4 also lists none for a plain \\dO, since the snapshot's
        collations all live in pg_catalog - so the assertion is that the
        command completes.
        """
        self.run_command(r"\dO")

    def test_list_functions_filtered_by_return_type_runs(self):
        """\\dfn and \\dft filter on a regtype literal.

        Both write ``p.prorettype <> 'pg_catalog.trigger'::pg_catalog.regtype``,
        where prorettype holds an oid -- so the literal has to resolve to the
        type's oid rather than stay a string, or the comparison asks the planner
        to read 'pg_catalog.trigger' as an integer.
        """
        self.run_command(r"\dfn")
        self.run_command(r"\dft")

    def test_list_privileges_names_the_fixture_tables(self):
        """\\dp lists every relation, privileges and policies columns included.

        The command that needed the whole correlated-ARRAY chain. Both its
        column-privileges and policies columns are
        ``array_to_string(ARRAY(SELECT ... WHERE attrelid = c.oid), ...)`` --
        a correlated sub-query returning many rows, which the ARRAY rewrite
        used to leave as a scalar sub-query for the analyzer to reject (task
        248). The policies column then nests a second ARRAY inside the first
        and correlates it on ``oid = ANY (polroles)``, where ``polroles``
        belongs to the enclosing ``pg_policy`` and psql writes it WITHOUT a
        table qualifier -- invisible to the lifter until it could resolve which
        relation declares which column (task 257).
        """
        rows = self.run_command(r"\dp")
        listed = {row[RELATION_NAME] for row in rows}
        for table in fixture_dataset.TABLE_NAMES:
            self.assertIn(table, listed)
        for row in rows:
            if row[RELATION_NAME] in fixture_dataset.TABLE_NAMES:
                self.assertEqual(
                    row[RELATION_SCHEMA],
                    fixture_dataset.SCHEMA_NAME,
                    row[RELATION_NAME],
                )


if __name__ == "__main__":
    unittest.main()
