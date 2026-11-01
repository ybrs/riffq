"""CloudBeaver tier: the fixture as seen by DBeaver's metadata engine.

DBeaver's PostgreSQL plugin does not read metadata through pgjdbc's
DatabaseMetaData the way the JDBC tier does. It issues its own SQL against
pg_class, pg_namespace, pg_attribute and friends, so this tier covers a catalog
query surface none of the other tiers reach. CloudBeaver is that same engine
served over GraphQL instead of a desktop GUI, which is what makes it drivable
here.

Each navigator expansion below is one of those catalog queries: listing
databases, schemas, tables, and a table's columns each run DBeaver's own SQL
against riffq.

This tier has no open gaps. Both it found -- a duplicated "public" schema and a
Tables folder that enumerated empty -- turned out to be one bug in
pg_namespace and are fixed; the tests that guarded them are now plain
assertions.

This tier is slower than the others -- it boots a Jetty server, an embedded H2
database, and the DBeaver platform before the first assertion -- so it runs one
server for the whole class.
"""
import os
import tempfile
import unittest

import fixture_dataset
from cloudbeaver_client import CloudBeaverClient
from cloudbeaver_server import CONNECTION_ID, start_server, stop_server
from server_case import FixtureServerCase
from toolchain import require_cloudbeaver


@require_cloudbeaver()
class CloudBeaverMetadataTest(FixtureServerCase):
    """What DBeaver's PostgreSQL model reads back from a fixture server."""

    # Must match nothing else in the suite: CloudBeaver's seeded connection
    # points at this port.
    PORT = 55571

    # CloudBeaver's own HTTP/GraphQL port.
    WEB_PORT = 8979

    @classmethod
    def setUpClass(cls):
        """Start the fixture server, then CloudBeaver, then connect it."""
        super().setUpClass()
        cls.cloudbeaver_log = os.path.join(
            tempfile.gettempdir(), f"cloudbeaver_{cls.WEB_PORT}.log"
        )
        cls.cloudbeaver = start_server(cls.WEB_PORT, cls.PORT, cls.cloudbeaver_log)
        try:
            cls.client = CloudBeaverClient(cls.WEB_PORT)
            cls.client.open_session()
            cls.connection = cls.client.connect(CONNECTION_ID)
        except Exception:
            stop_server(cls.cloudbeaver)
            super().tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        """Stop CloudBeaver, then the fixture server."""
        stop_server(cls.cloudbeaver)
        super().tearDownClass()

    def test_connection_is_established(self):
        """CloudBeaver connects its pre-seeded data source to riffq.

        Connecting alone runs DBeaver's server-introspection queries, so a
        failure here is the catalog refusing the connection handshake rather
        than any single metadata call.
        """
        self.assertTrue(self.connection["connected"])

    def test_database_is_listed(self):
        """The navigator lists the fixture database."""
        path = f"database://{CONNECTION_ID}/" + (
            "org.jkiss.dbeaver.ext.postgresql.model.PostgreDatabase"
        )
        self.assertIn(fixture_dataset.DATABASE_NAME, self.client.child_names(path))

    def test_public_schema_is_listed(self):
        """The navigator lists the fixture schema."""
        path = self.client.database_path(CONNECTION_ID, fixture_dataset.DATABASE_NAME)
        names = self.client.child_names(
            f"{path}/org.jkiss.dbeaver.ext.postgresql.model.PostgreSchema"
        )
        self.assertIn(fixture_dataset.SCHEMA_NAME, names)

    def test_schema_list_has_no_duplicates(self):
        """Each schema is listed once.

        pg_namespace used to carry two rows for public -- a built-in one at
        PostgreSQL's canonical oid 2200 and a generated one for the source
        schema -- because built-in rows were shadowed by oid, which a generated
        oid can never match. They are now shadowed by name.
        """
        path = self.client.database_path(CONNECTION_ID, fixture_dataset.DATABASE_NAME)
        names = self.client.child_names(
            f"{path}/org.jkiss.dbeaver.ext.postgresql.model.PostgreSchema"
        )
        self.assertEqual(len(names), len(set(names)), names)

    def test_tables_folder_lists_the_fixture_tables(self):
        """Expanding Tables lists the three fixture tables.

        This enumerated empty while pg_namespace held two public rows: DBeaver
        lists a schema's relations by the schema's oid, and the duplicate it
        bound to owned nothing. The per-table nodes kept working throughout
        because they resolve by name, which is what made the symptom look like
        a broken table query rather than a broken schema list.
        """
        path = self.client.tables_path(
            CONNECTION_ID, fixture_dataset.DATABASE_NAME, fixture_dataset.SCHEMA_NAME
        )
        listed = [
            name
            for name in self.client.child_names(path)
            if name in fixture_dataset.TABLE_NAMES
        ]
        self.assertEqual(sorted(listed), sorted(fixture_dataset.TABLE_NAMES))

    def test_columns_are_reported_for_each_table(self):
        """Every fixture table reports its columns, in order, to DBeaver.

        This is the tier's core assertion: the column list comes from DBeaver's
        own pg_attribute query, not from pgjdbc's getColumns, so it fails if
        riffq's pg_attribute or pg_class emulation drifts even while the JDBC
        tier still passes.
        """
        for table, columns in fixture_dataset.TABLE_COLUMNS.items():
            path = self.client.columns_path(
                CONNECTION_ID,
                fixture_dataset.DATABASE_NAME,
                fixture_dataset.SCHEMA_NAME,
                table,
            )
            self.assertEqual(
                self.client.child_names(path), [c.name for c in columns], table
            )


if __name__ == "__main__":
    unittest.main()
