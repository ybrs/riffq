"""The dataset under test and its expected metadata, as a single source of truth.

Both the ODBC and the JDBC test suites assert against the values defined here, so
there is exactly one place that says what the tables, columns, types, and rows
are. The pg_type OID for each column is derived with teleduck's shipped
``duckdb_type_to_oid`` (the same mapping the lazy catalog uses at run time), not
a hand-copied table, so the expectations cannot drift from what the server
actually reports.

The three tables are deliberately small and their rows are fixed literals -- no
generated data and no value derived from the current time -- so every assertion
compares against a constant. Nullable and non-nullable columns are both present
so column-nullability metadata is genuinely exercised, and the column types
(INTEGER, VARCHAR, DOUBLE, BOOLEAN, TIMESTAMP) cover the pg_type OIDs the ODBC and
JDBC drivers special-case.
"""
import datetime

from teleduck.server import duckdb_type_to_oid

# The DuckDB in-memory database the fixture server opens reports this name via
# duckdb_databases(); clients connect with it so catalog-scoped driver queries
# (which filter on the current database) line up with what the catalog returns.
DATABASE_NAME = "memory"

# DuckDB's default schema is "main"; the fixture creates the tables in a "public"
# schema instead so the server presents the schema every PostgreSQL client
# defaults to, and the driver metadata calls that filter on "public" resolve.
SCHEMA_NAME = "public"


# pg_type OID -> java.sql.Types constant, for the pg types the fixture uses. The
# JDBC driver reports these codes from getColumns / ResultSetMetaData, so this is
# the single place the expected JDBC type codes are defined.
#   4 INTEGER, 12 VARCHAR, 8 DOUBLE, 93 TIMESTAMP, -7 BIT (pgjdbc maps bool to BIT).
PG_OID_TO_JDBC_TYPE = {
    23: 4,    # int4  -> INTEGER
    25: 12,   # text  -> VARCHAR
    701: 8,   # float8 -> DOUBLE
    1114: 93,  # timestamp -> TIMESTAMP
    16: -7,   # bool  -> BIT
}

# JDBC DatabaseMetaData nullability codes.
JDBC_COLUMN_NO_NULLS = 0
JDBC_COLUMN_NULLABLE = 1


class Column:
    """One column of a fixture table and everything the tests assert about it.

    Attributes:
        name: The column name as it appears in the catalog and result sets.
        ddl_type: The DuckDB type used in the CREATE TABLE statement.
        nullable: Whether the column permits NULL (its catalog nullability).
    """

    def __init__(self, name, ddl_type, nullable):
        """Store the column's name, DDL type, and nullability."""
        self.name = name
        self.ddl_type = ddl_type
        self.nullable = nullable

    @property
    def pg_type_oid(self):
        """The pg_type OID the lazy catalog reports for this column's type."""
        return duckdb_type_to_oid(self.ddl_type)

    @property
    def jdbc_type(self):
        """The java.sql.Types code the JDBC driver reports for this column."""
        return PG_OID_TO_JDBC_TYPE[self.pg_type_oid]

    @property
    def jdbc_nullable(self):
        """The JDBC DatabaseMetaData nullability code for this column."""
        return JDBC_COLUMN_NULLABLE if self.nullable else JDBC_COLUMN_NO_NULLS


# Column layout per table, in ordinal order. Kept in a plain list so the tests
# can check ordinal positions directly.
TABLE_COLUMNS = {
    "customers": [
        Column("id", "INTEGER", nullable=False),
        Column("name", "VARCHAR", nullable=False),
        Column("email", "VARCHAR", nullable=True),
        Column("created_at", "TIMESTAMP", nullable=True),
    ],
    "orders": [
        Column("id", "INTEGER", nullable=False),
        Column("customer_id", "INTEGER", nullable=False),
        Column("amount", "DOUBLE", nullable=True),
        Column("status", "VARCHAR", nullable=True),
    ],
    "products": [
        Column("id", "INTEGER", nullable=False),
        Column("sku", "VARCHAR", nullable=False),
        Column("price", "DOUBLE", nullable=True),
        Column("in_stock", "BOOLEAN", nullable=True),
    ],
}

# Table names in a fixed order so "exactly these three tables" assertions and
# their ordering are stable.
TABLE_NAMES = ["customers", "orders", "products"]

# Fixed row data per table, ordered by the id column. Python-typed literals: the
# result-set tests assert that an INTEGER round-trips as int, DOUBLE as float,
# BOOLEAN as bool, TIMESTAMP as datetime, and a NULL as None.
TABLE_ROWS = {
    "customers": [
        (1, "Ada", "ada@example.com", datetime.datetime(2021, 1, 1, 9, 0, 0)),
        (2, "Bo", None, datetime.datetime(2021, 1, 2, 10, 30, 0)),
        (3, "Cy", "cy@example.com", datetime.datetime(2021, 1, 3, 11, 45, 0)),
    ],
    "orders": [
        (1, 1, 19.99, "shipped"),
        (2, 1, 5.50, "pending"),
        (3, 2, 100.0, None),
    ],
    "products": [
        (1, "SKU-1", 9.99, True),
        (2, "SKU-2", 19.50, False),
        (3, "SKU-3", None, True),
    ],
}


def create_table_statements():
    """Return the CREATE TABLE statements for the three fixture tables.

    Each column carries its NOT NULL constraint where the column is
    non-nullable, so the catalog nullability the drivers read back matches
    TABLE_COLUMNS.
    """
    statements = []
    for table in TABLE_NAMES:
        column_clauses = []
        for column in TABLE_COLUMNS[table]:
            null_clause = "" if column.nullable else " NOT NULL"
            column_clauses.append(f"{column.name} {column.ddl_type}{null_clause}")
        columns = ", ".join(column_clauses)
        statements.append(f"CREATE TABLE {SCHEMA_NAME}.{table} ({columns})")
    return statements


def insert_statements():
    """Return parameterless INSERT statements that load the fixed rows.

    The rows are formatted as SQL literals so the fixture can be loaded with a
    single execute per table and no client-side parameter binding, keeping the
    fixture setup independent of the code paths the tests are meant to exercise.
    """
    statements = []
    for table in TABLE_NAMES:
        rendered_rows = []
        for row in TABLE_ROWS[table]:
            rendered_rows.append("(" + ", ".join(_sql_literal(value) for value in row) + ")")
        values = ", ".join(rendered_rows)
        statements.append(f"INSERT INTO {SCHEMA_NAME}.{table} VALUES {values}")
    return statements


def _sql_literal(value):
    """Render one Python value as a DuckDB SQL literal for the fixture load."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, datetime.datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    return str(value)
