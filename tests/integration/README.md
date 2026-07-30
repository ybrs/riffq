# Driver integration tests (pyodbc + JDBC)

These tests prove that riffq's PostgreSQL wire protocol and its `pg_catalog` /
`information_schema` emulation work under the client stacks beyond psycopg:

- ODBC: `pyodbc` on unixODBC and the official PostgreSQL ODBC driver
  (`psqlodbc`), both the Unicode and ANSI flavours.
- JDBC: the official pgjdbc driver, driven from small Java harnesses, plus an
  optional second JDBC tool (SQL Workbench/J).
- .NET: the Npgsql driver, driven from a small C# harness. Npgsql implements
  the wire protocol from scratch rather than wrapping libpq, so it is the
  strictest protocol client here and loads its type catalogue from `pg_type` on
  connect.
- DBeaver: CloudBeaver, DBeaver's metadata engine served over GraphQL instead
  of a desktop GUI. DBeaver's PostgreSQL plugin issues its own SQL against
  `pg_class`, `pg_namespace`, and `pg_attribute` rather than going through
  pgjdbc's `DatabaseMetaData`, so it covers a catalog surface no other tier
  reaches.
- psql: the PostgreSQL client itself, running its backslash commands. These are
  not a driver's metadata API at all -- they are handwritten catalog SQL, and
  they are the queries this project exists to answer. `\dt` reads `pg_class`
  and `pg_namespace`, `\l` reads `pg_database` through `array_length` and
  `array_to_string`, `\dT` reads `pg_type` through `format_type` and
  `obj_description`, `\dC` joins `pg_cast` to `pg_type` twice, and the verbose
  forms add `pg_table_size`, `pg_database_size` and `pg_size_pretty`.

These drivers issue their own catalog SQL on connect and on every metadata call
(`SQLTables`, `SQLColumns`, `DatabaseMetaData.getTables`, `getColumns`,
`getTypeInfo`, `::regclass` casts, and so on). The tests assert on the
driver-level metadata results, not on hand-written SQL, so a regression in the
catalog emulation fails a test even when a plain `SELECT` still works.

## Running

The drivers, driver manager, JDK, and jars are not committed; a setup script
builds and downloads them into a gitignored `.toolchain/` directory. From the
`riffq/` project root:

```
make integration-toolchain      # one-time: build/download the toolchain
make integration-test           # run the suite
```

Add the optional SQL Workbench/J client with:

```
make integration-toolchain ARGS=--with-jdbc-tool
```

### The pinned psql version

`\dt`, `\d+` and friends are rewritten between PostgreSQL major releases, so
the tier is tied to one client: `PSQL_VERSION` in `toolchain.py` and the
`POSTGRESQL_ARTIFACT` pin in `setup_toolchain.sh`, currently 17.4.
`test_psql_is_the_pinned_version` compares the two, so a toolchain holding some
other psql fails as a version mismatch rather than as changed catalog
behaviour. Re-pinning means changing both together.

Unlike the other tiers, a missing psql is not a skip: `require_psql` raises. A
green run that never exercised psql's catalog SQL would be worse than a loud
failure, so the tier fails until `setup_toolchain.sh` has been run.

### The pgjdbc version matrix

`make integration-test` runs the JDBC layer against one pinned pgjdbc release
(42.7.13). Clients in the field carry whatever version their vendor shipped --
Tableau ships 42.7.8, and older BI installations pin the long-lived 42.2 line --
so `make integration-matrix` runs that layer once per release:

```
make integration-matrix          # 42.7.13, 42.7.8, 42.2.29, 42.2.14
```

It runs every version before reporting, so one failure does not hide the rest,
and `make all-tests` includes it. To drive a single version by hand, set the
same variable the target uses:

```
RIFFQ_PGJDBC_VERSION=42.2.14 python -m unittest discover -s . -t . -k jdbc
```

The versions live in two places that must agree: `PGJDBC_MATRIX_VERSIONS` in
`toolchain.py` (which resolves each to its jar) and the same-named variable in
the Makefile (which drives the loop). `setup_toolchain.sh` downloads all of
them by default -- each is about 1 MB, so there is no flag to skip them.

Adding a version means adding its pinned `<name>|<url>|<sha256>` line to
`PGJDBC_MATRIX_ARTIFACTS` in `setup_toolchain.sh` and the version to both lists
above. `test_pgjdbc_matrix.py` then fails until the jar is actually present, so
a half-added version cannot silently skip.

Which driver a run really loaded is asserted, not assumed:
`test_running_driver_is_the_selected_version` compares the version pgjdbc
reports over the wire against the selected one, so a jar swap that quietly did
nothing fails instead of passing four identical runs.

`make test` does not run these tests: they live in a non-package subdirectory
that `unittest discover -s tests` does not recurse into, so a bare machine stays
green. `make all-tests` runs `integration-test` last; tests whose toolchain
piece is missing skip themselves with a message naming what to install.

The toolchain build takes a while the first time and is idempotent afterwards:
unixODBC and psqlodbc are built from source, and a JDK, the pgjdbc jars, the
.NET SDK (about 200 MB) and CloudBeaver (about 470 MB) are downloaded. Nothing
is fetched at test time, so a run needs the network only for this step.

Every artifact is pinned and verified. The tarballs and jars are pinned by
SHA-256; the .NET SDK's pin was taken from a download whose SHA-512 matched
Microsoft's published checksum; Npgsql is pinned by an exact version plus a
NuGet lock file restored in locked mode; and CloudBeaver is pinned by container
image manifest digest, with every layer checked against the digest the manifest
lists. A changed upstream artifact fails loudly rather than being used.

CloudBeaver needs no container runtime. Upstream ships no standalone server
archive, so `oci_pull.py` fetches the image's layers over plain HTTPS and
unpacks them; the image bundles the JRE the server runs on.

### Running one layer in isolation

With the toolchain built, run a single module (the ODBC runtime environment is
applied automatically via a re-exec in `toolchain.py`):

```
python -m unittest tests.integration.test_pyodbc_catalog
python -m unittest tests.integration.test_jdbc_metadata
```

Run these from the `tests/integration/` directory (so the fixture modules are
importable), or via `make integration-test` which sets the paths up.

## Layout

- `setup_toolchain.sh` -- builds/downloads the toolchain into `.toolchain/`.
- `toolchain.py` -- locators, skip decorators, ODBC runtime environment.
- `fixture_dataset.py` -- the three tables and their expected metadata (single
  source of truth for both driver suites).
- `fixture_server.py` -- the riffq server under test, backed by an in-memory
  DuckDB via teleduck's `DuckdbCatalogSource`, with a JSON-lines query log.
- `server_case.py` -- shared TestCase base that runs one fixture server per class.
- `jdbc.py` -- compiles and runs the Java harnesses.
- `test_pgjdbc_matrix.py` -- covers the pgjdbc version selection itself.
- `dotnet/` -- the Npgsql harness project (C#), with its NuGet lock file.
- `dotnet_harness.py` -- runs the built Npgsql harness.
- `test_npgsql.py` -- the .NET tier.
- `oci_pull.py` -- fetches and unpacks a container image without a runtime.
- `cloudbeaver/` -- the CloudBeaver configs seeded into a fresh workspace.
- `cloudbeaver_server.py`, `cloudbeaver_client.py` -- start CloudBeaver and
  drive its GraphQL API.
- `test_cloudbeaver.py` -- the DBeaver-metadata tier.
- `psql_client.py` -- runs psql and parses its records.
- `test_psql_backslash.py` -- the psql tier.
- `test_fixture_smoke.py` -- validates the fixture with plain psycopg.
- `test_pyodbc_catalog.py`, `test_pyodbc_queries.py` -- ODBC tests.
- `test_jdbc_metadata.py`, `test_jdbc_extended.py` -- JDBC tests.
- `test_jdbc_tool.py` -- optional SQL Workbench/J tier.
- `java/` -- the JDBC harness sources.

## Reading the query log

`fixture_server.py` appends every statement its data path receives to a per-port
JSON-lines file under the system temp directory
(`riffq_integration_<port>.log`), each line recording the SQL and whether it
errored. When a driver test fails, the failure message includes the tail of this
log so you can see the exact statement the driver sent. Note that `pg_catalog` /
`information_schema` queries are answered by riffq's catalog emulation and do not
reach this log; it captures the data path only.

## Known gaps

Every gap the ODBC and JDBC tiers found has been fixed: the ANSI driver's text
decoding (a client-side pyodbc `setdecoding` fix), `getTables`,
`getImportedKeys` / `getIndexInfo`, `getPrimaryKeys`, `getTypeInfo`,
`SQLPrimaryKeys` / `SQLStatistics`, and boolean/timestamp / ODBC parameter
binding.

The .NET and DBeaver tiers each surfaced gaps that are still open, guarded by
`expectedFailure` tests naming their cause:

- A `table_type` predicate plus a bound parameter matches nothing
  (`test_npgsql.py`). See below; this is the only gap the .NET tier still has.

The .NET tier found two further gaps that are now fixed, both of which stopped
Npgsql connecting at all rather than merely degrading metadata:

- Multi-statement simple-query batches. Npgsql's startup sends
  `SELECT version();` and its `pg_type` queries as one message and riffq ran
  only the first statement. Fixed in riffq; covered by
  `tests/test_multi_statement_query.py`.
- A correlated scalar subquery inside a derived table, which the planner
  refused with "Correlated scalar subquery must be aggregated to return at most
  one row". Fixed in pg_catalog; covered by
  `pg_catalog/tests/correlated_scalar_subquery_in_derived_table.rs`.

The tier now connects with Npgsql's default settings, so its startup type
loading runs against the catalog on every run.
- A `table_type` predicate plus a bound parameter matches nothing
  (`test_npgsql.py`). Npgsql's `GetSchema("Tables")` both filters on
  `information_schema.tables.table_type` and binds the schema as a parameter,
  and returns empty. Isolated in
  `claude-scripts/isolate_npgsql_get_tables_query.py`: either half alone works,
  and selecting `table_type` on the parameterized path returns the right value,
  so only the comparison fails.
The DBeaver tier's two gaps turned out to be a single bug and are fixed:
`pg_namespace` carried two rows for `public` (a built-in one at PostgreSQL's
canonical oid 2200 plus a generated one), and DBeaver bound the empty one when
listing a schema's relations. Covered by the namespace-shadowing tests in
`pg_catalog/tests/lazy_pg_catalog.rs`.

The psql tier found six gaps. Four are fixed and are now plain assertions:

- `pg_catalog.array_length` was unregistered, so `\l` could not read a
  database's access privileges. It is now an alias of DataFusion's own
  `array_length`.
- `obj_description` / `col_description` / `shobj_description` were missing, so
  every verbose describe command failed. They report no comment, which is what
  PostgreSQL returns for an object that carries none.
- `pg_size_pretty`, `pg_table_size` and `pg_database_size` were missing, so
  `\dt+` and `\l+` failed. The sizes report zero -- the relations are views
  over DuckDB, not heap files -- and `pg_size_pretty` formats a byte count
  exactly as PostgreSQL does.
- The `*_is_visible` family only covered tables, functions and types, so `\dc`
  and `\do` failed. Conversions, operators, operator classes and families,
  collations, statistics objects and the text-search objects are covered now.

Two more were fixed while chasing `\d` and `\d+`:

- The rewriter lifted a sub-query that still named an outer alias into a CTE
  that could not resolve it. It now recognises the situation and declines to
  lift, so the rewriter is never itself the thing emitting unresolvable SQL.
- A set-returning function's output column now answers to its table alias, the
  way PostgreSQL names it: `FROM unnest(c.reloptions) x` makes `x` the column
  as well as the relation.

Three remain, guarded by `expectedFailure` tests in `test_psql_backslash.py`:

- `\d+ <table>` and `\dp` share one cause: a correlated `ARRAY(SELECT ...)`
  returning many rows -- storage options in one, column privileges and policies
  in the other. The ARRAY rewrite leaves a scalar subquery, which the analyzer
  rejects for returning more than one row.
- `\d <table>`. Its publication lookup iterates the published columns with
  `generate_series(0, array_upper(pr.prattrs, 1))`. DataFusion's
  generate_series takes literal bounds only, and a table function yields one
  static provider, so no decorrelation reaches it -- the construct itself has
  to be rewritten.

`\dD` needs `pg_get_constraintdef` (the same gap the .NET tier reported), and
`\dx` casts the schema-qualified literal `'pg_catalog.pg_extension'` to
`regclass`, which resolves bare relation names only.

If a future driver or version surfaces a new gap, record it here and guard it
with an `expectedFailure` test (naming its cause) until it is fixed -- an
`expectedFailure` that starts passing is reported by unittest as an unexpected
success, the signal to promote it back to a plain assertion.
