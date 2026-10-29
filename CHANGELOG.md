# Changelog

## Unreleased

### Session `SET` is answered by riffq

`SET` on a session variable the catalog stores (`application_name`,
`datestyle`, `search_path`, `timezone`, `extra_float_digits`) is now applied
to the connection's session and answered with the `SET` command tag, the way
`PostgreSQL` answers it. Previously the statement was forwarded to the host,
so the tag came from the host while the catalog session kept the old value
(`SHOW search_path` disagreeing with the `SET` just issued).

Details:

- Multi-value form works: `SET search_path TO a, b` stores the list.
- A prepared `SET` works: drivers that send the value as a bound parameter
  (`SET application_name = $1`) have the value applied.
- `SET LOCAL` on an owned variable answers `SET` but changes nothing, matching
  `PostgreSQL` outside a transaction.
- `SET` on any other variable still reaches the host (a DuckDB host owns
  `memory_limit`), and a server without catalog emulation still forwards every
  `SET`.

### Session identity and `SHOW` additions

- `current_user`, `session_user` and `current_role` report the authenticated
  connection user, including from inside catalog view bodies.
- `SHOW` now answers `timezone`, `session_user`, `session_authorization`,
  `server_version` and `transaction_isolation` from the connection's session,
  so `SHOW session_user` agrees with `SELECT session_user`.
- `SHOW ALL` answers one `name` / `setting` / `description` row per tracked
  variable instead of reaching the host.
- `SET session_user` is never applied: it is delegated to the host like any
  other unowned variable, so a client cannot rewrite its own role. Over the
  wire this changes nothing (it was forwarded before too); it closes the hole
  only for in-process users of the underlying catalog crate.

### Python API unchanged

`register_database(name)`, `register_schema(database, schema)` and
`register_table(database, schema, table, columns)` keep their signatures and
semantics (duplicates raise `ValueError`, unknown databases are refused with
`FATAL 3D000`). Their backing moved from per-statement catalog writes to
declarations served through the lazy catalog path; no caller changes needed.

### Catalog source pinning

`datafusion_pg_catalog` is now consumed as a git pin
(`rev = "b39f8bc995ab7d88146e20fdca3fd37b6f80c912"`) instead of a local path
dependency. Bump the rev and re-resolve the lockfile to flow a new catalog
main into riffq. Note for direct Rust users of that crate: its eager
registration functions (`register_user_database`, `register_user_tables`,
`register_schema`, ...) were removed in favor of declarations
(`DatabaseDef`/`SchemaDef`/`RelationDef` over `DeclaredCatalogSource`, OIDs
from `CatalogOidRegistry`); `pg_catalog/src/main.rs::demo_catalog` is the
worked migration example. This does not affect riffq's Python API.

### Dependencies

- `datafusion` 54.0.0 -> 55.2.0, `arrow`/`arrow-schema` 58.x -> 59.3.2.
- `pgwire` 0.40 -> 0.42 (the extended-protocol parser trait now returns
  `Option<Statement>`; empty queries answer as empty statements).
- `sqlparser` held at 0.62 and `arrow` held below 60: `datafusion` 55 pins
  both, and newer majors would fork the shared types. `pyo3` held at 0.28.
- Everything else floated to its compatible latest via `cargo update`.
