# Changelog

## Unreleased

### Lazy catalog path

The biggest change in this release is the lazy (callback-driven) catalog,
for hosts whose objects change at runtime. Instead of snapshotting
declarations at startup, the host supplies one source object and riffq pulls
`pg_catalog` / `information_schema` rows from it on every scan - a table
created after startup is visible immediately, with nothing cached.

- Four required methods, one per catalog level, each receiving a `callback`
  and invoking it with row dicts: `databases(callback)` -> `pg_database`,
  `schemas(database, callback)` -> `pg_namespace`,
  `relations(database, schema, callback)` -> `pg_class`,
  `columns(database, schema, relation, callback)` -> `pg_attribute`.
- Two optional methods override the server variables: `config(callback)`
  reports `pg_catalog.pg_config` rows, `settings(callback)` reports
  `pg_catalog.pg_settings` rows; a row whose name matches a built-in
  replaces it.
- Composable with the `register_database` / `register_schema` /
  `register_table` declarations: both configured means their union, with
  database or relation names reported by both refused loudly and shared
  schema names merging to the declared side (how live tables mount into a
  declared database's `public`).
- Each database gets its own catalog, built on first connect; connecting
  under an unregistered name is refused with `FATAL 3D000`, and a database
  the source starts reporting later is reachable without a restart. A server
  with catalog emulation and no databases at all refuses to start.
- The source owns its OIDs, written through verbatim: stable across calls,
  unique among its objects, above the built-in range (`16384`). Declared
  objects get exactly that from riffq's persistent registry
  (`start(oid_registry_path=...)`); lazy sources derive their own (teleduck
  does it over DuckDB's information schema, with a probe-on-collision
  fallback).
- Reference implementations: `example/lazy_catalog.py` (runnable, backed by
  an in-memory dict with a toy `CREATE TABLE` to watch the catalog update
  live) and teleduck (production embedding over a live DuckDB connection).
  Full contract in `docs/catalog.md`.

### Handshake metadata: `metadata` on connect, `get_client_metadata` later

A client announces itself in the startup handshake (`user`, `database`,
`application_name`, `client_encoding`, ...). riffq now snapshots that
handshake per connection and surfaces it two ways:

- The `on_connect` handler receives it as a `metadata` dict.
- `server.get_client_metadata(connection_id)` answers the same map for a
  live connection, so a query handler can ask who is asking without
  stashing anything itself.

Keys and values are verbatim what the client sent. This is a connect-time
snapshot, not live session state: a later `SET` updates the connection's
session, not this map. Unknown ids (never connected, refused, or already
disconnected and pruned) raise `KeyError`. Disconnects prune the snapshot,
so the store stays proportional to the live connection count.

Heads up for existing `on_connect` handlers: the new `metadata` keyword
argument breaks handlers with an exact signature (same as when
`server_name` was added). Accept `**kwargs` or add `metadata=None`.

### Live session settings: `get_client_settings`

`server.get_client_settings(connection_id)` answers the session's live
variables (`application_name`, `datestyle`, `search_path`, `timezone`,
`session_user`) - a `SET` the client sent shows up here, unlike
`get_client_metadata`, which stays the connect-time snapshot. Unknown or
disconnected ids raise `KeyError`; a live connection with no session (a
server without catalog emulation builds none) raises `RuntimeError`.
The Python `RiffqServer` forwards it, so `BaseConnection` subclasses reach
it through their server instance.

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

`datafusion_pg_catalog` is now consumed from git pin
(`rev = "b39f8bc995ab7d88146e20fdca3fd37b6f80c912"`). 
Note for direct Rust users of that crate: its eager
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
