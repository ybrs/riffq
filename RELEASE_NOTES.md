# Release Notes

## release-0.4.0

### Lazy (callback-driven) catalog

The host can now supply one source object and riffq pulls `pg_catalog` /
`information_schema` rows from it on every scan, so tables created after
startup show up immediately with nothing cached: `databases(callback)`,
`schemas(database, callback)`, `relations(database, schema, callback)` and
`columns(database, schema, relation, callback)`, plus optional
`config(callback)` / `settings(callback)` overrides for `pg_config` and
`pg_settings`. It composes with the `register_*` declarations (both
configured means their union, with doubly-reported databases and relations
refused loudly). The source owns its OIDs (stable, unique, above 16384);
declared objects get theirs from a persistent registry via
`start(oid_registry_path=...)`. See `docs/catalog.md` and
`example/lazy_catalog.py`; teleduck uses this path against live DuckDB.

### Handshake metadata and live session settings

`on_connect` handlers receive the client's startup handshake as a `metadata`
dict, and `server.get_client_metadata(connection_id)` answers the same
snapshot later (unknown ids raise `KeyError`). `SET` on a session variable
the catalog owns is now applied to the connection's session and answered
with the `SET` tag, and `server.get_client_settings(connection_id)` reads
the live variables back. `SHOW` additionally answers `timezone`,
`session_user`, `session_authorization`, `server_version` and
`transaction_isolation`, and `current_user` / `session_user` /
`current_role` report the authenticated user.

Heads up for existing `on_connect` handlers: the new `metadata` keyword
argument breaks handlers with an exact signature. Accept `**kwargs` or add
`metadata=None`.

### Dependencies

- `datafusion` 54.0.0 -> 55.2.0, `arrow` / `arrow-schema` 58.x -> 59.3.2,
  `pgwire` 0.40 -> 0.42.
- `datafusion_pg_catalog` is consumed from a pinned git rev instead of a
  version; its eager registration functions were removed in favor of
  declarations (`DatabaseDef` / `SchemaDef` / `RelationDef`). This does not
affect riffq's Python API.

## release-0.3.0

### Breaking: no hashing in riffq - `riffq.helpers.stable_oid` is removed

Deriving an OID from a name is not the catalog's job, and a 31-bit hash space
collides silently at scale. Declared objects now get their OIDs from a
persistent registry instead (below); a lazy source that derives its own
identifiers owns that derivation itself, including uniqueness (teleduck does
this over DuckDB's information schema, with a probe-on-collision fallback).

### OIDs come from a registry; pass `oid_registry_path` to persist them

`start(catalog_emulation=True, oid_registry_path="catalog-oids.redb")` records
every declared object's OID in a redb file (pure Rust, ACID). The first
registration of a name allocates the next OID; every later run - after a
restart, after reordering declarations - gets the recorded value back, so
clients that cache OIDs keep resolving. Allocation is one cluster-wide counter,
as PostgreSQL's own is: two databases' objects no longer share numbers.

Without `oid_registry_path`, an in-memory registry stands in and `start()`
emits a `DeprecationWarning`: OIDs are then stable only while declaration
order does not change between runs. This fallback will become an error.

### Breaking: duplicate registrations raise instead of being skipped

`register_database`, `register_schema` and `register_table` now raise
`ValueError` ("... already exists") for a name already registered, as
PostgreSQL refuses CREATE for an existing name. Previously a duplicate was
silently skipped.

### Breaking: declared+lazy name collisions are errors, not shadowing

With both `register_*` declarations and `set_lazy_catalog` configured, a
database or relation name reported by BOTH is refused at scan time, naming the
object - previously the declared side silently won. A shared SCHEMA name still
merges; that is how a live source mounts tables into a declared database's
`public`.


## release-0.2.0

### Breaking: a registered database is the only connectable database

With `catalog_emulation=True`, a client connecting under a database name the
host never registered is now refused with `FATAL 3D000 database "..." does not
exist`, the way PostgreSQL refuses it. Previously any name connected and was
quietly served some other database's catalog.

The fix is one line - connect under the name you registered:

```python
server.register_database("mydb")          # already there
# psql postgresql://user@host:5433/mydb   <- use this name
```

A server started with `catalog_emulation=True` and no databases registered at
all now raises from `start()` instead of binding a port and refusing every
client. Register one, or install a lazy source that reports one.

**Servers started without `catalog_emulation` are unaffected**: riffq holds no
catalog there, the host answers metadata queries itself, and any database name
still connects.

Most existing code already registers a database and connects under some other
name without noticing, because the old fallback hid it - worth checking the
connection strings rather than assuming.

### One catalog context per database

Each database now gets its own catalog, built in full the first time a client
connects to it rather than at startup. Consequences:

- A connection sees only its own database's schemas, tables and columns.
  `pg_database` still lists every database from any of them.
- `information_schema` reports the connected database in `table_catalog`, and it
  agrees with `current_database()`. On the lazy path both used to disagree, with
  48 views reporting the internal name `datafusion`.
- A database that appears after the server started - one an ATTACH or a lazy
  source turns up later - is connectable without a restart.
- The server binds its port immediately. The per-database build cost (roughly a
  second) is paid by the first connection to each database instead of by
  startup, and only for databases something actually connects to.

## release-0.1.10

The `release-0.1.8` and `release-0.1.9` tags never reached PyPI - both were built
while `Cargo.toml` still read `version = "0.1.7"`, so the publish step skipped the
wheels as already existing. This release carries everything since 0.1.7, including
what those notes described.

### Reliability

- A client that connected and never sent a byte no longer wedges the accept loop.
  GSSAPI encoding detection now runs per connection instead of inline, so one silent
  client can no longer stop the server from accepting any other.
- `accept()` errors (file descriptor exhaustion, aborted connections) are logged and
  retried instead of killing the accept task, which used to leave the process alive
  with a dead port until it was restarted.
- TLS keys in PKCS#1 and SEC1 form are accepted, not only PKCS#8.
- Arrow `Utf8View` string columns are encoded rather than sent as NULL.

### PostgreSQL compatibility

- Power BI can read the type list: catalog columns typed `regproc` (`pg_type.typreceive`,
  `pg_am.amhandler`, ...) hold a function name and now resolve to an OID where a query
  compares them against one, which also fixes `amhandler::oid` and the JDBC driver's
  array probe.
- `ORDER BY <name>` binds to the query's output column, as PostgreSQL does, instead of
  failing as ambiguous on catalog joins.
- Catalog objects written in upper case (`FROM PG_CLASS`) resolve, since PostgreSQL
  folds unquoted identifiers; quoted names stay case sensitive.
- Lazy catalog integration, with `pg_config` / `pg_settings` overrides.
- TLS SNI reaches `handle_connect`, so callbacks can route per host.

### API

- The shutdown callback is now `handle_shutdown`; the old `on_shutdown` name is gone.

### Dependencies

- `datafusion` 54, `pgwire` 0.40, and a `pyo3` upgrade.

### Documentation

- MkDocs site, GitHub Pages workflow, and guides for catalog emulation and using Redis
  as a database; Python type annotations and docstrings throughout the toolkit.

## release-0.1.9 (never published)

- Added TLS SNI propagation so `handle_connect` callbacks receive the negotiated server name, enabling per-host routing.
- Upgraded query engine dependencies (`datafusion` 50.2.0 and `pgwire` 0.34) 
- Introduced MkDocs-based documentation scaffolding, GitHub Pages workflow, and new guides covering catalog emulation and Redis as a database example.
- Expanded Python API annotations, docstrings, and supporting helpers to make the connection toolkit clearer to extend.
