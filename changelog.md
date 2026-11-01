# Changelog

The canonical histories live at the repo root: `CHANGELOG.md` (current
development) and `RELEASE_NOTES.md` (past releases). What follows are the
highlights of the unreleased work; see the root files for the full text.

## Unreleased

### Lazy catalog path

The biggest change: hosts with live schemas serve the catalog from a source
object (`set_lazy_catalog`) instead of startup snapshots. Four required
methods (`databases` / `schemas` / `relations` / `columns`, callback-driven),
two optional ones (`config` / `settings` overriding `pg_config` /
`pg_settings`), merged with any `register_*` declarations, one catalog per
database built on first connect, unknown databases refused with `FATAL
3D000`. Sources own their OIDs (stable, unique, above `16384`); declared
objects persist theirs via `start(oid_registry_path=...)`. See
`example/lazy_catalog.py`, teleduck, and the Catalog Emulation page.

### Handshake metadata

Each connection's startup handshake (`user`, `database`,
`application_name`, `client_encoding`, ...) is snapshotted at admit time.
The `on_connect` handler receives it as a `metadata` dict, and any later
query handler can ask for it back with
`server.get_client_metadata(connection_id)`. Unknown or disconnected ids
raise `KeyError`; disconnects prune the snapshot.

### Live session settings

`server.get_client_settings(connection_id)` answers the session's live
variables (`application_name`, `search_path`, `timezone`, ...) - a `SET` the
client sent shows up here, unlike the metadata snapshot, which never changes
after connect. A live connection with no session (no catalog emulation)
raises `RuntimeError`.

### Dependencies

`datafusion` 55, `arrow` 59, `pgwire` 0.42 (`parse_sql` now returns
`Option<Statement>`), with `sqlparser` held at 0.62 and `pyo3` at 0.28.
`datafusion_pg_catalog` is consumed as a git pin instead of a local path.
