# Release Notes

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
