# Integrating pg_catalog

This document captures notes on how to integrate the `datafusion_pg_catalog` crate into `riffq`.


## Example from the library

The `pg_catalog` repository contains an `example` crate showing how to route queries between a custom handler and the built‑in catalog emulation. The important pieces are:

1. Build a `SessionContext` using `get_base_session_context`:

```rust
let (ctx, _log) = get_base_session_context(None, "datafusion".to_string(), "public".to_string()).await?;
```

2. Use `dispatch_query` to decide whether a statement touches `pg_catalog` or `information_schema`. Catalog queries are executed via DataFusion, while others are delegated to a closure provided by the caller:

```rust
let handler = |_ctx, query: &str, _params, _types| async move {
    // execute query elsewhere
};

let (batches, schema) = dispatch_query(&ctx, sql, None, None, handler).await?;
```

See `pg_catalog/example/src/main.rs` for the complete flow.


## Attempted integration

`riffq` was updated to fetch the latest `datafusion_pg_catalog` and a `SessionContext` is created on server start. Query execution was routed through `dispatch_query` so that statements referencing `pg_catalog` are handled internally. Python query results were converted to and from Arrow `RecordBatch` values for compatibility.

However, catalog queries still returned the fallback value from the Python handler. Manual tests showed `SELECT datname FROM pg_catalog.pg_database` returning `1` instead of the expected string, indicating the router did not recognise the catalog tables in the context. Due to time limits this issue could not be resolved.

## Fixing compilation

After the initial attempt the project no longer compiled with `maturin build`.
The errors were mainly caused by missing dependencies and API changes in
`arrow`.  `datafusion` was not listed in `Cargo.toml` and the
`datafusion_pg_catalog` crate was imported under the wrong name.  Furthermore
`arrow` now expects `StringBuilder::new()` without a capacity and
`append_value/append_null` no longer return a `Result`.

Changes applied:

* Added `datafusion = "47.0.0"` and renamed the pg catalog dependency to
  `datafusion_pg_catalog`.
* Updated `rows_to_record_batch` to use `StringBuilder::new()` and removed
  `.unwrap()` on `append_value`/`append_null`.
* Implemented `run_via_router` for `MyExtendedQueryHandler` mirroring the logic
  in `RiffqProcessor`.
* Adjusted closures passed to `dispatch_query` to own the SQL string in order to
  satisfy lifetime requirements.

With these tweaks `maturin build` now finishes successfully and produces the
extension wheel.  Running the Python test-suite still fails as the server does
not start correctly, but compilation issues are resolved.
