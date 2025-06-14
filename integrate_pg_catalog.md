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
