# riffq

**riffq** is a Python-accessible toolkit (built in Rust) for building PostgreSQL wire-compatible databases.  
It allows you to serve data from Python over the PostgreSQL protocol — turning your Python-based logic or in-memory data into a queryable, network-exposed system.

---

## What It Does

- Implements the PostgreSQL wire protocol in Rust for performance and concurrency
- Sends raw SQL queries (Simple or Extended protocol) to Python for interpretation
- Implements postgres catalog compatibility layer (see [`pg_catalog_rs`](https://github.com/ybrs/pg_catalog)

Since you are in python, you can
- Allows you to connect to remote data sources (e.g., analytics DB, CRM) and expose them as a unified PostgreSQL database
- Enables serving Pandas DataFrames over the network as virtual SQL tables
- Can delegate SQL execution to DuckDB, Polars, or any other Python engine
- Acts as a programmable federated query engine or custom data service

---

## Example Use Cases

- Serve a Pandas DataFrame as a PostgreSQL table to BI tools
- Build a custom federated engine from multiple APIs or databases
- Implement your own data lake query frontend
- Expose dynamic ML feature stores for training or real-time inference
- Provide fine-grained, code-controlled access to internal metrics or logs

---

## Example

```python
# Python side
def handle_query(sql: str) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str]]]:
    df = duckdb.query(sql).to_df()
    columns = [(name, str(dtype)) for name, dtype in zip(df.columns, df.dtypes)]
    rows = df.to_dict(orient="records")
    return rows, columns
```

The Rust side calls this Python handler when a SQL query comes in via the PostgreSQL protocol.

---

## Architecture

- **Rust layer** handles:
  - PostgreSQL protocol (via [`pgwire`](https://crates.io/crates/pgwire))
  - Connection management
  - Query routing
  - Metadata compatibility (`pg_catalog` emulation)
- **Python layer** handles:
  - SQL execution (via any engine: DuckDB, Polars, etc.)
  - Data transformation
  - Custom logic and dynamic schema definitions

---

## Getting Started

```bash
pip install riffq
```

```python
import riffq

@riffq.query_handler
def handle_sql(sql: str):
    # Your query handling logic here
    ...
    
riffq.serve(port=5432)
```

Then connect using any PostgreSQL client:

```bash
psql -h localhost -p 5432
```

---

## Status

- ✅ Wire protocol support (simple + extended)
- ✅ Query dispatching to Python
- ✅ DuckDB, Pandas, Polars compatibility
- 🟡 Limited SQL parsing on Rust side (forwarded to Python)
- ❌ No authentication or TLS (yet)

---

## License

MIT or Apache 2.0 — your choice.

---

## Contributing

Contributions are welcome! Especially for:
- Better Python DX
- Support for pg_catalog views
- Example apps (data lake, feature store, etc.)

---

## Why "riffq"?

Because you're riffing on SQL. You don’t just store it — you remix it.

---
