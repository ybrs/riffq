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

### Enabling TLS

Generate a temporary certificate and key:

```bash
openssl req -newkey rsa:2048 -nodes -keyout server.key -x509 -days 1 -out server.crt -subj "/CN=localhost"
```

Start the server with TLS enabled:

```python
server = riffq.Server("127.0.0.1:5432")
server.set_tls("server.crt", "server.key")
server.start(tls=True)
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
- ✅ Optional TLS encryption

---

## Running Tests

Install the development requirements and run the test suite:

```bash
pip install -r requirements.txt
maturin build --profile=fast -i python3
pip install target/wheels/*.whl
python -m unittest discover -s tests
```

The tests require the Rust extension to build successfully; any build failure will cause the suite to fail.

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

## Development Notes

This repository contains tests that exercise the ability to return query results
as Arrow IPC streams from Python. The initial implementation in `src/lib.rs`
only accepted raw bytes without decoding them, resulting in empty result sets.

The code now invokes `pyarrow` to parse these IPC bytes back into ordinary
rows.  The parsing is performed inside the Rust callback while holding the
Python GIL so no additional Rust Arrow dependencies are required.

---
