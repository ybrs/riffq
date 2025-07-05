# riffq

**riffq** is a toolkit in python (built in Rust) for building PostgreSQL wire-compatible databases.  
It allows you to serve data from Python over the PostgreSQL protocol — turning your Python-based logic or in-memory data into a queryable, network-exposed system. We also have a catalog emulation system in rust with datafusion.

---

## What It Does

- Implements the PostgreSQL wire protocol in Rust for performance and concurrency
- Sends raw SQL queries (Simple or Extended protocol) to Python for interpretation
- Implements postgres catalog compatibility layer, see [`pg_catalog_rs`](https://github.com/ybrs/pg_catalog)

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
import logging
import duckdb
import pyarrow as pa
import riffq
logging.basicConfig(level=logging.DEBUG)

class Connection(riffq.BaseConnection):
    def handle_auth(self, user, password, host, database=None, callback=callable):
        # simple username/password check
        callback(user == "user" and password == "secret")

    def handle_connect(self, ip, port, callback=callable):
        # allow every incoming connection
        callback(True)

    def handle_disconnect(self, ip, port, callback=callable):
        # invoked when client disconnects
        callback(True)

    def _handle_query(self, sql, callback, **kwargs):
        cur = duckdb_con.cursor()
        try:
            if sql.strip().lower() == "select err":
                # custom error returned to client
                callback(("ERROR", "42846", "bad type"), is_error=True)
                return
            reader = cur.execute(sql).fetch_record_batch()
            self.send_reader(reader, callback)
        except Exception as exc:
            logging.exception("error on executing query")
            batch = self.arrow_batch(
                [pa.array(["ERROR"]), pa.array([str(exc)])],
                ["error", "message"],
            )
            self.send_reader(batch, callback)

    def handle_query(self, sql, callback=callable, **kwargs):
        self.executor.submit(self._handle_query, sql, callback, **kwargs)

def main():
    global duckdb_con
    duckdb_con = duckdb.connect()
    duckdb_con.execute(
        """
        CREATE VIEW klines AS 
        SELECT * 
        FROM 'data/klines.parquet'
        """
    )
    server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
    server.set_tls("certs/server.crt", "certs/server.key")
    server.start(tls=True)

if __name__ == "__main__":
    main()
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

### Zero Copy

We try to achieve zero-copy by using arrow/pycapsule. So data from duckdb comes to python as a pycapsule pointer, which goes to thread in python which goes to the callback in rust still as a pycapsule pointer. We then stream to network with postgresql using pgwire.  

https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html

---

## Getting Started

### Install the package

```bash
pip install riffq --pre
```

### Extend BaseConnection class

You can extend the base class for 
- handle_query
- handle_auth - To check for username/password
- handle_connect - To check ip/port restriction
- handle_disconnect - WIP

```python
class Connection(riffq.BaseConnection):
    def handle_auth(self, user, password, host, database=None, callback=callable):
        # simple username/password check
        callback(user == "user" and password == "secret")

    def handle_connect(self, ip, port, callback=callable):
        # allow every incoming connection
        callback(True)

    def handle_disconnect(self, ip, port, callback=callable):
        # invoked when client disconnects
        callback(True)

    def _handle_query(self, sql, callback, **kwargs):
        cur = duckdb_con.cursor()
        try:
            if sql.strip().lower() == "select err":
                # custom error returned to client
                callback(("ERROR", "42846", "bad type"), is_error=True)
                return
            reader = cur.execute(sql).fetch_record_batch()
            self.send_reader(reader, callback)
        except Exception as exc:
            logging.exception("error on executing query")
            batch = self.arrow_batch(
                [pa.array(["ERROR"]), pa.array([str(exc)])],
                ["error", "message"],
            )
            self.send_reader(batch, callback)

    def handle_query(self, sql, callback=callable, **kwargs):
        self.executor.submit(self._handle_query, sql, callback, **kwargs)

```

### Start the server

```python
    global duckdb_con
    duckdb_con = duckdb.connect()
    duckdb_con.execute(
        """
        CREATE VIEW klines AS 
        SELECT * 
        FROM 'data/klines.parquet'
        """
    )
    server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
    server.set_tls("certs/server.crt", "certs/server.key")
    server.start()
```

You can check server implementations on test_concurrency/ and example/ directory


Then connect using any PostgreSQL client:

```bash
psql -h localhost -p 5433
```


### Enabling TLS

Generate a temporary certificate and key:

```bash
openssl req -newkey rsa:2048 -nodes -keyout server.key -x509 -days 1 -out server.crt -subj "/CN=localhost"
```

### Enabling Catalog Emulation

Postgresql clients sends queries to pg_catalog schema to find out databases, schemas, tables, columns.

We have this (datafusion_pg_catalog)[https://github.com/ybrs/pg_catalog] for this purpose.

You can register your own database and your tables. 

For example

```python
    server = riffq.RiffqServer(f"127.0.0.1:{port}", connection_cls=Connection)
    server.set_tls("certs/server.crt", "certs/server.key")

    server._server.register_database("duckdb")

    tbls = duckdb_con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog','information_schema')"
    ).fetchall()

    for schema_name, table_name in tbls:
        server._server.register_schema("duckdb", schema_name)
        cols_info = duckdb_con.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema=? AND table_name=?",
            (schema_name, table_name),
        ).fetchall()
        columns = []
        for col_name, data_type, is_nullable in cols_info:
            columns.append(
                {
                    col_name: {
                        "type": map_type(data_type),
                        "nullable": is_nullable.upper() == "YES",
                    }
                }
            )
        server._server.register_table("duckdb", schema_name, table_name, columns)

    server.start(catalog_emulation=True)
```


---

## Status

- ✅ Wire protocol support (simple + extended)
- ✅ Query dispatching to Python
- ✅ Thread based non-blocking query execution (long queries don't block)
- ✅ DuckDB, Pandas, Polars compatibility
- ✅ Limited SQL parsing on Rust side (forwarded to Python)
- ✅ Optional TLS encryption
- ✅ Integration with optional catalog emulation layer
- 🟡 More examples
- 🟡 Better logging, monitoring, observability
---

## Installation

We currently have a pre release on pypi. You can install it with `--pre` tag.

```bash
pip install riffq --pre
```

## Running Locally

Install the development requirements and run the test suite:

```bash
git clone git@github.com:ybrs/riffq.git
cd riffq
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

maturin build --profile=fast -i python3
pip install target/wheels/*.whl
# or maturin developer
make all-tests
```

The tests require the Rust extension to build successfully; any build failure will cause the suite to fail.

---

## License

MIT or Apache 2.0 — your choice.

---

## Contributing

Contributions are welcome! Especially for:
- For the emulation layer, I am currently testing with
  - Intellij Datagrid
  - Dbeaver
  - psql cli
  - [Vscode Postgresql Extension](https://marketplace.visualstudio.com/items?itemName=ms-ossdata.vscode-pgsql)

  So testing with other clients, especially BI tools are very welcomed. 
  
- Better Python DX
- Example apps (data lake, feature store, etc.)

---
