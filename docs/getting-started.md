# Getting Started

This guide shows the fastest path to expose data over PostgreSQL using Riffq. You will:

- Subclass `riffq.BaseConnection`
- Implement `handle_auth` and `handle_query`
- Register your connection with `riffq.RiffqServer`

For a deeper tour (extra callbacks, TLS), see the sections below.

## Install

```bash
pip install riffq --pre
```

## Minimal server

The smallest useful server authenticates clients and responds to queries. Below we implement a toy handler that answers a couple of fixed SQLs; extend as needed.

```python
import riffq
import pyarrow as pa

class Connection(riffq.BaseConnection):
    def handle_auth(self, user, password, host, database=None, callback=callable):
        # Accept a single demo user
        callback(user == "user" and password == "secret")

    def handle_query(self, sql, callback=callable, **kwargs):
        q = sql.strip().lower()
        if q == "select 1":
            batch = self.arrow_batch([pa.array([1])], ["one"])
            self.send_reader(batch, callback)
        elif q == "select 'ok' as status":
            batch = self.arrow_batch([pa.array(["ok"])], ["status"])
            self.send_reader(batch, callback)
        else:
            # Send a simple error tuple (severity, code, message)
            callback(("ERROR", "42601", "unsupported demo query"), is_error=True)

server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
server.start()
```

Connect using any PostgreSQL client:

```bash
psql -h 127.0.0.1 -p 5433 -U user
Password for user user: secret
```

Try:

```sql
select 1;
select 'ok' as status;
```

## Using a real engine (DuckDB example)

For non‑trivial SQL, delegate to your favorite engine. This version runs queries in a thread pool and returns Arrow results directly.

```python
import logging, duckdb, pyarrow as pa, riffq
logging.basicConfig(level=logging.INFO)

class Connection(riffq.BaseConnection):
    def handle_auth(self, user, password, host, database=None, callback=callable):
        callback(user == "user" and password == "secret")

    def _exec(self, sql, callback):
        cur = duckdb_con.cursor()
        try:
            reader = cur.execute(sql).fetch_record_batch()
            self.send_reader(reader, callback)
        except Exception as exc:
            logging.exception("query error")
            batch = self.arrow_batch([pa.array(["ERROR"]), pa.array([str(exc)])], ["error","message"])
            self.send_reader(batch, callback)

    def handle_query(self, sql, callback=callable, **kwargs):
        self.executor.submit(self._exec, sql, callback)

duckdb_con = duckdb.connect()
server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
server.start()
```

## Extra callbacks

- `handle_connect(ip, port, callback)`: admit/deny connections (e.g., IP allowlist). Call `callback(True)` to accept.
- `handle_disconnect(ip, port, callback)`: cleanup on client disconnect.

## TLS (SSL)

Enable TLS with a certificate and key:

```bash
openssl req -newkey rsa:2048 -nodes -keyout server.key -x509 -days 1 -out server.crt -subj "/CN=localhost"
```

```python
server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
server.set_tls("server.crt", "server.key")
server.start(tls=True)
```

## Tips

- Use `self.arrow_batch([...], names=[...])` to construct quick result sets.
- Use `self.send_reader(reader, callback)` to return a `pyarrow.RecordBatchReader`.
- The server creates one `Connection` instance per client and reuses it.

See the README for a fuller example and more context.
