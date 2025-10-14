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

RiffqServer creates a new instance from `connection_cls` class, so for each connected client/user you will have another instance.  


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
            callback(("ERROR", "XX000", str(exc)), is_error=True)

    def handle_query(self, sql, callback=callable, **kwargs):
        self.executor.submit(self._exec, sql, callback)

duckdb_con = duckdb.connect()
server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
server.start()
```

## Returning errors 

If you call the `callback` on handle_query with is_error=True, the client will receive an error. 

Error codes are defined on here https://www.postgresql.org/docs/current/errcodes-appendix.html

For quick ref.

| error code | description          |
| -----------|--------------------- |
| 3D000      | invalid catalog name |
| 3F000      | invalid schema name  |
| 42601      | syntax_error         |
| 42P01      | undefined_table      |
| 42703      | undefined_column     |
| XX000      | internal error       |

Although these error codes don't show up on psql and various clients, some clients might be using these codes.

## Extra callbacks

### handle_connect 

Admit/deny connections (e.g., IP allowlist). Call `callback(True)` to accept.

```python
def handle_connect(ip, port, callback):
    return callback(True)
```  

For example you can blacklist an ip address here.

### handle_disconnect

cleanup on client disconnect.

```python
def handle_disconnect(ip, port, callback):
   # close open files etc.
   return callback()
```

## Context and Connection Id

As mentioned in each connection, we create a new `Connection` instance.

This creates a context. For example you'd want to direct queries by "current database"

```python
from sqlglot import parse_one, exp

class Connection(riffq.BaseConnection):
   database = None

   def handle_auth(self, user, password, host, database=None, callback=callable):
        # Accept a single demo user
        self.database = database
        callback(user == "user" and password == "secret")

   def handle_query(self, sql, callback=callable, **kwargs):
        # check if "use databasename" statement is coming
        sql_ast = parse_one(sql, read="postgres")
        if isinstance(sql_ast, exp.Use):
            target_database = sql_ast.this.name
            self.database = target_database
            # when switching a database, we don't return data, just a tag
            return callback("OK", is_tag=True)

        # now you can dispatch by current user's database name
        if self.database == "main":
            ...
        elif self.database = "remote":
            ...
        else:
            callback(("ERROR", "3D000", "unknown database"), is_error=True)


server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
server.start()

``` 

### Connection id

Sometimes this encapsulation may not be enough. So we also have an auto incrementing `conn_id` for each new connection. 

One example for this using external resources with one-on-one mapping. Say you build a postgresql frontend for redis

```python
import redis
from sqlglot import parse_one, exp

redis_connections = defaultdict(lambda: redis.Redis(host="localhost", 
            port=6379, db=0, password=None))

class Connection(riffq.BaseConnection):
    
    def handle_query(self, sql, callback=callable, **kwargs):
        # get the existing or a new connection to redis for this conn_id
        redis_conn = redis_connections[self.conn_id]

        parsed = parse_one(sql)
        if isinstance(parsed, exp.Select):
            # get which hashset from "select key, value from hashset" pattern
            # we can use tablename as hashset key           
            tables = list(parsed.find_all(exp.Table))
            if len(tables) != 1:
                return callback(..., is_error=True)

            batch = self.arrow_batch([pa.array([1])], ["key", "value"])
            return self.send_reader(batch, callback)
```

For a more complete example of accessing Redis with the PostgreSQL protocol, see the Redis example:

- [psql_redis.py](https://github.com/ybrs/riffq/blob/main/example/psql-redis/psql_redis.py)

## Catalog Emulation

Many PostgreSQL clients discover databases, schemas, and tables by querying `pg_catalog`.
Riffq can emulate `pg_catalog` and `information_schema` by using `pg_catalog_rs`, so your service can work as a real database server. Eg: you can connect dbeaver to your service.

To use it:

- Call `register_database`, `register_schema`, and `register_table` to declare what you want to expose.
- Start the server with `catalog_emulation=True`.

Example:

```python
server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
server.register_database("mydb")
server.register_schema("mydb", "public")
server.register_table(
    "mydb",
    "public",
    "users",
    [
        {"id": {"type": "int", "nullable": False}},
        {"name": {"type": "string", "nullable": True}},
    ],
)
server.start(catalog_emulation=True)
```

For a full walkthrough, see [Catalog Emulation](catalog.md). For a runnable example of registering databases, schemas, and tables, see the test: [tests/test_register_catalog.py](https://github.com/ybrs/riffq/blob/main/tests/test_register_catalog.py).


## TLS (SSL)

Enable TLS with a certificate and key:

```bash
openssl req -newkey rsa:2048 -nodes -keyout server.key \
 -x509 -days 1 -out server.crt -subj "/CN=localhost"
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
