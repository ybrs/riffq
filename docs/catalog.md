# Catalog Emulation

Riffq can emulate PostgreSQL system catalogs (pg_catalog, information_schema) so client tools can enumerate databases, schemas, tables, and columns.

At a high level:
- Define metadata with `register_database`, `register_schema`, `register_table`.
- Start your server with `catalog_emulation=True`.

Behind the scenes, Riffq uses a Rust helper crate [pg_catalog_rs](https://github.com/ybrs/pg_catalog) to answer catalog queries efficiently.

## Quick Start

```python
import riffq

class Connection(riffq.BaseConnection):
    def handle_query(self, sql, callback=callable, **kwargs):
        # return some rows or a command tag here
        ...

server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)

# 1) Register a database
server.register_database("mydb")

# 2) Register a schema within the database
server.register_schema("mydb", "public")

# 3) Register a table and its columns
server.register_table(
    "mydb",
    "public",
    "users",
    [
        {"id": {"type": "int", "nullable": False}},
        {"name": {"type": "string", "nullable": True}},
        {"created_at": {"type": "datetime", "nullable": False}},
    ],
)

server.start(catalog_emulation=True)
```

## Column Types

Supported column `type` strings align with the Arrow types used by Riffq:

- `int`, `float`, `bool`, `string`/`str`, `date`, `datetime`

These types inform clients about your table shape; your actual query results
can be produced by any engine (DuckDB, Polars, Pandas, etc.) as Arrow.

## Discovering From an Engine

For real data sources, enumerate schemas/tables/columns dynamically and
register them. 

For example with DuckDB (pseudo-code):

```python
def map_type(duckdb_type: str) -> str:
    t = duckdb_type.upper()
    if "INT" in t: return "int"
    if any(x in t for x in ["DOUBLE", "DECIMAL", "REAL", "FLOAT"]): return "float"
    if "BOOL" in t: return "bool"
    if t == "DATE": return "date"
    if "TIMESTAMP" in t or "DATETIME" in t: return "datetime"
    return "string"

server.register_database("duckdb")
for schema_name, table_name in list_tables_from_duckdb():
    server.register_schema("duckdb", schema_name)
    columns = []
    for col_name, dt, is_nullable in list_columns(schema_name, table_name):
        columns.append({col_name: {"type": map_type(dt), "nullable": is_nullable}})
    server.register_table("duckdb", schema_name, table_name, columns)

server.start(catalog_emulation=True)
```

## Examples

For a minimal end-to-end example that registers a database, schema, and table and asserts they appear via `pg_catalog`, see:

- [tests/test_register_catalog.py](https://github.com/ybrs/riffq/blob/main/tests/test_register_catalog.py)

```python
def main():
    server = riffq.RiffqServer("127.0.0.1:5444", connection_cls=Connection)

    # Register catalog: logical databases redis0..redis2 (limited for illustration).
    # Under each, register schema "public" and expose each Redis hash key as a
    # table with (key,value). Increase the range below in real deployments.
    # schema "public" and expose each Redis hash key as a table with (key,value).
    
    for db_index in range(3):
        db_name = f"redis{db_index}"

        r = redis.Redis(host="localhost", port=6379, 
          db=db_index, password=None, decode_responses=True)
        server.register_database(db_name)
        server.register_schema(db_name, "public")
        # Discover only hash keys. Uses SCAN TYPE hash (server-side filter)
        # so we don't issue a TYPE per key. Still a best-effort scan.
        for k in r.scan_iter(match="*", _type="hash"):
          server.register_table(
              db_name,
              "public",
              str(k),
              [
                  {"key": {"type": "string", "nullable": False}},
                  {"value": {"type": "string", "nullable": True}},
              ],
          )

```        

When you start the server and connect with psql you can see the tables

```
user=> \l
                                List of databases
   Name    |  Owner   | Encoding |   Collate   |    Ctype    | Access privileges
-----------+----------+----------+-------------+-------------+-------------------
 postgres  | postgres | UTF8     | nl_NL.UTF-8 | nl_NL.UTF-8 |
 redis0    | postgres | UTF8     | C           | C           | 
 redis1    | postgres | UTF8     | C           | C           | 
 redis2    | postgres | UTF8     | C           | C           | 
 template0 | postgres | UTF8     | nl_NL.UTF-8 | nl_NL.UTF-8 | 
 template1 | postgres | UTF8     | nl_NL.UTF-8 | nl_NL.UTF-8 | 
(6 rows)

user=> \c redis0
Password:
psql (14.17 (Homebrew), server 17.4.0)
WARNING: psql major version 14, server major version 17.
         Some psql features might not work.
You are now connected to database "redis0" as user "user".
redis0=> \dt
         List of relations
 Schema |  Name  | Type  |  Owner
--------+--------+-------+----------
 public | myhash | table | postgres
 public | test   | table | postgres
 public | test2  | table | postgres
 public | test3  | table | postgres
(4 rows)

redis0=> \d myhash
             Table "public.myhash"
 Column | Type | Collation | Nullable | Default
--------+------+-----------+----------+---------
 key    | text |           | not null |
 value  | text |           |          |


```

```
 redis0=> \d myhash
             Table "public.myhash"
 Column | Type | Collation | Nullable | Default
--------+------+-----------+----------+---------
 key    | text |           | not null |
 value  | text |           |          |
```

```
; pg_catalog_rs has all pg_catalog tables
; for example you can select tables directly from pg_class 

redis0=> SELECT c.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'public';
 relname
---------
 myhash
 test2
 test
 test3
(4 rows)
```

```
; or you can use ::regclass or ::oid type conversions as in 
; real postgresql pg_catalog

redis0=> SELECT oid,relname,reltype,relnamespace
FROM pg_class
WHERE oid = 'myhash'::oid;
  oid  | relname | reltype | relnamespace
-------+---------+---------+--------------
 50010 | myhash  |   50011 |         2200
(1 row)


```

## API Reference

### Register a logical database name 

```python
RiffqServer.register_database(database_name: str) -> None:
```

### Register a schema under a previously registered database.

```python
RiffqServer.register_schema(database_name: str, schema_name: str) -> None:
```

### Register a table and its columns.

Each column is `{ name: {"type": <str>, "nullable": <bool>} }`.

```python
RiffqServer.register_table(database_name: str, 
  schema_name: str, table_name: str, columns: list[dict]) -> None:
```


[pg_catalog_rs]: https://github.com/ybrs/pg_catalog
