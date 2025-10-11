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

