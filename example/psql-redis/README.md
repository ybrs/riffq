# PostgreSQL → Redis with Riffq (Example)

This example exposes Redis hash sets as PostgreSQL tables via Riffq.
Each SQL table name maps to a Redis hash key. Rows are `(id, value)` pairs
stored as fields in the hash.

- Table `items` → Redis hash key `items`
- Row `(id=123, value="foo")` → `HSET items 123 "foo"`

## Requirements

- Running Redis on `localhost:6379`
- Python deps for this example:
  ```bash
  pip install -r example/psql-redis/requirements.txt
  ```
- Riffq installed/built in your environment (from this repo or PyPI)

## Run the server

```bash
python example/psql-redis/psql_redis.py
```

The server listens on `127.0.0.1:5444`.

## Connect with psql

```bash
psql -h 127.0.0.1 -p 5444 -U user
Password for user user: secret
```

## Demo SQL

Insert rows (writes Redis HSET):
```sql
INSERT INTO items (id, value) VALUES (1, 'foo'), (2, 'bar');
```

Query rows:
```sql
SELECT * FROM items;               -- returns id, value pairs
SELECT id, value FROM items;       -- explicit columns
SELECT * FROM items WHERE id = 1;  -- single row
```

Update a value:
```sql
UPDATE items SET value = 'baz' WHERE id = 2;
```

Delete a row:
```sql
DELETE FROM items WHERE id = 1;
```

Switch Redis logical database (e.g., DB 1):
```sql
USE 1;
```

## Notes & limitations

- This is a minimal example intended for learning and prototyping.
- Only a single table per statement is supported.
- Only `(id, value)` columns are implemented.
- Responses are Arrow record batches under the hood; psql shows tabular results.
