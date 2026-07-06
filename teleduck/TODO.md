# Teleduck TODO

## Fixed — `duckdb_type_to_oid` column type OIDs

`duckdb_type_to_oid` in `src/teleduck/server.py` mapped two DuckDB types to the
wrong `pg_type` OID because of naive substring checks. Both are fixed and covered
by `tests/test_type_mapping.py`:

- **`TIMESTAMP WITH TIME ZONE`** → was `1114` (timestamp), now `1184`
  (timestamptz). DuckDB's `information_schema` spells it out and the string does
  not contain `"TZ"`, so the check now keys on `"TIME ZONE"`.
- **`INTERVAL`** → was `23` (int4, because `"INTERVAL"` contains `"INT"`), now
  `1186` (interval). `INTERVAL` is matched before the integer arms.
- Also corrected `TIME WITH TIME ZONE` → `1266` (timetz) while here.

## Remaining (best-effort coverage)

`duckdb_type_to_oid` still only distinguishes the common scalar types; unmapped
types fall back to `text` (25). If a consumer needs more fidelity (arrays,
structs, enums, blob/bit, uuid, json), extend the table and add cases to
`tests/test_type_mapping.py`.
