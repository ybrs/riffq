# Teleduck TODO

## ⚠️ Column type mapping — `duckdb_type_to_oid` returns wrong pg_type OIDs

`duckdb_type_to_oid` in `src/teleduck/server.py` maps a DuckDB declared type to a
PostgreSQL `pg_type` OID for `pg_attribute.atttypid` / `information_schema.columns`.
Two cases are wrong (verified empirically against DuckDB 1.5.3) because they rely
on naive substring checks. These should be fixed together with the broader
column-type-mapping work (see `../../pg_catalog/TODO.md`).

### 1. `TIMESTAMP WITH TIME ZONE` → reported as `timestamp` (1114) instead of `timestamptz` (1184)

DuckDB's `information_schema.columns.data_type` returns the **spelled-out** string,
not `TIMESTAMPTZ`:

```
CREATE TABLE t(a TIMESTAMPTZ);
SELECT data_type FROM information_schema.columns WHERE table_name='t';
-- => 'TIMESTAMP WITH TIME ZONE'

duckdb_type_to_oid("TIMESTAMP WITH TIME ZONE"):
    "TIMESTAMP" in dt  -> True
    return 1184 if "TZ" in dt else 1114
    "TZ" in "TIMESTAMP WITH TIME ZONE"  -> False     # <-- the bug
    => 1114  (timestamp, WRONG; should be 1184 timestamptz)
```

Fix: also test `"TIME ZONE" in dt` (or normalize the type string first).

### 2. `INTERVAL` → reported as `int4` (23)

`"INTERVAL".upper()` contains the substring `"INT"`, and the broad `if "INT" in dt`
arm matches before any interval handling:

```
CREATE TABLE t(b INTERVAL);
SELECT data_type FROM information_schema.columns WHERE table_name='t';
-- => 'INTERVAL'

duckdb_type_to_oid("INTERVAL"):
    "INT" in "INTERVAL"  -> True
    => 23  (int4, WRONG; should be 1186 interval)
```

Fix: handle `INTERVAL` (and guard against other `*INT*`-containing names like
`POINT`) before the bare `"INT" in dt` fallback.

### General

The whole `duckdb_type_to_oid` table is best-effort and only covers common scalar
types; unrecognized types fall back to `text` (25). When the dedicated
type-mapping branch lands, add a test that round-trips representative DuckDB types
(timestamptz, interval, decimal, the integer family, bool, float/double, date,
time) through `pg_attribute`/`information_schema.columns` and asserts the OIDs.
