"""Example showing how to produce an Arrow table from DuckDB."""

import duckdb
import pyarrow as pa

# Execute a query and fetch results as Arrow
con = duckdb.connect()
record_batch = con.execute("SELECT 1 AS a, 'foo' AS b").fetch_record_batch()

# 'record_batch' is a pyarrow.RecordBatch. It can be sent to the Rust layer
print(record_batch)
