"""Example showing how to produce an Arrow table from DuckDB."""

import duckdb
import pyarrow as pa
import ctypes

# Execute a query and fetch results as Arrow
con = duckdb.connect()
record_batch = con.execute("SELECT 1 AS a, 'foo' AS b").fetch_record_batch()

# Export as an Arrow C Stream pointer for zero-copy transfer
reader = pa.RecordBatchReader.from_batches(record_batch.schema, [record_batch])
if hasattr(reader, "__arrow_c_stream__"):
    capsule = reader.__arrow_c_stream__()
    stream_ptr = ctypes.cast(capsule, ctypes.c_void_p).value
else:
    from pyarrow.cffi import ffi
    c_stream = ffi.new("struct ArrowArrayStream*")
    reader._export_to_c(c_stream)
    stream_ptr = int(ffi.cast("uintptr_t", c_stream))

print("Arrow stream pointer:", stream_ptr)
