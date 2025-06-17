import pyarrow as pa

_type = {
    "int": pa.int64(),
    "float": pa.float64(),
    "bool": pa.bool_(),
    "str": pa.utf8(),
    "string": pa.utf8(),
    "date": pa.date32(),
    "datetime": pa.timestamp("us"),
}

def to_arrow(schema_desc, rows):
    arrays = []
    for col_ix, col in enumerate(schema_desc):
        ty  = _type[col["type"]]
        arr = pa.array([r[col_ix] for r in rows], type=ty)
        arrays.append(arr)
    batch  = pa.RecordBatch.from_arrays(arrays, names=[c["name"] for c in schema_desc])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    return reader.__arrow_c_stream__()         
