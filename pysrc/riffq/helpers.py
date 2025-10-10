"""Helper utilities for building Arrow results.

Currently provides `to_arrow` for constructing an Arrow C Stream from a simple
schema description and row data. This is handy for small, programmatic results
without depending on a database engine.
"""

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
    """Build an Arrow C Stream from schema and rows.

    The schema is a list of dicts like `{ "name": str, "type": str }` where
    `type` is one of: `int`, `float`, `bool`, `str`/`string`, `date`,
    `datetime`. Rows are sequences whose positional items match the schema
    order.

    Args:
        schema_desc: Column descriptors in display order.
        rows: Iterable of row sequences aligned to `schema_desc`.

    Returns:
        A PyCapsule containing an Arrow C Stream suitable for returning to the
        server callback.
    """
    arrays = []
    for col_ix, col in enumerate(schema_desc):
        ty = _type[col["type"]]
        arr = pa.array([r[col_ix] for r in rows], type=ty)
        arrays.append(arr)
    batch = pa.RecordBatch.from_arrays(arrays, names=[c["name"] for c in schema_desc])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    return reader.__arrow_c_stream__()
