# Task 101:

Only the first hop (Capsule → ArrowArrayStreamReader) is zero-copy. Every subsequent step converts the data at least once, often twice. In other words, after the pointer is turned into an iterator you immediately pay the full copy cost.

current issues: 

Keep RecordBatches until the wire step
Return Vec<RecordBatch> (plus the Schema) from the worker and teach pgwire encoding to consume Arrow arrays directly:

```
async fn execute(...) -> Result<(Vec<RecordBatch>, Arc<Schema>)>;
```

At encoding time, use Arrow’s typed array getters (as_primitive(), as_string(), …) directly inside the DataRowEncoder without first converting to String.

Avoid stringification in arrow_value_to_string
Remove arrow_value_to_string entirely; instead, carry the original ArrayRef vectors forward.

Map Arrow types to pgwire once
Replace the heuristic “scan the first non-null value to guess the type” with the Arrow field’s DataType → pgwire mapping. The schema already knows the correct logical type.


Goals:
- Keep Arrow RecordBatch values untouched from Python all the way to pgwire.	
- Encode Arrow values directly into pgwire DataRowEncoder (no String materialisation).	
- Eliminate runtime “type guessing” scan (adjusted_desc pass). You can leave Removing IPC-bytes fallback as is.
- Maintain current public Rust / Python API so user code requires no changes. We already doing zero-copy in python to rust

## Details:

### A- Change the internal result type

1- Modify trait QueryRunner

```
async fn execute(
    &self,
    query: String,
    params: Option<Vec<Option<Bytes>>>,
    param_types: Option<Vec<Type>>,
    do_describe: bool,
    connection_id: u64,
) -> datafusion::error::Result<(Vec<RecordBatch>, Arc<Schema>)>;
```

2- Update all implementors (RouterQueryRunner, DirectQueryRunner) to:

Drop record_batches_to_rows.

Return batches & schema straight from dispatch_query (router) or on_query (direct).

3- Remove rows_to_record_batch and record_batches_to_rows.
They will become unused once callers change.

### B- Arrow → pgwire encoder

1- New helper arrow_to_pg_rows in riffq::pg (suggested module):
```
pub fn arrow_to_pg_rows<'a>(
    schema: Arc<Schema>,
    batches: &'a [RecordBatch],
) -> impl Stream<Item = PgWireResult<DataRow>> + 'a
Iterates batch → column → row.
```
For each scalar value, uses typed Array::as_* accessors and calls DataRowEncoder::encode_field.

Maps arrow::datatypes::DataType → pgwire::api::Type once, build Vec<FieldInfo> up-front.

2- Add a mirror mapping function:

```
fn arrow_type_to_pgwire(dt: &DataType) -> Type
```

Replaces map_python_type_to_pgwire and the heuristic scan.

3- Update RiffqProcessor::do_query and MyExtendedQueryHandler:

After calling query_runner.execute, invoke arrow_to_pg_rows.

Build Describe... responses from schema.fields() rather than the previous schema_desc.


### C- Task C Delete the “adjust column types” scan
Remove both loops that mutate adjusted_desc.

Column types now come straight from Field::data_type.

Delete helper map_python_type_to_pgwire (superseded).

### D- Safety & compatibility glue
1- Python worker

CallbackWrapper::__call__:

Case A: PyCapsule → call ArrowArrayStreamReader and return batches+schema instead of rows.

Case B (IPC bytes): after decoding with PyArrow, call table.to_batches() to get Arrow RecordBatch objects; proceed identically.

Remove stringification.

2- Fallback rows-as-tuple path keeps current behaviour for now (convert to RecordBatch the same way the IPC path does).

3- Pgwire Describe requests (do_describe_portal)

For do_describe==true, still execute the query once, discard rows, use only schema.

