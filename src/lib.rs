use async_trait::async_trait;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_pg_catalog::session::{ClientOpts, execute_sql, set_session_user};
use futures::{Sink, SinkExt, Stream};
use log::{debug, error, info};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict, PyList, PyTuple};
use pyo3::{Bound, IntoPyObjectExt, PyAny};
use rustls_pemfile::{certs, private_key};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::signal;
use tokio::sync::OnceCell;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;

static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);

use bytes::Bytes;
use futures::stream;
use std::pin::Pin;

use arrow::array::{
    Array, BinaryArray, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, FixedSizeListArray,
    LargeBinaryArray, LargeListArray, ListArray, RecordBatch,
};
use arrow::ffi_stream::ArrowArrayStreamReader;
// no explicit import of i256 required; we only use to_string() on values
use arrow::array::cast::AsArray;
use arrow::array::{ArrayRef, StringBuilder};
use arrow::datatypes::{
    DataType, Field, Schema, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow::record_batch::RecordBatchReader;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionContext;
use datafusion_pg_catalog::catalog_oid_registry::{
    CatalogOidRegistry, InMemoryCatalogOidRegistry, RedbCatalogOidRegistry,
};
use datafusion_pg_catalog::combined_catalog_source::CombinedCatalogSource;
use datafusion_pg_catalog::declared_catalog_source::DeclaredCatalogSource;
use datafusion_pg_catalog::{
    ColumnDef, ColumnSpec, ConfigSettingDef, DatabaseDef, LazyCatalogOptions, LazyCatalogSource,
    RelationDef, RelationKind, SchemaDef, SetStatementDisposition, SettingDef,
    classify_set_statement, dispatch_query, get_base_session_context_with_lazy_catalog,
};
use postgres_types::FromSql;

use arrow::datatypes::TimeUnit;
use chrono::{DateTime, Duration, NaiveDate};

use pgwire::api::PgWireConnectionState;
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler, finish_authentication};
use pgwire::api::portal::Format;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::StoredStatement;
use pgwire::api::{ClientInfo, NoopHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::{Authentication, PasswordMessageFamily, Startup};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

mod helpers;
pub mod pg;
mod sql_batch;

/// The variable name clients read the isolation level under, and the level
/// riffq reports. Named here because two SHOW spellings answer with it.
const TRANSACTION_ISOLATION_VARIABLE: &str = "transaction_isolation";
/// The isolation level riffq reports. Fixed, because riffq runs every statement
/// on its own and offers no transaction machinery to isolate anything from.
const TRANSACTION_ISOLATION_LEVEL: &str = "read committed";
use helpers::_debug_parameters;
use pg::arrow_type_to_pgwire;
use sqlparser::ast::Statement;
use sqlparser::parser::Parser;

/// `PostgreSQL` version reported to clients during startup and via `SHOW server_version`.
pub const SERVER_VERSION: &str = "17.4.0";

/// Convert a Rust value pyo3 can turn into a Python object into a `Bound<PyAny>`,
/// returning None if the conversion fails.
fn param_to_py<'py, T: pyo3::IntoPyObject<'py>>(
    py: Python<'py>,
    value: T,
) -> Option<Bound<'py, PyAny>> {
    use pyo3::IntoPyObjectExt;
    value.into_bound_py_any(py).ok()
}

/// Decode one bound extended-protocol parameter into a Python object.
///
/// The binary representation for the parameter's declared `PostgreSQL` type is
/// tried first -- this is what psycopg and pgjdbc send for their typed
/// parameters. When the type is unknown or the binary decode fails, the bytes
/// are read as UTF-8 text and returned as a string instead: text-format
/// parameters, and parameters a client leaves untyped (type OID 0), arrive this
/// way -- pgjdbc's bound booleans and timestamps and every psqlodbc parameter
/// among them -- and the backend coerces the string to the column's type.
/// Returns None when neither decode succeeds.
fn decode_param<'py>(py: Python<'py>, bytes: &[u8], ty: &Type) -> Option<Bound<'py, PyAny>> {
    // A copy of the slice so a binary decode that advances it does not consume
    // the bytes the text fallback re-reads.
    let buf = bytes;
    let decoded = match ty {
        &Type::INT2 => i16::from_sql(ty, buf).ok().and_then(|v| param_to_py(py, v)),
        &Type::INT4 => i32::from_sql(ty, buf).ok().and_then(|v| param_to_py(py, v)),
        &Type::INT8 => i64::from_sql(ty, buf).ok().and_then(|v| param_to_py(py, v)),
        &Type::FLOAT4 => f32::from_sql(ty, buf).ok().and_then(|v| param_to_py(py, v)),
        &Type::FLOAT8 => f64::from_sql(ty, buf).ok().and_then(|v| param_to_py(py, v)),
        &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR => String::from_sql(ty, buf)
            .ok()
            .and_then(|v| param_to_py(py, v)),
        &Type::BOOL => bool::from_sql(ty, buf)
            .ok()
            .and_then(|v| param_to_py(py, v)),
        &Type::TIMESTAMP => chrono::NaiveDateTime::from_sql(ty, buf)
            .ok()
            .and_then(|v| param_to_py(py, v)),
        &Type::TIMESTAMPTZ => chrono::DateTime::<chrono::Utc>::from_sql(ty, buf)
            .ok()
            .and_then(|v| param_to_py(py, v)),
        _ => None,
    };
    decoded.or_else(|| {
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| param_to_py(py, text.to_string()))
    })
}

/// A unit of work handed from a tokio connection task to the single thread that
/// owns the Python callbacks.
///
/// Every variant that expects an answer carries its own `responder`: the Python
/// side replies by calling a callback object, which may happen after the
/// handler returns, so the reply cannot be the return value of the call.
pub enum WorkerMessage {
    /// Run a SQL statement (or, with `do_describe`, only work out its result
    /// schema) through the host's `on_query` callback.
    Query {
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
        responder: oneshot::Sender<QueryResult>,
    },
    /// Ask the host's `on_connect` callback whether to admit a client that has
    /// completed the startup handshake.
    Connect {
        connection_id: u64,
        ip: String,
        port: u16,
        server_name: Option<String>,
        responder: oneshot::Sender<BoolCallbackResult>,
    },
    /// Tell the host a client has gone. No responder: nothing waits on the
    /// answer, and the connection is already closed by the time this is sent.
    Disconnect {
        connection_id: u64,
        ip: String,
        port: u16,
    },
    /// Ask the host's `on_authentication` callback to verify a cleartext
    /// password. Sent before `Connect` for a server with authentication on.
    Authentication {
        connection_id: u64,
        user: Option<String>,
        database: Option<String>,
        host: String,
        password: String,
        responder: oneshot::Sender<BoolCallbackResult>,
    },
}

/// A host's yes/no verdict on a connect or authentication attempt, plus the
/// error the client should be told about when the answer is no.
///
/// The error is optional because a host may simply refuse without wording it;
/// the handler then supplies a generic `PostgreSQL` error itself.
pub struct BoolCallbackResult {
    pub allowed: bool,
    pub error: Option<Box<ErrorInfo>>,
}

/// The object a Python query handler calls to hand back a result set.
///
/// The handler may call it from any thread once it has the data, which is what
/// lets a host answer queries asynchronously. The `Option` empties on the first
/// call, so a handler that calls back twice is ignored the second time rather
/// than delivering a result nobody is waiting for.
#[pyclass]
struct CallbackWrapper {
    responder: Arc<Mutex<Option<oneshot::Sender<QueryResult>>>>,
}

/// The object a Python connect or authentication handler calls to allow or
/// refuse a client. Single-use in the same way as [`CallbackWrapper`].
#[pyclass]
struct BoolCallbackWrapper {
    responder: Arc<Mutex<Option<oneshot::Sender<BoolCallbackResult>>>>,
}

#[pymethods]
impl BoolCallbackWrapper {
    /// Deliver the host's verdict to the connection waiting on it.
    ///
    /// Python calls this as `callback(True)` to admit a client, or
    /// `callback(False)` to refuse it. Refusing may name the error the client
    /// sees: any of `message`, `severity` and `sqlstate` given builds an
    /// `ErrorInfo`, and the ones left out fall back to a FATAL XX000
    /// "rejected". A truthy value that is not a bool, or none of the three
    /// error fields, keeps the caller's own default error instead.
    #[pyo3(signature = (result, message=None, severity=None, sqlstate=None))]
    fn __call__(
        &self,
        result: &Bound<'_, PyAny>,
        message: Option<String>,
        severity: Option<String>,
        sqlstate: Option<String>,
    ) {
        if let Some(sender) = self.responder.lock().unwrap().take() {
            let val: bool = result.extract().unwrap_or(false);
            if val {
                let _ = sender.send(BoolCallbackResult {
                    allowed: true,
                    error: None,
                });
            } else {
                let err = if message.is_some() || severity.is_some() || sqlstate.is_some() {
                    let sev = severity.unwrap_or_else(|| "FATAL".to_string());
                    let state = sqlstate.unwrap_or_else(|| "XX000".to_string());
                    let msg = message.unwrap_or_else(|| "rejected".to_string());
                    Some(Box::new(ErrorInfo::new(sev, state, msg)))
                } else {
                    None
                };
                let _ = sender.send(BoolCallbackResult {
                    allowed: false,
                    error: err,
                });
            }
        }
    }
}

/// A result set described in plain Python: one metadata dict per column paired
/// with the rows themselves, which is the `(schema, rows)` tuple a host returns
/// when it has no Arrow data to hand over.
type PyDescribedRows = (Vec<HashMap<String, String>>, Vec<Vec<Py<PyAny>>>);

/// Build a single all-text `RecordBatch` from a host's `(schema, rows)` tuple.
///
/// Every column is typed Utf8 regardless of what the metadata dict says, since
/// the rows carry no type information the encoder could trust. A cell that is
/// Python `None`, or any object that does not extract as a `str`, becomes SQL
/// NULL rather than failing the whole query.
fn described_rows_to_arrow(
    py: Python<'_>,
    schema_desc: &[HashMap<String, String>],
    py_rows: Vec<Vec<Py<PyAny>>>,
) -> QueryResult {
    // turn PyObjects into Rust Option<String>
    let rows: Vec<Vec<Option<String>>> = py_rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|val| {
                    let val_bound = val.bind(py);
                    if val_bound.is_none() {
                        None
                    } else {
                        val_bound.extract::<String>().ok()
                    }
                })
                .collect()
        })
        .collect();

    // build arrow arrays column-wise
    let fields: Vec<Field> = schema_desc
        .iter()
        .map(|c| Field::new(c.get("name").unwrap(), DataType::Utf8, true))
        .collect();

    let mut builders: Vec<StringBuilder> = fields.iter().map(|_| StringBuilder::new()).collect();

    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            match cell {
                Some(s) => builders[i].append_value(s),
                None => builders[i].append_null(),
            }
        }
    }

    let arrays: Vec<ArrayRef> = builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect();

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    QueryResult::Arrow(vec![batch], schema)
}

#[pymethods]
impl CallbackWrapper {
    /// Deliver a Python query handler's result to the connection waiting on it.
    ///
    /// Python calls this as `callback(result)`, optionally with `is_tag=True`
    /// to send a bare command tag such as "INSERT 0 1", or `is_error=True` with
    /// a `(severity, sqlstate, message)` tuple to raise a `PostgreSQL` error on
    /// the client. Otherwise `result` is read as data, in descending order of
    /// preference: an Arrow C stream capsule, Arrow IPC `bytes`, or a
    /// `(schema, rows)` tuple of Python values.
    ///
    /// A result that matches none of those shapes sends nothing; the waiting
    /// connection then sees the channel close and answers with an empty result.
    #[pyo3(signature = (result, *, is_tag=false, is_error=false))]
    fn __call__(&self, result: &Bound<'_, PyAny>, is_tag: bool, is_error: bool) {
        let Some(sender) = self.responder.lock().unwrap().take() else {
            return;
        };
        let py = result.py();
        if is_tag {
            let tag = result.extract::<String>().unwrap_or_default();
            let ret = sender.send(QueryResult::Tag(tag));
            if ret.is_err() {
                error!("return for tag errored");
            }
            return;
        }
        if is_error {
            let err_tuple = result
                .extract::<(String, String, String)>()
                .unwrap_or_else(|_| {
                    (
                        "ERROR".to_string(),
                        "XX000".to_string(),
                        "unknown error".to_string(),
                    )
                });
            let err_info = ErrorInfo::new(err_tuple.0, err_tuple.1, err_tuple.2);
            let _ = sender.send(QueryResult::Error(Box::new(err_info)));
            return;
        }
        // Try Arrow C stream pointer first
        let type_name = result.get_type().name().map_or_else(
            |_| "<unknown>".to_string(),
            |s| s.to_string_lossy().into_owned(),
        );
        debug!("[RUST] result python type: {type_name}");
        if let Ok(capsule) = result.extract::<Bound<PyCapsule>>() {
            debug!("[RUST] received PyCapsule");

            // The Arrow PyCapsule interface names a stream capsule
            // "arrow_array_stream". Reading the pointer by that name
            // rejects a capsule carrying some other kind of C pointer,
            // which the cast below would otherwise reinterpret as an
            // ArrowArrayStream -- undefined behaviour rather than an error.
            let ptr = match capsule.pointer_checked(Some(c"arrow_array_stream")) {
                Ok(ptr) => ptr.as_ptr(),
                Err(err) => {
                    let err_info = ErrorInfo::new(
                        "ERROR".to_string(),
                        "XX000".to_string(),
                        format!("query callback returned an unusable capsule: {err}"),
                    );
                    let _ = sender.send(QueryResult::Error(Box::new(err_info)));
                    return;
                }
            };

            // `ptr` is a live ArrowArrayStream* produced by PyArrow
            // (CallbackWrapper received it directly from Python).
            // ArrowArrayStreamReader takes ownership and will call `release`
            // *when the reader itself is dropped*.  We read every batch,
            // clone them into `batches`, clone the schema, and only then let
            // `reader` fall out of scope, so the underlying C buffers stay
            // alive as long as any RecordBatch/Schema clones do.  No
            // use-after-free possible.
            unsafe {
                let mut reader = ArrowArrayStreamReader::from_raw(ptr.cast()).unwrap();
                let mut batches = Vec::new();
                while let Some(batch) = reader.next().transpose().unwrap() {
                    batches.push(batch);
                }
                let schema = reader.schema();
                let _ = sender.send(QueryResult::Arrow(batches, schema));
            }
            return;
        }

        // First try to treat the result as Arrow IPC bytes. When the
        // callback returns bytes we assume they contain an Arrow IPC
        // stream produced by ``pyarrow``.
        if let Ok(pybytes) = result.extract::<Bound<pyo3::types::PyBytes>>() {
            let data = pybytes.as_bytes();
            let cursor = std::io::Cursor::new(data);
            let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).unwrap();
            let schema = reader.schema().clone();
            let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
            let _ = sender.send(QueryResult::Arrow(batches, schema));
            return;
        }

        // Fallback: assume (schema_desc, rows) tuple, build batches
        let parsed: PyResult<PyDescribedRows> = result.extract::<PyDescribedRows>();
        if let Ok((schema_desc, py_rows)) = parsed {
            let _ = sender.send(described_rows_to_arrow(py, &schema_desc, py_rows));
        }
    }
}

/// A described result set ready for the wire: the column descriptions a client
/// receives in `RowDescription`, and the lazily encoded `DataRow` stream that
/// follows it.
type DescribedRowStream = (
    Arc<Vec<FieldInfo>>,
    Pin<Box<dyn Stream<Item = PgWireResult<DataRow>> + Send>>,
);

/// Turn Arrow batches into the column descriptions and row stream pgwire sends.
///
/// The rows are encoded lazily, one batch at a time as the client consumes
/// them, so a large result set is never fully materialised in wire format.
/// `formats` is the per-column text/binary choice the client asked for; a
/// column the client said nothing about is sent as text.
fn arrow_to_pg_rows(
    batches: Vec<RecordBatch>,
    schema: &Schema,
    formats: &[FieldFormat],
) -> DescribedRowStream {
    // column metadata
    let field_defs: Arc<Vec<FieldInfo>> = Arc::new(
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                let format = formats.get(idx).copied().unwrap_or(FieldFormat::Text);
                FieldInfo::new(
                    f.name().clone(),
                    None,
                    None,
                    arrow_type_to_pgwire(f.data_type()),
                    format,
                )
            })
            .collect(),
    );

    // lazy row stream
    let row_stream = stream::unfold((0usize, batches), {
        let meta_outer = field_defs.clone(); // captured by outer FnMut
        move |(mut row_idx, mut remaining_batches)| {
            // clone **inside** so the async move owns its copy
            let meta = meta_outer.clone();
            async move {
                loop {
                    if remaining_batches.is_empty() {
                        return None;
                    }
                    if row_idx == remaining_batches[0].num_rows() {
                        remaining_batches.remove(0);
                        row_idx = 0;
                        continue;
                    }

                    let batch = &remaining_batches[0];
                    let mut enc = DataRowEncoder::new(meta.clone());
                    for (col_idx, col) in batch.columns().iter().enumerate() {
                        let format = meta
                            .get(col_idx)
                            .map_or(FieldFormat::Text, pgwire::api::results::FieldInfo::format);
                        if let Err(e) = encode_arrow_value(&mut enc, col.as_ref(), row_idx, format)
                        {
                            return Some((Err(e), (row_idx + 1, remaining_batches)));
                        }
                    }
                    return Some((Ok(enc.take_row()), (row_idx + 1, remaining_batches)));
                }
            }
        }
    });

    // pin + box so it is Unpin
    (field_defs, Box::pin(row_stream))
}

/// Convert a count of nanoseconds since the Unix epoch into a UTC date-time.
///
/// Splits the count with Euclidean division so an instant before 1970 yields a
/// floored second and a non-negative sub-second remainder. Truncating division
/// would hand chrono a negative nanosecond count, which it rejects, turning
/// every pre-epoch timestamp into a failure instead of a date.
///
/// Returns None only for an instant outside the range chrono can represent.
fn timestamp_nanos_to_datetime(nanos: i128) -> Option<DateTime<chrono::Utc>> {
    let secs = i64::try_from(nanos.div_euclid(1_000_000_000)).ok()?;
    let subsec_nanos = u32::try_from(nanos.rem_euclid(1_000_000_000)).ok()?;
    DateTime::from_timestamp(secs, subsec_nanos)
}

/// The number of fractional digits a decimal column carries.
///
/// Arrow permits a negative scale (digits to the left of the point);
/// `PostgreSQL` numeric does not, and reading one as an unsigned exponent would
/// ask for an astronomically large power of ten and overflow. Such a column is
/// rendered unscaled instead.
fn decimal_fraction_digits(scale: i8) -> u8 {
    u8::try_from(scale).unwrap_or(0)
}

/// Render one Arrow value as the text `PostgreSQL` would send for it.
///
/// Returns None for a NULL cell and for any Arrow type riffq has no text
/// spelling for; both reach the client as SQL NULL.
///
/// The match below is the one place every Arrow type riffq renders is listed,
/// so a type that stops being handled - and would silently become NULL on the
/// wire - is still visible here; each arm hands the cell to the renderer for
/// its group of types.
fn arrow_value_to_string(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }

    match array.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => render_number_as_text(array, row),
        DataType::Boolean => Some(array.as_boolean().value(row).to_string()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            render_string_as_text(array, row)
        }
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            render_date_or_timestamp_as_text(array, row)
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            render_decimal_as_text(array, row)
        }
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            render_binary_as_hex_text(array, row)
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            render_list_as_braced_text(array, row)
        }
        _ => None,
    }
}

/// Render an Arrow number cell in Rust's decimal spelling of the value.
///
/// Handles Int8, Int16, Int32, Int64, `UInt8`, `UInt16`, `UInt32`, `UInt64`, Float32
/// and Float64. Any other type yields None, which the caller sends as NULL.
fn render_number_as_text(array: &dyn Array, row: usize) -> Option<String> {
    match array.data_type() {
        DataType::Int8 => Some(
            array
                .as_primitive::<arrow::array::types::Int8Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Int16 => Some(
            array
                .as_primitive::<arrow::array::types::Int16Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Int32 => Some(
            array
                .as_primitive::<arrow::array::types::Int32Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Int64 => Some(
            array
                .as_primitive::<arrow::array::types::Int64Type>()
                .value(row)
                .to_string(),
        ),
        DataType::UInt8 => Some(
            array
                .as_primitive::<arrow::array::types::UInt8Type>()
                .value(row)
                .to_string(),
        ),
        DataType::UInt16 => Some(
            array
                .as_primitive::<arrow::array::types::UInt16Type>()
                .value(row)
                .to_string(),
        ),
        DataType::UInt32 => Some(
            array
                .as_primitive::<arrow::array::types::UInt32Type>()
                .value(row)
                .to_string(),
        ),
        DataType::UInt64 => Some(
            array
                .as_primitive::<arrow::array::types::UInt64Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Float32 => Some(
            array
                .as_primitive::<arrow::array::types::Float32Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Float64 => Some(
            array
                .as_primitive::<arrow::array::types::Float64Type>()
                .value(row)
                .to_string(),
        ),
        _ => None,
    }
}

/// Copy an Arrow string cell out as an owned `String`.
///
/// Handles Utf8, `LargeUtf8` and `Utf8View`. `DataFusion` 54 returns many
/// `information_schema`/`pg_catalog` string columns as `Utf8View` (for example
/// `information_schema.columns.column_name`), so leaving that type out would
/// turn those columns into NULL on the wire. Any other type yields None.
fn render_string_as_text(array: &dyn Array, row: usize) -> Option<String> {
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(row).to_string()),
        DataType::Utf8View => Some(array.as_string_view().value(row).to_string()),
        _ => None,
    }
}

/// Render an Arrow date or timestamp cell as chrono's text spelling of the
/// instant it names.
///
/// Handles Date32 (days since the epoch), Date64 (milliseconds since the
/// epoch) and Timestamp in all four Arrow time units. Returns None for a date
/// or instant chrono cannot represent, and for any other type.
fn render_date_or_timestamp_as_text(array: &dyn Array, row: usize) -> Option<String> {
    match array.data_type() {
        DataType::Date32 => {
            let days = i64::from(
                array
                    .as_primitive::<arrow::array::types::Date32Type>()
                    .value(row),
            );
            let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days);
            Some(date.to_string())
        }
        DataType::Date64 => {
            let ms = array
                .as_primitive::<arrow::array::types::Date64Type>()
                .value(row);
            let dt = DateTime::from_timestamp_millis(ms)?;
            Some(dt.to_string())
        }
        DataType::Timestamp(unit, _) => {
            let dt = timestamp_nanos_to_datetime(timestamp_value_as_nanos(array, row, *unit))?;
            Some(dt.to_string())
        }
        _ => None,
    }
}

/// Read one Arrow timestamp cell as a count of nanoseconds since the epoch.
///
/// The count is an i128 because a second- or millisecond-unit timestamp scaled
/// up to nanoseconds does not fit in the i64 Arrow stores it in.
fn timestamp_value_as_nanos(array: &dyn Array, row: usize, unit: TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => {
            i128::from(array.as_primitive::<TimestampSecondType>().value(row)) * 1_000_000_000
        }
        TimeUnit::Millisecond => {
            i128::from(array.as_primitive::<TimestampMillisecondType>().value(row)) * 1_000_000
        }
        TimeUnit::Microsecond => {
            i128::from(array.as_primitive::<TimestampMicrosecondType>().value(row)) * 1_000
        }
        TimeUnit::Nanosecond => {
            i128::from(array.as_primitive::<TimestampNanosecondType>().value(row))
        }
    }
}

/// Render an Arrow decimal cell with its point placed by the column's scale.
///
/// Handles Decimal128, whose unscaled value divides in Rust, and Decimal256,
/// whose i256 value is rendered as digits and then has the point inserted.
/// Any other type yields None.
fn render_decimal_as_text(array: &dyn Array, row: usize) -> Option<String> {
    match array.data_type() {
        DataType::Decimal128(_precision, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw: i128 = arr.value(row);
            Some(format_decimal_i128(raw, decimal_fraction_digits(*scale)))
        }
        DataType::Decimal256(_precision, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let s = arr.value(row).to_string();
            Some(insert_decimal_point(&s, decimal_fraction_digits(*scale)))
        }
        _ => None,
    }
}

/// Render an Arrow binary cell in `PostgreSQL`'s `\x` hex bytea spelling.
///
/// Handles Binary, `LargeBinary` and `FixedSizeBinary`. Any other type yields
/// None.
fn render_binary_as_hex_text(array: &dyn Array, row: usize) -> Option<String> {
    match array.data_type() {
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Some(hex_bytea(arr.value(row)))
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Some(hex_bytea(arr.value(row)))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            Some(hex_bytea(arr.value(row)))
        }
        _ => None,
    }
}

/// Render an Arrow list cell in `PostgreSQL`'s `{a,b,c}` array literal
/// spelling, with a NULL element written as the word NULL.
///
/// Handles List, `LargeList` and `FixedSizeList`. Any other type yields None.
fn render_list_as_braced_text(array: &dyn Array, row: usize) -> Option<String> {
    let values = list_cell_values(array, row)?;
    let parts: Vec<String> = (0..values.len())
        .map(|i| arrow_value_to_string(values.as_ref(), i).unwrap_or_else(|| "NULL".to_string()))
        .collect();
    Some(format!("{{{}}}", parts.join(",")))
}

/// The array holding the elements of one Arrow list cell.
///
/// Handles List, `LargeList` and `FixedSizeList`, whose elements are reached
/// through three different concrete array types. Returns None for an array
/// that is not a list at all.
fn list_cell_values(array: &dyn Array, row: usize) -> Option<ArrayRef> {
    match array.data_type() {
        DataType::List(_) => Some(
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(row),
        ),
        DataType::LargeList(_) => Some(
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .value(row),
        ),
        DataType::FixedSizeList(_, _) => Some(
            array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap()
                .value(row),
        ),
        _ => None,
    }
}

/// Render one Arrow list cell as the vector of element texts pgwire encodes
/// into a `PostgreSQL` array.
///
/// A NULL element becomes an empty string, because the encoder takes a plain
/// `Vec<String>` with no way to say "this element is NULL". A non-list array
/// yields an empty vector.
fn arrow_list_to_vec(array: &dyn Array, row: usize) -> Vec<String> {
    let Some(values) = list_cell_values(array, row) else {
        return vec![];
    };
    (0..values.len())
        .map(|i| arrow_value_to_string(values.as_ref(), i).unwrap_or_default())
        .collect()
}

/// Append one Arrow value to a `DataRow` in the format the client asked for.
///
/// `format` only matters for the types with two representations - bytea is sent
/// raw in binary and as `\x...` hex in text. An Arrow type riffq cannot encode
/// is written as NULL rather than aborting the row, so an unexpected column
/// type costs one value instead of the whole query.
///
/// The match below is the one place every Arrow type riffq encodes is listed,
/// so a type that stops being handled - and would silently become NULL on the
/// wire - is still visible here; each arm hands the cell to the encoder for
/// its group of types.
fn encode_arrow_value(
    encoder: &mut DataRowEncoder,
    array: &dyn Array,
    row: usize,
    format: FieldFormat,
) -> PgWireResult<()> {
    if array.is_null(row) {
        return encoder.encode_field(&Option::<i32>::None); // type will be ignored
    }
    match array.data_type() {
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            encode_decimal_field(encoder, array, row)
        }
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            encode_binary_field(encoder, array, row, format)
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => encode_number_field(encoder, array, row),
        DataType::Boolean => encoder.encode_field(&Some(array.as_boolean().value(row))),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            encode_string_field(encoder, array, row)
        }
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            encode_date_or_timestamp_field(encoder, array, row)
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let vec = arrow_list_to_vec(array, row);
            encoder.encode_field(&vec)
        }
        _ => encoder.encode_field(&Option::<&str>::None),
    }
}

/// Encode an Arrow decimal cell as digits with the point placed by the
/// column's scale.
///
/// Handles Decimal128, whose unscaled value divides in Rust, and Decimal256,
/// whose i256 value is rendered as digits and then has the point inserted.
/// Any other type is written as NULL.
fn encode_decimal_field(
    encoder: &mut DataRowEncoder,
    array: &dyn Array,
    row: usize,
) -> PgWireResult<()> {
    match array.data_type() {
        DataType::Decimal128(_precision, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw: i128 = arr.value(row);
            let s = format_decimal_i128(raw, decimal_fraction_digits(*scale));
            encoder.encode_field(&Some(s))
        }
        DataType::Decimal256(_precision, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let s =
                insert_decimal_point(&arr.value(row).to_string(), decimal_fraction_digits(*scale));
            encoder.encode_field(&Some(s))
        }
        _ => encoder.encode_field(&Option::<&str>::None),
    }
}

/// Encode an Arrow binary cell as bytea: the raw bytes in binary format, and
/// the `\x...` hex spelling in text format.
///
/// Handles Binary, `LargeBinary` and `FixedSizeBinary`. Any other type is written
/// as NULL.
fn encode_binary_field(
    encoder: &mut DataRowEncoder,
    array: &dyn Array,
    row: usize,
    format: FieldFormat,
) -> PgWireResult<()> {
    let bytes = match array.data_type() {
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(row),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(row),
        DataType::FixedSizeBinary(_) => array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(row),
        _ => return encoder.encode_field(&Option::<&str>::None),
    };
    match format {
        FieldFormat::Binary => encoder.encode_field(&Some(bytes)),
        FieldFormat::Text => {
            let s = hex_bytea(bytes);
            encoder.encode_field(&Some(s))
        }
    }
}

/// Encode an Arrow number cell as the `PostgreSQL` integer or float type that
/// `src/pg/arrow_map.rs` advertises for its Arrow type.
///
/// Handles Int8, Int16, Int32, Int64, `UInt8`, `UInt16`, `UInt32`, `UInt64`, Float32
/// and Float64. `UInt8`, `UInt16` and `UInt32` widen into the next signed type up,
/// which is what the advertised OID promises the client; `UInt64` has nothing
/// wider to widen into, as the comment on its arm explains. Any other type is
/// written as NULL.
fn encode_number_field(
    encoder: &mut DataRowEncoder,
    array: &dyn Array,
    row: usize,
) -> PgWireResult<()> {
    match array.data_type() {
        DataType::Int8 => encoder.encode_field(&Some(i16::from(
            array
                .as_primitive::<arrow::array::types::Int8Type>()
                .value(row),
        ))),
        DataType::Int16 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::Int16Type>()
                .value(row),
        )),
        DataType::Int32 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::Int32Type>()
                .value(row),
        )),
        DataType::Int64 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::Int64Type>()
                .value(row),
        )),
        DataType::UInt8 => encoder.encode_field(&Some(i16::from(
            array
                .as_primitive::<arrow::array::types::UInt8Type>()
                .value(row),
        ))),
        DataType::UInt16 => encoder.encode_field(&Some(i32::from(
            array
                .as_primitive::<arrow::array::types::UInt16Type>()
                .value(row),
        ))),
        DataType::UInt32 => encoder.encode_field(&Some(i64::from(
            array
                .as_primitive::<arrow::array::types::UInt32Type>()
                .value(row),
        ))),
        // Not a bounded conversion: a UInt64 value above i64::MAX is reinterpreted
        // as a negative i64 and the client is told a wrong number. It cannot be
        // encoded correctly here because src/pg/arrow_map.rs advertises UInt64
        // columns as INT8 and PostgreSQL has no wider integer type, so the value
        // has nowhere to go on the wire. Sending it right needs the column
        // advertised as a different type (NUMERIC), which is a wire-format change
        // and out of scope here.
        DataType::UInt64 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::UInt64Type>()
                .value(row)
                .cast_signed(),
        )),
        DataType::Float32 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::Float32Type>()
                .value(row),
        )),
        DataType::Float64 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::Float64Type>()
                .value(row),
        )),
        _ => encoder.encode_field(&Option::<&str>::None),
    }
}

/// Encode an Arrow string cell as `PostgreSQL` text.
///
/// Handles Utf8, `LargeUtf8` and `Utf8View`. `DataFusion` 54 returns many
/// `information_schema`/`pg_catalog` string columns as `Utf8View` (for example
/// `information_schema.columns.column_name`), so leaving that type out would
/// turn those columns into NULL on the wire. Any other type is written as
/// NULL.
fn encode_string_field(
    encoder: &mut DataRowEncoder,
    array: &dyn Array,
    row: usize,
) -> PgWireResult<()> {
    match array.data_type() {
        DataType::Utf8 => encoder.encode_field(&Some(array.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => encoder.encode_field(&Some(array.as_string::<i64>().value(row))),
        DataType::Utf8View => encoder.encode_field(&Some(array.as_string_view().value(row))),
        _ => encoder.encode_field(&Option::<&str>::None),
    }
}

/// Encode an Arrow date or timestamp cell as the chrono value pgwire turns
/// into a `PostgreSQL` date or timestamp.
///
/// Handles Date32 (days since the epoch), Date64 (milliseconds since the
/// epoch) and Timestamp in all four Arrow time units. A date or instant chrono
/// cannot represent is written as NULL, which costs one value rather than the
/// whole row. Any other type is written as NULL too.
fn encode_date_or_timestamp_field(
    encoder: &mut DataRowEncoder,
    array: &dyn Array,
    row: usize,
) -> PgWireResult<()> {
    match array.data_type() {
        DataType::Date32 => {
            let days = i64::from(
                array
                    .as_primitive::<arrow::array::types::Date32Type>()
                    .value(row),
            );
            let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days);
            encoder.encode_field(&Some(date))
        }
        DataType::Date64 => {
            let ms = array
                .as_primitive::<arrow::array::types::Date64Type>()
                .value(row);
            match DateTime::from_timestamp_millis(ms) {
                Some(dt) => encoder.encode_field(&Some(dt)),
                None => encoder.encode_field(&Option::<&str>::None),
            }
        }
        DataType::Timestamp(unit, _) => {
            match timestamp_nanos_to_datetime(timestamp_value_as_nanos(array, row, *unit)) {
                Some(dt) => encoder.encode_field(&Some(dt)),
                None => encoder.encode_field(&Option::<&str>::None),
            }
        }
        _ => encoder.encode_field(&Option::<&str>::None),
    }
}

/// Render an i128 unscaled decimal value as text with `scale` fractional
/// digits, keeping the sign in front of the whole number.
///
/// The scale is a `u8` because it always comes from Arrow's `i8` scale, so the
/// power of ten below can never be asked for an exponent that overflows.
fn format_decimal_i128(val: i128, scale: u8) -> String {
    if scale == 0 {
        return val.to_string();
    }
    let neg = val < 0;
    let abs = if neg { -val } else { val };
    let ten_pow = 10i128.pow(u32::from(scale));
    let int_part = abs / ten_pow;
    let frac_part = abs % ten_pow;
    let s = format!(
        "{}.{:0width$}",
        int_part,
        frac_part,
        width = usize::from(scale)
    );
    if neg { format!("-{s}") } else { s }
}

/// Place a decimal point `scale` digits from the right of an already-rendered
/// integer string, padding with leading zeros when there are fewer digits than
/// the scale.
///
/// Used for Decimal256, whose values riffq only ever has as text: i256 has no
/// division here, so the point is inserted rather than computed.
fn insert_decimal_point(s: &str, scale: u8) -> String {
    let scale = usize::from(scale);
    let mut neg = false;
    let mut digits = s.to_string();
    if let Some(first) = digits.chars().next()
        && first == '-'
    {
        neg = true;
        digits.remove(0);
    }
    let len = digits.len();
    let result = if scale == 0 {
        digits
    } else if len > scale {
        format!(
            "{}.{:0width$}",
            &digits[..len - scale],
            digits[len - scale..].to_string(),
            width = scale
        )
    } else {
        // pad with leading zeros
        let mut tmp = String::from("0.");
        tmp.push_str(&"0".repeat(scale - len));
        tmp.push_str(&digits);
        tmp
    };
    if neg { format!("-{result}") } else { result }
}

/// Format bytes as `PostgreSQL`'s hex bytea text, a `\x` prefix followed by
/// lowercase hex digits - the representation every modern client expects when
/// a bytea column is sent in text format.
fn hex_bytea(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod encode_tests {
    use super::*;

    /// A Decimal128 column renders with its point placed by the column scale,
    /// keeps the sign in front, and reports a NULL cell as None.
    #[test]
    fn test_decimal128_to_string() {
        let dt = DataType::Decimal128(16, 6);
        let arr = Decimal128Array::from(vec![Some(123_456_789_i128), Some(-42i128), None])
            .with_data_type(dt.clone());
        let a: &dyn Array = &arr;
        assert_eq!(arrow_value_to_string(a, 0).as_deref(), Some("123.456789"));
        assert_eq!(arrow_value_to_string(a, 1).as_deref(), Some("-0.000042"));
        assert_eq!(arrow_value_to_string(a, 2), None);
    }

    /// An instant before 1970 splits into a floored second and a non-negative
    /// sub-second remainder, the only split chrono accepts.
    #[test]
    fn test_timestamp_before_epoch_keeps_positive_subsecond() {
        let dt = timestamp_nanos_to_datetime(-1_500_000_000).unwrap();
        assert_eq!(dt.timestamp(), -2);
        assert_eq!(dt.timestamp_subsec_nanos(), 500_000_000);
        assert_eq!(dt.timestamp_millis(), -1_500);
    }

    /// Arrow's negative decimal scale, which `PostgreSQL` numeric cannot express,
    /// renders the value unscaled instead of asking for a huge power of ten.
    #[test]
    fn test_negative_decimal_scale_renders_unscaled() {
        assert_eq!(decimal_fraction_digits(-2), 0);
        assert_eq!(
            format_decimal_i128(1234, decimal_fraction_digits(-2)),
            "1234"
        );
        assert_eq!(
            insert_decimal_point("1234", decimal_fraction_digits(-2)),
            "1234"
        );
    }

    /// A bytea column in text format is the `\x` prefix plus lowercase hex.
    #[test]
    fn test_binary_to_hex_text() {
        let arr = BinaryArray::from(vec![Some(&[0xab][..]), Some(&[0xde, 0xad, 0xbe, 0xef][..])]);
        let a: &dyn Array = &arr;
        assert_eq!(arrow_value_to_string(a, 0).as_deref(), Some("\\xab"));
        assert_eq!(arrow_value_to_string(a, 1).as_deref(), Some("\\xdeadbeef"));
    }
}

/// The single thread that owns the host's Python callbacks, and the channel
/// tokio tasks use to reach it.
///
/// Everything Python touches is funnelled through one thread so the callbacks
/// see a stable, serialised world regardless of how many connections are live.
/// `auth_cb` is also kept here, unsent, because the startup handler has to know
/// whether a password should be demanded before it has anything to ask about.
pub struct PythonWorker {
    sender: Sender<WorkerMessage>,
    auth_cb: CallbackSlot,
}

/// Where one of the host's Python callbacks lives: empty until the host
/// installs it, and shared between the worker thread and the API that installs
/// it, which is why it is behind an `Arc<Mutex<..>>`.
type CallbackSlot = Arc<Mutex<Option<Py<PyAny>>>>;

/// The Python object a host installed in `slot`, or None if it installed
/// nothing.
///
/// The object is cloned under the GIL so the caller can invoke it without
/// holding the slot's lock, which leaves the host free to replace the callback
/// while one of its calls is running.
///
/// # Panics
///
/// Panics if the slot's lock is poisoned, which means a previous caller
/// panicked while holding it.
fn clone_installed_callback(slot: &CallbackSlot) -> Option<Py<PyAny>> {
    Python::attach(|py| slot.lock().unwrap().as_ref().map(|cb| cb.clone_ref(py)))
}

/// Serve `WorkerMessage`s on the Python thread until the channel closes.
///
/// Initialises the interpreter and then handles one message at a time, so
/// every host callback is invoked from the same thread in the order the
/// messages were sent. The match below is the one place every `WorkerMessage`
/// variant is listed, so a variant that stops being answered - which would
/// leave its sender waiting on a responder forever - is still visible here.
///
/// Returns when the last `PythonWorker` is dropped and the channel closes.
fn run_python_worker_loop(
    receiver: &Receiver<WorkerMessage>,
    query_cb: &CallbackSlot,
    connect_cb: &CallbackSlot,
    disconnect_cb: &CallbackSlot,
    auth_cb: &CallbackSlot,
) {
    info!("[PY_WORKER] Thread started");
    pyo3::Python::initialize();
    loop {
        debug!("[PY_WORKER] waiting to receive on rx...");
        let Ok(msg) = receiver.recv() else {
            info!("[PY_WORKER] Channel closed");
            break;
        };
        match msg {
            WorkerMessage::Query {
                query,
                params,
                param_types,
                do_describe,
                connection_id,
                responder,
            } => run_query_callback(
                query_cb,
                &query,
                params.as_deref(),
                param_types.as_deref(),
                do_describe,
                connection_id,
                responder,
            ),
            WorkerMessage::Connect {
                connection_id,
                ip,
                port,
                server_name,
                responder,
            } => ask_connect_callback(
                connect_cb,
                connection_id,
                &ip,
                port,
                server_name.as_deref(),
                responder,
            ),
            WorkerMessage::Disconnect {
                connection_id,
                ip,
                port,
            } => announce_disconnect_to_callback(disconnect_cb, connection_id, &ip, port),
            WorkerMessage::Authentication {
                connection_id,
                user,
                database,
                host,
                password,
                responder,
            } => ask_authentication_callback(
                auth_cb,
                connection_id,
                user.as_deref(),
                database.as_deref(),
                &host,
                &password,
                responder,
            ),
        }
    }
}

/// Hand one statement to the host's `on_query` callback.
///
/// The callback is given a `CallbackWrapper` to answer through, so a host may
/// reply from another thread, at any later time. With no callback installed
/// the responder is dropped instead, which its sender reads as an empty
/// result: there is nobody who could run the query.
///
/// # Panics
///
/// Panics if Python rejects the argument objects riffq builds here, which
/// would mean the interpreter is in an unusable state.
fn run_query_callback(
    query_cb: &CallbackSlot,
    query: &str,
    params: Option<&[Option<Bytes>]>,
    param_types: Option<&[Type]>,
    do_describe: bool,
    connection_id: u64,
    responder: oneshot::Sender<QueryResult>,
) {
    debug!("[PY_WORKER] received query: {connection_id} -- {query}");
    let Some(cb) = clone_installed_callback(query_cb) else {
        return;
    };
    Python::attach(|py| {
        debug!("[PY_WORKER] GIL acquired, invoking callback");
        let wrapper = Py::new(
            py,
            CallbackWrapper {
                responder: Arc::new(Mutex::new(Some(responder))),
            },
        )
        .unwrap();

        let args = PyTuple::new(
            py,
            [
                query.into_py_any(py).unwrap(),
                wrapper.clone_ref(py).into_py_any(py).unwrap(),
            ],
        )
        .unwrap();
        let kwargs = PyDict::new(py);

        // Add do_describe flag
        kwargs.set_item("do_describe", do_describe).unwrap();

        // Connection identifier
        kwargs.set_item("connection_id", connection_id).unwrap();

        if let Some(params) = params {
            let py_args = bound_parameters_to_py_list(py, params, param_types);
            kwargs.set_item("query_args", py_args).unwrap();
        }

        if let Err(e) = cb.call(py, args, Some(&kwargs)) {
            e.print(py);
        }
    });
}

/// Decode the parameter values bound to one statement into a Python list, in
/// the order the client sent them.
///
/// Iterates the values (rather than zipping them with the declared types): a
/// client may send more values than declared types -- psqlodbc sends parameter
/// values with no types at all -- and every value must still reach the
/// handler. A value with no declared type is decoded as `Type::UNKNOWN`, which
/// `decode_param` reads as text. A value riffq cannot decode becomes Python
/// None.
///
/// # Panics
///
/// Panics if appending to a freshly created Python list fails, which would
/// mean the interpreter is in an unusable state.
fn bound_parameters_to_py_list<'py>(
    py: Python<'py>,
    params: &[Option<Bytes>],
    param_types: Option<&[Type]>,
) -> Bound<'py, PyList> {
    let py_args = PyList::empty(py);
    for (index, val) in params.iter().enumerate() {
        let ty = param_types
            .and_then(|types| types.get(index))
            .unwrap_or(&Type::UNKNOWN);
        let decoded = match val {
            None => None,
            Some(bytes) => decode_param(py, &bytes[..], ty),
        };
        match decoded {
            Some(obj) => py_args.append(obj).unwrap(),
            None => py_args.append(py.None()).unwrap(),
        }
    }
    py_args
}

/// Ask the host's `on_connect` callback whether to admit a client.
///
/// The callback answers through a `BoolCallbackWrapper`, so a host may decide
/// asynchronously. With no callback installed every client is admitted, which
/// is what a server that never asked to vet connections expects.
///
/// # Panics
///
/// Panics if Python rejects the argument objects riffq builds here, which
/// would mean the interpreter is in an unusable state.
fn ask_connect_callback(
    connect_cb: &CallbackSlot,
    connection_id: u64,
    ip: &str,
    port: u16,
    server_name: Option<&str>,
    responder: oneshot::Sender<BoolCallbackResult>,
) {
    let Some(cb) = clone_installed_callback(connect_cb) else {
        let _ = responder.send(BoolCallbackResult {
            allowed: true,
            error: None,
        });
        return;
    };
    Python::attach(|py| {
        let wrapper = Py::new(
            py,
            BoolCallbackWrapper {
                responder: Arc::new(Mutex::new(Some(responder))),
            },
        )
        .unwrap();
        let args = PyTuple::new(
            py,
            [
                connection_id.into_py_any(py).unwrap(),
                ip.into_py_any(py).unwrap(),
                port.into_py_any(py).unwrap(),
            ],
        )
        .unwrap();
        let kwargs = PyDict::new(py);
        kwargs.set_item("callback", wrapper.clone_ref(py)).unwrap();
        if let Some(name) = server_name {
            let _ = kwargs.set_item("server_name", name);
        } else {
            let _ = kwargs.set_item("server_name", py.None());
        }
        if let Err(e) = cb.call(py, args, Some(&kwargs)) {
            e.print(py);
        }
    });
}

/// Tell the host's `on_disconnect` callback that a client has gone.
///
/// Nothing waits on an answer: the connection is already closed by the time
/// this runs, so a host with no callback installed simply never hears about
/// it.
///
/// # Panics
///
/// Panics if Python rejects the argument objects riffq builds here, which
/// would mean the interpreter is in an unusable state.
fn announce_disconnect_to_callback(
    disconnect_cb: &CallbackSlot,
    connection_id: u64,
    ip: &str,
    port: u16,
) {
    let Some(cb) = clone_installed_callback(disconnect_cb) else {
        return;
    };
    Python::attach(|py| {
        let args = PyTuple::new(
            py,
            [
                connection_id.into_py_any(py).unwrap(),
                ip.into_py_any(py).unwrap(),
                port.into_py_any(py).unwrap(),
            ],
        )
        .unwrap();
        if let Err(e) = cb.call1(py, args) {
            e.print(py);
        }
    });
}

/// Ask the host's `on_authentication` callback to verify a cleartext password.
///
/// The callback answers through a `BoolCallbackWrapper`, so a host may check
/// the password asynchronously. With no callback installed every client is let
/// in: a server whose host never installed one does not ask for a password in
/// the first place.
///
/// # Panics
///
/// Panics if Python rejects the argument objects riffq builds here, which
/// would mean the interpreter is in an unusable state.
fn ask_authentication_callback(
    auth_cb: &CallbackSlot,
    connection_id: u64,
    user: Option<&str>,
    database: Option<&str>,
    host: &str,
    password: &str,
    responder: oneshot::Sender<BoolCallbackResult>,
) {
    let Some(cb) = clone_installed_callback(auth_cb) else {
        let _ = responder.send(BoolCallbackResult {
            allowed: true,
            error: None,
        });
        return;
    };
    Python::attach(|py| {
        let wrapper = Py::new(
            py,
            BoolCallbackWrapper {
                responder: Arc::new(Mutex::new(Some(responder))),
            },
        )
        .unwrap();
        let args = PyTuple::new(
            py,
            [
                connection_id.into_py_any(py).unwrap(),
                user.into_py_any(py).unwrap(),
                password.into_py_any(py).unwrap(),
                host.into_py_any(py).unwrap(),
            ],
        )
        .unwrap();
        let kwargs = PyDict::new(py);
        kwargs.set_item("callback", wrapper.clone_ref(py)).unwrap();
        if let Some(db) = database {
            kwargs.set_item("database", db).unwrap();
        }
        if let Err(e) = cb.call(py, args, Some(&kwargs)) {
            e.print(py);
        }
    });
}

impl PythonWorker {
    /// Start the worker thread and return the handle tasks send messages to.
    ///
    /// The thread initialises the Python interpreter and then serves messages
    /// until the channel closes, which happens when the last `PythonWorker` is
    /// dropped. Each callback is held behind its own lock so a host may still
    /// be installing them while the thread is already running.
    fn new(
        query_cb: CallbackSlot,
        connect_cb: CallbackSlot,
        disconnect_cb: CallbackSlot,
        auth_cb: CallbackSlot,
    ) -> Self {
        let (tx, rx) = channel::<WorkerMessage>();
        let auth_cb_thread = auth_cb.clone();
        thread::spawn(move || {
            run_python_worker_loop(&rx, &query_cb, &connect_cb, &disconnect_cb, &auth_cb_thread);
        });

        PythonWorker {
            sender: tx,
            auth_cb,
        }
    }

    /// Run `query` through the host's `on_query` callback and wait for its
    /// answer.
    ///
    /// With `do_describe` set, the host is asked only for the result schema, so
    /// an extended-protocol Describe does not execute the statement. A host that
    /// never calls its callback back leaves this future pending, which is what
    /// lets a handler answer from another thread.
    ///
    /// A worker that dies mid-query drops the responder; that is reported as an
    /// empty result rather than an error, so one lost query does not tear down
    /// the connection.
    ///
    /// # Panics
    ///
    /// Panics if the worker thread has exited and its receiver is gone, which
    /// leaves the server unable to answer anything.
    pub async fn on_query(
        &self,
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
    ) -> QueryResult {
        let (tx, rx) = oneshot::channel::<QueryResult>();
        debug!("[RUST] Sending query to worker: {query}");
        self.sender
            .send(WorkerMessage::Query {
                query,
                params,
                param_types,
                do_describe,
                connection_id,
                responder: tx,
            })
            .expect("Send failed!");

        rx.await.unwrap_or_else(|e| {
            error!("[RUST] Worker failed: {e:?}");
            QueryResult::Arrow(Vec::new(), Arc::new(Schema::empty()))
        })
    }

    /// Ask the host's `on_connect` callback whether to admit a client.
    ///
    /// `server_name` is the TLS SNI name the client asked for, so a host can
    /// route or refuse by hostname. A server with no `on_connect` callback
    /// admits everyone. A worker that dies before answering refuses the client,
    /// which is the safe direction when the host cannot be consulted.
    ///
    /// # Panics
    ///
    /// Panics if the worker thread has exited and its receiver is gone.
    pub async fn on_connect(
        &self,
        connection_id: u64,
        ip: String,
        port: u16,
        server_name: Option<&str>,
    ) -> BoolCallbackResult {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WorkerMessage::Connect {
                connection_id,
                ip,
                port,
                server_name: server_name.map(std::string::ToString::to_string),
                responder: tx,
            })
            .expect("Send failed!");
        rx.await.unwrap_or(BoolCallbackResult {
            allowed: false,
            error: None,
        })
    }

    /// Whether the host installed an `on_authentication` callback, and so
    /// whether the startup handler should demand a cleartext password.
    ///
    /// # Panics
    ///
    /// Panics if the callback lock is poisoned, which means a previous caller
    /// panicked while holding it.
    #[must_use]
    pub fn authentication_enabled(&self) -> bool {
        self.auth_cb.lock().unwrap().is_some()
    }

    /// Ask the host's `on_authentication` callback to verify a client's
    /// cleartext password.
    ///
    /// Only called for a server with authentication enabled. Unlike `on_query`
    /// and `on_connect` a failed send is not fatal here: it is reported as a
    /// refusal, so a dead worker denies access rather than crashing the
    /// connection.
    pub async fn on_authentication(
        &self,
        connection_id: u64,
        user: Option<String>,
        database: Option<String>,
        host: String,
        password: String,
    ) -> BoolCallbackResult {
        // info!("new authentication {} {}", connection_id, database.clone().unwrap_or_default());
        let (tx, rx) = oneshot::channel();
        let _ = self.sender.send(WorkerMessage::Authentication {
            connection_id,
            user,
            database,
            host,
            password,
            responder: tx,
        });
        rx.await.unwrap_or(BoolCallbackResult {
            allowed: false,
            error: None,
        })
    }

    /// Tell the host's `on_disconnect` callback that a client has gone.
    ///
    /// Returns as soon as the message is queued: nothing waits on the host's
    /// answer, and a send that fails because the worker is already shutting
    /// down is ignored, since there is no connection left to report it to.
    pub fn on_disconnect(&self, connection_id: u64, ip: String, port: u16) {
        let _ = self.sender.send(WorkerMessage::Disconnect {
            connection_id,
            ip,
            port,
        });
    }
}

/// What a connection's statements are run against.
///
/// The implementation is chosen once per connection: a catalog-emulating server
/// answers `pg_catalog` and `information_schema` itself and forwards only user
/// queries, while a plain server passes every statement straight to the host.
// async_trait marks the boxed future `must_use` on top of the `Result`'s
// own, which clippy 1.99 reports as a double `must_use`; the attribute is
// the macro's, not written here.
#[allow(clippy::double_must_use)]
#[async_trait]
trait QueryRunner: Send + Sync {
    /// Run one statement and produce its rows, its command tag, or its error.
    ///
    /// With `do_describe` set, only the result schema is wanted: the rows in the
    /// returned batches are ignored, so an implementation may skip producing
    /// them. `connection_id` is passed through to the host so it can attribute
    /// the statement to a session.
    async fn execute(
        &self,
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
    ) -> datafusion::error::Result<QueryResult>;
}

/// The three things a statement can produce.
///
/// `Error` is a value rather than an `Err` because it is the host's own
/// `PostgreSQL` error, meant to reach the client verbatim, not an internal
/// failure to be wrapped and reported as a riffq problem.
pub enum QueryResult {
    /// A result set: the batches and the schema describing them.
    Arrow(Vec<RecordBatch>, Arc<Schema>),
    /// A command tag such as "INSERT 0 1" for a statement that returns no rows.
    Tag(String),
    /// An error the host raised, to be sent to the client as-is.
    Error(Box<ErrorInfo>),
}

/// A host-raised `PostgreSQL` error travelling through `DataFusion`'s error
/// type.
///
/// `dispatch_query` only knows how to carry a `DataFusionError`, so a host error
/// is boxed as `DataFusionError::External` and downcast back out on the far
/// side. Without that round trip the client would be told riffq failed to plan
/// the query instead of being given the error the host actually raised.
#[derive(Debug)]
struct UserQueryError(Box<ErrorInfo>);

impl std::fmt::Display for UserQueryError {
    /// Show the wrapped `PostgreSQL` error, so a `UserQueryError` that is logged
    /// rather than unwrapped still says what went wrong.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for UserQueryError {}

/// The runner for a catalog-emulating connection: `dispatch_query` decides
/// which statements riffq's own catalog answers and which reach the host.
struct RouterQueryRunner {
    py_worker: Arc<PythonWorker>,
    /// The connection's catalog context, empty until its database is known.
    catalog_ctx: Arc<Mutex<Option<Arc<SessionContext>>>>,
}

#[async_trait]
impl QueryRunner for RouterQueryRunner {
    /// Route one statement through the catalog, sending whatever the catalog
    /// does not answer to the host.
    ///
    /// A host error is carried through `dispatch_query` as an external error and
    /// unwrapped here, so the client sees the host's own error rather than a
    /// planning failure. A host that replies with a command tag has it stashed
    /// aside, since `dispatch_query`'s handler can only return batches.
    async fn execute(
        &self,
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
    ) -> datafusion::error::Result<QueryResult> {
        // pgwire delivers the startup message, which names the database, before
        // any query, so this is a protocol violation rather than a state a
        // well-behaved client can reach.
        let ctx = self.catalog_ctx.lock().unwrap().clone().ok_or_else(|| {
            DataFusionError::Execution(
                "a query arrived before the connection selected a database".to_string(),
            )
        })?;
        let py_worker = self.py_worker.clone();

        let tag_holder = Arc::new(Mutex::new(None));
        let tag_clone = tag_holder.clone();

        let handler = move |_ctx: &SessionContext, sql: &str, p, t| {
            let py_worker = py_worker.clone();
            let tag_store = tag_clone.clone();
            let sql_owned = sql.to_string();
            async move {
                match py_worker
                    .on_query(sql_owned, p, t, do_describe, connection_id)
                    .await
                {
                    QueryResult::Arrow(b, s) => Ok((b, s)),
                    QueryResult::Tag(tag) => {
                        *tag_store.lock().unwrap() = Some(tag);
                        Ok((Vec::new(), Arc::new(Schema::empty())))
                    }
                    QueryResult::Error(e) => Err(datafusion::error::DataFusionError::External(
                        Box::new(UserQueryError(e)),
                    )),
                }
            }
        };

        let (batches, schema) =
            match dispatch_query(&ctx, &query, params, param_types, handler).await {
                Ok(v) => v,
                Err(datafusion::error::DataFusionError::External(e)) => {
                    match e.downcast::<UserQueryError>() {
                        Ok(user_err) => return Ok(QueryResult::Error(user_err.0)),
                        Err(e) => return Err(datafusion::error::DataFusionError::External(e)),
                    }
                }
                Err(e) => return Err(e),
            };

        if let Some(tag) = tag_holder.lock().unwrap().take() {
            Ok(QueryResult::Tag(tag))
        } else {
            Ok(QueryResult::Arrow(batches, schema))
        }
    }
}

/// The runner for a server started without catalog emulation: every statement,
/// including catalog queries, is the host's to answer.
struct DirectQueryRunner {
    py_worker: Arc<PythonWorker>,
}

#[async_trait]
impl QueryRunner for DirectQueryRunner {
    /// Hand the statement to the host unchanged and return its answer as it
    /// came, without re-planning or rebuilding the batches.
    async fn execute(
        &self,
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
    ) -> datafusion::error::Result<QueryResult> {
        Ok(
            self.py_worker
                .on_query(query, params, param_types, do_describe, connection_id)
                .await, // QueryResult, no rebuilding
        )
    }
}

/// One table declared through `register_table`: its database, schema and table
/// name, followed by its columns.
///
/// Each column is its own single-entry map of column name to definition, which
/// is how the Python caller spells a column list and what
/// `DeclaredCatalogSource` consumes, so the order of the list is the order of
/// the columns.
type RegisteredTable = (String, String, String, Vec<BTreeMap<String, ColumnDef>>);

/// The one catalog source a catalog-emulating server serves its databases
/// from, however the host described them.
///
/// A host either installs a live source (`set_lazy_catalog`), consulted afresh
/// on every catalog scan so a database created after startup is connectable
/// without a restart, or declares its objects up front (`register_database` /
/// `register_schema` / `register_table`), which are wrapped in a
/// [`DeclaredCatalogSource`]. Both are the SAME mechanism from here on: one
/// trait, one merge path, one set of semantics. The declared wrapper is what
/// retired the second, INSERT-based registration path.
struct CatalogSource {
    /// The source every context is built over, shared by all databases.
    source: Arc<dyn LazyCatalogSource>,
}

impl CatalogSource {
    /// The databases a client may connect to.
    ///
    /// Asked afresh every time rather than cached, because a live source can
    /// gain a database while the server runs and refusing to connect to it
    /// until a restart is the thing lazy building exists to avoid.
    fn connectable_databases(&self) -> DFResult<Vec<String>> {
        let mut names = Vec::new();
        self.source.databases(&mut |defs| {
            names.extend(defs.into_iter().map(|def| def.datname));
        })?;
        Ok(names)
    }

    /// Build one database's catalog context, in full, from scratch.
    ///
    /// Nothing is shared with another database's context and nothing is cloned
    /// from a common base: each carries its own built-in catalog, its own
    /// functions and its own views, planned against its own objects. That is
    /// what stops one connection from seeing another database's tables, and it
    /// is why `default_catalog` is the database name - `current_database()`
    /// reports the session's default catalog, so a context whose two names
    /// disagree serves one database's rows while naming another.
    ///
    /// The default schema is "public", matching `PostgreSQL`'s own default: the
    /// router substitutes it for "$user" when probing whether a name is a user
    /// table, and reports an unresolvable name under it.
    async fn build_context(&self, database: &str) -> DFResult<SessionContext> {
        let (ctx, _log) = get_base_session_context_with_lazy_catalog(
            None,
            database.to_string(),
            "public".to_string(),
            self.source.clone(),
            LazyCatalogOptions::all(),
            database.to_string(),
        )
        .await?;
        Ok(ctx)
    }
}

/// The catalog contexts a catalog-emulating server serves, one per database,
/// each built the first time a client connects to that database.
///
/// Building costs on the order of a second, so it is deferred until someone
/// actually asks for that database and then kept for the life of the server. A
/// context is never evicted: a database the host stops reporting keeps its
/// context until restart.
struct CatalogContexts {
    catalog_source: CatalogSource,
    /// One cell per database. The `Mutex` guards which databases have a cell;
    /// the cell itself guards the build.
    contexts: Mutex<HashMap<String, Arc<OnceCell<Arc<SessionContext>>>>>,
}

impl CatalogContexts {
    /// Wrap `catalog_source` with an empty context cache.
    fn new(catalog_source: CatalogSource) -> Self {
        Self {
            catalog_source,
            contexts: Mutex::new(HashMap::new()),
        }
    }

    /// The base context for `database`, building it if this is the first
    /// connection to it.
    ///
    /// Concurrent first connections to one database build it once. The `Mutex`
    /// is held only long enough to hand out that database's cell - never across
    /// the build - and `get_or_try_init` makes the losers wait for the winner
    /// instead of each building their own copy. A build that fails leaves the
    /// cell empty, so the next connection retries rather than inheriting a
    /// failure that may have been transient in the host's callback.
    async fn base_context(&self, database: &str) -> DFResult<Arc<SessionContext>> {
        let cell = {
            let mut contexts = self.contexts.lock().unwrap();
            contexts.entry(database.to_string()).or_default().clone()
        };

        cell.get_or_try_init(|| async {
            let started = std::time::Instant::now();
            let ctx = self.catalog_source.build_context(database).await?;
            info!(
                "built catalog context for database {} in {:?}",
                database,
                started.elapsed()
            );
            Ok(Arc::new(ctx))
        })
        .await
        .map(Arc::clone)
    }
}

/// The error a client gets for a database this server does not have.
///
/// `PostgreSQL` sends no hint here, but riffq does: naming the databases that do
/// exist turns the most likely mistake - connecting under a name the host never
/// registered - into a self-answering error.
fn database_does_not_exist(database: &str, connectable: &[String]) -> PgWireError {
    let mut error = ErrorInfo::new(
        "FATAL".to_string(),
        "3D000".to_string(),
        format!("database \"{database}\" does not exist"),
    );
    error.hint = Some(if connectable.is_empty() {
        "this server registered no databases; call register_database(name) or \
         set_lazy_catalog(source) before start(catalog_emulation=True)"
            .to_string()
    } else {
        format!("available databases: {}", connectable.join(", "))
    });
    PgWireError::UserError(Box::new(error))
}

/// The pgwire handler for one client connection: startup, simple queries and
/// the extended protocol are all served by the same instance.
///
/// One is built per accepted socket, because almost everything it holds is
/// per-connection state - the connection id, the database context, the runner.
pub struct RiffqProcessor {
    py_worker: Arc<PythonWorker>,
    conn_id_sender: Arc<Mutex<Option<oneshot::Sender<u64>>>>,
    query_runner: Arc<dyn QueryRunner>,
    /// The per-database contexts, or `None` when the server was started without
    /// `catalog_emulation`: then the host answers catalog queries itself and
    /// riffq has no catalog to isolate.
    catalog: Option<Arc<CatalogContexts>>,
    /// This connection's own context, installed once its database is known.
    ///
    /// `None` until then. A query cannot legitimately arrive first - pgwire
    /// delivers the startup message before anything else - so a query that finds
    /// it empty is a protocol violation and says so, rather than being served by
    /// whatever context happened to be lying around.
    ctx: Arc<Mutex<Option<Arc<SessionContext>>>>,
    server_version: String,
}

impl RiffqProcessor {
    /// This connection's session context, or None before the startup message
    /// has named a database for it.
    fn get_ctx(&self) -> Option<Arc<SessionContext>> {
        self.ctx.lock().unwrap().clone()
    }

    /// Admit this connection to `database`, give it a context, and record the
    /// role it authenticated as.
    ///
    /// Refuses a database the host never registered, as `PostgreSQL` does. Does
    /// nothing on a server started without `catalog_emulation`, which has no
    /// catalog and therefore no databases to admit anyone to.
    ///
    /// Takes the names rather than the client because `on_startup`'s bounds do
    /// not include `C: Sync`, so a borrow of the client cannot be held across an
    /// await point and they have to be copied out before this is called.
    async fn install_context_for_database(
        &self,
        database: Option<String>,
        user: Option<String>,
    ) -> PgWireResult<()> {
        let Some(catalog) = self.catalog.as_ref() else {
            return Ok(());
        };
        let Some(database) = database else {
            return Ok(());
        };

        // Already admitted. on_startup runs twice when authentication is
        // enabled - once for the Startup message and again for the password -
        // and the second pass must not rebuild or re-admit.
        if self.get_ctx().is_some() {
            return Ok(());
        }

        let connectable = catalog
            .catalog_source
            .connectable_databases()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if !connectable.contains(&database) {
            return Err(database_does_not_exist(&database, &connectable));
        }

        let base = catalog
            .base_context(&database)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        // A per-connection context over the shared base. The cached context is
        // never handed out directly: per-connection state lives in the session
        // config (ClientOpts) and in the identity UDFs registered below, so
        // sharing one context would leak the first connection's user and
        // settings to every later connection to the same database.
        //
        // The query runner reads the same cell, so writing it here is all that
        // is needed to point this connection's queries at the new context.
        let conn_ctx = Arc::new(SessionContext::new_with_state(base.state().clone()));

        // Authentication is riffq's job, so riffq is what tells the catalog who
        // connected. current_user / session_user / current_role read this back
        // out of the session config when they are called, including from inside
        // a view body planned long before this connection existed.
        if let Some(user) = user {
            set_session_user(&conn_ctx, &user).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }

        *self.ctx.lock().unwrap() = Some(conn_ctx);
        log::debug!("installed context for database {database}");
        Ok(())
    }

    /// Build a one-row, one-text-column response, the shape SHOW replies take.
    fn single_text_response(name: &str, value: &str, format: FieldFormat) -> Option<Response> {
        let fields = Arc::new(vec![FieldInfo::new(
            name.to_string(),
            None,
            None,
            Type::TEXT,
            format,
        )]);

        let mut encoder = DataRowEncoder::new(fields.clone());
        encoder.encode_field(&Some(value)).ok()?;
        let row = encoder.take_row();
        let rows = stream::iter(vec![Ok(row)]);
        Some(Response::Query(QueryResponse::new(fields, rows)))
    }

    /// Answer `SHOW <name>` from riffq's own session state, or None to let the
    /// host answer it.
    ///
    /// Only the variables riffq is the authority on are handled here - the ones
    /// that live in the connection's `ClientOpts` plus the server version and
    /// the isolation level. Anything else is not riffq's to report.
    fn show_variable_response(&self, name: &str, format: FieldFormat) -> Option<Response> {
        // transaction_isolation is answered without touching ClientOpts: the
        // value is fixed, and psqlodbc asks for it while connecting, before
        // anything has set client options.
        if name == TRANSACTION_ISOLATION_VARIABLE {
            return Self::single_text_response(name, TRANSACTION_ISOLATION_LEVEL, format);
        }

        // `SHOW ALL` lists every tracked variable rather than one value, so
        // it has its own multi-row response.
        if name == "all" {
            return self.show_all_variables_response(format);
        }

        // No context means no ClientOpts to read: either the server does not
        // emulate the catalog, or this variable was asked for before the startup
        // message named a database. Answering None lets the host handle it.
        let ctx = self.get_ctx()?;
        let state = ctx.state();
        let opts = state.config_options().extensions.get::<ClientOpts>()?;

        let value = match name {
            "application_name" => opts.application_name.as_str(),
            "datestyle" => opts.datestyle.as_str(),
            "search_path" => opts.search_path.as_str(),
            "timezone" => opts.timezone.as_str(),
            // `session_user` and its GUC spelling `session_authorization`
            // report the authenticated role, so `SHOW session_user` agrees
            // with `SELECT session_user` instead of reaching the host.
            "session_user" | "session_authorization" => opts.session_user.as_str(),
            "server_version" => self.server_version.as_str(),
            _ => return None,
        };

        Self::single_text_response(name, value, format)
    }

    /// Build the `SHOW ALL` result: one `name` / `setting` / `description`
    /// row per session variable riffq tracks, plus `server_version` and the
    /// fixed isolation level.
    ///
    /// Returns None when the connection has no catalog context to read, so
    /// the caller lets the host answer the statement.
    fn show_all_variables_response(&self, format: FieldFormat) -> Option<Response> {
        use datafusion::common::config::ExtensionOptions;

        let ctx = self.get_ctx()?;
        let state = ctx.state();
        let opts = state.config_options().extensions.get::<ClientOpts>()?;

        let fields = Arc::new(vec![
            FieldInfo::new("name".to_string(), None, None, Type::TEXT, format),
            FieldInfo::new("setting".to_string(), None, None, Type::TEXT, format),
            FieldInfo::new("description".to_string(), None, None, Type::TEXT, format),
        ]);

        let mut entries: Vec<(String, String, String)> = opts
            .entries()
            .into_iter()
            .map(|entry| {
                (
                    entry.key,
                    entry.value.unwrap_or_default(),
                    entry.description.to_string(),
                )
            })
            .collect();
        entries.push((
            "server_version".to_string(),
            self.server_version.clone(),
            String::new(),
        ));
        entries.push((
            TRANSACTION_ISOLATION_VARIABLE.to_string(),
            TRANSACTION_ISOLATION_LEVEL.to_string(),
            String::new(),
        ));

        let mut rows = Vec::with_capacity(entries.len());
        for (name, setting, description) in &entries {
            let mut encoder = DataRowEncoder::new(fields.clone());
            encoder.encode_field(&Some(name.as_str())).ok()?;
            encoder.encode_field(&Some(setting.as_str())).ok()?;
            encoder.encode_field(&Some(description.as_str())).ok()?;
            rows.push(Ok(encoder.take_row()));
        }
        Some(Response::Query(QueryResponse::new(
            fields,
            stream::iter(rows),
        )))
    }

    /// Answer a `SET` on a session variable the catalog stores, or None to
    /// let the host see the statement.
    ///
    /// This is what puts the `SET` command tag on the wire for the variables
    /// riffq is the authority on: forwarded to a host, a `SET search_path`
    /// either got whatever tag the host chose to supply or none at all, and
    /// the catalog's own session state never changed - `SHOW search_path`
    /// kept answering the old value (task 27). A `SET` on any other variable
    /// still reaches the host, which may be its real owner (a `DuckDB` host
    /// owns `memory_limit`), and a server without catalog emulation has no
    /// session state of its own, so every `SET` is the host's there.
    ///
    /// A `SET LOCAL` on an owned variable is acknowledged without applying:
    /// LOCAL scopes the value to the enclosing transaction, which riffq does
    /// not model, and `PostgreSQL` outside a transaction warns and changes
    /// nothing - so the tag without the effect is the faithful answer.
    ///
    /// `param_values` and `param_types` are the extended-protocol bindings
    /// of the statement, forwarded untouched: a driver that prepares its
    /// `SET` sends the value as `$1`, and the catalog can only apply the
    /// statement with the bound value in hand. The simple protocol passes
    /// `None` for both.
    async fn session_set_response(
        &self,
        sql: &str,
        param_values: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
    ) -> Option<PgWireResult<Response>> {
        let ctx = self.get_ctx()?;
        match classify_set_statement(sql) {
            SetStatementDisposition::ApplyToSession => Some(
                match execute_sql(&ctx, sql, param_values, param_types).await {
                    Ok(_) => Ok(Response::Execution(Tag::new("SET"))),
                    Err(e) => Err(PgWireError::ApiError(Box::new(e))),
                },
            ),
            SetStatementDisposition::AcknowledgeWithoutApplying => {
                Some(Ok(Response::Execution(Tag::new("SET"))))
            }
            SetStatementDisposition::NotASessionSet => None,
        }
    }

    /// The variable name of a statement that is exactly one `SHOW <name>`, or
    /// None for anything else.
    ///
    /// Parsing rather than string matching so spelling and whitespace variants
    /// all resolve to the same name; a multi-statement string or a SHOW with
    /// several parts is left alone for the normal query path.
    fn parse_show_variable(sql: &str) -> Option<String> {
        let dialect = PostgreSqlDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).ok()?;
        if statements.len() != 1 {
            return None;
        }
        match statements.pop()? {
            Statement::ShowVariable { variable } if variable.len() == 1 => {
                Some(variable[0].value.clone())
            }
            _ => None,
        }
    }
}

#[async_trait]
impl StartupHandler for RiffqProcessor {
    /// Carry the connection through the `PostgreSQL` startup handshake.
    ///
    /// Runs once for a server without authentication, and twice for one with
    /// it: first for the Startup message, which only asks the client for a
    /// password, and again when the password arrives. Both paths allocate the
    /// connection id, consult the host, admit the connection to its database,
    /// and only then send `ReadyForQuery`: after that the client believes it is
    /// connected and a refusal would arrive too late to stop it.
    ///
    /// The match below is the one place every startup message riffq answers is
    /// listed; each arm hands the message to the step of the handshake it
    /// belongs to.
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Set server version here using configured value (defaults to SERVER_VERSION)
        let mut params = DefaultServerParameterProvider::default();
        params.server_version = self.server_version.clone();

        // With authentication enabled the Startup message only asks the client
        // for a password, and this handler is called again with it. Noted here
        // because `message` is consumed by the match below.
        let is_startup_message = matches!(message, PgWireFrontendMessage::Startup(_));

        let outcome = match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                self.begin_startup_handshake(client, startup, &params)
                    .await?
            }
            PgWireFrontendMessage::PasswordMessageFamily(password_message) => {
                self.finish_password_handshake(client, password_message, &params)
                    .await?
            }
            // A message riffq does not answer here leaves the client exactly as
            // it was, still waiting on the handshake it started.
            _ => StartupHandshakeOutcome::ClientConnected,
        };

        // A rejected client has already been sent its error and had its socket
        // closed, so there is nothing left to say to it.
        if matches!(outcome, StartupHandshakeOutcome::ClientRejected) {
            return Ok(());
        }

        // Nothing below has a context to work with until the connection has been
        // admitted, and with authentication enabled that has not happened yet:
        // the Startup message only asked the client for a password, and this
        // handler runs again once it arrives.
        if is_startup_message && self.py_worker.authentication_enabled() {
            return Ok(());
        }

        let user = client.metadata().get(pgwire::api::METADATA_USER).cloned();
        let database = client
            .metadata()
            .get(pgwire::api::METADATA_DATABASE)
            .cloned();
        log::debug!("database: {database:?} {user:?}");

        Ok(())
    }
}

/// What a startup message left the client in: still talking to riffq, or told
/// why it was refused and disconnected.
///
/// Carried back to `on_startup` so it knows whether there is still a client to
/// go on with, instead of every refusal having to return out of the handler
/// itself.
enum StartupHandshakeOutcome {
    ClientConnected,
    ClientRejected,
}

impl RiffqProcessor {
    /// Answer the client's Startup message.
    ///
    /// With authentication enabled this only asks for a cleartext password and
    /// the handshake continues in `finish_password_handshake` once it arrives.
    /// Otherwise the host is asked whether to admit the client, and the
    /// handshake is completed here.
    async fn begin_startup_handshake<C>(
        &self,
        client: &mut C,
        startup: &Startup,
        params: &DefaultServerParameterProvider,
    ) -> PgWireResult<StartupHandshakeOutcome>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
        if self.py_worker.authentication_enabled() {
            client.set_state(PgWireConnectionState::AuthenticationInProgress);
            client
                .send(PgWireBackendMessage::Authentication(
                    Authentication::CleartextPassword,
                ))
                .await?;
            return Ok(StartupHandshakeOutcome::ClientConnected);
        }

        let id = self.assign_connection_id(client);
        let addr = client.socket_addr();
        // Obtain server_name (SNI) via pgwire ClientInfo helper
        let allowed = self
            .py_worker
            .on_connect(
                id,
                addr.ip().to_string(),
                addr.port(),
                client.sni_server_name(),
            )
            .await;
        if !allowed.allowed {
            refuse_client(client, allowed.error, "28000", "Connection rejected").await?;
            return Ok(StartupHandshakeOutcome::ClientRejected);
        }

        self.admit_connection_and_finish_handshake(client, params)
            .await?;
        Ok(StartupHandshakeOutcome::ClientConnected)
    }

    /// Answer the password the client sent after being asked for one.
    ///
    /// The host verifies the password first and is then asked whether to admit
    /// the client at all, so a server may accept a user's credentials and still
    /// refuse the connection.
    async fn finish_password_handshake<C>(
        &self,
        client: &mut C,
        password_message: PasswordMessageFamily,
        params: &DefaultServerParameterProvider,
    ) -> PgWireResult<StartupHandshakeOutcome>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let pwd = password_message.into_password()?;
        let id = self.assign_connection_id(client);

        let login_info = pgwire::api::auth::LoginInfo::from_client_info(client);
        let allowed = self
            .py_worker
            .on_authentication(
                id,
                login_info.user().map(std::string::ToString::to_string),
                login_info.database().map(std::string::ToString::to_string),
                login_info.host().to_string(),
                pwd.password,
            )
            .await;
        if !allowed.allowed {
            refuse_client(client, allowed.error, "28P01", "Authentication failed").await?;
            return Ok(StartupHandshakeOutcome::ClientRejected);
        }

        let addr = client.socket_addr();
        let allowed = self
            .py_worker
            .on_connect(
                id,
                addr.ip().to_string(),
                addr.port(),
                client.sni_server_name(),
            )
            .await;
        if !allowed.allowed {
            refuse_client(client, allowed.error, "28000", "Connection rejected").await?;
            return Ok(StartupHandshakeOutcome::ClientRejected);
        }

        self.admit_connection_and_finish_handshake(client, params)
            .await?;
        Ok(StartupHandshakeOutcome::ClientConnected)
    }

    /// Take the next connection id, record it in the client's metadata, and
    /// hand it to whoever asked to be told the id of the next connection.
    ///
    /// # Panics
    ///
    /// Panics if the `conn_id_sender` lock is poisoned, which means a previous
    /// caller panicked while holding it.
    fn assign_connection_id<C>(&self, client: &mut C) -> u64
    where
        C: ClientInfo,
    {
        let id = CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst);
        client
            .metadata_mut()
            .insert("connection_id".to_string(), id.to_string());
        if let Some(sender) = self.conn_id_sender.lock().unwrap().take() {
            let _ = sender.send(id);
        }
        id
    }

    /// Give the connection its database context and complete the handshake.
    ///
    /// The context is installed first on purpose: `finish_authentication` sends
    /// `ReadyForQuery`, after which the client considers itself connected and a
    /// refusal arrives too late to stop it.
    async fn admit_connection_and_finish_handshake<C>(
        &self,
        client: &mut C,
        params: &DefaultServerParameterProvider,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.install_context_for_database(
            client
                .metadata()
                .get(pgwire::api::METADATA_DATABASE)
                .cloned(),
            client.metadata().get(pgwire::api::METADATA_USER).cloned(),
        )
        .await?;
        finish_authentication(client, params).await
    }
}

/// Tell the client why it is not being let in, and close the connection.
///
/// `host_error` is the error the host supplied with its refusal; a host that
/// refused without wording it gets the generic FATAL error built from
/// `sqlstate` and `description` instead, so a refused client always learns
/// something rather than watching the socket drop.
async fn refuse_client<C>(
    client: &mut C,
    host_error: Option<Box<ErrorInfo>>,
    sqlstate: &str,
    description: &str,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: std::fmt::Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let err_info = host_error.unwrap_or_else(|| {
        Box::new(ErrorInfo::new(
            "FATAL".to_string(),
            sqlstate.to_string(),
            description.to_string(),
        ))
    });
    let error = ErrorResponse::from(*err_info);
    client
        .feed(PgWireBackendMessage::ErrorResponse(error))
        .await?;
    client.close().await?;
    Ok(())
}

impl RiffqProcessor {
    /// Run one statement of a simple-query batch and build its response.
    ///
    /// Split out of `do_query` so every statement in a batch goes through the
    /// same handling, including the SHOW special cases, rather than only the
    /// first one.
    async fn execute_simple_statement(
        &self,
        statement: &str,
        connection_id: u64,
    ) -> PgWireResult<Response> {
        // TODO: this should be up to the user to be handled here
        let lowercase = statement.trim().to_lowercase();
        // "SHOW TRANSACTION ISOLATION LEVEL" is three keywords rather than one
        // variable name, so it does not reach parse_show_variable below and is
        // matched here instead. Both spellings answer with the same value.
        if lowercase == "show transaction isolation level" {
            if let Some(resp) = Self::single_text_response(
                TRANSACTION_ISOLATION_VARIABLE,
                TRANSACTION_ISOLATION_LEVEL,
                FieldFormat::Text,
            ) {
                return Ok(resp);
            }
        } else if let Some(var) = Self::parse_show_variable(lowercase.as_str())
            && let Some(resp) = self.show_variable_response(&var.to_lowercase(), FieldFormat::Text)
        {
            return Ok(resp);
        }

        if let Some(response) = self.session_set_response(statement, None, None).await {
            return response;
        }

        let result = self
            .query_runner
            .execute(statement.to_string(), None, None, false, connection_id)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        match result {
            QueryResult::Arrow(batches, schema) => {
                // Simple query protocol always uses text format
                let formats: Vec<FieldFormat> = vec![FieldFormat::Text; schema.fields().len()];
                let (schema, data_row_stream) = arrow_to_pg_rows(batches, &schema, &formats);
                Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
            }
            QueryResult::Tag(tag) => Ok(Response::Execution(Tag::new(&tag))),
            QueryResult::Error(e) => Err(PgWireError::UserError(e)),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for RiffqProcessor {
    /// Run a simple-protocol Query message and produce one response per
    /// statement it contained.
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        debug!("[PGWIRE] do_query called with: {query}");
        let connection_id = client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        // One Query message may carry several statements separated by
        // semicolons; PostgreSQL runs each and replies with one response per
        // statement, which is the Vec this returns. Clients that build their
        // own SQL rely on it -- Npgsql sends its whole startup type-loading
        // batch this way -- so reading only the first statement makes the
        // server unusable for them.
        let statements = sql_batch::split_statements(query);
        if statements.is_empty() {
            return Ok(vec![Response::Execution(Tag::new(""))]);
        }

        let mut responses = Vec::with_capacity(statements.len());
        for statement in statements {
            // Returning on the first error abandons the rest of the batch,
            // which is how PostgreSQL treats a failure mid-batch.
            responses.push(
                self.execute_simple_statement(statement, connection_id)
                    .await?,
            );
        }
        Ok(responses)
    }
}

// pub struct MyExtendedQueryHandler {
//     query_runner: Arc<dyn QueryRunner>,
// }
/// A prepared statement as riffq keeps it: just the SQL text.
///
/// Nothing is parsed at Parse time because the host, not riffq, decides what a
/// statement means; the text is replayed to it at Describe and Execute.
#[derive(Clone)]
pub struct MyStatement {
    pub query: String,
}

/// The pgwire `QueryParser` for riffq, which stores statement text verbatim
/// instead of analysing it.
pub struct MyQueryParser;

/// Give every prepared-statement parameter a concrete `PostgreSQL` type.
///
/// pgwire reports a parameter the client left unspecified as None; riffq's query
/// path needs a type for each one, and UNKNOWN is what makes the decoder read
/// such a parameter as text - which is what an untyped parameter's bytes are.
fn resolve_param_types(types: &[Option<Type>]) -> Vec<Type> {
    // pgwire 0.40 reports each prepared-statement parameter type as Option<Type>
    // (None = the client left it unspecified). riffq's query path wants concrete
    // types, so fall back to UNKNOWN for any unspecified parameter, matching the
    // pre-0.40 behavior where parameter_types was a plain Vec<Type>.
    let mut resolved = Vec::with_capacity(types.len());

    for ty in types {
        resolved.push(ty.clone().unwrap_or(Type::UNKNOWN));
    }

    resolved
}

#[async_trait]
impl pgwire::api::stmt::QueryParser for MyQueryParser {
    /// What a Parse message turns into: the statement text, unexamined.
    type Statement = MyStatement;

    /// Store the SQL text of a Parse message without interpreting it.
    ///
    /// The declared parameter types are ignored here; the real types come from
    /// the portal at Bind time, where the client's actual values are known.
    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(MyStatement {
            query: sql.to_string(),
        })
    }

    /// Always empty: riffq answers Describe itself rather than through the
    /// parser.
    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        // riffq overrides do_describe_statement/portal with real schema lookup,
        // so pgwire's parser-driven describe auto-impl is unused; no static
        // parameter types to report.
        Ok(Vec::new())
    }

    /// Always empty, for the same reason as `get_parameter_types`.
    fn get_result_schema(
        &self,
        _stmt: &Self::Statement,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        // see get_parameter_types: the real schema is computed in do_describe_*.
        Ok(Vec::new())
    }
}

#[async_trait]
impl ExtendedQueryHandler for RiffqProcessor {
    /// The prepared statement riffq keeps between Parse and Execute.
    type Statement = MyStatement;
    /// The parser that produces it.
    type QueryParser = MyQueryParser;

    /// The parser pgwire should use for Parse messages on this connection.
    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(MyQueryParser)
    }

    /// Execute a bound portal and produce its response.
    ///
    /// The handful of statements clients send while connecting - an empty
    /// query, DISCARD ALL, and the SHOW variants - are answered here rather
    /// than sent to the host, which may not implement them. `max_rows` is
    /// ignored: riffq returns the whole result set, never a partial fetch.
    // _debug_parameters lives in helpers.rs under a leading underscore; it is a
    // real debugging aid, not an unused placeholder, and renaming it belongs to
    // that module.
    #[allow(clippy::used_underscore_items)]
    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = &portal.statement.statement.query;
        debug!(
            "[PGWIRE EXTENDED] do_query: {} {}",
            portal.statement.statement.query,
            _debug_parameters(
                &portal.parameters,
                &resolve_param_types(&portal.statement.parameter_types)
            )
        );

        // Lowercase a COPY for the command matching below, and run the query as
        // the client wrote it. Shadowing `query` with the lowercased text - which
        // this did - rewrites every string LITERAL in the statement too, so
        // `WHERE table_type = 'BASE TABLE'` silently matched nothing and
        // `SELECT 'MiXeD CaSe'` came back as 'mixed case'. The simple-query path
        // in execute_simple_statement already keeps the two separate.
        let query = query.trim();
        let lowercase = query.to_lowercase();

        if lowercase.is_empty() {
            return Ok(Response::Execution(Tag::new("")));
        } else if lowercase.starts_with("discard all") {
            return Ok(Response::Execution(Tag::new("DISCARD ALL")));
        } else if lowercase == "show transaction isolation level" {
            let field_infos = Arc::new(vec![FieldInfo::new(
                "transaction_isolation".to_string(),
                None,
                None,
                Type::TEXT,
                portal.result_column_format.format_for(0),
            )]);

            let mut encoder = DataRowEncoder::new(field_infos.clone());
            encoder.encode_field(&Some("read committed"))?;
            let row = encoder.take_row();
            let rows = stream::iter(vec![Ok(row)]);
            return Ok(Response::Query(QueryResponse::new(field_infos, rows)));
        } else if let Some(var) = Self::parse_show_variable(lowercase.as_str())
            && let Some(resp) = self.show_variable_response(
                &var.to_lowercase(),
                portal.result_column_format.format_for(0),
            )
        {
            return Ok(resp);
        }

        if let Some(response) = self
            .session_set_response(
                query,
                Some(portal.parameters.clone()),
                Some(resolve_param_types(&portal.statement.parameter_types)),
            )
            .await
        {
            return response;
        }

        let _ = max_rows; // currently unused until partial fetch is supported

        let connection_id = client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let result = self
            .query_runner
            .execute(
                query.to_string(),
                Some(portal.parameters.clone()),
                Some(resolve_param_types(&portal.statement.parameter_types)),
                false,
                connection_id,
            )
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        match result {
            QueryResult::Arrow(batches, schema) => {
                // Extract formats from portal for each column
                let formats: Vec<FieldFormat> = (0..schema.fields().len())
                    .map(|i| portal.result_column_format.format_for(i))
                    .collect();

                let (schema, data_row_stream) = arrow_to_pg_rows(batches, &schema, &formats);
                Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
            }
            QueryResult::Tag(tag) => Ok(Response::Execution(Tag::new(&tag))),
            QueryResult::Error(e) => Err(PgWireError::UserError(e)),
        }
    }

    /// Answer a Describe on a prepared statement with its parameter types and
    /// result columns.
    ///
    /// The host is asked in describe mode, so it produces the schema without
    /// running the statement. Every column is described as text format here:
    /// the client has not bound a portal yet, so it has not said what formats
    /// it wants.
    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Build response similar to do_describe_portal, but using the stored statement
        let connection_id = client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let query = &statement.statement.query;
        let param_types = resolve_param_types(&statement.parameter_types);

        let result = self
            .query_runner
            .execute(
                query.clone(),
                None,                      // no parameter values at statement describe
                Some(param_types.clone()), // but pass parameter type hints
                true,                      // describe mode to get only schema
                connection_id,
            )
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let schema = match result {
            QueryResult::Arrow(_, schema) => schema,
            QueryResult::Tag(_) => Arc::new(Schema::empty()),
            QueryResult::Error(e) => return Err(PgWireError::UserError(e)),
        };

        let fields: Vec<FieldInfo> = schema
            .fields()
            .iter()
            .map(|f| {
                FieldInfo::new(
                    f.name().clone(),
                    None,
                    None,
                    arrow_type_to_pgwire(f.data_type()),
                    FieldFormat::Text,
                )
            })
            .collect();
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    /// Answer a Describe on a bound portal with its result columns.
    ///
    /// Unlike the statement form, each column is described in the format the
    /// portal asked for, since the client's Bind message has already said
    /// which columns it wants in binary.
    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = &portal.statement.statement.query;

        let connection_id = client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let result = self
            .query_runner
            .execute(
                query.clone(),
                Some(portal.parameters.clone()),
                Some(resolve_param_types(&portal.statement.parameter_types)),
                true,
                connection_id,
            )
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let schema = match result {
            QueryResult::Arrow(_, schema) => schema,
            QueryResult::Tag(_) => Arc::new(Schema::empty()),
            QueryResult::Error(e) => return Err(PgWireError::UserError(e)),
        };
        let fields: Vec<FieldInfo> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                let format = portal.result_column_format.format_for(idx);
                FieldInfo::new(
                    f.name().clone(),
                    None,
                    None,
                    arrow_type_to_pgwire(f.data_type()),
                    format,
                )
            })
            .collect();
        info!("sending back the describe portal {fields:?}");
        Ok(DescribePortalResponse::new(fields))
    }
}

/// The set of handlers pgwire asks for when it serves one socket.
///
/// Both fields hold the same `RiffqProcessor`, because startup, simple queries
/// and the extended protocol all need the one connection's state.
struct RiffqProcessorFactory {
    handler: Arc<RiffqProcessor>,
    extended_handler: Arc<RiffqProcessor>,
}

impl PgWireServerHandlers for RiffqProcessorFactory {
    /// The handler for simple-protocol Query messages.
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    /// The handler for Parse/Bind/Describe/Execute.
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.extended_handler.clone()
    }

    /// The handler for the startup and authentication handshake.
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.handler.clone()
    }

    /// No COPY support: riffq answers a COPY attempt with pgwire's own
    /// unsupported-operation error rather than pretending to accept the data.
    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        Arc::new(NoopHandler)
    }

    /// No error post-processing: an error is sent to the client as built.
    fn error_handler(&self) -> Arc<impl pgwire::api::ErrorHandler> {
        Arc::new(NoopHandler)
    }
}

/// How long to wait for a client's first bytes when sniffing for a GSSAPI
/// encryption request. Well-behaved clients send SSLRequest/StartupMessage
/// immediately after connecting; on timeout the socket is handed to the
/// protocol handler untouched.
const GSSENCMODE_DETECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The request code a client sends to ask for GSSAPI encryption, as it appears
/// in the 8-byte request that precedes the startup message.
const GSSENC_REQUEST_CODE: u32 = 80_877_104;

/// Answer a client's GSSAPI encryption request with a refusal, so it falls back
/// to a plain or TLS connection instead of waiting for a reply riffq will never
/// send.
///
/// libpq defaults to `gssencmode=prefer` where GSSAPI is available, and asks
/// before anything else. Returns the socket for the protocol handler to serve;
/// a socket carrying no GSSAPI request is handed back untouched.
async fn detect_gssencmode(mut socket: TcpStream) -> Option<TcpStream> {
    let mut buf = [0u8; 8];

    // peek() waits until the client sends something, so it must be bounded:
    // a client that connects and stays silent would otherwise pin this future
    // until the peer goes away -- potentially forever if the peer vanishes
    // without FIN/RST.
    match tokio::time::timeout(GSSENCMODE_DETECT_TIMEOUT, socket.peek(&mut buf)).await {
        Ok(Ok(8)) => {
            let request_code = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
            if request_code == GSSENC_REQUEST_CODE {
                if let Err(e) = socket.read_exact(&mut buf).await {
                    error!("Failed to consume GSSAPI request: {e:?}");
                }
                if let Err(e) = socket.write_all(b"N").await {
                    error!("Failed to send rejection message: {e:?}");
                }
            }
        }
        Ok(_) => {}
        Err(_) => {
            debug!(
                "no data within gssencmode detection window; continuing without GSSAPI sniffing"
            );
        }
    }

    Some(socket)
}

/// Build the TLS acceptor a server uses, from a PEM certificate chain and
/// private key on disk.
///
/// Advertises the "postgresql" ALPN protocol, which clients that pin ALPN
/// require before they will complete the handshake. Every failure is an
/// `IOError` so `set_tls` can raise it in Python instead of aborting the
/// process.
fn setup_tls(cert_path: &str, key_path: &str) -> Result<TlsAcceptor, IOError> {
    let cert = certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<Vec<CertificateDer>, IOError>>()?;

    // private_key() understands PKCS#8, PKCS#1 and SEC1 PEM, so a "BEGIN RSA
    // PRIVATE KEY" file parses along with the other two forms.
    let key: PrivateKeyDer =
        private_key(&mut BufReader::new(File::open(key_path)?))?.ok_or_else(|| {
            IOError::new(
                ErrorKind::InvalidInput,
                format!("no PEM private key found in {key_path} (expected PKCS#8, PKCS#1 or SEC1)"),
            )
        })?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert, key)
        .map_err(|err| IOError::new(ErrorKind::InvalidInput, err))?;

    config.alpn_protocols = vec![b"postgresql".to_vec()];

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Convert a Python exception raised by a lazy catalog source into a
/// `DataFusionError`, so the failure propagates to the SQL client instead of
/// being swallowed (the lazy catalog contract forbids failing silently).
// Takes the error by value so it can be used directly as `map_err(py_to_df)`,
// which is how all of its callers reach it.
#[allow(clippy::needless_pass_by_value)]
fn py_to_df(e: PyErr) -> DataFusionError {
    DataFusionError::Execution(format!("lazy catalog source error: {e}"))
}

/// The synchronous callback object handed to a Python lazy-catalog method. The
/// Python source calls it with the list of rows it produced; we capture that
/// list so the surrounding Rust method can marshal it. Mirrors the
/// `&mut dyn FnMut(Vec<...>)` callback of the Rust `LazyCatalogSource` trait.
#[pyclass]
struct CatalogCallback {
    rows: Arc<Mutex<Option<Py<PyAny>>>>,
}

#[pymethods]
impl CatalogCallback {
    /// Record the rows the Python source passed in. Expected to be invoked once,
    /// synchronously, before the calling method returns.
    fn __call__(&self, rows: Py<PyAny>) {
        *self.rows.lock().unwrap() = Some(rows);
    }
}

/// Read a required integer field from a row dict.
fn req_i32(d: &Bound<'_, PyDict>, key: &str) -> DFResult<i32> {
    match d.get_item(key).map_err(py_to_df)? {
        Some(v) => v.extract::<i32>().map_err(py_to_df),
        None => Err(DataFusionError::Execution(format!(
            "lazy catalog row is missing required field '{key}'"
        ))),
    }
}

/// Read a required string field from a row dict.
fn req_str(d: &Bound<'_, PyDict>, key: &str) -> DFResult<String> {
    match d.get_item(key).map_err(py_to_df)? {
        Some(v) => v.extract::<String>().map_err(py_to_df),
        None => Err(DataFusionError::Execution(format!(
            "lazy catalog row is missing required field '{key}'"
        ))),
    }
}

/// Read a required boolean field from a row dict.
fn req_bool(d: &Bound<'_, PyDict>, key: &str) -> DFResult<bool> {
    match d.get_item(key).map_err(py_to_df)? {
        Some(v) => v.extract::<bool>().map_err(py_to_df),
        None => Err(DataFusionError::Execution(format!(
            "lazy catalog row is missing required field '{key}'"
        ))),
    }
}

/// Read an optional integer field from a row dict (absent or `None` -> `None`).
fn opt_i32(d: &Bound<'_, PyDict>, key: &str) -> DFResult<Option<i32>> {
    match d.get_item(key).map_err(py_to_df)? {
        Some(v) if !v.is_none() => Ok(Some(v.extract::<i32>().map_err(py_to_df)?)),
        _ => Ok(None),
    }
}

/// Read an optional string field from a row dict (absent or `None` -> `None`).
fn opt_str(d: &Bound<'_, PyDict>, key: &str) -> DFResult<Option<String>> {
    match d.get_item(key).map_err(py_to_df)? {
        Some(v) if !v.is_none() => Ok(Some(v.extract::<String>().map_err(py_to_df)?)),
        _ => Ok(None),
    }
}

/// Read an optional boolean field from a row dict, defaulting to `default` when
/// absent or `None`.
fn opt_bool_or(d: &Bound<'_, PyDict>, key: &str, default: bool) -> DFResult<bool> {
    match d.get_item(key).map_err(py_to_df)? {
        Some(v) if !v.is_none() => v.extract::<bool>().map_err(py_to_df),
        _ => Ok(default),
    }
}

/// Downcast the Python value a source returned into a list of row dicts.
fn row_dicts<'py>(method: &str, list: &Bound<'py, PyAny>) -> DFResult<Vec<Bound<'py, PyDict>>> {
    let list: &Bound<'py, PyList> = list.cast().map_err(|e| {
        DataFusionError::Execution(format!("{method}() must pass a list of dicts: {e}"))
    })?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter() {
        let d: Bound<'py, PyDict> = item.cast_into().map_err(|e| {
            DataFusionError::Execution(format!("{method}() rows must be dicts: {e}"))
        })?;
        out.push(d);
    }
    Ok(out)
}

/// Parse `databases()` rows: `{oid, name, [datdba]}` -> [`DatabaseDef`].
fn parse_databases(list: &Bound<'_, PyAny>) -> DFResult<Vec<DatabaseDef>> {
    row_dicts("databases", list)?
        .iter()
        .map(|d| {
            let oid = req_i32(d, "oid")?;
            let name = req_str(d, "name")?;
            let datdba = opt_i32(d, "datdba")?.unwrap_or(10);
            Ok(DatabaseDef::new(oid, name, datdba))
        })
        .collect()
}

/// Parse `schemas()` rows: `{oid, name, [owner_oid]}` -> [`SchemaDef`].
fn parse_schemas(list: &Bound<'_, PyAny>) -> DFResult<Vec<SchemaDef>> {
    row_dicts("schemas", list)?
        .iter()
        .map(|d| {
            Ok(SchemaDef {
                oid: req_i32(d, "oid")?,
                name: req_str(d, "name")?,
                owner_oid: opt_i32(d, "owner_oid")?,
            })
        })
        .collect()
}

/// Parse `relations()` rows: `{oid, reltype_oid, name, [kind]}` -> [`RelationDef`].
fn parse_relations(list: &Bound<'_, PyAny>) -> DFResult<Vec<RelationDef>> {
    row_dicts("relations", list)?
        .iter()
        .map(|d| {
            let kind = match opt_str(d, "kind")?.as_deref() {
                Some("table") | None => RelationKind::Table,
                Some("view") => RelationKind::View,
                Some("materialized_view" | "matview") => RelationKind::MaterializedView,
                Some(other) => {
                    return Err(DataFusionError::Execution(format!(
                        "unknown relation kind '{other}' (use table/view/materialized_view)"
                    )));
                }
            };
            Ok(RelationDef {
                oid: req_i32(d, "oid")?,
                reltype_oid: req_i32(d, "reltype_oid")?,
                name: req_str(d, "name")?,
                kind,
                owner_oid: opt_i32(d, "owner_oid")?,
                has_index: opt_bool_or(d, "has_index", false)?,
                has_rules: opt_bool_or(d, "has_rules", false)?,
                has_triggers: opt_bool_or(d, "has_triggers", false)?,
                row_security: opt_bool_or(d, "row_security", false)?,
            })
        })
        .collect()
}

/// Parse `columns()` rows: `{name, type_oid, nullable}` -> [`ColumnSpec`].
fn parse_columns(list: &Bound<'_, PyAny>) -> DFResult<Vec<ColumnSpec>> {
    row_dicts("columns", list)?
        .iter()
        .map(|d| {
            Ok(ColumnSpec::new(
                req_str(d, "name")?,
                req_i32(d, "type_oid")?,
                req_bool(d, "nullable")?,
            ))
        })
        .collect()
}

/// Parse `config()` rows: `{name, setting}` -> [`ConfigSettingDef`] (`pg_config`).
fn parse_config(list: &Bound<'_, PyAny>) -> DFResult<Vec<ConfigSettingDef>> {
    row_dicts("config", list)?
        .iter()
        .map(|d| {
            Ok(ConfigSettingDef {
                name: req_str(d, "name")?,
                setting: req_str(d, "setting")?,
            })
        })
        .collect()
}

/// Parse `settings()` rows: `{name, setting}` -> [`SettingDef`] (`pg_settings`).
fn parse_settings(list: &Bound<'_, PyAny>) -> DFResult<Vec<SettingDef>> {
    row_dicts("settings", list)?
        .iter()
        .map(|d| {
            Ok(SettingDef {
                name: req_str(d, "name")?,
                setting: req_str(d, "setting")?,
            })
        })
        .collect()
}

/// A [`LazyCatalogSource`] backed by a Python object whose `databases`,
/// `schemas`, `relations`, and `columns` methods each accept a callback and
/// invoke it with a list of row dicts. Each trait method acquires the GIL, hands
/// Python a [`CatalogCallback`], and marshals the captured rows into the
/// `pg_catalog` definition types. Errors raised in Python surface as
/// `DataFusionError` to the SQL client.
struct PyLazyCatalogSource {
    obj: Py<PyAny>,
}

impl PyLazyCatalogSource {
    /// Call `method` on the Python source with `str_args` followed by a fresh
    /// callback, returning whatever list the callback captured (or `None` if the
    /// source never invoked it).
    fn pull(&self, py: Python<'_>, method: &str, str_args: &[&str]) -> DFResult<Option<Py<PyAny>>> {
        let cell: Arc<Mutex<Option<Py<PyAny>>>> = Arc::new(Mutex::new(None));
        let wrapper = Py::new(py, CatalogCallback { rows: cell.clone() }).map_err(py_to_df)?;

        let mut items: Vec<Py<PyAny>> = Vec::with_capacity(str_args.len() + 1);
        for s in str_args {
            items.push((*s).into_py_any(py).map_err(py_to_df)?);
        }
        items.push(wrapper.into_py_any(py).map_err(py_to_df)?);
        let args = PyTuple::new(py, items).map_err(py_to_df)?;

        self.obj
            .bind(py)
            .call_method1(method, args)
            .map_err(py_to_df)?;

        let captured = cell.lock().unwrap().take();
        Ok(captured)
    }
}

impl LazyCatalogSource for PyLazyCatalogSource {
    /// The databases the Python source reports, feeding `pg_database`.
    fn databases(&self, callback: &mut dyn FnMut(Vec<DatabaseDef>)) -> DFResult<()> {
        let defs = Python::attach(|py| -> DFResult<Vec<DatabaseDef>> {
            match self.pull(py, "databases", &[])? {
                Some(list) => parse_databases(list.bind(py)),
                None => Ok(Vec::new()),
            }
        })?;
        callback(defs);
        Ok(())
    }

    /// The schemas one database contains, feeding `pg_namespace`.
    fn schemas(&self, database: &str, callback: &mut dyn FnMut(Vec<SchemaDef>)) -> DFResult<()> {
        let defs = Python::attach(|py| -> DFResult<Vec<SchemaDef>> {
            match self.pull(py, "schemas", &[database])? {
                Some(list) => parse_schemas(list.bind(py)),
                None => Ok(Vec::new()),
            }
        })?;
        callback(defs);
        Ok(())
    }

    /// The tables and views one schema contains, feeding `pg_class`.
    fn relations(
        &self,
        database: &str,
        schema: &str,
        callback: &mut dyn FnMut(Vec<RelationDef>),
    ) -> DFResult<()> {
        let defs = Python::attach(|py| -> DFResult<Vec<RelationDef>> {
            match self.pull(py, "relations", &[database, schema])? {
                Some(list) => parse_relations(list.bind(py)),
                None => Ok(Vec::new()),
            }
        })?;
        callback(defs);
        Ok(())
    }

    /// The columns of one relation, feeding `pg_attribute` and
    /// `information_schema.columns`.
    fn columns(
        &self,
        database: &str,
        schema: &str,
        relation: &str,
        callback: &mut dyn FnMut(Vec<ColumnSpec>),
    ) -> DFResult<()> {
        let defs = Python::attach(|py| -> DFResult<Vec<ColumnSpec>> {
            match self.pull(py, "columns", &[database, schema, relation])? {
                Some(list) => parse_columns(list.bind(py)),
                None => Ok(Vec::new()),
            }
        })?;
        callback(defs);
        Ok(())
    }

    /// The build-time settings the source overrides in `pg_config`. Optional:
    /// a source without the method keeps the built-in defaults.
    fn config(&self, callback: &mut dyn FnMut(Vec<ConfigSettingDef>)) -> DFResult<()> {
        let defs = Python::attach(|py| -> DFResult<Vec<ConfigSettingDef>> {
            // Optional method: a source without `config` keeps the built-in pg_config defaults.
            if !self.obj.bind(py).hasattr("config").map_err(py_to_df)? {
                return Ok(Vec::new());
            }
            match self.pull(py, "config", &[])? {
                Some(list) => parse_config(list.bind(py)),
                None => Ok(Vec::new()),
            }
        })?;
        callback(defs);
        Ok(())
    }

    /// The run-time settings the source overrides in `pg_settings`. Optional in
    /// the same way as `config`.
    fn settings(&self, callback: &mut dyn FnMut(Vec<SettingDef>)) -> DFResult<()> {
        let defs = Python::attach(|py| -> DFResult<Vec<SettingDef>> {
            // Optional method: a source without `settings` keeps the built-in pg_settings snapshot.
            if !self.obj.bind(py).hasattr("settings").map_err(py_to_df)? {
                return Ok(Vec::new());
            }
            match self.pull(py, "settings", &[])? {
                Some(list) => parse_settings(list.bind(py)),
                None => Ok(Vec::new()),
            }
        })?;
        callback(defs);
        Ok(())
    }
}

/// The Python-facing server object: `riffq.Server(addr)`.
///
/// Everything a host configures - the callbacks, TLS, and the catalog it
/// declares - is recorded here and only acted on when `start()` is called.
#[pyclass]
pub struct Server {
    addr: String,
    on_query_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    on_connect_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    on_disconnect_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    on_authentication_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    handle_shutdown_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    tls_acceptor: Arc<Mutex<Option<TlsAcceptor>>>,
    databases: Vec<String>,
    schemas: Vec<(String, String)>,
    tables: Vec<RegisteredTable>,
    lazy_catalog_source: Arc<Mutex<Option<Py<PyAny>>>>,
}

#[pymethods]
impl Server {
    /// Create a server that will listen on `addr` ("host:port").
    ///
    /// Nothing is bound and no thread is started until `start()` is called, so
    /// a Python caller can install callbacks and declare a catalog first.
    #[new]
    fn new(addr: String) -> Self {
        Server {
            addr,
            on_query_cb: Arc::new(Mutex::new(None)),
            on_connect_cb: Arc::new(Mutex::new(None)),
            on_disconnect_cb: Arc::new(Mutex::new(None)),
            on_authentication_cb: Arc::new(Mutex::new(None)),
            handle_shutdown_cb: Arc::new(Mutex::new(None)),
            tls_acceptor: Arc::new(Mutex::new(None)),
            databases: Vec::new(),
            schemas: Vec::new(),
            tables: Vec::new(),
            lazy_catalog_source: Arc::new(Mutex::new(None)),
        }
    }

    /// Install the query handler, called as `cb(sql, callback, do_describe=...,
    /// connection_id=..., query_args=[...])`.
    ///
    /// The handler answers by calling `callback` with a result; it may do so
    /// from another thread, which is how a host serves queries asynchronously.
    /// Required: `start()` refuses to run without one.
    fn on_query(&mut self, _py: Python, cb: Py<PyAny>) {
        *self.on_query_cb.lock().unwrap() = Some(cb);
    }

    /// Install the connect handler, called as `cb(connection_id, ip, port,
    /// callback=..., server_name=...)` once a client finishes the handshake.
    ///
    /// The handler calls `callback(True)` to admit the client or
    /// `callback(False, message=..., severity=..., sqlstate=...)` to turn it
    /// away. With no handler installed every client is admitted.
    fn on_connect(&mut self, _py: Python, cb: Py<PyAny>) {
        *self.on_connect_cb.lock().unwrap() = Some(cb);
    }

    /// Install the disconnect handler, called as `cb(connection_id, ip, port)`
    /// after a client's socket is closed. Nothing waits on its return.
    fn on_disconnect(&mut self, _py: Python, cb: Py<PyAny>) {
        *self.on_disconnect_cb.lock().unwrap() = Some(cb);
    }

    /// Install the authentication handler, called as `cb(connection_id, user,
    /// password, host, callback=..., database=...)`.
    ///
    /// Installing one is what turns authentication on: the server then demands
    /// a cleartext password from every client instead of admitting it straight
    /// away. The handler answers through `callback` exactly as `on_connect`
    /// does.
    fn on_authentication(&mut self, _py: Python, cb: Py<PyAny>) {
        *self.on_authentication_cb.lock().unwrap() = Some(cb);
    }

    /// Install the shutdown handler, called as `cb()` with no arguments.
    fn handle_shutdown(&mut self, _py: Python, cb: Py<PyAny>) {
        // Called once after the server stops accepting connections on SIGINT or
        // SIGTERM, letting Python flush/checkpoint state (e.g. DuckDB) before exit.
        *self.handle_shutdown_cb.lock().unwrap() = Some(cb);
    }

    /// Load the PEM certificate chain and private key the server presents when
    /// started with `tls=True`.
    ///
    /// Reads and validates both files immediately, so a bad path or an
    /// unreadable key raises `OSError` here rather than failing every client
    /// once the server is running. The key may be PKCS#8, PKCS#1 or SEC1.
    fn set_tls(&mut self, cert_path: &str, key_path: &str) -> PyResult<()> {
        match setup_tls(cert_path, key_path) {
            Ok(acceptor) => {
                *self.tls_acceptor.lock().unwrap() = Some(acceptor);
                Ok(())
            }
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }

    /// Install a lazy catalog source. `source` is a Python object whose
    /// `databases(callback)`, `schemas(database, callback)`,
    /// `relations(database, schema, callback)`, and
    /// `columns(database, schema, relation, callback)` methods each call their
    /// `callback` with a list of row dicts. The source is consulted afresh on
    /// every catalog scan, so `pg_catalog`/`information_schema` reflect it
    /// live.
    ///
    /// Composable with the `register_*` declarations: with both configured the
    /// catalog serves their UNION - declared base databases plus whatever the
    /// source reports, a database both contribute to merging its contents. A
    /// database or relation NAME both report is refused at scan time with an
    /// error naming it, so a runtime mount can never silently shadow a base
    /// the host pinned explicitly; a shared schema name merges to the declared
    /// side, which is how live tables mount into a declared database's public.
    /// Requires `start(catalog_emulation=True)` for the catalog queries to be
    /// routed here.
    fn set_lazy_catalog(&mut self, _py: Python, source: Py<PyAny>) {
        *self.lazy_catalog_source.lock().unwrap() = Some(source);
    }

    /// Declare a database clients may connect to.
    ///
    /// Every registered database appears in `pg_database` from any connection,
    /// which is what makes `\l` list them all. Composable with
    /// `set_lazy_catalog`: with both configured the declarations and the
    /// source are merged, and a name both report is refused at scan time.
    ///
    /// Raises `ValueError` for a name already registered, as `PostgreSQL`
    /// refuses CREATE DATABASE for an existing name.
    fn register_database(&mut self, database_name: String) -> PyResult<()> {
        if self.databases.contains(&database_name) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "database \"{database_name}\" already exists"
            )));
        }
        self.databases.push(database_name);
        Ok(())
    }

    /// Declare a schema inside an already registered database.
    ///
    /// Unlike databases, a schema is only visible from its own database's
    /// connections.
    ///
    /// Raises `ValueError` for a schema already registered under that
    /// database, as `PostgreSQL` refuses CREATE SCHEMA for an existing name.
    fn register_schema(&mut self, database_name: String, schema_name: String) -> PyResult<()> {
        if self
            .schemas
            .iter()
            .any(|(db, schema)| *db == database_name && *schema == schema_name)
        {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "schema \"{schema_name}\" already exists in database \"{database_name}\""
            )));
        }
        self.schemas.push((database_name, schema_name));
        Ok(())
    }

    /// Declare a table and its columns inside a database and schema.
    ///
    /// `columns` is a list of single-key dicts, `[{"id": {"type": "int4",
    /// "nullable": False}}, ...]`, in column order. Raises `ValueError` for a
    /// column dict with more than one key, missing `type`/`nullable`, or a
    /// table already registered under that schema - as `PostgreSQL` refuses
    /// CREATE TABLE for an existing name. The schema is created if
    /// `register_schema` did not already declare it.
    fn register_table(
        &mut self,
        _py: Python,
        database_name: String,
        schema_name: String,
        table_name: String,
        columns: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if self.tables.iter().any(|(db, schema, table, _)| {
            *db == database_name && *schema == schema_name && *table == table_name
        }) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "relation \"{table_name}\" already exists in schema \
                 \"{database_name}\".\"{schema_name}\""
            )));
        }
        let list: &Bound<'_, PyList> = columns.cast()?;
        let mut cols: Vec<BTreeMap<String, ColumnDef>> = Vec::new();
        for item in list.iter() {
            let mapping: &Bound<'_, PyDict> = item.cast()?;
            if mapping.len() != 1 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "each column must be a single-key dict",
                ));
            }
            let (name, def_obj) = mapping.iter().next().unwrap();
            let name_str: String = name.extract()?;
            let def_dict: &Bound<'_, PyDict> = def_obj.cast()?;
            let col_type: String = def_dict
                .get_item("type")?
                .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("missing type"))?
                .extract()?;
            let nullable: bool = def_dict
                .get_item("nullable")?
                .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("missing nullable"))?
                .extract()?;
            let mut m = BTreeMap::new();
            m.insert(
                name_str,
                ColumnDef {
                    col_type,
                    nullable,
                    has_default: false,
                },
            );
            cols.push(m);
        }
        self.tables
            .push((database_name, schema_name, table_name, cols));
        Ok(())
    }

    /// Bind the listen address and serve clients until the process is asked to
    /// stop.
    ///
    /// Blocks the calling Python thread, releasing the GIL so the worker thread
    /// can run the host's callbacks. Returns after SIGINT or SIGTERM, once
    /// `handle_shutdown` has run. Raises `ValueError` when
    /// `catalog_emulation=True` but no database was declared, and `OSError`
    /// when the address cannot be bound.
    ///
    /// With `catalog_emulation=True` riffq answers `pg_catalog` and
    /// `information_schema` queries from the declared catalog and forwards only
    /// user queries; otherwise every statement goes to the host.
    /// `server_version` overrides what clients are told during startup and by
    /// `SHOW server_version`.
    #[pyo3(signature = (tls=false, catalog_emulation=false, server_version=None, oid_registry_path=None))]
    fn start(
        &self,
        py: Python,
        tls: bool,
        catalog_emulation: bool,
        server_version: Option<String>,
        oid_registry_path: Option<String>,
    ) -> PyResult<()> {
        // A catalog-emulating server serves one context per registered database
        // and refuses any other, so with nothing registered it would bind a port
        // and then refuse every client. Say so now instead: unlike a lazy
        // source, which can gain a database while the server runs, the eager
        // registrations are fixed at this point and this can never come right.
        if catalog_emulation
            && self.databases.is_empty()
            && self.lazy_catalog_source.lock().unwrap().is_none()
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "start(catalog_emulation=True) needs at least one database: call \
                 register_database(name) for each one, or set_lazy_catalog(source) to \
                 report them from a live source",
            ));
        }

        // Declared objects get their oids from the registry. With no path the
        // in-memory registry stands in - stable within this run only - and
        // that fallback is deprecated, so the host is told rather than finding
        // out when a client's cached oids stop resolving after a restart.
        let declared_anything =
            !self.databases.is_empty() || !self.schemas.is_empty() || !self.tables.is_empty();
        if catalog_emulation && declared_anything && oid_registry_path.is_none() {
            let message = "no oid_registry_path given: declared objects use the in-memory \
                 oid registry, so their oids are not stable across server restarts. This \
                 default is deprecated and will become an error; pass \
                 start(oid_registry_path=...) to persist them.";
            log::warn!("{message}");
            PyErr::warn(
                py,
                &py.get_type::<pyo3::exceptions::PyDeprecationWarning>(),
                &std::ffi::CString::new(message).expect("static message has no NUL"),
                1,
            )?;
        }

        // surface a failed bind (e.g. the port is taken) as a python OSError
        // instead of panicking the worker process.
        py.detach(|| self.run_server(tls, catalog_emulation, server_version, oid_registry_path))
            .map_err(|err| pyo3::exceptions::PyOSError::new_err(err.to_string()))
    }

    /// Drive the tokio runtime that owns the listener, the Python worker thread
    /// and every connection task, for as long as the server runs.
    ///
    /// Separate from `start` so `start` can hold the Python-facing concerns -
    /// validating the arguments, releasing the GIL, turning an I/O failure into
    /// an `OSError` - and this can be plain Rust.
    ///
    /// # Panics
    ///
    /// Panics if no `on_query` callback was installed, since a server with no
    /// way to answer a query would accept clients and then hang on the first
    /// statement, and if the tokio runtime cannot be built.
    fn run_server(
        &self,
        tls: bool,
        catalog_emulation: bool,
        server_version: Option<String>,
        oid_registry_path: Option<String>,
    ) -> std::io::Result<()> {
        let addr = self.addr.clone();
        let query_cb = self.on_query_cb.clone();
        let connect_cb = self.on_connect_cb.clone();
        let disconnect_cb = self.on_disconnect_cb.clone();
        let auth_cb = self.on_authentication_cb.clone();
        let shutdown_cb = self.handle_shutdown_cb.clone();
        let server_version = server_version.unwrap_or_else(|| SERVER_VERSION.to_string());

        assert!(
            !query_cb.lock().unwrap().is_none(),
            "No callback set. Use on_query() before starting the server."
        );

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let py_worker = Arc::new(PythonWorker::new(
                query_cb,
                connect_cb,
                disconnect_cb,
                auth_cb,
            ));
            let catalog = self
                .catalog_to_serve(catalog_emulation, oid_registry_path.as_deref())
                .map_err(|error| std::io::Error::other(error.to_string()))?;

            let listener = bind_listener(&addr)?;
            info!("Listening on {addr}");

            let tls_acceptor = if tls {
                self.tls_acceptor.lock().unwrap().clone()
            } else {
                None
            };
            // The worker is cloned rather than moved so this scope keeps it
            // alive: the shutdown callback below runs after the accept task is
            // aborted, and dropping the last handle would close the worker
            // channel while it may still be needed.
            let server_task = tokio::spawn(accept_connections(
                listener,
                tls_acceptor,
                py_worker.clone(),
                catalog,
                server_version,
            ));

            wait_for_shutdown_signal().await;
            info!("Shutting down server");
            server_task.abort();
            run_shutdown_callback(&shutdown_cb);
            Ok(())
        })
    }
}

impl Server {
    /// The catalog this server will serve, or None when it is not emulating one
    /// and the host answers catalog queries itself.
    ///
    /// A live source and the `register_*` declarations are MERGED, not ranked:
    /// with both configured the catalog serves their union, a database or
    /// relation name both report refused at scan time and a shared schema name
    /// merging (how live tables mount into a declared database's public).
    ///
    /// Built here: the declared source itself - the oid registry is opened and
    /// every declared oid resolved, so a bad registry path or a duplicate
    /// declaration fails `start()`, not the first connection. The per-database
    /// CONTEXTS stay lazy: each is built the first time a client connects to
    /// that database, so the server binds immediately and a database the live
    /// source gains later needs no restart.
    ///
    /// # Panics
    ///
    /// Panics if the lazy-source lock is poisoned, which means a previous caller
    /// panicked while holding it.
    fn catalog_to_serve(
        &self,
        catalog_emulation: bool,
        oid_registry_path: Option<&str>,
    ) -> DFResult<Option<Arc<CatalogContexts>>> {
        if !catalog_emulation {
            return Ok(None);
        }
        let lazy_source = self
            .lazy_catalog_source
            .lock()
            .unwrap()
            .as_ref()
            .map(|o| Python::attach(|py| o.clone_ref(py)));

        // All origins produce the SAME kind of source. Up-front register_*
        // declarations are wrapped in a DeclaredCatalogSource, their oids
        // resolved through the registry so they survive restarts and
        // declaration reordering; a live Python object is wrapped as-is.
        //
        // A host may use BOTH: declared base databases plus a live source for
        // what appears at runtime. The two are then merged; a database or
        // relation name both report is refused at scan time, while a shared
        // schema name merges - that is how live tables mount into a declared
        // database's public.
        let declared_anything =
            !self.databases.is_empty() || !self.schemas.is_empty() || !self.tables.is_empty();
        let declared = || -> DFResult<Arc<DeclaredCatalogSource>> {
            let registry: Box<dyn CatalogOidRegistry> = match oid_registry_path {
                Some(path) => Box::new(RedbCatalogOidRegistry::open(std::path::Path::new(path))?),
                // Deprecated fallback; start() has already warned the host.
                None => Box::new(InMemoryCatalogOidRegistry::new()),
            };
            Ok(Arc::new(DeclaredCatalogSource::from_declarations(
                &self.databases,
                &self.schemas,
                &self.tables,
                registry.as_ref(),
            )?))
        };
        let source: Arc<dyn LazyCatalogSource> = match lazy_source {
            Some(obj) if declared_anything => Arc::new(CombinedCatalogSource::new(vec![
                declared()?,
                Arc::new(PyLazyCatalogSource { obj }),
            ])),
            Some(obj) => Arc::new(PyLazyCatalogSource { obj }),
            None => declared()?,
        };
        Ok(Some(Arc::new(CatalogContexts::new(CatalogSource {
            source,
        }))))
    }
}

/// Accept clients until the task is aborted, serving each on its own task.
///
/// An accept failure is logged and retried after a short pause rather than
/// ending the loop: accept fails transiently (EMFILE/ENFILE on fd exhaustion,
/// ECONNABORTED, ...), and returning here would drop the listener, leaving the
/// process alive with a dead port until a restart.
async fn accept_connections(
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
    py_worker: Arc<PythonWorker>,
    catalog: Option<Arc<CatalogContexts>>,
    server_version: String,
) {
    loop {
        let (socket, addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to accept connection: {e:?}; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };

        // The connection has no context yet: which database it belongs to
        // arrives in the startup message, and that database's context may not
        // be built.
        let conn_ctx: Arc<Mutex<Option<Arc<SessionContext>>>> = Arc::new(Mutex::new(None));

        // A catalog to serve means riffq answers catalog queries from it and
        // forwards only what it does not own; without one every statement goes
        // straight to the host.
        let query_runner: Arc<dyn QueryRunner> = if catalog.is_some() {
            Arc::new(RouterQueryRunner {
                py_worker: py_worker.clone(),
                catalog_ctx: conn_ctx.clone(),
            })
        } else {
            Arc::new(DirectQueryRunner {
                py_worker: py_worker.clone(),
            })
        };

        let tls_acceptor_ref = tls_acceptor.clone();
        let (id_tx, id_rx) = oneshot::channel();

        let handler = Arc::new(RiffqProcessor {
            ctx: conn_ctx,
            catalog: catalog.clone(),
            py_worker: py_worker.clone(),
            conn_id_sender: Arc::new(Mutex::new(Some(id_tx))),
            query_runner,
            server_version: server_version.clone(),
        });
        let factory = Arc::new(RiffqProcessorFactory {
            handler: handler.clone(),
            extended_handler: handler,
        });

        let py_worker_clone = py_worker.clone();
        let ip = addr.ip().to_string();
        let port = addr.port();

        tokio::spawn(async move {
            // detect_gssencmode waits for the client's first bytes, so it must
            // run inside the per-connection task rather than the accept loop:
            // awaiting it there would let one client that connects and sends
            // nothing block every accept, since the kernel keeps completing
            // handshakes into the listen backlog while this task waits on just
            // the one connection.
            let Some(socket) = detect_gssencmode(socket).await else {
                return;
            };
            if let Err(e) = process_socket(socket, tls_acceptor_ref, factory).await {
                error!("process_socket error: {e:?}");
            }
            let connection_id = id_rx.await.unwrap_or(0);
            py_worker_clone.on_disconnect(connection_id, ip, port);
        });
    }
}

/// Bind the listen socket for `addr` ("host:port", `IPv4` or `IPv6`).
fn bind_listener(addr: &str) -> std::io::Result<TcpListener> {
    // Bind with SO_REUSEADDR so a port left in TIME_WAIT by a just-stopped
    // server (its closed client connections) can be reused immediately. Without
    // it, restarting on the same port races "address already in use". A genuine
    // failure (port owned by another process, bad address) returns an Err so
    // start() can raise it as a python OSError instead of panicking.
    let socket_addr: std::net::SocketAddr = addr.parse().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid listen address: {addr}"),
        )
    })?;

    let socket = if socket_addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };

    socket.set_reuseaddr(true)?;
    socket.bind(socket_addr)?;
    socket.listen(1024)
}

/// Wait until the process is asked to stop.
///
/// # Panics
///
/// Panics if the SIGTERM handler cannot be installed or the SIGINT stream
/// fails, both of which would leave the server unable to shut down cleanly.
#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    // Resolves on SIGINT (ctrl-c) or SIGTERM (the signal kill/docker stop/k8s
    // send for graceful shutdown). tokio drives these through its own reactor,
    // so they fire even though the python main thread is parked in this runtime.
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .expect("Failed to install SIGTERM handler");

    tokio::select! {
        result = signal::ctrl_c() => {
            result.expect("Failed to listen for SIGINT");
        }
        _ = sigterm.recv() => {}
    }
}

/// Wait until the process is asked to stop.
///
/// # Panics
///
/// Panics if the console control handlers cannot be installed or the ctrl-c
/// stream fails.
#[cfg(windows)]
async fn wait_for_shutdown_signal() {
    // Windows has no SIGTERM, and tokio::signal::unix does not exist there, so
    // the equivalent "you are being asked to stop" events are console control
    // events: CTRL_CLOSE_EVENT when the console window is closed and
    // CTRL_SHUTDOWN_EVENT when the system is shutting down. Note the OS gives a
    // service only a few seconds after these before killing the process, so the
    // shutdown callback must be quick to finish on this platform.
    let mut ctrl_close =
        signal::windows::ctrl_close().expect("Failed to install CTRL_CLOSE handler");
    let mut ctrl_shutdown =
        signal::windows::ctrl_shutdown().expect("Failed to install CTRL_SHUTDOWN handler");

    tokio::select! {
        result = signal::ctrl_c() => {
            result.expect("Failed to listen for ctrl-c");
        }
        _ = ctrl_close.recv() => {}
        _ = ctrl_shutdown.recv() => {}
    }
}

/// Run the host's `handle_shutdown` callback, if one was installed.
///
/// # Panics
///
/// Panics if the callback lock is poisoned.
fn run_shutdown_callback(shutdown_cb: &Arc<Mutex<Option<Py<PyAny>>>>) {
    // Invokes the python handle_shutdown callback (if any), reacquiring the GIL
    // released by start()'s allow_threads. Errors are logged, not panicked, so
    // a failing callback cannot abort shutdown.
    let cb = shutdown_cb.lock().unwrap();

    if let Some(callback) = cb.as_ref() {
        Python::attach(|py| {
            // a SIGINT leaves python's default handler pending, which would
            // otherwise surface as KeyboardInterrupt on the first bytecode of
            // the callback. consume it here so the callback runs cleanly.
            let _ = py.check_signals();

            if let Err(err) = callback.call0(py) {
                error!("handle_shutdown callback failed: {err:?}");
            }
        });
    }
}

/// Build the `riffq._riffq` extension module Python imports.
///
/// Also installs a logger, defaulting to the `info` level, so a host that never
/// configures logging still sees the server's startup and error messages;
/// `RUST_LOG` overrides it. Initialising twice is not an error, since a process
/// may import the module after something else has already set a logger.
#[pymodule]
fn _riffq(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();

    module.add_class::<Server>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Regression tests for the value-encoding paths.
    //!
    //! A panic in any of these would abort the client's connection rather than
    //! return an error, so the edge cases below are exercised directly instead
    //! of relying on the Python suites or the driver tiers to hit them.

    use super::{
        arrow_value_to_string, decimal_fraction_digits, format_decimal_i128,
        timestamp_nanos_to_datetime,
    };
    use arrow::array::{Array, TimestampNanosecondArray};

    /// A pre-1970 instant is a date, not a failure.
    ///
    /// The nanosecond count is negative before the epoch, and splitting it with
    /// truncating division left a negative sub-second remainder that chrono
    /// rejects. Euclidean division floors instead, so the remainder stays
    /// non-negative and the second moves back by one.
    #[test]
    fn timestamp_before_the_epoch_converts() {
        // 1969-07-20T20:17:40Z, negative because it precedes 1970.
        let nanos = -14_182_940i128 * 1_000_000_000;
        let moment = timestamp_nanos_to_datetime(nanos).expect("a representable pre-epoch instant");
        assert_eq!(moment.to_string(), "1969-07-20 20:17:40 UTC");
    }

    /// One nanosecond before the epoch keeps its sub-second part.
    ///
    /// This is the case truncating division got most wrong: the remainder is
    /// -1, which as a u32 became 4294967295 and put chrono out of range.
    #[test]
    fn timestamp_one_nanosecond_before_the_epoch_converts() {
        let moment = timestamp_nanos_to_datetime(-1).expect("one nanosecond before the epoch");
        assert_eq!(moment.timestamp(), -1);
        assert_eq!(moment.timestamp_subsec_nanos(), 999_999_999);
    }

    /// The epoch itself, to confirm an ordinary instant is not shifted.
    #[test]
    fn timestamp_at_the_epoch_converts() {
        let moment = timestamp_nanos_to_datetime(0).expect("the epoch");
        assert_eq!(moment.to_string(), "1970-01-01 00:00:00 UTC");
    }

    /// An instant too far out for chrono is reported as absent, not panicked on.
    #[test]
    fn timestamp_beyond_chrono_range_is_absent() {
        assert!(timestamp_nanos_to_datetime(i128::MAX).is_none());
        assert!(timestamp_nanos_to_datetime(i128::MIN).is_none());
    }

    /// A pre-epoch timestamp column renders through the real encoder path.
    ///
    /// `timestamp_nanos_to_datetime` is only correct if `arrow_value_to_string`
    /// actually routes through it, so this exercises the encoder path itself
    /// rather than testing the conversion function in isolation.
    #[test]
    fn pre_epoch_timestamp_column_renders() {
        let column = TimestampNanosecondArray::from(vec![-14_182_940i64 * 1_000_000_000]);
        let rendered = arrow_value_to_string(&column, 0).expect("a rendered value");
        assert!(
            rendered.starts_with("1969-07-20 20:17:40"),
            "pre-epoch timestamp rendered as {rendered}"
        );
    }

    /// A NULL cell is still absent rather than rendered.
    #[test]
    fn null_timestamp_cell_is_absent() {
        let column = TimestampNanosecondArray::from(vec![None::<i64>]);
        assert!(column.is_null(0));
        assert!(arrow_value_to_string(&column, 0).is_none());
    }

    /// Arrow's negative scale means digits left of the point; `PostgreSQL` has no
    /// such thing, so the column renders unscaled instead of asking for a
    /// negative power of ten.
    #[test]
    fn negative_decimal_scale_renders_unscaled() {
        assert_eq!(decimal_fraction_digits(-2), 0);
        assert_eq!(decimal_fraction_digits(i8::MIN), 0);
        assert_eq!(
            format_decimal_i128(1234, decimal_fraction_digits(-2)),
            "1234"
        );
    }

    /// An ordinary scale is unchanged, including for a negative value.
    #[test]
    fn positive_decimal_scale_is_unchanged() {
        assert_eq!(decimal_fraction_digits(2), 2);
        assert_eq!(format_decimal_i128(1234, 2), "12.34");
        assert_eq!(format_decimal_i128(-1234, 2), "-12.34");
        assert_eq!(format_decimal_i128(5, 3), "0.005");
    }
}
