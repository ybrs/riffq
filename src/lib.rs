use async_trait::async_trait;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_pg_catalog::session::ClientOpts;
use futures::{Sink, SinkExt, Stream};
use log::{debug, error, info};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict, PyList, PyTuple};
use pyo3::{Bound, IntoPyObjectExt, PyAny};
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::signal;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::thread;

static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);

use bytes::Bytes;
use futures::stream;
use std::ffi::c_void;
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
use datafusion_pg_catalog::{
    ColumnDef, ColumnSpec, ConfigSettingDef, DatabaseDef, LazyCatalogOptions, LazyCatalogSource,
    RelationDef, RelationKind, SchemaDef, SettingDef, dispatch_query, get_base_session_context,
    get_base_session_context_with_lazy_catalog, register_schema, register_user_database,
    register_user_tables,
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
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

mod helpers;
pub mod pg;
use helpers::_debug_parameters;
use pg::arrow_type_to_pgwire;
use sqlparser::ast::Statement;
use sqlparser::parser::Parser;

/// PostgreSQL version reported to clients during startup and via `SHOW server_version`.
pub const SERVER_VERSION: &str = "17.4.0";

pub enum WorkerMessage {
    Query {
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
        responder: oneshot::Sender<QueryResult>,
    },
    Connect {
        connection_id: u64,
        ip: String,
        port: u16,
        server_name: Option<String>,
        responder: oneshot::Sender<BoolCallbackResult>,
    },
    Disconnect {
        connection_id: u64,
        ip: String,
        port: u16,
    },
    Authentication {
        connection_id: u64,
        user: Option<String>,
        database: Option<String>,
        host: String,
        password: String,
        responder: oneshot::Sender<BoolCallbackResult>,
    },
}

pub struct BoolCallbackResult {
    pub allowed: bool,
    pub error: Option<Box<ErrorInfo>>,
}

#[pyclass]
struct CallbackWrapper {
    responder: Arc<Mutex<Option<oneshot::Sender<QueryResult>>>>,
}

#[pyclass]
struct BoolCallbackWrapper {
    responder: Arc<Mutex<Option<oneshot::Sender<BoolCallbackResult>>>>,
}

#[pymethods]
impl BoolCallbackWrapper {
    #[pyo3(signature = (result, message=None, severity=None, sqlstate=None))]
    fn __call__(
        &self,
        result: Py<PyAny>,
        message: Option<String>,
        severity: Option<String>,
        sqlstate: Option<String>,
    ) {
        if let Some(sender) = self.responder.lock().unwrap().take() {
            Python::attach(|py| {
                let val: bool = result.extract(py).unwrap_or(false);
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
            });
        }
    }
}

#[pymethods]
impl CallbackWrapper {
    #[pyo3(signature = (result, *, is_tag=false, is_error=false))]
    fn __call__(&self, result: Py<PyAny>, is_tag: bool, is_error: bool) {
        if let Some(sender) = self.responder.lock().unwrap().take() {
            Python::attach(|py| {
                if is_tag {
                    let tag = result.extract::<String>(py).unwrap_or_default();
                    let ret = sender.send(QueryResult::Tag(tag));
                    if ret.is_err() {
                        error!("return for tag errored");
                    }
                    return;
                }
                if is_error {
                    let err_tuple = result
                        .extract::<(String, String, String)>(py)
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
                let result_bound = result.bind(py);
                let type_name = result_bound
                    .get_type()
                    .name()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                debug!("[RUST] result python type: {}", type_name);
                if let Ok(capsule) = result_bound.extract::<Bound<PyCapsule>>() {
                    debug!("[RUST] received PyCapsule");
                    let ptr = capsule.pointer() as *mut c_void;

                    // `ptr` is a live ArrowArrayStream* produced by PyArrow
                    // (CallbackWrapper received it directly from Python).
                    // ArrowArrayStreamReader takes ownership and will call `release`
                    // *when the reader itself is dropped*.  We read every batch,
                    // clone them into `batches`, clone the schema, and only then let
                    // `reader` fall out of scope, so the underlying C buffers stay
                    // alive as long as any RecordBatch/Schema clones do.  No
                    // use-after-free possible.
                    unsafe {
                        let mut reader = ArrowArrayStreamReader::from_raw(ptr as *mut _).unwrap();
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
                if let Ok(pybytes) = result_bound.extract::<Bound<pyo3::types::PyBytes>>() {
                    let data = pybytes.as_bytes();
                    let cursor = std::io::Cursor::new(data);
                    let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).unwrap();
                    let schema = reader.schema().clone();
                    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
                    let _ = sender.send(QueryResult::Arrow(batches, schema));
                    return;
                }

                // Fallback: assume (schema_desc, rows) tuple, build batches
                let parsed: PyResult<(Vec<HashMap<String, String>>, Vec<Vec<Py<PyAny>>>)> =
                    result_bound.extract::<(Vec<HashMap<String, String>>, Vec<Vec<Py<PyAny>>>)>();
                if let Ok((schema_desc, py_rows)) = parsed {
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

                    let mut builders: Vec<StringBuilder> =
                        fields.iter().map(|_| StringBuilder::new()).collect();

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
                    let _ = sender.send(QueryResult::Arrow(vec![batch], schema));
                }
            });
        }
    }
}

fn arrow_to_pg_rows(
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
    formats: &[FieldFormat],
) -> (
    Arc<Vec<FieldInfo>>,
    Pin<Box<dyn Stream<Item = PgWireResult<DataRow>> + Send>>,
) {
    // column metadata
    let field_defs: Arc<Vec<FieldInfo>> = Arc::new(
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                let format = formats.get(idx).copied().unwrap_or(FieldFormat::Text);
                FieldInfo::new(
                    f.name().clone().into(),
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
                            .map(|f| f.format())
                            .unwrap_or(FieldFormat::Text);
                        if let Err(e) = encode_arrow_value(&mut enc, col.as_ref(), row_idx, format)
                        {
                            return Some((Err(e), (row_idx + 1, remaining_batches)));
                        }
                    }
                    let row = enc.finish();
                    return Some((row, (row_idx + 1, remaining_batches)));
                }
            }
        }
    });

    // pin + box so it is Unpin
    (field_defs, Box::pin(row_stream))
}

fn arrow_value_to_string(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }

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
        DataType::Boolean => Some(array.as_boolean().value(row).to_string()),
        DataType::Utf8 => Some(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(row).to_string()),
        // DataFusion 54 returns many information_schema/pg_catalog string columns as
        // Utf8View (e.g. information_schema.columns.column_name); without this arm they
        // fall through to None and reach the client as NULL.
        DataType::Utf8View => Some(array.as_string_view().value(row).to_string()),
        DataType::Date32 => {
            let days = array
                .as_primitive::<arrow::array::types::Date32Type>()
                .value(row) as i64;
            let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days);
            Some(date.to_string())
        }
        DataType::Date64 => {
            let ms = array
                .as_primitive::<arrow::array::types::Date64Type>()
                .value(row);
            let dt = DateTime::from_timestamp(ms / 1000, (ms % 1000 * 1_000_000) as u32).unwrap();
            Some(dt.to_string())
        }
        DataType::Timestamp(unit, _) => {
            let nanos: i128 = match unit {
                TimeUnit::Second => {
                    array.as_primitive::<TimestampSecondType>().value(row) as i128 * 1_000_000_000
                }
                TimeUnit::Millisecond => {
                    array.as_primitive::<TimestampMillisecondType>().value(row) as i128 * 1_000_000
                }
                TimeUnit::Microsecond => {
                    array.as_primitive::<TimestampMicrosecondType>().value(row) as i128 * 1_000
                }
                TimeUnit::Nanosecond => {
                    array.as_primitive::<TimestampNanosecondType>().value(row) as i128
                }
            };
            let secs = (nanos / 1_000_000_000) as i64;
            let nsec = (nanos % 1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(secs, nsec).unwrap();
            Some(dt.to_string())
        }
        DataType::Decimal128(_p, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw: i128 = arr.value(row);
            Some(format_decimal_i128(raw, *scale as u32))
        }
        DataType::Decimal256(_p, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let s = arr.value(row).to_string();
            Some(insert_decimal_point(&s, *scale as usize))
        }
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
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list.value(row);
            let mut parts = Vec::new();
            for i in 0..values.len() {
                match arrow_value_to_string(values.as_ref(), i) {
                    Some(v) => parts.push(v),
                    None => parts.push("NULL".to_string()),
                }
            }
            Some(format!("{{{}}}", parts.join(",")))
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let values = list.value(row);
            let mut parts = Vec::new();
            for i in 0..values.len() {
                match arrow_value_to_string(values.as_ref(), i) {
                    Some(v) => parts.push(v),
                    None => parts.push("NULL".to_string()),
                }
            }
            Some(format!("{{{}}}", parts.join(",")))
        }
        DataType::FixedSizeList(_, _) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let values = list.value(row);
            let mut parts = Vec::new();
            for i in 0..values.len() {
                match arrow_value_to_string(values.as_ref(), i) {
                    Some(v) => parts.push(v),
                    None => parts.push("NULL".to_string()),
                }
            }
            Some(format!("{{{}}}", parts.join(",")))
        }
        _ => None,
    }
}

fn arrow_list_to_vec(array: &dyn Array, row: usize) -> Vec<String> {
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list.value(row);
            (0..values.len())
                .map(|i| arrow_value_to_string(values.as_ref(), i).unwrap_or_default())
                .collect()
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let values = list.value(row);
            (0..values.len())
                .map(|i| arrow_value_to_string(values.as_ref(), i).unwrap_or_default())
                .collect()
        }
        DataType::FixedSizeList(_, _) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let values = list.value(row);
            (0..values.len())
                .map(|i| arrow_value_to_string(values.as_ref(), i).unwrap_or_default())
                .collect()
        }
        _ => vec![],
    }
}

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
        DataType::Decimal128(_p, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw: i128 = arr.value(row);
            let s = format_decimal_i128(raw, *scale as u32);
            encoder.encode_field(&Some(s))
        }
        DataType::Decimal256(_p, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let s = insert_decimal_point(&arr.value(row).to_string(), *scale as usize);
            encoder.encode_field(&Some(s))
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = arr.value(row);
            match format {
                FieldFormat::Binary => encoder.encode_field(&Some(bytes)),
                FieldFormat::Text => {
                    let s = hex_bytea(bytes);
                    encoder.encode_field(&Some(s))
                }
            }
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let bytes = arr.value(row);
            match format {
                FieldFormat::Binary => encoder.encode_field(&Some(bytes)),
                FieldFormat::Text => {
                    let s = hex_bytea(bytes);
                    encoder.encode_field(&Some(s))
                }
            }
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let bytes = arr.value(row);
            match format {
                FieldFormat::Binary => encoder.encode_field(&Some(bytes)),
                FieldFormat::Text => {
                    let s = hex_bytea(bytes);
                    encoder.encode_field(&Some(s))
                }
            }
        }
        DataType::Int8 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::Int8Type>()
                .value(row) as i16,
        )),
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
        DataType::UInt8 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::UInt8Type>()
                .value(row) as i16,
        )),
        DataType::UInt16 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::UInt16Type>()
                .value(row) as i32,
        )),
        DataType::UInt32 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::UInt32Type>()
                .value(row) as i64,
        )),
        DataType::UInt64 => encoder.encode_field(&Some(
            array
                .as_primitive::<arrow::array::types::UInt64Type>()
                .value(row) as i64,
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
        DataType::Boolean => encoder.encode_field(&Some(array.as_boolean().value(row))),
        DataType::Utf8 => encoder.encode_field(&Some(array.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => encoder.encode_field(&Some(array.as_string::<i64>().value(row))),
        // DataFusion 54 returns many information_schema/pg_catalog string columns as
        // Utf8View (e.g. information_schema.columns.column_name); without this arm they
        // fall through to the NULL default and reach the client as NULL.
        DataType::Utf8View => encoder.encode_field(&Some(array.as_string_view().value(row))),
        DataType::Date32 => {
            let days = array
                .as_primitive::<arrow::array::types::Date32Type>()
                .value(row) as i64;
            let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days);
            encoder.encode_field(&Some(date))
        }
        DataType::Date64 => {
            let ms = array
                .as_primitive::<arrow::array::types::Date64Type>()
                .value(row);
            let dt = DateTime::from_timestamp(ms / 1000, (ms % 1000 * 1_000_000) as u32).unwrap();
            encoder.encode_field(&Some(dt))
        }
        DataType::Timestamp(unit, _) => {
            let nanos: i128 = match unit {
                TimeUnit::Second => {
                    array.as_primitive::<TimestampSecondType>().value(row) as i128 * 1_000_000_000
                }
                TimeUnit::Millisecond => {
                    array.as_primitive::<TimestampMillisecondType>().value(row) as i128 * 1_000_000
                }
                TimeUnit::Microsecond => {
                    array.as_primitive::<TimestampMicrosecondType>().value(row) as i128 * 1_000
                }
                TimeUnit::Nanosecond => {
                    array.as_primitive::<TimestampNanosecondType>().value(row) as i128
                }
            };
            let secs = (nanos / 1_000_000_000) as i64;
            let nsec = (nanos % 1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(secs, nsec).unwrap();
            encoder.encode_field(&Some(dt))
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let vec = arrow_list_to_vec(array, row);
            encoder.encode_field(&vec)
        }
        _ => encoder.encode_field(&Option::<&str>::None),
    }
}

// helper: format i128 with given scale into decimal string (handles sign)
fn format_decimal_i128(val: i128, scale: u32) -> String {
    if scale == 0 {
        return val.to_string();
    }
    let neg = val < 0;
    let abs = if neg { -val } else { val };
    let ten_pow = 10i128.pow(scale);
    let int_part = abs / ten_pow;
    let frac_part = abs % ten_pow;
    let s = format!("{}.{:0width$}", int_part, frac_part, width = scale as usize);
    if neg { format!("-{}", s) } else { s }
}

// helper: insert decimal point into a (possibly signed) integer string
fn insert_decimal_point(s: &str, scale: usize) -> String {
    let mut neg = false;
    let mut digits = s.to_string();
    if let Some(first) = digits.chars().next() {
        if first == '-' {
            neg = true;
            digits.remove(0);
        }
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
    if neg { format!("-{}", result) } else { result }
}

// helper: format bytea as Postgres hex text (\x...) lowercased
fn hex_bytea(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

#[cfg(test)]
mod encode_tests {
    use super::*;

    #[test]
    fn test_decimal128_to_string() {
        let dt = DataType::Decimal128(16, 6);
        let arr = Decimal128Array::from(vec![Some(123456789i128), Some(-42i128), None])
            .with_data_type(dt.clone());
        let a: &dyn Array = &arr;
        assert_eq!(arrow_value_to_string(a, 0).as_deref(), Some("123.456789"));
        assert_eq!(arrow_value_to_string(a, 1).as_deref(), Some("-0.000042"));
        assert_eq!(arrow_value_to_string(a, 2), None);
    }

    #[test]
    fn test_binary_to_hex_text() {
        let arr = BinaryArray::from(vec![Some(&[0xab][..]), Some(&[0xde, 0xad, 0xbe, 0xef][..])]);
        let a: &dyn Array = &arr;
        assert_eq!(arrow_value_to_string(a, 0).as_deref(), Some("\\xab"));
        assert_eq!(arrow_value_to_string(a, 1).as_deref(), Some("\\xdeadbeef"));
    }
}

pub struct PythonWorker {
    sender: Sender<WorkerMessage>,
    auth_cb: Arc<Mutex<Option<Py<PyAny>>>>,
}

impl PythonWorker {
    fn new(
        query_cb: Arc<Mutex<Option<Py<PyAny>>>>,
        connect_cb: Arc<Mutex<Option<Py<PyAny>>>>,
        disconnect_cb: Arc<Mutex<Option<Py<PyAny>>>>,
        auth_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    ) -> Self {
        let (tx, rx) = channel::<WorkerMessage>();
        let auth_cb_thread = auth_cb.clone();
        thread::spawn(move || {
            info!("[PY_WORKER] Thread started");
            pyo3::Python::initialize();
            loop {
                debug!("[PY_WORKER] waiting to receive on rx...");
                match rx.recv() {
                    Ok(msg) => match msg {
                        WorkerMessage::Query {
                            query,
                            params,
                            param_types,
                            do_describe,
                            connection_id,
                            responder,
                        } => {
                            debug!("[PY_WORKER] received query: {} -- {}", connection_id, query);
                            let cb_opt = Python::attach(|py| {
                                query_cb.lock().unwrap().as_ref().map(|cb| cb.clone_ref(py))
                            });

                            if let Some(cb) = cb_opt {
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
                                            query.clone().into_py_any(py).unwrap(),
                                            wrapper.clone_ref(py).into_py_any(py).unwrap(),
                                        ],
                                    )
                                    .unwrap();
                                    let kwargs = PyDict::new(py);

                                    // Add do_describe flag
                                    kwargs.set_item("do_describe", do_describe).unwrap();

                                    // Connection identifier
                                    kwargs.set_item("connection_id", connection_id).unwrap();

                                    // Add query_args if present
                                    if let (Some(params), Some(param_types)) =
                                        (&params, &param_types)
                                    {
                                        let py_args = PyList::empty(py);
                                        for (val, ty) in params.iter().zip(param_types.iter()) {
                                            match val {
                                                None => {
                                                    py_args.append(py.None()).unwrap();
                                                }
                                                Some(bytes) => {
                                                    let mut buf = &bytes[..];
                                                    match ty {
                                                        &Type::INT2 => {
                                                            if let Ok(v) =
                                                                i16::from_sql(ty, &mut buf)
                                                            {
                                                                py_args.append(v).unwrap();
                                                            } else {
                                                                py_args.append(py.None()).unwrap();
                                                            }
                                                        }
                                                        &Type::INT4 => {
                                                            if let Ok(v) =
                                                                i32::from_sql(ty, &mut buf)
                                                            {
                                                                py_args.append(v).unwrap();
                                                            } else {
                                                                py_args.append(py.None()).unwrap();
                                                            }
                                                        }
                                                        &Type::INT8 => {
                                                            if let Ok(v) =
                                                                i64::from_sql(ty, &mut buf)
                                                            {
                                                                py_args.append(v).unwrap();
                                                            } else {
                                                                py_args.append(py.None()).unwrap();
                                                            }
                                                        }
                                                        &Type::FLOAT4 => {
                                                            if let Ok(v) =
                                                                f32::from_sql(ty, &mut buf)
                                                            {
                                                                py_args.append(v).unwrap();
                                                            } else {
                                                                py_args.append(py.None()).unwrap();
                                                            }
                                                        }
                                                        &Type::FLOAT8 => {
                                                            if let Ok(v) =
                                                                f64::from_sql(ty, &mut buf)
                                                            {
                                                                py_args.append(v).unwrap();
                                                            } else {
                                                                py_args.append(py.None()).unwrap();
                                                            }
                                                        }
                                                        &Type::TEXT
                                                        | &Type::VARCHAR
                                                        | &Type::BPCHAR => {
                                                            if let Ok(v) =
                                                                String::from_sql(ty, &mut buf)
                                                            {
                                                                py_args.append(v).unwrap();
                                                            } else {
                                                                py_args.append(py.None()).unwrap();
                                                            }
                                                        }
                                                        _ => {
                                                            info!(
                                                                "unknown query argument type {:}?",
                                                                ty
                                                            );
                                                            py_args.append(py.None()).unwrap();
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        kwargs.set_item("query_args", py_args).unwrap();
                                    }

                                    if let Err(e) = cb.call(py, args, Some(&kwargs)) {
                                        e.print(py);
                                    }
                                });
                            }
                        }
                        WorkerMessage::Connect {
                            connection_id,
                            ip,
                            port,
                            server_name,
                            responder,
                        } => {
                            let cb_opt = Python::attach(|py| {
                                connect_cb
                                    .lock()
                                    .unwrap()
                                    .as_ref()
                                    .map(|cb| cb.clone_ref(py))
                            });
                            if let Some(cb) = cb_opt {
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
                                            ip.clone().into_py_any(py).unwrap(),
                                            port.into_py_any(py).unwrap(),
                                        ],
                                    )
                                    .unwrap();
                                    let kwargs = PyDict::new(py);
                                    kwargs.set_item("callback", wrapper.clone_ref(py)).unwrap();
                                    if let Some(name) = server_name.clone() {
                                        let _ = kwargs.set_item("server_name", name);
                                    } else {
                                        let _ = kwargs.set_item("server_name", py.None());
                                    }
                                    if let Err(e) = cb.call(py, args, Some(&kwargs)) {
                                        e.print(py);
                                    }
                                });
                            } else {
                                let _ = responder.send(BoolCallbackResult {
                                    allowed: true,
                                    error: None,
                                });
                            }
                        }
                        WorkerMessage::Disconnect {
                            connection_id,
                            ip,
                            port,
                        } => {
                            let cb_opt = Python::attach(|py| {
                                disconnect_cb
                                    .lock()
                                    .unwrap()
                                    .as_ref()
                                    .map(|cb| cb.clone_ref(py))
                            });
                            if let Some(cb) = cb_opt {
                                Python::attach(|py| {
                                    let args = PyTuple::new(
                                        py,
                                        [
                                            connection_id.into_py_any(py).unwrap(),
                                            ip.clone().into_py_any(py).unwrap(),
                                            port.into_py_any(py).unwrap(),
                                        ],
                                    )
                                    .unwrap();
                                    if let Err(e) = cb.call1(py, args) {
                                        e.print(py);
                                    }
                                });
                            }
                        }
                        WorkerMessage::Authentication {
                            connection_id,
                            user,
                            database,
                            host,
                            password,
                            responder,
                        } => {
                            let cb_opt = Python::attach(|py| {
                                auth_cb_thread
                                    .lock()
                                    .unwrap()
                                    .as_ref()
                                    .map(|cb| cb.clone_ref(py))
                            });
                            if let Some(cb) = cb_opt {
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
                                            user.clone().into_py_any(py).unwrap(),
                                            password.clone().into_py_any(py).unwrap(),
                                            host.clone().into_py_any(py).unwrap(),
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
                            } else {
                                let _ = responder.send(BoolCallbackResult {
                                    allowed: true,
                                    error: None,
                                });
                            }
                        }
                    },
                    Err(_) => {
                        info!("[PY_WORKER] Channel closed");
                        break;
                    }
                }
            }
        });

        PythonWorker {
            sender: tx,
            auth_cb,
        }
    }

    pub async fn on_query(
        &self,
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
    ) -> QueryResult {
        let (tx, rx) = oneshot::channel::<QueryResult>();
        debug!("[RUST] Sending query to worker: {}", query);
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
            error!("[RUST] Worker failed: {:?}", e);
            QueryResult::Arrow(Vec::new(), Arc::new(Schema::empty()))
        })
    }

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
                server_name: server_name.map(|s| s.to_string()),
                responder: tx,
            })
            .expect("Send failed!");
        rx.await.unwrap_or(BoolCallbackResult {
            allowed: false,
            error: None,
        })
    }

    pub fn authentication_enabled(&self) -> bool {
        self.auth_cb.lock().unwrap().is_some()
    }

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

    pub async fn on_disconnect(&self, connection_id: u64, ip: String, port: u16) {
        let _ = self.sender.send(WorkerMessage::Disconnect {
            connection_id,
            ip,
            port,
        });
    }
}

#[async_trait]
trait QueryRunner: Send + Sync {
    async fn execute(
        &self,
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
    ) -> datafusion::error::Result<QueryResult>;

    fn set_ctx(&self, _ctx: Arc<SessionContext>) {}
}

pub enum QueryResult {
    Arrow(Vec<RecordBatch>, Arc<Schema>),
    Tag(String),
    Error(Box<ErrorInfo>),
}

#[derive(Debug)]
struct UserQueryError(Box<ErrorInfo>);

impl std::fmt::Display for UserQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for UserQueryError {}

struct RouterQueryRunner {
    py_worker: Arc<PythonWorker>,
    catalog_ctx: Arc<Mutex<Arc<SessionContext>>>,
}

#[async_trait]
impl QueryRunner for RouterQueryRunner {
    async fn execute(
        &self,
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
    ) -> datafusion::error::Result<QueryResult> {
        let ctx = self.catalog_ctx.lock().unwrap().clone();
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

    fn set_ctx(&self, ctx: Arc<SessionContext>) {
        *self.catalog_ctx.lock().unwrap() = ctx;
    }
}

struct DirectQueryRunner {
    py_worker: Arc<PythonWorker>,
}

#[async_trait]
impl QueryRunner for DirectQueryRunner {
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

    fn set_ctx(&self, _ctx: Arc<SessionContext>) {}
}

pub struct RiffqProcessor {
    py_worker: Arc<PythonWorker>,
    conn_id_sender: Arc<Mutex<Option<oneshot::Sender<u64>>>>,
    query_runner: Arc<dyn QueryRunner>,
    ctx_map: Arc<HashMap<String, Arc<SessionContext>>>,
    ctx: Arc<Mutex<Arc<SessionContext>>>,
    server_version: String,
}

use datafusion::{
    common::ScalarValue,
    logical_expr::{ColumnarValue, Volatility, create_udf},
};

impl RiffqProcessor {
    fn get_ctx(&self) -> Arc<SessionContext> {
        self.ctx.lock().unwrap().clone()
    }

    fn update_ctx_from_client<C>(&self, client: &C)
    where
        C: ClientInfo + ?Sized,
    {
        if let Some(db) = client
            .metadata()
            .get(pgwire::api::METADATA_DATABASE)
            .cloned()
        {
            if let Some(base) = self.ctx_map.get(&db) {
                let new_ctx = Arc::new(SessionContext::new_with_state(base.state().clone()));
                *self.ctx.lock().unwrap() = new_ctx.clone();
                self.query_runner.set_ctx(new_ctx);
                log::debug!("updated context for db {}", db);
            }
        }
    }

    fn register_current_database<C>(&self, client: &C) -> datafusion::error::Result<()>
    where
        C: ClientInfo + ?Sized,
    {
        static KEY: &str = "current_database";

        let ctx = self.get_ctx();
        if ctx.state().scalar_functions().contains_key(KEY) {
            return Ok(());
        }

        if let Some(db) = client
            .metadata()
            .get(pgwire::api::METADATA_DATABASE)
            .cloned()
        {
            let fun = Arc::new(move |_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(db.clone()))))
            });
            let udf = create_udf(KEY, vec![], DataType::Utf8, Volatility::Stable, fun.clone());
            ctx.register_udf(udf);
            // udf.with_aliases("pg_catalog.current_database");
            let udf = create_udf(
                "pg_catalog.current_database",
                vec![],
                DataType::Utf8,
                Volatility::Stable,
                fun.clone(),
            );
            ctx.register_udf(udf);
        }

        Ok(())
    }

    fn register_session_user<C>(&self, client: &C) -> datafusion::error::Result<()>
    where
        C: ClientInfo + ?Sized,
    {
        static KEY: &str = "session_user";
        let ctx = self.get_ctx();
        if ctx.state().scalar_functions().contains_key(KEY) {
            return Ok(());
        }

        if let Some(user) = client.metadata().get(pgwire::api::METADATA_USER).cloned() {
            let fun = Arc::new(move |_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(user.clone()))))
            });
            let udf = create_udf(KEY, vec![], DataType::Utf8, Volatility::Stable, fun);
            ctx.register_udf(udf);
        }

        Ok(())
    }

    fn register_current_user<C>(&self, client: &C) -> datafusion::error::Result<()>
    where
        C: ClientInfo + ?Sized,
    {
        static KEY: &str = "current_user";

        let ctx = self.get_ctx();
        if ctx.state().scalar_functions().contains_key(KEY) {
            return Ok(());
        }

        if let Some(user) = client.metadata().get(pgwire::api::METADATA_USER).cloned() {
            let fun = Arc::new(move |_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(user.clone()))))
            });
            let udf = create_udf(KEY, vec![], DataType::Utf8, Volatility::Stable, fun.clone());
            ctx.register_udf(udf);
            let udf = create_udf(
                "pg_catalog.current_user",
                vec![],
                DataType::Utf8,
                Volatility::Stable,
                fun,
            );
            ctx.register_udf(udf);
        }

        Ok(())
    }

    fn show_variable_response(&self, name: &str, format: FieldFormat) -> Option<Response> {
        let ctx = self.get_ctx();
        let state = ctx.state();
        let opts = state.config_options().extensions.get::<ClientOpts>()?;

        let value = match name {
            "application_name" => opts.application_name.as_str(),
            "datestyle" => opts.datestyle.as_str(),
            "search_path" => opts.search_path.as_str(),
            "server_version" => self.server_version.as_str(),
            _ => return None,
        };

        let fields = Arc::new(vec![FieldInfo::new(
            name.to_string(),
            None,
            None,
            Type::TEXT,
            format,
        )]);

        let mut encoder = DataRowEncoder::new(fields.clone());
        encoder.encode_field(&Some(value)).ok()?;
        let row = encoder.finish().ok()?;
        let rows = stream::iter(vec![Ok(row)]);
        Some(Response::Query(QueryResponse::new(fields, rows)))
    }

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

        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                if self.py_worker.authentication_enabled() {
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::CleartextPassword,
                        ))
                        .await?;
                } else {
                    let id = CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst);
                    client
                        .metadata_mut()
                        .insert("connection_id".to_string(), id.to_string());
                    if let Some(sender) = self.conn_id_sender.lock().unwrap().take() {
                        let _ = sender.send(id);
                    }
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
                        let err_info = allowed.error.unwrap_or_else(|| {
                            Box::new(ErrorInfo::new(
                                "FATAL".to_string(),
                                "28000".to_string(),
                                "Connection rejected".to_string(),
                            ))
                        });
                        let error = ErrorResponse::from(*err_info);
                        client
                            .feed(PgWireBackendMessage::ErrorResponse(error))
                            .await?;
                        client.close().await?;
                        return Ok(());
                    }
                    finish_authentication(client, &params).await?;
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let id = CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst);
                client
                    .metadata_mut()
                    .insert("connection_id".to_string(), id.to_string());
                if let Some(sender) = self.conn_id_sender.lock().unwrap().take() {
                    let _ = sender.send(id);
                }

                let login_info = pgwire::api::auth::LoginInfo::from_client_info(client);
                let allowed = self
                    .py_worker
                    .on_authentication(
                        id,
                        login_info.user().map(|s| s.to_string()),
                        login_info.database().map(|s| s.to_string()),
                        login_info.host().to_string(),
                        pwd.password,
                    )
                    .await;
                if !allowed.allowed {
                    let err_info = allowed.error.unwrap_or_else(|| {
                        Box::new(ErrorInfo::new(
                            "FATAL".to_string(),
                            "28P01".to_string(),
                            "Authentication failed".to_string(),
                        ))
                    });
                    let error = ErrorResponse::from(*err_info);
                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                    return Ok(());
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
                    let err_info = allowed.error.unwrap_or_else(|| {
                        Box::new(ErrorInfo::new(
                            "FATAL".to_string(),
                            "28000".to_string(),
                            "Connection rejected".to_string(),
                        ))
                    });
                    let error = ErrorResponse::from(*err_info);
                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                    return Ok(());
                }

                finish_authentication(client, &params).await?;
            }
            _ => {}
        }

        let user = client.metadata().get(pgwire::api::METADATA_USER).cloned();
        let database = client
            .metadata()
            .get(pgwire::api::METADATA_DATABASE)
            .cloned();
        log::debug!("database: {:?} {:?}", database, user);
        self.update_ctx_from_client(client);

        let _ = self.register_current_database(client);
        let _ = self.register_session_user(client);
        let _ = self.register_current_user(client);

        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for RiffqProcessor {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // TODO: this should be up to the user to be handled here
        let trimmed = query.trim();
        let lowercase = trimmed.to_lowercase();
        if lowercase == "show transaction isolation level" {
            let field_infos = Arc::new(vec![FieldInfo::new(
                "transaction_isolation".to_string(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )]);

            let mut encoder = DataRowEncoder::new(field_infos.clone());
            encoder.encode_field(&Some("read committed"))?;
            let row = encoder.finish()?;

            let rows = stream::iter(vec![Ok(row)]);
            return Ok(vec![Response::Query(QueryResponse::new(field_infos, rows))]);
        } else if let Some(var) = Self::parse_show_variable(lowercase.as_str()) {
            if let Some(resp) = self.show_variable_response(&var.to_lowercase(), FieldFormat::Text)
            {
                return Ok(vec![resp]);
            }
        } else if lowercase == "" {
            return Ok(vec![Response::Execution(Tag::new(""))]);
        }

        debug!("[PGWIRE] do_query called with: {}", query);
        let connection_id = client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let result = self
            .query_runner
            .execute(query.to_string(), None, None, false, connection_id)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        match result {
            QueryResult::Arrow(batches, schema) => {
                // Simple query protocol always uses text format
                let formats: Vec<FieldFormat> = vec![FieldFormat::Text; schema.fields().len()];
                let (schema, data_row_stream) = arrow_to_pg_rows(batches, schema, &formats);
                Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    data_row_stream,
                ))])
            }
            QueryResult::Tag(tag) => Ok(vec![Response::Execution(Tag::new(&tag))]),
            QueryResult::Error(e) => Err(PgWireError::UserError(e)),
        }
    }
}

// pub struct MyExtendedQueryHandler {
//     query_runner: Arc<dyn QueryRunner>,
// }
#[derive(Clone)]
pub struct MyStatement {
    pub query: String,
}
pub struct MyQueryParser;

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
    type Statement = MyStatement;

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

    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        // riffq overrides do_describe_statement/portal with real schema lookup,
        // so pgwire's parser-driven describe auto-impl is unused; no static
        // parameter types to report.
        Ok(Vec::new())
    }

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
    type Statement = MyStatement;
    type QueryParser = MyQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(MyQueryParser)
    }

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

        let query = query.trim().to_lowercase();

        if query.is_empty() {
            return Ok(Response::Execution(Tag::new("")));
        } else if query.starts_with("discard all") {
            return Ok(Response::Execution(Tag::new("DISCARD ALL")));
        } else if query == "show transaction isolation level" {
            let field_infos = Arc::new(vec![FieldInfo::new(
                "transaction_isolation".to_string(),
                None,
                None,
                Type::TEXT,
                portal.result_column_format.format_for(0),
            )]);

            let mut encoder = DataRowEncoder::new(field_infos.clone());
            encoder.encode_field(&Some("read committed"))?;
            let row = encoder.finish()?;
            let rows = stream::iter(vec![Ok(row)]);
            return Ok(Response::Query(QueryResponse::new(field_infos, rows)));
        } else if let Some(var) = Self::parse_show_variable(query.as_str()) {
            if let Some(resp) = self.show_variable_response(
                &var.to_lowercase(),
                portal.result_column_format.format_for(0),
            ) {
                return Ok(resp);
            }
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

                let (schema, data_row_stream) = arrow_to_pg_rows(batches, schema, &formats);
                Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
            }
            QueryResult::Tag(tag) => Ok(Response::Execution(Tag::new(&tag))),
            QueryResult::Error(e) => Err(PgWireError::UserError(e)),
        }
    }

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
                query.to_string(),
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
                    f.name().clone().into(),
                    None,
                    None,
                    arrow_type_to_pgwire(f.data_type()),
                    FieldFormat::Text,
                )
            })
            .collect();
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

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
                query.to_string(),
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
                    f.name().clone().into(),
                    None,
                    None,
                    arrow_type_to_pgwire(f.data_type()),
                    format,
                )
            })
            .collect();
        info!("sending back the describe portal {:?}", fields);
        Ok(DescribePortalResponse::new(fields))
    }
}

struct RiffqProcessorFactory {
    handler: Arc<RiffqProcessor>,
    extended_handler: Arc<RiffqProcessor>,
}

impl PgWireServerHandlers for RiffqProcessorFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.extended_handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        Arc::new(NoopHandler)
    }

    fn error_handler(&self) -> Arc<impl pgwire::api::ErrorHandler> {
        Arc::new(NoopHandler)
    }
}

async fn detect_gssencmode(mut socket: TcpStream) -> Option<TcpStream> {
    let mut buf = [0u8; 8];

    if let Ok(n) = socket.peek(&mut buf).await {
        if n == 8 {
            let request_code = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
            if request_code == 80877104 {
                if let Err(e) = socket.read_exact(&mut buf).await {
                    error!("Failed to consume GSSAPI request: {:?}", e);
                }
                if let Err(e) = socket.write_all(b"N").await {
                    error!("Failed to send rejection message: {:?}", e);
                }
            }
        }
    }

    Some(socket)
}

fn setup_tls(cert_path: &str, key_path: &str) -> Result<TlsAcceptor, IOError> {
    let cert = certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<Vec<CertificateDer>, IOError>>()?;

    let key = pkcs8_private_keys(&mut BufReader::new(File::open(key_path)?))
        .map(|key| key.map(PrivateKeyDer::from))
        .collect::<Result<Vec<PrivateKeyDer>, IOError>>()?
        .remove(0);

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
    let list: &Bound<'py, PyList> = list.downcast().map_err(|e| {
        DataFusionError::Execution(format!("{method}() must pass a list of dicts: {e}"))
    })?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter() {
        let d: Bound<'py, PyDict> = item.downcast_into().map_err(|e| {
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
                Some("materialized_view") | Some("matview") => RelationKind::MaterializedView,
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

/// Parse `config()` rows: `{name, setting}` -> [`ConfigSettingDef`] (pg_config).
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

/// Parse `settings()` rows: `{name, setting}` -> [`SettingDef`] (pg_settings).
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
/// pg_catalog definition types. Errors raised in Python surface as
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
    tables: Vec<(String, String, String, Vec<BTreeMap<String, ColumnDef>>)>,
    lazy_catalog_source: Arc<Mutex<Option<Py<PyAny>>>>,
}

#[pymethods]
impl Server {
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

    fn on_query(&mut self, _py: Python, cb: Py<PyAny>) {
        *self.on_query_cb.lock().unwrap() = Some(cb);
    }

    fn on_connect(&mut self, _py: Python, cb: Py<PyAny>) {
        *self.on_connect_cb.lock().unwrap() = Some(cb);
    }

    fn on_disconnect(&mut self, _py: Python, cb: Py<PyAny>) {
        *self.on_disconnect_cb.lock().unwrap() = Some(cb);
    }

    fn on_authentication(&mut self, _py: Python, cb: Py<PyAny>) {
        *self.on_authentication_cb.lock().unwrap() = Some(cb);
    }

    fn handle_shutdown(&mut self, _py: Python, cb: Py<PyAny>) {
        // Called once after the server stops accepting connections on SIGINT or
        // SIGTERM, letting Python flush/checkpoint state (e.g. DuckDB) before exit.
        *self.handle_shutdown_cb.lock().unwrap() = Some(cb);
    }

    fn set_tls(&mut self, cert_path: String, key_path: String) -> PyResult<()> {
        match setup_tls(&cert_path, &key_path) {
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
    /// `callback` with a list of row dicts. When set, the base catalog context is
    /// built with the lazy providers (so `pg_catalog`/`information_schema` reflect
    /// the source live on every scan) and the eager `register_database`/
    /// `register_schema`/`register_table` registrations are skipped. Requires
    /// `start(catalog_emulation=True)` for the catalog queries to be routed here.
    fn set_lazy_catalog(&mut self, _py: Python, source: Py<PyAny>) {
        *self.lazy_catalog_source.lock().unwrap() = Some(source);
    }

    fn register_database(&mut self, database_name: String) {
        self.databases.push(database_name);
    }

    fn register_schema(&mut self, database_name: String, schema_name: String) {
        self.schemas.push((database_name, schema_name));
    }

    fn register_table(
        &mut self,
        _py: Python,
        database_name: String,
        schema_name: String,
        table_name: String,
        columns: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let list: &Bound<'_, PyList> = columns.downcast()?;
        let mut cols: Vec<BTreeMap<String, ColumnDef>> = Vec::new();
        for item in list.iter() {
            let mapping: &Bound<'_, PyDict> = item.downcast()?;
            if mapping.len() != 1 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "each column must be a single-key dict",
                ));
            }
            let (name, def_obj) = mapping.iter().next().unwrap();
            let name_str: String = name.extract()?;
            let def_dict: &Bound<'_, PyDict> = def_obj.downcast()?;
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

    #[pyo3(signature = (tls=false, catalog_emulation=false, server_version=None))]
    fn start(
        &self,
        py: Python,
        tls: bool,
        catalog_emulation: bool,
        server_version: Option<String>,
    ) -> PyResult<()> {
        // surface a failed bind (e.g. the port is taken) as a python OSError
        // instead of panicking the worker process.
        py.detach(|| self.run_server(tls, catalog_emulation, server_version))
            .map_err(|err| pyo3::exceptions::PyOSError::new_err(err.to_string()))
    }

    fn run_server(
        &self,
        tls: bool,
        catalog_emulation: bool,
        server_version: Option<String>,
    ) -> std::io::Result<()> {
        let addr = self.addr.clone();
        let query_cb = self.on_query_cb.clone();
        let connect_cb = self.on_connect_cb.clone();
        let disconnect_cb = self.on_disconnect_cb.clone();
        let auth_cb = self.on_authentication_cb.clone();
        let shutdown_cb = self.handle_shutdown_cb.clone();
        let server_version = server_version.unwrap_or_else(|| SERVER_VERSION.to_string());

        if query_cb.lock().unwrap().is_none() {
            panic!("No callback set. Use on_query() before starting the server.");
        }

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
            let mut ctx_map: HashMap<String, Arc<SessionContext>> = HashMap::new();

            // Clone the Python lazy-catalog source out of the server, if one was set.
            let lazy_source = self
                .lazy_catalog_source
                .lock()
                .unwrap()
                .as_ref()
                .map(|o| Python::attach(|py| o.clone_ref(py)));

            if let Some(obj) = lazy_source {
                // Lazy path: a single catalog context whose pg_catalog /
                // information_schema tables (and views) are sourced from Python on
                // every scan. Built BEFORE the views are created so they bind to
                // the lazy providers. Eager register_* is skipped below.
                let source: Arc<dyn LazyCatalogSource> = Arc::new(PyLazyCatalogSource { obj });
                let (raw_ctx, _) = get_base_session_context_with_lazy_catalog(
                    None,
                    "datafusion".to_string(),
                    "public".to_string(),
                    None,
                    source,
                    LazyCatalogOptions::all(),
                )
                .await
                .unwrap();
                ctx_map.insert("datafusion".to_string(), Arc::new(raw_ctx));
            } else if self.databases.is_empty() {
                if catalog_emulation {
                    let (raw_ctx, _) = get_base_session_context(
                        None,
                        "datafusion".to_string(),
                        "public".to_string(),
                        None,
                    )
                    .await
                    .unwrap();
                    ctx_map.insert("datafusion".to_string(), Arc::new(raw_ctx));
                } else {
                    let raw_ctx = SessionContext::new();
                    ctx_map.insert("datafusion".to_string(), Arc::new(raw_ctx));
                }
            } else {
                for db in &self.databases {
                    let (raw_ctx, _) =
                        get_base_session_context(None, db.to_string(), "main".to_string(), None)
                            .await
                            .unwrap();
                    ctx_map.insert(db.clone(), Arc::new(raw_ctx));
                }
            }

            // The eager registrations are mutually exclusive with the lazy source:
            // when a source is installed it is authoritative for user objects.
            if self.lazy_catalog_source.lock().unwrap().is_none() {
                for ctx in ctx_map.values() {
                    for db in &self.databases {
                        register_user_database(ctx, db).await.unwrap();
                    }
                }

                for (db, schema) in &self.schemas {
                    if let Some(c) = ctx_map.get(db) {
                        register_schema(c, db, schema).await.unwrap();
                    }
                }

                for (db, schema, table, cols) in &self.tables {
                    if let Some(c) = ctx_map.get(db) {
                        // register_user_tables identifies the schema by OID; register_schema
                        // is idempotent and returns the OID of the existing-or-created schema.
                        let schema_oid = register_schema(c, db, schema).await.unwrap();
                        register_user_tables(c, db, schema_oid, table, cols.clone())
                            .await
                            .unwrap();
                    }
                }
            }

            let ctx_map = Arc::new(ctx_map);
            let default_ctx = ctx_map.values().next().unwrap().clone();

            let listener = bind_listener(&addr)?;
            info!("Listening on {}", addr);

            let server_task = tokio::spawn({
                let tls_acceptor = if tls {
                    self.tls_acceptor.lock().unwrap().clone()
                } else {
                    None
                };
                let py_worker = py_worker.clone();
                let ctx_map = ctx_map.clone();
                let default_ctx = default_ctx.clone();
                let server_version = server_version.clone();
                async move {
                    loop {
                        let (socket, addr) = listener.accept().await.unwrap();
                        if let Some(socket) = detect_gssencmode(socket).await {
                            let conn_ctx =
                                SessionContext::new_with_state(default_ctx.state().clone());
                            let conn_ctx = Arc::new(conn_ctx);

                            let query_runner: Arc<dyn QueryRunner> = if catalog_emulation {
                                Arc::new(RouterQueryRunner {
                                    py_worker: py_worker.clone(),
                                    catalog_ctx: Arc::new(Mutex::new(conn_ctx.clone())),
                                })
                            } else {
                                Arc::new(DirectQueryRunner {
                                    py_worker: py_worker.clone(),
                                })
                            };

                            let tls_acceptor_ref = tls_acceptor.clone();
                            let (id_tx, id_rx) = oneshot::channel();

                            let handler = Arc::new(RiffqProcessor {
                                ctx: Arc::new(Mutex::new(conn_ctx.clone())),
                                ctx_map: ctx_map.clone(),
                                py_worker: py_worker.clone(),
                                conn_id_sender: Arc::new(Mutex::new(Some(id_tx))),
                                query_runner: query_runner.clone(),
                                server_version: server_version.clone(),
                            });
                            let factory = Arc::new(RiffqProcessorFactory {
                                handler: handler.clone(),
                                extended_handler: handler.clone(),
                            });

                            let py_worker_clone = py_worker.clone();
                            let ip = addr.ip().to_string();
                            let port = addr.port();

                            tokio::spawn(async move {
                                if let Err(e) =
                                    process_socket(socket, tls_acceptor_ref, factory).await
                                {
                                    error!("process_socket error: {:?}", e);
                                }
                                let connection_id = id_rx.await.unwrap_or(0);
                                py_worker_clone.on_disconnect(connection_id, ip, port).await;
                            });
                        }
                    }
                }
            });

            wait_for_shutdown_signal().await;
            info!("Shutting down server");
            server_task.abort();
            run_shutdown_callback(&shutdown_cb);
            Ok(())
        })
    }
}

fn bind_listener(addr: &str) -> std::io::Result<TcpListener> {
    // Bind with SO_REUSEADDR so a port left in TIME_WAIT by a just-stopped
    // server (its closed client connections) can be reused immediately. Without
    // it, restarting on the same port races "address already in use". A genuine
    // failure (port owned by another process, bad address) returns an Err so
    // start() can raise it as a python OSError instead of panicking.
    let socket_addr: std::net::SocketAddr = addr.parse().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid listen address: {}", addr),
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
                error!("handle_shutdown callback failed: {:?}", err);
            }
        });
    }
}

#[pymodule]
fn _riffq(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();

    module.add_class::<Server>()?;
    Ok(())
}
