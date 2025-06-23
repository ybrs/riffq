use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_pg_catalog::session::ClientOpts;
use pyo3::prelude::*;
use pyo3::{PyAny, Bound, IntoPyObjectExt};
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use futures::{Sink, SinkExt, Stream};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::oneshot;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::TcpStream;
use pyo3::types::{PyDict, PyList, PyTuple, PyCapsule};
use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use log::{debug, error, info};

use std::collections::HashMap;
use std::collections::BTreeMap;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::sync::atomic::{AtomicU64, Ordering};

static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);

use bytes::Bytes;
use std::ffi::c_void;
use std::pin::Pin;
use futures::stream::{self, BoxStream};

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::array::{Array, RecordBatch};
use arrow::array::cast::AsArray;
use arrow::record_batch::RecordBatchReader;
use arrow::datatypes::{DataType, Field, Schema, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType};
use arrow::array::{ArrayRef, StringBuilder};
use datafusion::execution::context::SessionContext;
use datafusion_pg_catalog::{
    dispatch_query,
    get_base_session_context,
    register_user_database,
    register_schema,
    register_user_tables,
    ColumnDef,
};
use postgres_types::FromSql;

use chrono::{DateTime, Duration, NaiveDate};
use arrow::datatypes::TimeUnit;


use pgwire::api::auth::{finish_authentication, DefaultServerParameterProvider, StartupHandler};
use pgwire::api::PgWireConnectionState;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{SimpleQueryHandler, ExtendedQueryHandler};
use pgwire::api::results::{DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::messages::data::DataRow;
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::api::portal::Portal;
use pgwire::error::{PgWireError, PgWireResult, ErrorInfo};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::messages::startup::Authentication;
use pgwire::messages::response::ErrorResponse;
use pgwire::tokio::process_socket;
use pgwire::api::stmt::{StoredStatement};

pub mod pg;
mod helpers;
use pg::arrow_type_to_pgwire;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;
use helpers::_debug_parameters;


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
        responder: oneshot::Sender<bool>,
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
        responder: oneshot::Sender<bool>,
    },
}

#[pyclass]
struct CallbackWrapper {
    responder: Arc<Mutex<Option<oneshot::Sender<QueryResult>>>>,
}

#[pyclass]
struct BoolCallbackWrapper {
    responder: Arc<Mutex<Option<oneshot::Sender<bool>>>>,
}

#[pymethods]
impl BoolCallbackWrapper {
    fn __call__(&self, result: PyObject) {
        if let Some(sender) = self.responder.lock().unwrap().take() {
            Python::with_gil(|py| {
                let val: bool = result.extract(py).unwrap_or(false);
                let _ = sender.send(val);
            });
        }
    }
}

#[pymethods]
impl CallbackWrapper {
    #[pyo3(signature = (result, *, is_tag=false))]
    fn __call__(&self, result: PyObject, is_tag: bool) {
        if let Some(sender) = self.responder.lock().unwrap().take() {
            Python::with_gil(|py| {
                if is_tag {
                    let tag = result.extract::<String>(py).unwrap_or_default();
                    let ret = sender.send(QueryResult::Tag(tag));
                    if ret.is_err() {
                        error!("return for tag errored");
                    }
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
                let parsed: PyResult<(Vec<HashMap<String, String>>, Vec<Vec<PyObject>>)> = result_bound.extract::<(Vec<HashMap<String, String>>, Vec<Vec<PyObject>>)>() ;
                if let Ok((schema_desc, py_rows)) = parsed {
                    // turn PyObjects into Rust Option<String>
                    let rows: Vec<Vec<Option<String>>> = py_rows.into_iter()
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
                    let fields: Vec<Field> = schema_desc.iter()
                        .map(|c| Field::new(c.get("name").unwrap(), DataType::Utf8, true))
                        .collect();

                    let mut builders: Vec<StringBuilder> =
                        fields.iter().map(|_| StringBuilder::new()).collect();

                    for row in &rows {
                        for (i, cell) in row.iter().enumerate() {
                            match cell {
                                Some(s) => builders[i].append_value(s),
                                None    => builders[i].append_null(),
                            }
                        }
                    }

                    let arrays: Vec<ArrayRef> = builders
                        .into_iter()
                        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
                        .collect();

                    let schema = Arc::new(Schema::new(fields));
                    let batch  = RecordBatch::try_new(schema.clone(), arrays).unwrap();
                    let _      = sender.send(QueryResult::Arrow(vec![batch], schema));
                }


            });
        }
    }
}

fn arrow_to_pg_rows(
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
) -> (
    Arc<Vec<FieldInfo>>,
    Pin<Box<dyn Stream<Item = PgWireResult<DataRow>> + Send>>,
) {
    // column metadata
    let field_defs: Arc<Vec<FieldInfo>> = Arc::new(
        schema
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
            .collect(),
    );

    // lazy row stream
    let row_stream = stream::unfold((0usize, batches), {
        let meta_outer = field_defs.clone();          // captured by outer FnMut
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
                    for col in batch.columns() {
                        if let Err(e) = encode_arrow_value(&mut enc, col.as_ref(), row_idx) {
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

fn encode_arrow_value(
    encoder: &mut DataRowEncoder,
    array: &dyn Array,
    row: usize,
) -> PgWireResult<()> {
    if array.is_null(row) {
        return encoder.encode_field(&Option::<i32>::None); // type will be ignored
    }
    match array.data_type() {
        DataType::Int8 => encoder.encode_field(&Some(array.as_primitive::<arrow::array::types::Int8Type>().value(row) as i16)),
        DataType::Int16 => encoder.encode_field(&Some(array.as_primitive::<arrow::array::types::Int16Type>().value(row))),
        DataType::Int32 => encoder.encode_field(&Some(array.as_primitive::<arrow::array::types::Int32Type>().value(row))),
        DataType::Int64 => encoder.encode_field(&Some(array.as_primitive::<arrow::array::types::Int64Type>().value(row))),
        DataType::UInt8 => encoder.encode_field(&Some(array.as_primitive::<arrow::array::types::UInt8Type>().value(row) as i16)),
        DataType::UInt16 => encoder.encode_field(&Some(array.as_primitive::<arrow::array::types::UInt16Type>().value(row) as i32)),
        DataType::UInt32 => encoder.encode_field(&Some(array.as_primitive::<arrow::array::types::UInt32Type>().value(row) as i64)),
        DataType::UInt64 => encoder.encode_field(&Some(array.as_primitive::<arrow::array::types::UInt64Type>().value(row) as i64)),
        DataType::Float32 => encoder.encode_field(&Some(array.as_primitive::<arrow::array::types::Float32Type>().value(row))),
        DataType::Float64 => encoder.encode_field(&Some(array.as_primitive::<arrow::array::types::Float64Type>().value(row))),
        DataType::Boolean => encoder.encode_field(&Some(array.as_boolean().value(row))),
        DataType::Utf8 => encoder.encode_field(&Some(array.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => encoder.encode_field(&Some(array.as_string::<i64>().value(row))),
        DataType::Date32 => {
            let days = array.as_primitive::<arrow::array::types::Date32Type>().value(row) as i64;
            let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days);
            encoder.encode_field(&Some(date))
        }
        DataType::Date64 => {
            let ms = array.as_primitive::<arrow::array::types::Date64Type>().value(row);
            let dt = DateTime::from_timestamp(ms / 1000, (ms % 1000 * 1_000_000) as u32).unwrap();
            encoder.encode_field(&Some(dt))
        }
        DataType::Timestamp(unit, _) => {
            let nanos: i128 = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row) as i128 * 1_000_000_000,
                TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(row) as i128 * 1_000_000,
                TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row) as i128 * 1_000,
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row) as i128,
            };
            let secs = (nanos / 1_000_000_000) as i64;
            let nsec = (nanos % 1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(secs, nsec).unwrap();
            encoder.encode_field(&Some(dt))
        }
        _ => encoder.encode_field(&Option::<&str>::None),
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
            pyo3::prepare_freethreaded_python();
            loop {
                debug!("[PY_WORKER] waiting to receive on rx...");
                match rx.recv() {
                    Ok(msg) => match msg {
                        WorkerMessage::Query { query, params, param_types, do_describe, connection_id, responder } => {
                            debug!("[PY_WORKER] received query: {}", query);
                            let cb_opt = Python::with_gil(|py| {
                                query_cb
                                    .lock()
                                    .unwrap()
                                    .as_ref()
                                    .map(|cb| cb.clone_ref(py))
                            });

                            if let Some(cb) = cb_opt {
                                Python::with_gil(|py| {
                                    debug!("[PY_WORKER] GIL acquired, invoking callback");
                                    let wrapper = Py::new(py, CallbackWrapper {
                                        responder: Arc::new(Mutex::new(Some(responder))),
                                    }).unwrap();

                                    let args = PyTuple::new(py, [
                                        query.clone().into_py_any(py).unwrap(),
                                        wrapper.clone_ref(py).into_py_any(py).unwrap(),
                                    ]).unwrap();
                                    let kwargs = PyDict::new(py);

                                    // Add do_describe flag
                                    kwargs.set_item("do_describe", do_describe).unwrap();

                                    // Connection identifier
                                    kwargs.set_item("connection_id", connection_id).unwrap();                                    

                                    // Add query_args if present
                                    if let (Some(params), Some(param_types)) = (&params, &param_types) {
                                        let py_args = PyList::empty(py);
                                        for (val, ty) in params.iter().zip(param_types.iter()) {
                                            match val {
                                                None => {
                                                    py_args.append(py.None()).unwrap();
                                                }
                                                Some(bytes) => {
                                                    let mut buf = &bytes[..];
                                                    match ty {
                                                        &Type::INT2 => if let Ok(v) = i16::from_sql(ty, &mut buf) { py_args.append(v).unwrap(); } else { py_args.append(py.None()).unwrap(); },
                                                        &Type::INT4 => if let Ok(v) = i32::from_sql(ty, &mut buf) { py_args.append(v).unwrap(); } else { py_args.append(py.None()).unwrap(); },
                                                        &Type::INT8 => if let Ok(v) = i64::from_sql(ty, &mut buf) { py_args.append(v).unwrap(); } else { py_args.append(py.None()).unwrap(); },
                                                        &Type::FLOAT4 => if let Ok(v) = f32::from_sql(ty, &mut buf) { py_args.append(v).unwrap(); } else { py_args.append(py.None()).unwrap(); },
                                                        &Type::FLOAT8 => if let Ok(v) = f64::from_sql(ty, &mut buf) { py_args.append(v).unwrap(); } else { py_args.append(py.None()).unwrap(); },
                                                        &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR => if let Ok(v) = String::from_sql(ty, &mut buf) { py_args.append(v).unwrap(); } else { py_args.append(py.None()).unwrap(); },
                                                        _ => { 
                                                            info!("unknown query argument type {:}?", ty);
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
                        WorkerMessage::Connect { connection_id, ip, port, responder } => {
                            let cb_opt = Python::with_gil(|py| {
                                connect_cb
                                    .lock()
                                    .unwrap()
                                    .as_ref()
                                    .map(|cb| cb.clone_ref(py))
                            });
                            if let Some(cb) = cb_opt {
                                Python::with_gil(|py| {
                                    let wrapper = Py::new(py, BoolCallbackWrapper {
                                        responder: Arc::new(Mutex::new(Some(responder))),
                                    }).unwrap();
                                    let args = PyTuple::new(py, [
                                        connection_id.into_py_any(py).unwrap(),
                                        ip.clone().into_py_any(py).unwrap(),
                                        port.into_py_any(py).unwrap(),
                                    ]).unwrap();
                                    let kwargs = PyDict::new(py);
                                    kwargs.set_item("callback", wrapper.clone_ref(py)).unwrap();
                                    if let Err(e) = cb.call(py, args, Some(&kwargs)) {
                                        e.print(py);
                                    }
                                });
                            } else {
                                let _ = responder.send(true);
                            }
                        }
                        WorkerMessage::Disconnect { connection_id, ip, port } => {
                            let cb_opt = Python::with_gil(|py| {
                                disconnect_cb
                                    .lock()
                                    .unwrap()
                                    .as_ref()
                                    .map(|cb| cb.clone_ref(py))
                            });
                            if let Some(cb) = cb_opt {
                                Python::with_gil(|py| {
                                    let args = PyTuple::new(py, [
                                        connection_id.into_py_any(py).unwrap(),
                                        ip.clone().into_py_any(py).unwrap(),
                                        port.into_py_any(py).unwrap(),
                                    ]).unwrap();
                                    if let Err(e) = cb.call1(py, args) {
                                        e.print(py);
                                    }
                                });
                            }
                        }
                        WorkerMessage::Authentication { connection_id, user, database, host, password, responder } => {
                            let cb_opt = Python::with_gil(|py| {
                                auth_cb_thread
                                    .lock()
                                    .unwrap()
                                    .as_ref()
                                    .map(|cb| cb.clone_ref(py))
                            });
                            if let Some(cb) = cb_opt {
                                Python::with_gil(|py| {
                                    let wrapper = Py::new(py, BoolCallbackWrapper {
                                        responder: Arc::new(Mutex::new(Some(responder))),
                                    }).unwrap();
                                    let args = PyTuple::new(py, [
                                        connection_id.into_py_any(py).unwrap(),
                                        user.clone().into_py_any(py).unwrap(),
                                        password.clone().into_py_any(py).unwrap(),
                                        host.clone().into_py_any(py).unwrap(),
                                    ]).unwrap();
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
                                let _ = responder.send(true);
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

        PythonWorker { sender: tx, auth_cb }
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


    pub async fn on_connect(&self, connection_id: u64, ip: String, port: u16) -> bool {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WorkerMessage::Connect {
                connection_id,
                ip,
                port,
                responder: tx,
            })
            .expect("Send failed!");
        rx.await.unwrap_or(false)
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
    ) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .sender
            .send(WorkerMessage::Authentication {
                connection_id,
                user,
                database,
                host,
                password,
                responder: tx,
            });
        rx.await.unwrap_or(false)
    }

    pub async fn on_disconnect(&self, connection_id: u64, ip: String, port: u16) {
        let _ = self
            .sender
            .send(WorkerMessage::Disconnect {
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
}

pub enum QueryResult {
    Arrow(Vec<RecordBatch>, Arc<Schema>),
    Tag(String),
}

struct RouterQueryRunner {
    py_worker: Arc<PythonWorker>,
    catalog_ctx: Arc<SessionContext>,
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
        let ctx = self.catalog_ctx.clone();
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
                }
            }
        };

        let (batches, schema) =
            dispatch_query(&ctx, &query, params, param_types, handler).await?;

        if let Some(tag) = tag_holder.lock().unwrap().take() {
            Ok(QueryResult::Tag(tag))
        } else {
            Ok(QueryResult::Arrow(batches, schema))
        }
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
                .await,                     // QueryResult, no rebuilding
        )
    }
}

pub struct RiffqProcessor {
    py_worker: Arc<PythonWorker>,
    conn_id_sender: Arc<Mutex<Option<oneshot::Sender<u64>>>>,
    query_runner: Arc<dyn QueryRunner>,
    ctx: Arc<SessionContext>,
}

use datafusion::{
    logical_expr::{create_udf, Volatility, ColumnarValue},
    common::ScalarValue,
};


impl RiffqProcessor {

    fn register_current_database<C>(&self, client: &C) -> datafusion::error::Result<()>
    where
        C: ClientInfo + ?Sized,
    {
        static KEY: &str = "current_database";

        if self.ctx.state().scalar_functions().contains_key(KEY){
            return Ok(());
        }

        if let Some(db) = client.metadata().get(pgwire::api::METADATA_DATABASE).cloned() {
            let fun = Arc::new(move |_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(db.clone()))))
            });
            let udf = create_udf(KEY, vec![], DataType::Utf8, Volatility::Stable, fun.clone());
            self.ctx.register_udf(udf);
            // udf.with_aliases("pg_catalog.current_database");
            let udf = create_udf("pg_catalog.current_database", vec![], DataType::Utf8, Volatility::Stable, fun.clone());
            self.ctx.register_udf(udf);
        }

        Ok(())
    }

    fn register_session_user<C>(&self, client: &C) -> datafusion::error::Result<()>
    where
        C: ClientInfo + ?Sized,
    {
        static KEY: &str = "session_user";
        if self.ctx.state().scalar_functions().contains_key(KEY){
            return Ok(());
        }

        if let Some(user) = client.metadata().get(pgwire::api::METADATA_USER).cloned() {
            let fun = Arc::new(move |_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(user.clone()))))
            });
            let udf = create_udf(KEY, vec![], DataType::Utf8, Volatility::Stable, fun);
            self.ctx.register_udf(udf);
        }

        Ok(())
    }

    fn register_current_user<C>(&self, client: &C) -> datafusion::error::Result<()>
    where
        C: ClientInfo + ?Sized,
    {
        static KEY: &str = "current_user";

        if self.ctx.state().scalar_functions().contains_key(KEY) {
            return Ok(());
        }

        if let Some(user) = client.metadata().get(pgwire::api::METADATA_USER).cloned() {
            let fun = Arc::new(move |_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(user.clone()))))
            });
            let udf = create_udf(KEY, vec![], DataType::Utf8, Volatility::Stable, fun.clone());
            self.ctx.register_udf(udf);
            let udf = create_udf(
                "pg_catalog.current_user",
                vec![],
                DataType::Utf8,
                Volatility::Stable,
                fun,
            );
            self.ctx.register_udf(udf);
        }

        Ok(())
    }

    fn show_variable_response<'a>(&self, name: &str, format: FieldFormat) -> Option<Response<'a>> {
        let state = self.ctx.state();
        let opts = state
            .config_options()
            .extensions
            .get::<ClientOpts>()?;

        let value = match name {
            "application_name" => opts.application_name.as_str(),
            "datestyle" => opts.datestyle.as_str(),
            "search_path" => opts.search_path.as_str(),
            _ => return None,
        };

        let fields = Arc::new(vec![
            FieldInfo::new(name.to_string(), None, None, Type::TEXT, format),
        ]);

        let mut encoder = DataRowEncoder::new(fields.clone());
        encoder.encode_field(&Some(value)).ok()?;
        let row = encoder.finish().ok()?;
        let rows = stream::iter(vec![Ok(row)]);
        let rows: BoxStream<'a, PgWireResult<DataRow>> = Box::pin(rows);
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
        // We set server version here. We also should make it optional in future. 
        let mut params = DefaultServerParameterProvider::default();
        params.server_version = SERVER_VERSION.to_string();

        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                if self.py_worker.authentication_enabled() {
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(Authentication::CleartextPassword))
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
                    let allowed = self
                        .py_worker
                        .on_connect(id, addr.ip().to_string(), addr.port())
                        .await;
                    if !allowed {
                        let error = ErrorResponse::from(ErrorInfo::new(
                            "FATAL".to_string(),
                            "28000".to_string(),
                            "Connection rejected".to_string(),
                        ));
                        client.feed(PgWireBackendMessage::ErrorResponse(error)).await?;
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
                if !allowed {
                    let error_info = ErrorInfo::new(
                        "FATAL".to_string(),
                        "28P01".to_string(),
                        "Authentication failed".to_string(),
                    );
                    let error = ErrorResponse::from(error_info);
                    client.feed(PgWireBackendMessage::ErrorResponse(error)).await?;
                    client.close().await?;
                    return Ok(());
                }

                let addr = client.socket_addr();
                let allowed = self
                    .py_worker
                    .on_connect(id, addr.ip().to_string(), addr.port())
                    .await;
                if !allowed {
                    let error = ErrorResponse::from(ErrorInfo::new(
                        "FATAL".to_string(),
                        "28000".to_string(),
                        "Connection rejected".to_string(),
                    ));
                    client.feed(PgWireBackendMessage::ErrorResponse(error)).await?;
                    client.close().await?;
                    return Ok(());
                }

                finish_authentication(client, &params).await?;
            }
            _ => {}
        }

        let user = client.metadata().get(pgwire::api::METADATA_USER).cloned();
        let database = client.metadata().get(pgwire::api::METADATA_DATABASE).cloned();
        log::debug!("database: {:?} {:?}", database, user);

        let _ = self.register_current_database(client);
        let _ = self.register_session_user(client);
        let _ = self.register_current_user(client);


        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for RiffqProcessor {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
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
            if let Some(resp) = self.show_variable_response(&var.to_lowercase(), FieldFormat::Text) {
                return Ok(vec![resp]);
            }
        } else if lowercase == "" {
            return Ok(vec![Response::Execution(Tag::new(""))]);
        }

        debug!("[PGWIRE] do_query called with: {}", query);
        let connection_id = _client
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
                let (schema, data_row_stream) = arrow_to_pg_rows(batches, schema);
                Ok(vec![Response::Query(QueryResponse::new(schema, data_row_stream))])
            }
            QueryResult::Tag(tag) => Ok(vec![Response::Execution(Tag::new(&tag))]),
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

#[async_trait]
impl pgwire::api::stmt::QueryParser for MyQueryParser {
    type Statement = MyStatement;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        Ok(MyStatement {
            query: sql.to_string(),
        })
    }
}


#[async_trait]
impl ExtendedQueryHandler for RiffqProcessor {
    type Statement = MyStatement;
    type QueryParser = MyQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(MyQueryParser)
    }


    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = &portal.statement.statement.query;
        debug!("[PGWIRE EXTENDED] do_query: {} {}", portal.statement.statement.query, _debug_parameters(&portal.parameters, &portal.statement.parameter_types));

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
            if let Some(resp) = self.show_variable_response(&var.to_lowercase(), portal.result_column_format.format_for(0)) {
                return Ok(resp);
            }
        }

        let connection_id = _client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let result = self
            .query_runner
            .execute(
                query.to_string(),
                Some(portal.parameters.clone()),
                Some(portal.statement.parameter_types.clone()),
                false,
                connection_id,
            )
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        match result {
            QueryResult::Arrow(batches, schema) => {
                let (schema, data_row_stream) = arrow_to_pg_rows(batches, schema);
                Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
            }
            QueryResult::Tag(tag) => Ok(Response::Execution(Tag::new(&tag))),
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(DescribeStatementResponse::new(vec![], vec![]))
    }


    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = &portal.statement.statement.query;        

        let connection_id = _client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let result = self
            .query_runner
            .execute(
                query.to_string(),
                Some(portal.parameters.clone()),
                Some(portal.statement.parameter_types.clone()),
                true,
                connection_id,
            )
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let schema = match result {
            QueryResult::Arrow(_, schema) => schema,
            QueryResult::Tag(_) => Arc::new(Schema::empty()),
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
        Ok(DescribePortalResponse::new(fields))
    }



}

struct RiffqProcessorFactory {
    handler: Arc<RiffqProcessor>,
    extended_handler: Arc<RiffqProcessor>,
}

impl PgWireServerHandlers for RiffqProcessorFactory {
    type StartupHandler = RiffqProcessor;
    type SimpleQueryHandler = RiffqProcessor;
    type ExtendedQueryHandler = RiffqProcessor;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.extended_handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
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

fn setup_tls(cert_path: &str, key_path: &str) -> Result<Arc<TlsAcceptor>, IOError> {
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

    Ok(Arc::new(TlsAcceptor::from(Arc::new(config))))
}

#[pyclass]
pub struct Server {
    addr: String,
    on_query_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    on_connect_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    on_disconnect_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    on_authentication_cb: Arc<Mutex<Option<Py<PyAny>>>>,
    tls_acceptor: Arc<Mutex<Option<Arc<TlsAcceptor>>>>,
    databases: Vec<String>,
    schemas: Vec<(String, String)>,
    tables: Vec<(String, String, String, Vec<BTreeMap<String, ColumnDef>>)>,
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
            tls_acceptor: Arc::new(Mutex::new(None)),
            databases: Vec::new(),
            schemas: Vec::new(),
            tables: Vec::new(),
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

    fn set_tls(&mut self, cert_path: String, key_path: String) -> PyResult<()> {
        match setup_tls(&cert_path, &key_path) {
            Ok(acceptor) => {
                *self.tls_acceptor.lock().unwrap() = Some(acceptor);
                Ok(())
            }
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
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
            m.insert(name_str, ColumnDef { col_type, nullable });
            cols.push(m);
        }
        self.tables.push((database_name, schema_name, table_name, cols));
        Ok(())
    }


    #[pyo3(signature = (tls=false, catalog_emulation=false))]
    fn start(&self, py: Python, tls: bool, catalog_emulation: bool) {
        py.allow_threads(|| {
            self.run_server(tls, catalog_emulation);
        });
    }

    fn run_server(&self, tls: bool, catalog_emulation: bool) {
        let addr = self.addr.clone();
        let query_cb = self.on_query_cb.clone();
        let connect_cb = self.on_connect_cb.clone();
        let disconnect_cb = self.on_disconnect_cb.clone();
        let auth_cb = self.on_authentication_cb.clone();

        if query_cb.lock().unwrap().is_none() {
            panic!("No callback set. Use on_query() before starting the server.");
        }

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let py_worker = Arc::new(PythonWorker::new(query_cb, connect_cb, disconnect_cb, auth_cb));
            let (raw_ctx, _) = get_base_session_context(None, "datafusion".to_string(), "public".to_string()).await.unwrap();
            let ctx = Arc::new(raw_ctx);
            for db in &self.databases {
                register_user_database(&ctx, db).await.unwrap();
            }
            for (db, schema) in &self.schemas {
                register_schema(&ctx, db, schema).await.unwrap();
            }
            for (db, schema, table, cols) in &self.tables {
                register_user_tables(&ctx, db, schema, table, cols.clone()).await.unwrap();
            }

            let listener = TcpListener::bind(&addr).await.unwrap();
            info!("Listening on {}", addr);

            let server_task = tokio::spawn({
                let tls_acceptor = if tls { self.tls_acceptor.lock().unwrap().clone() } else { None };
                let py_worker = py_worker.clone();
                async move {
                    loop {
                        let (socket, addr) = listener.accept().await.unwrap();
                        if let Some(socket) = detect_gssencmode(socket).await {

                            let conn_ctx = SessionContext::new_with_state(
                                ctx.state().clone(),
                            );
                            let conn_ctx = Arc::new(conn_ctx);

                            let query_runner: Arc<dyn QueryRunner> = if catalog_emulation {
                                Arc::new(RouterQueryRunner { py_worker: py_worker.clone(), catalog_ctx: conn_ctx.clone() })
                            } else {
                                Arc::new(DirectQueryRunner { py_worker: py_worker.clone() })
                            };

                            let tls_acceptor_ref = tls_acceptor.clone();
                            let (id_tx, id_rx) = oneshot::channel();
                            
                            let handler = Arc::new(RiffqProcessor {
                                ctx: conn_ctx.clone(),
                                py_worker: py_worker.clone(),
                                conn_id_sender: Arc::new(Mutex::new(Some(id_tx))),
                                query_runner: query_runner.clone(),
                            });
                            let factory = Arc::new(RiffqProcessorFactory {
                                handler: handler.clone(),
                                extended_handler: handler.clone(),
                            });
                            
                            let py_worker_clone = py_worker.clone();
                            let ip = addr.ip().to_string();
                            let port = addr.port();
                            
                            tokio::spawn(async move {
                                if let Err(e) = process_socket(socket, tls_acceptor_ref, factory).await {
                                    error!("process_socket error: {:?}", e);
                                }
                                let connection_id = id_rx.await.unwrap_or(0);
                                py_worker_clone.on_disconnect(connection_id, ip, port).await;
                            });
                        }
                    }
                }
            });

            signal::ctrl_c().await.expect("Failed to listen for shutdown signal");
            info!("Shutting down server");
            server_task.abort();
        });
    }
}

#[pymodule]
fn _riffq(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).try_init();

    module.add_class::<Server>()?;
    Ok(())
}
