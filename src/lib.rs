use pyo3::prelude::*;
use pyo3::{FromPyObject, PyAny};
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt, Stream};
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
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::sync::atomic::{AtomicU64, Ordering};

static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);

use bytes::Bytes;
use std::ffi::c_void;

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::array::{Array, RecordBatch};
use arrow::array::cast::AsArray;
use arrow::record_batch::RecordBatchReader;
use arrow::datatypes::{DataType, Field, Schema, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType};
use arrow::array::{ArrayRef, StringBuilder};
use datafusion::execution::context::SessionContext;
use datafusion_pg_catalog::{dispatch_query, get_base_session_context};
use postgres_types::FromSql;

use chrono::{NaiveDate, NaiveDateTime, Duration};
use arrow::datatypes::TimeUnit;


use pgwire::api::auth::{LoginInfo, DefaultServerParameterProvider, StartupHandler, finish_authentication};
use pgwire::api::PgWireConnectionState;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{SimpleQueryHandler, ExtendedQueryHandler};
use pgwire::api::results::{DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::messages::data::DataRow;
use pgwire::api::{ClientInfo, ClientPortalStore, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::api::portal::Portal;
use pgwire::error::{PgWireError, PgWireResult, ErrorInfo};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::messages::startup::Authentication;
use pgwire::messages::response::ErrorResponse;
use pgwire::tokio::process_socket;
use pgwire::api::stmt::{StoredStatement, NoopQueryParser};

pub mod pg;
use pg::arrow_type_to_pgwire;

pub enum WorkerMessage {
    Query {
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
        responder: oneshot::Sender<ArrowQueryResult>,
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
    responder: Arc<Mutex<Option<oneshot::Sender<ArrowQueryResult>>>>,  
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
    fn __call__(&self, result: PyObject) {
        if let Some(sender) = self.responder.lock().unwrap().take() {
            Python::with_gil(|py| {
                // Try Arrow C stream pointer first
                debug!("[RUST] result python type: {}", result.as_ref(py).get_type().name().unwrap_or("<unknown>"));
                if let Ok(capsule) = result.extract::<&PyCapsule>(py) {
                    debug!("[RUST] received PyCapsule");
                    let ptr = capsule.pointer() as *mut c_void;
                    unsafe {
                        let mut reader = ArrowArrayStreamReader::from_raw(ptr as *mut _).unwrap();
                        let mut batches = Vec::new();
                        while let Some(batch) = reader.next().transpose().unwrap() {
                            batches.push(batch);
                        }
                        let schema = reader.schema();
                        let _ = sender.send((batches, schema));
                    }
                    return;
                }

                // First try to treat the result as Arrow IPC bytes. When the
                // callback returns bytes we assume they contain an Arrow IPC
                // stream produced by ``pyarrow``.
                if let Ok(pybytes) = result.extract::<&pyo3::types::PyBytes>(py) {
                    let data = pybytes.as_bytes();
                    let cursor = std::io::Cursor::new(data);
                    let mut reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).unwrap();
                    let schema = reader.schema().clone();
                    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
                    let _ = sender.send((batches, schema));
                    return;
                }

                // Fallback: assume (schema_desc, rows) tuple, build batches
                let parsed: PyResult<(Vec<HashMap<String, String>>, Vec<Vec<PyObject>>)> = result.extract(py);
                if let Ok((schema_desc, py_rows)) = parsed {
                    // turn PyObjects into Rust Option<String>
                    let rows: Vec<Vec<Option<String>>> = py_rows.into_iter()
                        .map(|row| {
                            row.into_iter()
                                .map(|val| {
                                    Python::with_gil(|py| {
                                        if val.as_ref(py).is_none() {
                                            None
                                        } else {
                                            val.extract::<String>(py).ok()
                                        }
                                    })
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
                    let _      = sender.send((vec![batch], schema));
                }


            });
        }
    }
}

fn arrow_to_pg_rows(
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
) -> (Arc<Vec<FieldInfo>>, impl Stream<Item = PgWireResult<DataRow>>) {
    let mut fields = Vec::new();
    for field in schema.fields() {
        fields.push(FieldInfo::new(
            field.name().clone().into(),
            None,
            None,
            arrow_type_to_pgwire(field.data_type()),
            FieldFormat::Text,
        ));
    }
    let schema_arc = Arc::new(fields);
    let schema_ref = schema_arc.clone();

    let mut rows = Vec::new();
    for batch in batches {
        let columns = batch.columns().to_vec();
        let row_count = batch.num_rows();
        for i in 0..row_count {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            for col in &columns {
                encode_arrow_value(&mut encoder, col.as_ref(), i).unwrap();
            }
            rows.push(encoder.finish());
        }
    }

    let stream = futures::stream::iter(rows);
    (schema_arc, stream)
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
            let dt = NaiveDateTime::from_timestamp_opt(ms / 1000, (ms % 1000 * 1_000_000) as u32).unwrap();
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
            let dt = NaiveDateTime::from_timestamp_opt(secs, nsec).unwrap();
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
                            let cb_opt = query_cb.lock().unwrap().clone();

                            if let Some(cb) = cb_opt {
                                Python::with_gil(|py| {
                                    debug!("[PY_WORKER] GIL acquired, invoking callback");
                                    let wrapper = Py::new(py, CallbackWrapper {
                                        responder: Arc::new(Mutex::new(Some(responder))),
                                    }).unwrap();

                                    let args = PyTuple::new(py, &[query.into_py(py), wrapper.into_py(py)]);
                                    let kwargs = PyDict::new(py);

                                    // Add do_describe flag
                                    kwargs.set_item("do_describe", do_describe).unwrap();

                                    // Connection identifier
                                    kwargs.set_item("connection_id", connection_id).unwrap();                                    

                                    // Add query_args if present
                                    if let (Some(params), Some(param_types)) = (&params, &param_types) {
                                        let py_args = PyList::empty(py);
                                        for (val, ty) in params.iter().zip(param_types.iter()) {
                                            let py_obj = match val {
                                                None => py.None(),
                                                Some(bytes) => {
                                                    let mut buf = &bytes[..];
                                                    match ty {
                                                        &Type::INT2 => i16::from_sql(ty, &mut buf).map(|v| v.into_py(py)),
                                                        &Type::INT4 => i32::from_sql(ty, &mut buf).map(|v| v.into_py(py)),
                                                        &Type::INT8 => i64::from_sql(ty, &mut buf).map(|v| v.into_py(py)),
                                                        &Type::FLOAT4 => f32::from_sql(ty, &mut buf).map(|v| v.into_py(py)),
                                                        &Type::FLOAT8 => f64::from_sql(ty, &mut buf).map(|v| v.into_py(py)),
                                                        &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR => {
                                                            String::from_sql(ty, &mut buf).map(|v| v.into_py(py))
                                                        }
                                                        _ => Ok(py.None()),
                                                    }.unwrap_or_else(|_| py.None())
                                                }
                                            };
                                            py_args.append(py_obj).unwrap();
                                        }

                                        kwargs.set_item("query_args", py_args).unwrap();
                                    }

                                    let _ = cb.call(py, args, Some(kwargs));
                                });
                            }
                        }
                        WorkerMessage::Connect { connection_id, ip, port, responder } => {
                            let cb_opt = connect_cb.lock().unwrap().clone();
                            if let Some(cb) = cb_opt {
                                Python::with_gil(|py| {
                                    let wrapper = Py::new(py, BoolCallbackWrapper {
                                        responder: Arc::new(Mutex::new(Some(responder))),
                                    }).unwrap();
                                    let args = PyTuple::new(py, &[connection_id.into_py(py), ip.into_py(py), port.into_py(py)]);
                                    let kwargs = PyDict::new(py);
                                    kwargs.set_item("callback", wrapper.into_py(py)).unwrap();
                                    if let Err(e) = cb.call(py, args, Some(kwargs)) {
                                        e.print(py);
                                    }
                                });
                            } else {
                                let _ = responder.send(true);
                            }
                        }
                        WorkerMessage::Disconnect { connection_id, ip, port } => {
                            let cb_opt = disconnect_cb.lock().unwrap().clone();
                            if let Some(cb) = cb_opt {
                                Python::with_gil(|py| {
                                    let args = PyTuple::new(py, &[connection_id.into_py(py), ip.into_py(py), port.into_py(py)]);
                                    if let Err(e) = cb.call1(py, args) {
                                        e.print(py);
                                    }
                                });
                            }
                        }
                        WorkerMessage::Authentication { connection_id, user, database, host, password, responder } => {
                            let cb_opt = auth_cb_thread.lock().unwrap().clone();
                            if let Some(cb) = cb_opt {
                                Python::with_gil(|py| {
                                    let wrapper = Py::new(py, BoolCallbackWrapper {
                                        responder: Arc::new(Mutex::new(Some(responder))),
                                    }).unwrap();
                                    let args = PyTuple::new(py, &[
                                        connection_id.into_py(py),
                                        user.into_py(py),
                                        password.into_py(py),
                                        host.into_py(py),
                                    ]);
                                    let kwargs = PyDict::new(py);
                                    kwargs.set_item("callback", wrapper.into_py(py)).unwrap();
                                    if let Some(db) = database {
                                        kwargs.set_item("database", db).unwrap();
                                    }
                                    if let Err(e) = cb.call(py, args, Some(kwargs)) {
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
    ) -> ArrowQueryResult {
        let (tx, rx) = oneshot::channel::<ArrowQueryResult>();
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
            (Vec::new(), Arc::new(Schema::empty()))
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
    ) -> datafusion::error::Result<ArrowQueryResult>;
}

type ArrowQueryResult = (Vec<RecordBatch>, Arc<Schema>);

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
    ) -> datafusion::error::Result<ArrowQueryResult> {
        let ctx = self.catalog_ctx.clone();
        let py_worker = self.py_worker.clone();
        let handler = move |_ctx: &SessionContext, sql: &str, p, t| {
            let py_worker = py_worker.clone();
            let sql_owned = sql.to_string();
            async move {
                let res = py_worker
                    .on_query(sql_owned, p, t, do_describe, connection_id)
                    .await;                  // already ArrowQueryResult
                Ok(res)
            }
        };

        dispatch_query(&ctx, &query, params, param_types, handler).await
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
    ) -> datafusion::error::Result<ArrowQueryResult> {
        Ok(
            self.py_worker
                .on_query(query, params, param_types, do_describe, connection_id)
                .await,                     // ArrowQueryResult, no rebuilding
        )
    }
}

pub struct RiffqProcessor {
    py_worker: Arc<PythonWorker>,
    conn_id_sender: Arc<Mutex<Option<oneshot::Sender<u64>>>>,
    query_runner: Arc<dyn QueryRunner>,
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
                    finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
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

                finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
            }
            _ => {}
        }
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
        let lowercase = query.trim().to_lowercase();
        if lowercase.starts_with("begin") {
            return Ok(vec![Response::Execution(Tag::new("BEGIN"))]);
        } else if lowercase.starts_with("commit") {
            return Ok(vec![Response::Execution(Tag::new("COMMIT"))]);
        } else if lowercase.starts_with("rollback") {
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        }


        debug!("[PGWIRE] do_query called with: {}", query);
        let connection_id = _client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let (schema, data_row_stream) = {
            let (batches, schema) = self
                .query_runner
                .execute(query.to_string(), None, None, false, connection_id)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            arrow_to_pg_rows(batches, schema)
        };

        Ok(vec![Response::Query(QueryResponse::new(schema, data_row_stream))])
    }
}

pub struct MyExtendedQueryHandler {
    query_runner: Arc<dyn QueryRunner>,
}
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


fn _debug_parameters(params: &[Option<Bytes>], types: &[Type]) -> String {
    params.iter().zip(types.iter()).map(|(param, ty)| {
        match param {
            None => "NULL".to_string(),
            Some(bytes) => {
                let mut buf = &bytes[..];
                let decoded = match ty {
                    &Type::INT2 => i16::from_sql(ty, &mut buf).map(|v| v.to_string()),
                    &Type::INT4 => i32::from_sql(ty, &mut buf).map(|v| v.to_string()),
                    &Type::INT8 => i64::from_sql(ty, &mut buf).map(|v| v.to_string()),
                    &Type::FLOAT4 => f32::from_sql(ty, &mut buf).map(|v| v.to_string()),
                    &Type::FLOAT8 => f64::from_sql(ty, &mut buf).map(|v| v.to_string()),
                    &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR => {
                        String::from_sql(ty, &mut buf).map(|s| format!("{:?}", s))
                    }
                    _ => Err("unsupported type".into())
                };
                decoded.unwrap_or_else(|_| format!("0x{}", hex::encode(bytes)))
            }
        }
    }).collect::<Vec<_>>().join(", ")
}


#[async_trait]
impl ExtendedQueryHandler for MyExtendedQueryHandler {
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


        let connection_id = _client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let (schema, data_row_stream) = {
            let (batches, schema) = self
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
            arrow_to_pg_rows(batches, schema)
        };

        Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
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
        let params = &portal.parameters;

        let connection_id = _client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let (batches, schema) = self
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
    extended_handler: Arc<MyExtendedQueryHandler>,
}

impl PgWireServerHandlers for RiffqProcessorFactory {
    type StartupHandler = RiffqProcessor;
    type SimpleQueryHandler = RiffqProcessor;
    type ExtendedQueryHandler = MyExtendedQueryHandler;
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
            let (ctx, _) = get_base_session_context(None, "datafusion".to_string(), "public".to_string()).await.unwrap();
            let catalog_ctx = Arc::new(ctx);
            let query_runner: Arc<dyn QueryRunner> = if catalog_emulation {
                Arc::new(RouterQueryRunner { py_worker: py_worker.clone(), catalog_ctx: catalog_ctx.clone() })
            } else {
                Arc::new(DirectQueryRunner { py_worker: py_worker.clone() })
            };

            let listener = TcpListener::bind(&addr).await.unwrap();
            info!("Listening on {}", addr);

            let server_task = tokio::spawn({
                let tls_acceptor = if tls { self.tls_acceptor.lock().unwrap().clone() } else { None };
                let py_worker = py_worker.clone();
                async move {
                    loop {
                        let (socket, addr) = listener.accept().await.unwrap();
                        if let Some(socket) = detect_gssencmode(socket).await {
                            let tls_acceptor_ref = tls_acceptor.clone();
                            let (id_tx, id_rx) = oneshot::channel();
                            let handler = Arc::new(RiffqProcessor {
                                py_worker: py_worker.clone(),
                                conn_id_sender: Arc::new(Mutex::new(Some(id_tx))),
                                query_runner: query_runner.clone(),
                            });
                            let factory = Arc::new(RiffqProcessorFactory {
                                handler: handler.clone(),
                                extended_handler: Arc::new(MyExtendedQueryHandler { query_runner: query_runner.clone() }),
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
fn _riffq(_py: Python, m: &PyModule) -> PyResult<()> {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).try_init();

    m.add_class::<Server>()?;
    Ok(())
}
