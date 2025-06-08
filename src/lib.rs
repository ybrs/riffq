use pyo3::prelude::*;
use pyo3::{FromPyObject, PyAny};
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt};
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
use arrow::datatypes::{DataType, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType};
use postgres_types::FromSql;

use chrono::{NaiveDate, NaiveDateTime, Duration};
use arrow::datatypes::TimeUnit;


use pgwire::api::auth::{LoginInfo, DefaultServerParameterProvider, StartupHandler, finish_authentication};
use pgwire::api::PgWireConnectionState;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{SimpleQueryHandler, ExtendedQueryHandler};
use pgwire::api::results::{DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, ClientPortalStore, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::api::portal::Portal;
use pgwire::error::{PgWireError, PgWireResult, ErrorInfo};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::messages::startup::Authentication;
use pgwire::messages::response::ErrorResponse;
use pgwire::tokio::process_socket;
use pgwire::api::stmt::{StoredStatement, NoopQueryParser};

fn map_python_type_to_pgwire(t: &str) -> Type {
    match t {
        "int" => Type::INT8,
        "float" => Type::FLOAT8,
        "str" | "string" => Type::VARCHAR,
        "bool" => Type::BOOL,
        "date" => Type::DATE,
        "datetime" => Type::TIMESTAMP,
        _ => Type::VARCHAR,
    }
}

fn map_arrow_type(dt: &DataType) -> &'static str {
    use DataType::*;
    match dt {
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => "int",
        Float16 | Float32 | Float64 => "float",
        Boolean => "bool",
        Utf8 | LargeUtf8 => "str",
        Date32 | Date64 => "date",
        Timestamp(_, _) => "datetime",
        _ => "str",
    }
}


pub enum WorkerMessage {
    Query {
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
        connection_id: u64,
        responder: oneshot::Sender<(Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>)>,
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
    responder: Arc<Mutex<Option<oneshot::Sender<(Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>)>>>>,
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
                println!("[RUST] result python type: {}", result.as_ref(py).get_type().name().unwrap_or("<unknown>"));
                if let Ok(capsule) = result.extract::<&PyCapsule>(py) {
                    println!("[RUST] received PyCapsule");
                    let ptr = capsule.pointer() as *mut c_void;
                    if let Ok(res) = arrow_stream_to_rows(ptr) {
                        let _ = sender.send(res);
                    } else {
                        println!("[RUST] arrow_stream_to_rows failed for capsule");
                    }
                    return;
                } 

                // First try to treat the result as Arrow IPC bytes. When the
                // callback returns bytes we assume they contain an Arrow IPC
                // stream produced by ``pyarrow``.  Use ``pyarrow`` again to
                // decode the bytes into a schema description and rows so that
                // the rest of the code can operate on plain Rust types.
                if let Ok(pybytes) = result.extract::<&pyo3::types::PyBytes>(py) {
                    let locals = PyDict::new(py);
                    locals.set_item("data", pybytes).unwrap();
                    // The Python snippet reads the IPC stream and converts it
                    // to two Python objects: ``schema_desc`` describing the
                    // columns and ``rows`` containing the record batches as a
                    // list of rows (lists). The column types are normalised to
                    // simple strings that map to pgwire types later on.
                    py.run(
                        r#"
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.types as pat
reader = ipc.open_stream(data)
table = reader.read_all()
def map_type(field):
    t = field.type
    if pat.is_integer(t):
        return "int"
    if pat.is_floating(t):
        return "float"
    if pat.is_boolean(t):
        return "bool"
    if pat.is_string(t) or pat.is_large_string(t):
        return "str"
    if pat.is_date(t):
        return "date"
    if pat.is_timestamp(t):
        return "datetime"
    return "str"
schema_desc = [{"name": f.name, "type": map_type(f)} for f in table.schema]
rows = [[row.get(f.name) for f in table.schema] for row in table.to_pylist()]
result_py = (schema_desc, rows)
"#,
                        None,
                        Some(locals),
                    )
                    .unwrap();
                    let tuple_any = match PyDict::get_item(locals, "result_py")
                        .unwrap()
                    {
                        Some(v) => v,
                        None => panic!("result_py missing"),
                    };
                    let parsed: (
                        Vec<HashMap<String, String>>,
                        Vec<Vec<PyObject>>,
                    ) = <(
                        Vec<HashMap<String, String>>,
                        Vec<Vec<PyObject>>,
                    ) as FromPyObject>::extract(tuple_any).unwrap();
                    let converted_rows: Vec<Vec<Option<String>>> = parsed
                        .1
                        .into_iter()
                        .map(|row| {
                            row.into_iter()
                                .map(|val| {
                                    Python::with_gil(|py| {
                                        let v = val.as_ref(py);
                                        if v.is_none() {
                                            Ok::<Option<String>, ()>(None)
                                        } else {
                                            match v.str() {
                                                Ok(pystr) => Ok(Some(pystr.to_str().unwrap_or("").to_string())),
                                                Err(_) => Ok(None),
                                            }
                                        }
                                    })
                                    .unwrap_or(None)
                                })
                                .collect()
                        })
                        .collect();
                    let _ = sender.send((parsed.0, converted_rows));
                    return;
                }

                let parsed: PyResult<(Vec<HashMap<String, String>>, Vec<Vec<PyObject>>)> = result.extract(py);
                if let Ok((schema_desc, py_rows)) = parsed {
                    let converted_rows: Vec<Vec<Option<String>>> = py_rows
                        .into_iter()
                        .map(|row| {
                            row.into_iter()
                                .map(|val| {
                                    Python::with_gil(|py| {
                                        let v = val.as_ref(py);
                                        if v.is_none() {
                                            Ok::<Option<String>, ()>(None)
                                        } else {
                                            match v.str() {
                                                Ok(pystr) => Ok(Some(pystr.to_str().unwrap_or("").to_string())),
                                                Err(_) => Ok(None),
                                            }
                                        }
                                    }).unwrap_or(None)
                                })
                                .collect()
                        })
                        .collect();
                    let _ = sender.send((schema_desc, converted_rows));
                }
            });
        }
    }
}

fn arrow_value_to_string(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Int8 => Some(array.as_primitive::<arrow::array::types::Int8Type>().value(row).to_string()),
        DataType::Int16 => Some(array.as_primitive::<arrow::array::types::Int16Type>().value(row).to_string()),
        DataType::Int32 => Some(array.as_primitive::<arrow::array::types::Int32Type>().value(row).to_string()),
        DataType::Int64 => Some(array.as_primitive::<arrow::array::types::Int64Type>().value(row).to_string()),
        DataType::UInt8 => Some(array.as_primitive::<arrow::array::types::UInt8Type>().value(row).to_string()),
        DataType::UInt16 => Some(array.as_primitive::<arrow::array::types::UInt16Type>().value(row).to_string()),
        DataType::UInt32 => Some(array.as_primitive::<arrow::array::types::UInt32Type>().value(row).to_string()),
        DataType::UInt64 => Some(array.as_primitive::<arrow::array::types::UInt64Type>().value(row).to_string()),
        DataType::Float32 => Some(array.as_primitive::<arrow::array::types::Float32Type>().value(row).to_string()),
        DataType::Float64 => Some(array.as_primitive::<arrow::array::types::Float64Type>().value(row).to_string()),
        DataType::Boolean => Some(array.as_boolean().value(row).to_string()),
        DataType::Utf8 => Some(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(row).to_string()),
        DataType::Date32 => {
            let days = array.as_primitive::<arrow::array::types::Date32Type>().value(row) as i64;
            let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days);
            Some(date.format("%Y-%m-%d").to_string())
        }
        DataType::Date64 => {
            let ms = array.as_primitive::<arrow::array::types::Date64Type>().value(row);
            let dt = NaiveDateTime::from_timestamp_opt(ms / 1000, (ms % 1000 * 1_000_000) as u32).unwrap();
            Some(dt.format("%Y-%m-%d %H:%M:%S").to_string())
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
            let dt = NaiveDateTime::from_timestamp_opt(secs, nsec).unwrap();
            Some(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        }
        _ => {
            println!("unknown data type");
            Some("".to_string())
        },
    }
}

fn arrow_stream_to_rows(ptr: *mut c_void) -> Result<(Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>), String> {
    unsafe {
        println!("[RUST] arrow_stream_to_rows: ptr={:?}", ptr);
        let mut reader = ArrowArrayStreamReader::from_raw(ptr as *mut _).map_err(|e| e.to_string())?;
        println!("[RUST] reader constructed");
        let schema = reader.schema();
        let mut schema_desc = Vec::new();
        for field in schema.fields() {
            let mut map = HashMap::new();
            map.insert("name".to_string(), field.name().to_string());
            map.insert("type".to_string(), map_arrow_type(field.data_type()).to_string());
            schema_desc.push(map);
        }
        let mut rows = Vec::new();
        while let Some(batch) = reader.next().transpose().map_err(|e| e.to_string())? {
            let num_rows = batch.num_rows();
            for i in 0..num_rows {
                let mut row = Vec::new();
                for col in batch.columns() {
                    row.push(arrow_value_to_string(col.as_ref(), i));
                }
                rows.push(row);
            }
        }
        Ok((schema_desc, rows))
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
            println!("[PY_WORKER] Thread started");
            pyo3::prepare_freethreaded_python();
            loop {
                println!("[PY_WORKER] waiting to receive on rx...");
                match rx.recv() {
                    Ok(msg) => match msg {
                        WorkerMessage::Query { query, params, param_types, do_describe, connection_id, responder } => {
                            println!("[PY_WORKER] received query: {}", query);
                            let cb_opt = query_cb.lock().unwrap().clone();

                            if let Some(cb) = cb_opt {
                                Python::with_gil(|py| {
                                    println!("[PY_WORKER] GIL acquired, invoking callback");
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
                        println!("[PY_WORKER] Channel closed");
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
    ) -> (Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>) {
        let (tx, rx) = oneshot::channel();
        println!("[RUST] Sending query to worker: {}", query);

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
            println!("[RUST] Worker failed: {:?}", e);
            (vec![], vec![])
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

pub struct RiffqProcessor {
    py_worker: Arc<PythonWorker>,
    conn_id_sender: Arc<Mutex<Option<oneshot::Sender<u64>>>>,
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


        println!("[PGWIRE] do_query called with: {}", query);
        // let (schema_desc, rows_list) = self.py_worker.query(query.to_string()).await;
        let connection_id = _client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let (schema_desc, rows_list) = self
            .py_worker
            .on_query(query.to_string(), None, None, false, connection_id)
            .await;
        println!("[PGWIRE] Schema and rows received");


        let mut fields = Vec::new();
        for col in &schema_desc {
            let col_name = col.get("name").unwrap();
            let col_type = col.get("type").unwrap();

            fields.push(FieldInfo::new(
                col_name.clone().into(),
                None,
                None,
                map_python_type_to_pgwire(col_type),
                FieldFormat::Text,
            ));
        }

        let schema = Arc::new(fields);
        let schema_ref = schema.clone();

        let data_row_stream =
            futures::stream::iter(rows_list.into_iter()).map(move |row| {
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                for val in row {
                    encoder.encode_field(&val)?;
                }
                encoder.finish()
            });

        Ok(vec![Response::Query(QueryResponse::new(schema, data_row_stream))])
    }
}

pub struct MyExtendedQueryHandler {
    pub py_worker: Arc<PythonWorker>,
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
        println!("[PGWIRE EXTENDED] do_query: {} {}", portal.statement.statement.query, _debug_parameters(&portal.parameters, &portal.statement.parameter_types));


        let connection_id = _client
            .metadata()
            .get("connection_id")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let (schema_desc, rows_list) = self
            .py_worker
            .on_query(
                query.to_string(),
                Some(portal.parameters.clone()),
                Some(portal.statement.parameter_types.clone()),
                false,
                connection_id,
            )
            .await;

        let mut fields = Vec::new();
        for col in &schema_desc {
            let col_name = col.get("name").unwrap();
            let col_type = col.get("type").unwrap();
            fields.push(FieldInfo::new(
                col_name.clone().into(),
                None,
                None,
                map_python_type_to_pgwire(col_type),
                FieldFormat::Text,
            ));
        }

        let schema = Arc::new(fields);
        let schema_ref = schema.clone();

        let data_row_stream = futures::stream::iter(rows_list.into_iter()).map(move |row| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            for val in row {
                encoder.encode_field(&val)?;
            }
            encoder.finish()
        });

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

        let (schema_desc, rows_list) = self
            .py_worker
            .on_query(
                query.to_string(),
                Some(portal.parameters.clone()),
                Some(portal.statement.parameter_types.clone()),
                true,
                connection_id,
            )
            .await;

        let mut fields = Vec::new();
        for col in &schema_desc {
            let col_name = col.get("name").unwrap();
            let col_type = col.get("type").unwrap();
            fields.push(FieldInfo::new(
                col_name.clone().into(),
                None,
                None,
                map_python_type_to_pgwire(col_type),
                FieldFormat::Text,
            ));
        }

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
                    println!("Failed to consume GSSAPI request: {:?}", e);
                }
                if let Err(e) = socket.write_all(b"N").await {
                    println!("Failed to send rejection message: {:?}", e);
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


    #[pyo3(signature = (tls=false))]
    fn start(&self, py: Python, tls: bool) {
        py.allow_threads(|| {
            self.run_server(tls);
        });
    }

    fn run_server(&self, tls: bool) {
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

            let listener = TcpListener::bind(&addr).await.unwrap();
            println!("Listening on {}", addr);

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
                            });
                            let factory = Arc::new(RiffqProcessorFactory {
                                handler: handler.clone(),
                                extended_handler: Arc::new(MyExtendedQueryHandler { py_worker: py_worker.clone() }),
                            });
                            let py_worker_clone = py_worker.clone();
                            let ip = addr.ip().to_string();
                            let port = addr.port();
                            tokio::spawn(async move {
                                if let Err(e) = process_socket(socket, tls_acceptor_ref, factory).await {
                                    eprintln!("process_socket error: {:?}", e);
                                }
                                let connection_id = id_rx.await.unwrap_or(0);
                                py_worker_clone.on_disconnect(connection_id, ip, port).await;
                            });
                        }
                    }
                }
            });

            signal::ctrl_c().await.expect("Failed to listen for shutdown signal");
            println!("Shutting down server");
            server_task.abort();
        });
    }
}

#[pymodule]
fn riffq(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Server>()?;
    Ok(())
}
