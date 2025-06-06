use pyo3::prelude::*;
use pyo3::{FromPyObject, PyAny};
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use futures::{Sink, StreamExt};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::oneshot;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::TcpStream;
use pyo3::types::{PyDict, PyList, PyTuple, PyCapsule};
use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::thread;

use bytes::Bytes;
use std::error::Error;
use std::ffi::c_void;

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::array::{Array, RecordBatch};
use arrow::array::cast::AsArray;
use arrow::record_batch::RecordBatchReader;
use arrow::datatypes::DataType;
use postgres_types::FromSql;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{SimpleQueryHandler, ExtendedQueryHandler};
use pgwire::api::results::{DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, ClientPortalStore, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::api::portal::Portal;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
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


pub struct WorkerMessage {
    pub query: String,
    pub params: Option<Vec<Option<Bytes>>>,
    pub param_types: Option<Vec<Type>>,
    pub do_describe: bool,
    pub responder: oneshot::Sender<(Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>)>,
}

#[pyclass]
struct CallbackWrapper {
    responder: Arc<Mutex<Option<oneshot::Sender<(Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>)>>>>,
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
                        return;
                    } else {
                        println!("[RUST] arrow_stream_to_rows failed for capsule");
                    }
                } else if let Ok(ptr_val) = result.extract::<usize>(py) {
                    println!("[RUST] received usize pointer: {}", ptr_val);
                    let ptr = ptr_val as *mut c_void;
                    if let Ok(res) = arrow_stream_to_rows(ptr) {
                        let _ = sender.send(res);
                        return;
                    } else {
                        println!("[RUST] arrow_stream_to_rows failed for int");
                    }
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
        _ => Some("".to_string()),
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
}

impl PythonWorker {
    fn new(callback: Arc<Mutex<Option<Py<PyAny>>>>) -> Self {
        let (tx, rx) = channel::<WorkerMessage>();

        thread::spawn(move || {
            println!("[PY_WORKER] Thread started");
            pyo3::prepare_freethreaded_python();
            loop {
                println!("[PY_WORKER] waiting to receive on rx...");
                match rx.recv() {
                    Ok(msg) => {
                        let WorkerMessage {
                            query,
                            params,
                            param_types,
                            do_describe,
                            responder,
                        } = msg;

                        println!("[PY_WORKER] received query: {}", query);
                        let cb_opt = callback.lock().unwrap().clone();

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

                    // Ok((query, responder)) => {
                    //     println!("[PY_WORKER] received query: {}", query);
                    //     let cb_opt = callback.lock().unwrap().clone();
                    //     if let Some(cb) = cb_opt {
                    //         Python::with_gil(|py| {
                    //             println!("[PY_WORKER] GIL acquired, invoking callback");
                    //             let wrapper = Py::new(py, CallbackWrapper {
                    //                 responder: Arc::new(Mutex::new(Some(responder))),
                    //             }).unwrap();
                    //             // TODO: for extended query we need to pass another keyword argument "query_args"
                    //             // TODO: for extended query we need to pass another keyword argument "do_describe"
                    //             //   so user can decide what to do
                    //             let _ = cb.call1(py, PyTuple::new(py, &[query.into_py(py), wrapper.into_py(py)]));
                    //         });
                    //     }
                    // }
                    Err(_) => {
                        println!("[PY_WORKER] Channel closed");
                        break;
                    }
                }
            }
        });

        PythonWorker { sender: tx }
    }


    pub async fn query(
        &self,
        query: String,
        params: Option<Vec<Option<Bytes>>>,
        param_types: Option<Vec<Type>>,
        do_describe: bool,
    ) -> (Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>) {
        let (tx, rx) = oneshot::channel();
        println!("[RUST] Sending query to worker: {}", query);

        self.sender.send(WorkerMessage {
            query,
            params,
            param_types,
            do_describe,
            responder: tx,
        }).expect("Send failed!");

        rx.await.unwrap_or_else(|e| {
            println!("[RUST] Worker failed: {:?}", e);
            (vec![], vec![])
        })
    }
}

pub struct DummyProcessor {
    py_worker: Arc<PythonWorker>,
}

#[async_trait]
impl NoopStartupHandler for DummyProcessor {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!("connected {:?}: {:?}", client.socket_addr(), client.metadata());
        println!("Received message: {:?}", _message);
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
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
        let (schema_desc, rows_list) = self.py_worker
            .query(query.to_string(), None, None, false, )
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


        let (schema_desc, rows_list) = self.py_worker
            .query(
                query.to_string(),
                Some(portal.parameters.clone()),
                Some(portal.statement.parameter_types.clone()),
                false,
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




        let (schema_desc, rows_list) = self.py_worker
            .query(
                query.to_string(),
                Some(portal.parameters.clone()),
                Some(portal.statement.parameter_types.clone()),
                true,
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

struct DummyProcessorFactory {
    handler: Arc<DummyProcessor>,
    extended_handler: Arc<MyExtendedQueryHandler>,
}

impl PgWireServerHandlers for DummyProcessorFactory {
    type StartupHandler = DummyProcessor;
    type SimpleQueryHandler = DummyProcessor;
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

#[pyclass]
pub struct Server {
    addr: String,
    callback: Arc<Mutex<Option<Py<PyAny>>>>,
}

#[pymethods]
impl Server {
    #[new]
    fn new(addr: String) -> Self {
        Server {
            addr,
            callback: Arc::new(Mutex::new(None)),
        }
    }

    fn set_callback(&mut self, _py: Python, cb: Py<PyAny>) {
        *self.callback.lock().unwrap() = Some(cb);
    }


    fn start(&self, py: Python) {
        py.allow_threads(|| {
            self.run_server();
        });
    }

    fn run_server(&self) {
        let addr = self.addr.clone();
        let callback = self.callback.clone();

        if callback.lock().unwrap().is_none() {
            panic!("No callback set. Use set_callback() before starting the server.");
        }

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let py_worker = Arc::new(PythonWorker::new(callback));

            let factory = Arc::new(DummyProcessorFactory {
                handler: Arc::new(DummyProcessor { py_worker: py_worker.clone() }),
                extended_handler: Arc::new(MyExtendedQueryHandler { py_worker }),
            });

            let listener = TcpListener::bind(&addr).await.unwrap();
            println!("Listening on {}", addr);

            let server_task = tokio::spawn({
                let factory = factory.clone();
                async move {
                    loop {
                        let (socket, _) = listener.accept().await.unwrap();
                        if let Some(socket) = detect_gssencmode(socket).await {
                            let factory_ref = factory.clone();
                            tokio::spawn(async move {
                                if let Err(e) = process_socket(socket, None, factory_ref).await {
                                    eprintln!("process_socket error: {:?}", e);
                                }
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
