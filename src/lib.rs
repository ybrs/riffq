use pyo3::prelude::*;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use futures::{Sink, StreamExt};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::oneshot;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::TcpStream;
use pyo3::types::PyTuple;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::thread;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{SimpleQueryHandler, PlaceholderExtendedQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag
};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

fn map_python_type_to_pgwire(t: &str) -> Type {
    match t {
        "int" => Type::INT8,
        "float" => Type::FLOAT8,
        "str" => Type::VARCHAR,
        "bool" => Type::BOOL,
        "date" => Type::DATE,
        "datetime" => Type::TIMESTAMP,
        _ => Type::VARCHAR,
    }
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

pub struct PythonWorker {
    sender: Sender<(String, oneshot::Sender<(Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>)>)>,
}

impl PythonWorker {
    fn new(callback: Arc<Mutex<Option<Py<PyAny>>>>) -> Self {
        let (tx, rx): (
            Sender<(String, oneshot::Sender<(Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>)>)>,
            std::sync::mpsc::Receiver<(String, oneshot::Sender<(Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>)>)>
        ) = channel();

        thread::spawn(move || {
            println!("[PY_WORKER] Thread started");
            pyo3::prepare_freethreaded_python();
            loop {
                println!("[PY_WORKER] waiting to receive on rx...");
                match rx.recv() {
                    Ok((query, responder)) => {
                        println!("[PY_WORKER] received query: {}", query);
                        let cb_opt = callback.lock().unwrap().clone();
                        if let Some(cb) = cb_opt {
                            Python::with_gil(|py| {
                                println!("[PY_WORKER] GIL acquired, invoking callback");
                                let wrapper = Py::new(py, CallbackWrapper {
                                    responder: Arc::new(Mutex::new(Some(responder))),
                                }).unwrap();
                                let _ = cb.call1(py, PyTuple::new(py, &[query.into_py(py), wrapper.into_py(py)]));
                            });
                        }
                    }
                    Err(_) => {
                        println!("[PY_WORKER] Channel closed");
                        break;
                    }
                }
            }
        });

        PythonWorker { sender: tx }
    }


    async fn query(&self, query: String) -> (Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>) {
        let (tx, rx) = oneshot::channel();
        println!("[RUST] Sending query to worker: {}", query);
        self.sender.send((query, tx)).expect("Send failed!");
        match rx.await {
            Ok(result) => result,
            Err(e) => {
                println!("[RUST] Worker failed: {:?}", e);
                (vec![], vec![])
            }
        }
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
        println!("[PGWIRE] 1 do_query called with: {}", query);
        let (schema_desc, rows_list) = self.py_worker.query(query.to_string()).await;
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

struct DummyProcessorFactory {
    handler: Arc<DummyProcessor>,
}

impl PgWireServerHandlers for DummyProcessorFactory {
    type StartupHandler = DummyProcessor;
    type SimpleQueryHandler = DummyProcessor;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
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
                handler: Arc::new(DummyProcessor { py_worker }),
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
