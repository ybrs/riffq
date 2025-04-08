use pyo3::prelude::*;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use futures::{Sink, StreamExt};
use tokio::net::TcpListener;
use tokio::signal;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;



use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{SimpleQueryHandler, PlaceholderExtendedQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag
};
use pgwire::api::{
    ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type
};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

pub struct DummyProcessor(Arc<Mutex<Option<Py<PyAny>>>>);

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


#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!("Received query: {}", query);

        let lowercase = query.trim().to_lowercase();
        if lowercase.starts_with("begin") {
            // Return "BEGIN" as the tag
            return Ok(vec![Response::Execution(Tag::new("BEGIN"))]);
        } else if lowercase.starts_with("commit") {
            return Ok(vec![Response::Execution(Tag::new("COMMIT"))]);
        } else if lowercase.starts_with("rollback") {
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        }

        println!("Received query 1.2: {}", query);

        if let Some(cb) = self.0.lock().unwrap().as_ref() {

            println!("Received query 2: {}", query);


            let result = Python::with_gil(|py| {
                cb.call1(py, (query,))
                    .and_then(|obj| {
                        obj.extract::<(
                            Vec<std::collections::HashMap<String, String>>,
                            Vec<Vec<PyObject>>,
                        )>(py)
                    })
                    .and_then(|(schema_desc, py_rows)| {
                        println!("Received query 3: {}", query);

                        // Pre-convert rows to Vec<Vec<Option<String>>>
                        let converted_rows: Vec<Vec<Option<String>>> = py_rows
                            .into_iter()
                            .map(|row| {
                                row.into_iter()
                                    .map(|val| {
                                        let s: Result<Option<String>, ()> = Python::with_gil(|py| {
                                            let v = val.as_ref(py);
                                            if v.is_none() {
                                                Ok(None)
                                            } else {
                                                match v.str() {
                                                    Ok(pystr) => Ok(Some(pystr.to_str().unwrap_or("").to_string())),
                                                    Err(_) => Ok(None),
                                                }
                                            }
                                        });
                                        s.unwrap_or(None)
                                    })
                                    .collect()
                            })
                            .collect();
                        Ok((schema_desc, converted_rows))
                    })
            });

            if let Ok((schema_desc, rows_list)) = result {
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

                return Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    data_row_stream,
                ))]);
            }
        }

        Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))])
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

    // Peek at the first 8 bytes without consuming them
    if let Ok(n) = socket.peek(&mut buf).await {
        println!("peeked 8 bytes: {:?}", &buf[..n]);

        if n == 8 {
            let request_code = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
            println!("request_code: {:?}", request_code);

            if request_code == 80877104 {
                println!("Client attempted GSSAPI encryption, but it is not supported.");
                if let Err(e) = socket.read_exact(&mut buf).await {
                    println!("Failed to consume GSSAPI request: {:?}", e);
                }
                // Send 'N' response to indicate GSSAPI encryption is not available
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

    fn start(&self) {
        pyo3::prepare_freethreaded_python();

        let addr = self.addr.clone();
        let callback = self.callback.clone();
        if callback.lock().unwrap().is_none() {
            panic!("No callback set. Use set_callback() before starting the server.");
        }
        //
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let factory = Arc::new(DummyProcessorFactory {
                handler: Arc::new(DummyProcessor(callback)),
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
