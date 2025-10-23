"""Connection primitives and the high-level server wrapper.

This module defines two main concepts:

- `BaseConnection`: an abstract per client connection that you subclass to
  implement authentication, query execution and lifecycle hooks.
- `RiffqServer`: a small orchestrator that owns the Rust `Server`, creates
  `BaseConnection` instances on demand, and forwards events to them.

Callbacks

The Rust layer invokes Python callbacks with a `callback` function argument
that must be called to deliver results back to the server. Query callbacks
accept an Arrow C Stream capsule for result sets, or an error.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Type
import pyarrow as pa
from ._riffq import Server
from abc import ABCMeta, abstractmethod
import logging
import os

logger = logging.getLogger('riffq:connection')

class BaseConnection(metaclass=ABCMeta):
    """Abstract per client connection.

    Subclass this to implement authentication and query handling. One instance
    is created per remote client and reused for its lifecycle.

    Args:
        conn_id: Unique identifier assigned by the server.
        executor: Thread pool used for offloading blocking work.
    """

    conn_id = None

    def __init__(self, conn_id: int, executor: ThreadPoolExecutor) -> None:
        self.conn_id = conn_id
        self.executor = executor

    def send_reader(self, reader: Any, callback: Callable[..., None]) -> None:
        """Send a PyArrow reader back to the server.

        Converts a `pyarrow.RecordBatchReader` (or compatible) into an Arrow C
        Stream capsule and passes it to the provided `callback`.

        Args:
            reader: A `RecordBatchReader` (or object exposing `__arrow_c_stream__`).
            callback: Callable receiving a single C Stream capsule object.
        """
        if hasattr(reader, "__arrow_c_stream__"):
            capsule = reader.__arrow_c_stream__()
        else:
            # old pyarrow support
            from pyarrow.cffi import export_stream

            capsule = export_stream(reader)
        callback(capsule)

    def arrow_batch(self, values: Sequence[pa.Array], names: Sequence[str]) -> pa.RecordBatchReader:
        """Create a `RecordBatchReader` from arrays and names.

        Args:
            values: Sequence of `pyarrow.Array` values, one per column.
            names: Column names aligned with `values`.

        Returns:
            pyarrow.RecordBatchReader: Reader yielding a single batch.
        """
        return pa.RecordBatchReader.from_batches(
            pa.schema(list(zip(names, [v.type for v in values]))),
            [pa.record_batch(values, names=names)],
        )

    @abstractmethod
    def handle_auth(self, user: str, password: str, host: str, database: Optional[str] = None, callback: Callable[..., None] = lambda *a, **k: None) -> None:
        """Authenticate a client.

        Args:
            user: Username supplied by the client.
            password: Password supplied by the client.
            host: Requested host/server name.
            database: Optional initial database name.
            callback: Invoke with `True`/`False` or raise to signal errors.
        """
        return callback(user == "user" and password == "secret")

    def handle_connect(self, ip: str, port: int, server_name:str=None, callback: Callable[..., None] = lambda *a, **k: None) -> None:
        """Handle successful TCP connection establishment.

        Args:
            ip: Remote peer IP address.
            port: Remote peer port.
            callback: Invoke with `True` to accept or raise to reject.
        """
        return callback(True)

    @abstractmethod
    def handle_query(self, sql: str, callback: Callable[..., None] = lambda *a, **k: None, **kwargs: Any) -> None:
        """Execute a SQL statement and return results.

        Implementations should produce a `pyarrow.RecordBatchReader` and pass
        its Arrow C Stream capsule to `callback`. To indicate an error, raise an
        exception or pass an error to the callback if supported.

        Args:
            sql: The SQL text to execute.
            callback: Callable to receive an Arrow C Stream capsule.
            **kwargs: Transport or driver specific flags (e.g., timeouts).
        """
        pass

    def handle_disconnect(self, ip: str, port: int, callback: Callable[..., None] = lambda *a, **k: None) -> None:
        """Handle client disconnect cleanup.

        Args:
            ip: Remote peer IP address.
            port: Remote peer port.
            callback: Invoke with `True` to acknowledge.
        """
        return callback(True)


class RiffqServer:
    """High level server that manages connections and forwards events.

    Args:
        listen_addr: Address string (e.g., "127.0.0.1:5432") to bind.
        connection_cls: Subclass of `BaseConnection` used per client.
    """

    def __init__(self, listen_addr: str, connection_cls: Type[BaseConnection] = BaseConnection) -> None:
        # we aren't making the server class in rust/pyo3 subclassable
        # this way it's simpler
        self._server = Server(listen_addr)
        # TODO: max cpu
        self.executor = ThreadPoolExecutor(max_workers=4)
        self._server.on_authentication(self.handle_auth)
        self._server.on_query(self.handle_query)
        self._server.on_connect(self.handle_connect)
        self._server.on_disconnect(self.handle_disconnect)
        self.connections: Dict[int, BaseConnection] = {}
        self.connection_cls: Type[BaseConnection] = connection_cls

    def set_tls(self, crt:str, key:str):
        """Enable TLS with certificate and key files.

        Args:
            crt: Path to PEM encoded server certificate.
            key: Path to PEM encoded private key.

        Raises:
            OSError: If `crt` or `key` does not exist.
        """
        if not os.path.exists(crt):
            raise OSError(f"Certificate not found in {crt}")
        if not os.path.exists(key):
            raise OSError(f"Certificate keyfile not found in {key}")

        self._server.set_tls(crt, key)

    def register_database(self, database_name: str) -> None:
        """Register a logical database for catalog emulation.

        When `start(catalog_emulation=True)` is used, the server responds to
        client metadata queries (pg_catalog) using entries registered via these
        helpers.

        Args:
            database_name: Name of the database to expose via `pg_catalog`.
        """
        self._server.register_database(database_name)

    def register_schema(self, database_name: str, schema_name: str) -> None:
        """Register a schema under a database for catalog emulation.

        Args:
            database_name: Existing database registered via `register_database`.
            schema_name: Schema name to add under the database.
        """
        self._server.register_schema(database_name, schema_name)

    def register_table(self, database_name: str, schema_name: str, table_name: str, columns: List[Dict[str, Dict[str, Any]]]) -> None:
        """Register a table and its columns for catalog emulation.

        The `columns` argument describes each column as a single key dict mapping
        the column name to a small descriptor: `{ "name": { "type": <str>, "nullable": <bool> } }`.

        Supported `type` strings are aligned with `riffq.helpers.to_arrow` mapping
        and include: `int`, `float`, `bool`, `str`/`string`, `date`, `datetime`.

        Args:
            database_name: Target database name.
            schema_name: Target schema name.
            table_name: Table name to register.
            columns: Column descriptors as described above.
        """
        self._server.register_table(database_name, schema_name, table_name, columns)

    def get_connection(self, connection_id: int) -> BaseConnection:
        """Get or create the `BaseConnection` for a given `connection_id`."""
        conn = self.connections.get(connection_id, None)
        if not conn:
            conn = self.connection_cls(connection_id, self.executor)
            self.connections[connection_id] = conn
        return conn

    def handle_auth(self, connection_id: int, user: str, password: str, host: str, database: Optional[str] = None, callback: Callable[..., None] = lambda *a, **k: None) -> None:
        """Forward an authentication request to the connection instance.

        Args:
            connection_id: Server assigned identifier for this client.
            user: Username supplied by the client.
            password: Password supplied by the client.
            host: Requested host/server name.
            database: Optional initial database name.
            callback: Function to receive auth result.
        """
        # logger.info(f"new auth {connection_id} {user} {host} {database}")
        conn = self.get_connection(connection_id=connection_id)
        conn.handle_auth(user, password, host, database=database, callback=callback)

    def handle_connect(self, connection_id: int, ip: str, port: int, server_name:str=None, callback: Callable[..., None] = lambda *a, **k: None) -> None:
        """Forward a connect notification to the connection instance.

        Args:
            connection_id: Server assigned identifier for this client.
            ip: Remote peer IP address.
            port: Remote peer port.
            callback: Function to acknowledge handling.
        """
        # logger.info(f"new connnection {connection_id} {ip} {port}")
        conn = self.get_connection(connection_id=connection_id)
        conn.handle_connect(ip, port, callback=callback, server_name=server_name)

    def handle_query(self, sql: str, callback: Callable[..., None], connection_id: Optional[int] = None, **kwargs: Any) -> None:
        """Forward a query to the connection instance.

        Args:
            sql: SQL string to execute.
            callback: Function to receive an Arrow C Stream capsule.
            connection_id: Server assigned identifier for this client.
            **kwargs: Driver specific flags forwarded as is.
        """
        # logger.debug(f"python query {connection_id} {sql}")
        conn = self.get_connection(connection_id=connection_id)
        conn.handle_query(sql, callback=callback, **kwargs)

    def handle_disconnect(self, connection_id: int, ip: str, port: int, callback: Callable[..., None] = lambda *a, **k: None) -> None:
        """Forward a disconnect notification and release the connection.

        Args:
            connection_id: Server assigned identifier for this client.
            ip: Remote peer IP address.
            port: Remote peer port.
            callback: Function to acknowledge handling.
        """
        # logger.info(f"python on disconnect {connection_id}")
        conn = self.get_connection(connection_id=connection_id)
        conn.handle_disconnect(ip, port, callback=callback)
        try:
            del self.connections[connection_id]
        except KeyError:
            logger.exception("Connection disconnected but not in self.connections")

    def start(self, tls: bool = False, catalog_emulation: bool = False, server_version: Optional[str] = None) -> None:
        """Start the server event loop.

        Args:
            tls: Turn ssl/tls on or off. When tls is true, remember you need to set server.set_tls(cert_path, key_path)
            catalog_emulation: Turn pg_catalog & information_schema query handling by riffq.
            server_version: Server version string eg: "17.6 (Homebrew)" If you omit this, we use hardcoded string in src/lib.rs
        Returns:
            None. Starts the underlying Rust server loop.
        """
        return self._server.start(tls=tls, catalog_emulation=catalog_emulation, server_version=server_version)
