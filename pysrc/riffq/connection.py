from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import pyarrow as pa
from ._riffq import Server
from abc import ABCMeta, abstractmethod
import logging
import os

logger = logging.getLogger('riffq:connection')

class BaseConnection(metaclass=ABCMeta):
    conn_id = None

    def __init__(self, conn_id, executor:ThreadPoolExecutor):
        self.conn_id = conn_id
        self.executor = executor

    def send_reader(self, reader, callback):
        if hasattr(reader, "__arrow_c_stream__"):
            capsule = reader.__arrow_c_stream__()
        else:
            # old pyarrow support
            from pyarrow.cffi import export_stream
            capsule = export_stream(reader)
        callback(capsule)

    def arrow_batch(self, values:list, names:list):
        return pa.RecordBatchReader.from_batches(
            pa.schema(list(zip(names, [v.type for v in values]))),
            [pa.record_batch(values, names=names)],
        )

    @abstractmethod
    def handle_auth(self, user, password, host, database=None, callback=callable):
        return callback(user == "user" and password == "secret")

    def handle_connect(self, ip, port, callback=callable):
        return callback(True)
    
    @abstractmethod
    def handle_query(self, sql, callback=callable, **kwargs):
        pass

    def handle_disconnect(self, ip, port, callback=callable):
        return callback(True)


class RiffqServer:
    def __init__(self, listen_addr, connection_cls=BaseConnection):
        # we aren't making the server class in rust/pyo3 subclassable
        # this way it's simpler
        self._server = Server(listen_addr)
        # TODO: max cpu
        self.executor = ThreadPoolExecutor(max_workers=4)
        self._server.on_authentication(self.handle_auth)
        self._server.on_query(self.handle_query)
        self._server.on_connect(self.handle_connect)
        self._server.on_disconnect(self.handle_disconnect)
        self.connections = {}
        self.connection_cls = connection_cls
    
    def set_tls(self, crt, key):
        if not os.path.exists(crt):
            raise OSError(f"Certificate not found in {crt}")
        if not os.path.exists(key):
            raise OSError(f"Certificate keyfile not found in {key}")

        self._server.set_tls(crt, key)

    def get_connection(self, connection_id) -> BaseConnection:
        conn = self.connections.get(connection_id, None)
        if not conn:
            conn = self.connection_cls(connection_id, self.executor)
            self.connections[connection_id] = conn
        return conn

    def handle_auth(self, connection_id, user, password, host, database=None, callback=callable):
        # logger.info(f"new auth {connection_id} {user} {host} {database}")
        conn = self.get_connection(connection_id=connection_id)
        conn.handle_auth(user, password, host, database=database, callback=callback)

    def handle_connect(self, connection_id, ip, port, callback=callable):
        # logger.info(f"new connnection {connection_id} {ip} {port}")
        conn = self.get_connection(connection_id=connection_id)
        conn.handle_connect(ip, port, callback=callback)

    def handle_query(self, sql, callback, connection_id=None, **kwargs):
        # logger.debug(f"python query {connection_id} {sql}")
        conn = self.get_connection(connection_id=connection_id)
        conn.handle_query(sql, callback=callback, **kwargs)

    def handle_disconnect(self, connection_id, ip, port, callback=callable):
        # logger.info(f"python on disconnect {connection_id}")
        conn = self.get_connection(connection_id=connection_id)
        conn.handle_disconnect(ip, port, callback=callback)
        try:
            del self.connections[connection_id]
        except KeyError:
            logger.exception("Connection disconnected but not in self.connections")

    def start(self, **kw):
        return self._server.start(**kw)
