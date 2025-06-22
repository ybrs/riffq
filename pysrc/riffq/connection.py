from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import pyarrow as pa
from ._riffq import Server
from abc import ABCMeta, abstractmethod

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
        self.connections = {}
        self.connection_cls = connection_cls
    
    def set_tls(self, crt, key):
        self._server.set_tls(crt, key)

    def get_connection(self, conn_id) -> BaseConnection:
        conn = self.connections.get(conn_id, None)
        if not conn:
            conn = self.connection_cls(conn_id, self.executor)
            self.connections[conn_id] = conn
        return conn

    def handle_auth(self, conn_id, user, password, host, database=None, callback=callable):
        conn = self.get_connection(conn_id=conn_id)
        conn.handle_auth(user, password, host, database=database, callback=callback)

    def handle_connect(self, conn_id, ip, port, callback=callable):
        conn = self.get_connection(conn_id=conn_id)
        conn.handle_connect(ip, port, callback=callback)

    def handle_query(self, sql, callback, conn_id=None, **kwargs):
        conn = self.get_connection(conn_id=conn_id)
        conn.handle_query(sql, callback=callback, **kwargs)

    def start(self, **kw):
        return self._server.start(**kw)
