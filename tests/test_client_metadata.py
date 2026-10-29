"""Handshake metadata: `on_connect` receives it, `get_client_metadata` serves it.

A client announces itself in the startup handshake (`user`, `database`,
`application_name`, `client_encoding`, ...). riffq snapshots that handshake
per connection: the `on_connect` handler receives it as `metadata`, and any
later query handler can ask for it back with
`server.get_client_metadata(connection_id)`.

The query handler here answers sentinel statements by reading the metadata
store itself, so every assertion below runs over the wire against a real
connection. A shared `Manager` dict carries the `on_connect`-seen metadata
back to the test process; `connection_id`-keyed lookups go through
`get_client_metadata` in the server process, which is also where `KeyError`
for unknown or disconnected ids must surface.
"""
import multiprocessing
import socket
import time
import unittest

import psycopg
from helpers import stop_server

import pyarrow as pa

# Application name the test connections announce in the startup handshake.
ANNOUNCED_APP_NAME = "metadata-probe"


def _run_server(port: int, seen, gone):
    """Serve metadata answers for sentinel statements, record handshakes."""
    import riffq

    def handle_query(sql, callback, **kwargs):
        connection_id = kwargs["connection_id"]
        if sql == "NOTE_MY_ID":
            batch = pa.record_batch(
                [pa.array([str(connection_id)], pa.string())], names=["val"]
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())
        elif sql == "WHAT_IS_MY_APP":
            try:
                metadata = server.get_client_metadata(connection_id)
            except KeyError:
                outcome = "GONE"
            else:
                outcome = metadata["application_name"]
            batch = pa.record_batch(
                [pa.array([outcome], pa.string())],
                names=["val"],
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())
        elif sql.startswith("IS_GONE "):
            unknown = int(sql.split(" ", 1)[1])
            try:
                server.get_client_metadata(unknown)
            except KeyError:
                outcome = "PRUNED"
            else:
                outcome = "STILL_THERE"
            batch = pa.record_batch(
                [pa.array([outcome], pa.string())], names=["val"]
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())
        elif sql == "UNKNOWN_ID":
            try:
                server.get_client_metadata(2**60)
            except KeyError:
                outcome = "KEYERROR"
            else:
                outcome = "NO-ERROR"
            batch = pa.record_batch(
                [pa.array([outcome], pa.string())], names=["val"]
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())
        else:
            batch = pa.record_batch(
                [pa.array(["unreachable"], pa.string())], names=["val"]
            )
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            callback(reader.__arrow_c_stream__())

    def handle_connect(conn_id, ip, port, *, callback, server_name=None, metadata=None):
        seen[conn_id] = dict(metadata)
        callback(True)

    def handle_disconnect(conn_id, ip, port):
        gone.append(conn_id)

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_connect(handle_connect)
    server.on_disconnect(handle_disconnect)
    server.start()


class ClientMetadataTest(unittest.TestCase):
    """The handshake snapshot is visible at connect and queryable later."""

    @classmethod
    def setUpClass(cls):
        """Start the server and share the seen-handshakes map with it."""
        cls.port = 55493
        cls.manager = multiprocessing.Manager()
        cls.seen = cls.manager.dict()
        cls.gone = cls.manager.list()
        cls.proc = multiprocessing.Process(
            target=_run_server, args=(cls.port, cls.seen, cls.gone), daemon=True
        )
        cls.proc.start()
        start = time.time()
        while time.time() - start < 10:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", cls.port)) == 0:
                    break
            time.sleep(0.1)
        else:
            stop_server(cls.proc)
            raise RuntimeError("Server did not start")

    @classmethod
    def tearDownClass(cls):
        """Stop the server and the manager holding the shared map."""
        stop_server(cls.proc)
        cls.manager.shutdown()

    def connect(self, **kwargs):
        """Open one connection announcing this test's application name."""
        return psycopg.connect(
            f"postgresql://user@127.0.0.1:{self.port}/db",
            application_name=ANNOUNCED_APP_NAME,
            **kwargs,
        )

    def query_text(self, sql):
        """Run one sentinel statement and return its single text value."""
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]

    def test_on_connect_receives_the_handshake(self):
        """The `on_connect` handler sees the client's startup parameters."""
        # Other tests in this class connect too, and every handshake is
        # recorded, so scope the assertion to this test's own connection.
        self.seen.clear()
        with self.connect():
            pass
        by_id = dict(self.seen)
        self.assertEqual(len(by_id), 1)
        metadata = next(iter(by_id.values()))
        self.assertEqual(metadata["application_name"], ANNOUNCED_APP_NAME)
        self.assertEqual(metadata["user"], "user")
        self.assertEqual(metadata["database"], "db")

    def test_query_handler_reads_back_the_same_snapshot(self):
        """`get_client_metadata` answers what `on_connect` received."""
        self.assertEqual(self.query_text("WHAT_IS_MY_APP"), ANNOUNCED_APP_NAME)

    def test_unknown_id_raises_key_error(self):
        """An id that never connected answers `KeyError`, not a guess."""
        self.assertEqual(self.query_text("UNKNOWN_ID"), "KEYERROR")

    def test_handshakeless_socket_disturbs_nothing(self):
        """A socket that closes without a startup message is a no-op.

        It never receives a connection id, so cleanup must skip it: inventing
        id 0 used to prune the first real client's metadata and fire a
        phantom `on_disconnect(0)`. A bare TCP connect/close (what a health
        check does) exercises exactly that path.
        """
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute("NOTE_MY_ID")
            first_id = int(cur.fetchone()[0])
            with socket.socket() as sock:
                sock.connect(("127.0.0.1", self.port))
            # The server-side cleanup runs on socket close; this waits it out.
            time.sleep(2.0)
            cur.execute("WHAT_IS_MY_APP")
            self.assertEqual(cur.fetchone()[0], ANNOUNCED_APP_NAME)
            self.assertNotIn(first_id, list(self.gone))
        finally:
            conn.close()

    def test_disconnect_prunes_the_snapshot(self):
        """A disconnected id is gone, so repeated churn cannot grow the store.

        Each connection notes its own id, disconnects, and a later connection
        asks after it: every one must answer pruned, which is what bounds the
        store to the live connection count.
        """
        gone = []
        for _ in range(3):
            with self.connect() as conn, conn.cursor() as cur:
                cur.execute("NOTE_MY_ID")
                gone.append(cur.fetchone()[0])
        for connection_id in gone:
            with self.connect() as conn, conn.cursor() as cur:
                cur.execute(f"IS_GONE {connection_id}")
                self.assertEqual(cur.fetchone()[0], "PRUNED")


if __name__ == "__main__":
    unittest.main()
