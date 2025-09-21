import unittest
import importlib.util
from concurrent.futures import ThreadPoolExecutor


def load_proxy_module():
    spec = importlib.util.spec_from_file_location(
        "proxy_under_test", "query-proxy/proxy.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class UpstreamError(Exception):
    def __init__(self, msg, code="XX000"):
        super().__init__(msg)
        self.pgcode = code
        self.pgerror = msg


class FakeAbortCursor:
    def __init__(self, conn):
        self._conn = conn
        self.description = None
        self.statusmessage = ""

    def execute(self, sql):
        s = sql.strip().lower()
        if self._conn.aborted:
            raise UpstreamError("ERROR:  current transaction is aborted, commands ignored until end of transaction block")
        if s == "select bad":
            self._conn.aborted = True
            raise UpstreamError("ERROR:  bad query", code="42846")
        if s == "select 1":
            self.description = [("?column?", None, None, None, None, None, None)]
            self.statusmessage = "SELECT 1"
        else:
            self.description = [("val", None, None, None, None, None, None)]
            self.statusmessage = "SELECT 1"

    def fetchall(self):
        if self.description and self.description[0][0] == "?column?":
            return [(1,)]
        return [(1,)]

    def close(self):
        pass


class FakeAbortConn:
    def __init__(self):
        self.aborted = False

    def cursor(self):
        return FakeAbortCursor(self)

    def rollback(self):
        self.aborted = False


class QueryProxyAbortTest(unittest.TestCase):
    def test_error_then_success(self):
        proxy = load_proxy_module()
        proxy.upstream_conn = FakeAbortConn()

        # Capture reader
        captured_reader = {}
        orig_send_reader = proxy.Connection.send_reader

        def patched_send_reader(self, reader, callback):
            captured_reader["reader"] = reader
            return orig_send_reader(self, reader, callback)

        proxy.Connection.send_reader = patched_send_reader  # type: ignore
        conn = proxy.Connection(conn_id=3, executor=ThreadPoolExecutor(max_workers=1))

        # First query errors
        captured = {}
        conn._handle_query("SELECT bad", lambda result, **kw: captured.update({"result": result, "kw": kw}))
        self.assertIn("kw", captured)
        self.assertTrue(captured["kw"].get("is_error"))

        # Next query should succeed (rollback happened)
        captured2 = {}
        conn._handle_query("SELECT 1", lambda result, **kw: captured2.update({"result": result, "kw": kw}))
        self.assertIn("reader", captured_reader)
        batch = captured_reader["reader"].read_next_batch()
        self.assertEqual(batch.to_pydict(), {"?column?": [1]})

        proxy.Connection.send_reader = orig_send_reader  # restore


if __name__ == "__main__":
    unittest.main()

