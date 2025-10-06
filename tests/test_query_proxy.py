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


class FakeCursorSelect:
    def __init__(self):
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        self.description = [
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None),
        ]
        self.statusmessage = "SELECT 2"

    def execute(self, sql):
        self._sql = sql

    def fetchall(self):
        return [
            (1, "Alice"),
            (2, None),
        ]


class FakeCursorDML:
    description = None
    statusmessage = "UPDATE 3"

    def execute(self, sql):
        self._sql = sql

    def fetchall(self):
        return []


class FakeConn:
    def __init__(self, cursor_impl):
        self._cursor_impl = cursor_impl

    def cursor(self):
        return self._cursor_impl


class QueryProxyTest(unittest.TestCase):
    def test_forward_select_rows(self):
        proxy = load_proxy_module()
        # Inject fake upstream connection
        proxy.upstream_conn = FakeConn(FakeCursorSelect())

        # Patch send_reader to capture the built Arrow reader
        captured_reader = {}
        original_send_reader = proxy.Connection.send_reader

        def patched_send_reader(self, reader, callback):
            captured_reader["reader"] = reader
            return original_send_reader(self, reader, callback)

        proxy.Connection.send_reader = patched_send_reader  # type: ignore

        # Build connection instance
        conn = proxy.Connection(conn_id=1, executor=ThreadPoolExecutor(max_workers=1))

        captured = {}

        def cb(result, **kwargs):
            captured["result"] = result
            captured["kwargs"] = kwargs

        from io import StringIO
        import contextlib
        buf = StringIO()
        with contextlib.redirect_stdout(buf):
            conn._handle_query("select id, name from t", cb)
        out = buf.getvalue()

        self.assertIn("SELECT", out)

        # Expect an Arrow reader was built with correct schema and values
        import pyarrow as pa

        self.assertIn("reader", captured_reader)
        batch = captured_reader["reader"].read_next_batch()
        self.assertEqual(batch.column_names, ["id", "name"])
        # types preserved: int64 and string
        self.assertTrue(pa.types.is_int64(batch.schema.field(0).type))
        self.assertTrue(pa.types.is_string(batch.schema.field(1).type))
        self.assertEqual(batch.to_pydict(), {"id": [1, 2], "name": ["Alice", None]})

        # Columns and types line printed
        self.assertIn("Columns:", out)
        self.assertIn("id: int64", out)
        self.assertIn("name: string", out)

        # Restore original method
        proxy.Connection.send_reader = original_send_reader  # type: ignore

    def test_forward_dml_tag(self):
        proxy = load_proxy_module()
        proxy.upstream_conn = FakeConn(FakeCursorDML())
        conn = proxy.Connection(conn_id=2, executor=ThreadPoolExecutor(max_workers=1))

        captured = {}

        def cb(result, **kwargs):
            captured["result"] = result
            captured["kwargs"] = kwargs

        conn._handle_query("UPDATE t SET a=1", cb)

        self.assertEqual(captured["result"], "UPDATE 3")
        self.assertTrue(captured["kwargs"].get("is_tag"))


if __name__ == "__main__":
    unittest.main()
