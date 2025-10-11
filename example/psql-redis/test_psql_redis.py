import types
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import importlib.util


class MockRedis:
    def __init__(self, host="localhost", port=6379, db=0, password=None, decode_responses=True):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.decode_responses = decode_responses
        self._hashes = {}

    # Hash operations
    def hset(self, key: str, field: str, value: str) -> int:
        h = self._hashes.setdefault(key, {})
        is_new = field not in h
        h[field] = value
        return 1 if is_new else 0

    def hget(self, key: str, field: str):
        return self._hashes.get(key, {}).get(field)

    def hgetall(self, key: str):
        return dict(self._hashes.get(key, {}))

    def hexists(self, key: str, field: str) -> bool:
        return field in self._hashes.get(key, {})

    def hdel(self, key: str, field: str) -> int:
        h = self._hashes.get(key)
        if not h or field not in h:
            return 0
        del h[field]
        return 1


class Capture:
    def __init__(self):
        self.tag = None
        self.error = None
        self.rows = None
        self.schema = None

    def callback(self, result, *args, **kwargs):
        if kwargs.get("is_error"):
            self.error = result
            return
        if kwargs.get("is_tag"):
            self.tag = result
            return
        # assume (schema_desc, rows) tuple in tests
        self.schema, self.rows = result


def _load_example_module():
    # Load the example module from its file path to avoid package name issues
    file_path = Path(__file__).with_name("psql_redis.py")
    spec = importlib.util.spec_from_file_location("psql_redis_example", file_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # type: ignore
    return mod


def make_conn():
    mod = _load_example_module()

    # Patch redis factory to our MockRedis for USE switching
    fake_redis_mod = types.SimpleNamespace(Redis=MockRedis)
    mod.redis = fake_redis_mod

    # Build connection with deterministic id and executor
    conn = mod.Connection(conn_id="test", executor=ThreadPoolExecutor(max_workers=1))
    # Seed mapping for this connection id
    mod.redis_connections[conn.conn_id] = MockRedis()

    # Monkeypatch send_reader to return (schema, rows) tuple for easy assertions
    def _send_reader_tuple(reader, cb):
        # Convert a RecordBatchReader to a simple (schema_desc, rows) tuple
        import pyarrow as pa
        batches = list(reader)
        if not batches:
            cb(([{"name": "id", "type": "string"}], []))
            return
        table = pa.Table.from_batches(batches)
        schema_desc = [{"name": f.name, "type": "string"} for f in table.schema]
        rows = [list(map(str, row)) for row in zip(*[table.column(i).to_pylist() for i in range(table.num_columns)])]
        cb((schema_desc, rows))

    conn.send_reader = _send_reader_tuple  # type: ignore
    return mod, conn


import unittest


class TestRedisExample(unittest.TestCase):
    def test_insert_quoted_and_unquoted_ids_unify(self):
        mod, conn = make_conn()
        cap = Capture()

        conn._handle_query('INSERT INTO items (id, value) VALUES ("foo", 1)', cap.callback)
        self.assertIn(cap.tag, ("INSERT 0 1", "INSERT 0 0"))

        cap2 = Capture()
        conn._handle_query('INSERT INTO items (id, value) VALUES (foo, 1)', cap2.callback)
        # second may be update of same id -> affected can be 0 or 1 depending on semantics
        self.assertIn(cap2.tag, ("INSERT 0 1", "INSERT 0 0"))

        store = mod.redis_connections[conn.conn_id]._hashes
        self.assertEqual(store["items"]["foo"], "1")

    def test_select_star_returns_rows(self):
        mod, conn = make_conn()
        # seed data
        r = mod.redis_connections[conn.conn_id]
        r.hset("items", "a", "10")
        r.hset("items", "b", "20")

        cap = Capture()
        conn._handle_query("SELECT * FROM items", cap.callback)
        self.assertIsNone(cap.error)
        self.assertIsNone(cap.tag)
        # Order is unspecified for hashes; check set semantics
        returned = {tuple(row) for row in (cap.rows or [])}
        self.assertEqual(returned, {("a", "10"), ("b", "20")})

    def test_update_and_delete_tags_and_effects(self):
        mod, conn = make_conn()
        r = mod.redis_connections[conn.conn_id]
        r.hset("items", "x", "1")

        cap_up = Capture()
        conn._handle_query("UPDATE items SET value = '2' WHERE id = 'x'", cap_up.callback)
        self.assertEqual(cap_up.tag, "UPDATE 1")
        self.assertEqual(r.hget("items", "x"), "2")

        cap_del = Capture()
        conn._handle_query("DELETE FROM items WHERE id = 'x'", cap_del.callback)
        self.assertEqual(cap_del.tag, "DELETE 1")
        self.assertIsNone(r.hget("items", "x"))

    def test_use_switches_db_instance(self):
        mod, conn = make_conn()
        before = mod.redis_connections[conn.conn_id].db
        cap = Capture()
        conn._handle_query("USE db2", cap.callback)
        after = mod.redis_connections[conn.conn_id].db
        self.assertEqual(before, 0)
        self.assertEqual(after, 2)
