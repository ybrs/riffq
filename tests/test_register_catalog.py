import psycopg
import unittest
from helpers import launch_server_process, stop_server


def _run_server(port: int):
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    server = riffq.Server(f"127.0.0.1:{port}")
    server.register_database("mydb")
    server.register_schema("mydb", "myschema")
    server.register_table(
        "mydb",
        "myschema",
        "users",
        [{"id": {"type": "int", "nullable": False}}],
    )
    server.on_query(handle_query)
    server.start(catalog_emulation=True)


class RegisterCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55444
        cls.proc = launch_server_process(_run_server, cls.port)

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)

    def test_registered_objects(self):
        conn = psycopg.connect(f"postgresql://user@127.0.0.1:{self.port}/mydb")
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname='mydb'")
            self.assertEqual(cur.fetchone()[0], "mydb")
            cur.execute("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname='myschema'")
            self.assertEqual(cur.fetchone()[0], "myschema")
            cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname='users'")
            self.assertEqual(cur.fetchone()[0], "users")
        conn.close()


if __name__ == "__main__":
    unittest.main()
