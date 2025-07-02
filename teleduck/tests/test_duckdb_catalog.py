import multiprocessing
import socket
import time
import tempfile
from pathlib import Path
import psycopg
import unittest
import duckdb


def _run_server(db_file: str, port: int):
    import sys
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))
    for mod in ["teleduck.server", "teleduck"]:
        if mod in sys.modules:
            del sys.modules[mod]
    from teleduck.server import run_server
    run_server(db_file, port, read_only=False)


class DuckDbCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55441
        fd, cls.db_file = tempfile.mkstemp(suffix=".db")
        Path(cls.db_file).unlink()  # remove so DuckDB can create it
        with duckdb.connect(cls.db_file) as con:
            con.execute("CREATE TABLE users(id INTEGER, name VARCHAR)")
            con.execute("CREATE TABLE projects(id INTEGER, name VARCHAR)")
            con.execute("CREATE TABLE tasks(id INTEGER, project_id INTEGER, description VARCHAR)")
            
            cls.database_name = con.execute(
            "SELECT database_name, path, type FROM duckdb_databases() where internal=false"
            ).fetchall()[0][0]

        cls.proc = multiprocessing.Process(
            target=_run_server, args=(cls.db_file, cls.port), daemon=True
        )
        cls.proc.start()
        start = time.time()
        while time.time() - start < 60:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", cls.port)) == 0:
                    break
            time.sleep(0.1)
        else:
            cls.proc.terminate()
            cls.proc.join()
            raise RuntimeError("Server did not start")

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.join()
        Path(cls.db_file).unlink(missing_ok=True)

    def test_catalog_entries(self):
        conn = psycopg.connect(f"postgresql://user:123@127.0.0.1:{self.port}/db")
        with conn.cursor() as cur:
            cur.execute(f"SELECT datname FROM pg_catalog.pg_database WHERE datname ='{self.database_name}' ")
            self.assertEqual(cur.fetchone()[0], self.database_name)
            cur.execute("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname='main'")
            self.assertEqual(cur.fetchone()[0], "main")
            for table in ("users", "projects", "tasks"):
                cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname=%s", (table,))
                self.assertEqual(cur.fetchone()[0], table)
        conn.close()


if __name__ == "__main__":
    unittest.main()
