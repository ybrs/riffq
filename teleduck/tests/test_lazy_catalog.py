import multiprocessing
import socket
import time
import tempfile
from pathlib import Path
import psycopg
import unittest
import duckdb
from server_readiness import wait_for_catalog, stop_server


def _run_server(db_file: str, port: int):
    import sys
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))
    for mod in ["teleduck.server", "teleduck"]:
        if mod in sys.modules:
            del sys.modules[mod]
    from teleduck.server import run_server
    run_server(db_file, port, read_only=False)


class DuckDbLazyCatalogTest(unittest.TestCase):
    """Proves the catalog is lazy: a table created after the server started is
    visible in pg_catalog without any re-registration."""

    @classmethod
    def setUpClass(cls):
        cls.port = 55491
        fd, cls.db_file = tempfile.mkstemp(suffix=".db")
        Path(cls.db_file).unlink()  # remove so DuckDB can create it
        with duckdb.connect(cls.db_file) as con:
            con.execute("CREATE TABLE users(id INTEGER, name VARCHAR)")
            cls.database_name = con.execute(
                "SELECT database_name FROM duckdb_databases() WHERE internal=false"
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
            stop_server(cls.proc)
            raise RuntimeError("Server did not start")

        wait_for_catalog(
            cls.port,
            "db",
            f"SELECT datname FROM pg_catalog.pg_database WHERE datname='{cls.database_name}'",
            cls.database_name,
        )

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)
        Path(cls.db_file).unlink(missing_ok=True)

    def test_table_created_after_startup_is_visible(self):
        conn = psycopg.connect(
            f"postgresql://user:123@127.0.0.1:{self.port}/db", autocommit=True
        )
        with conn.cursor() as cur:
            # The table does not exist yet ...
            cur.execute("SELECT count(*) FROM pg_catalog.pg_class WHERE relname='late_arrivals'")
            self.assertEqual(cur.fetchone()[0], 0)

            # ... create it through the (DuckDB-backed) data path ...
            cur.execute("CREATE TABLE late_arrivals(id INTEGER, note VARCHAR)")

            # ... and the lazy catalog reflects it on the next scan.
            cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname='late_arrivals'")
            self.assertEqual(cur.fetchone()[0], "late_arrivals")

            # Its columns flow through information_schema too.
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='late_arrivals' ORDER BY ordinal_position"
            )
            self.assertEqual([r[0] for r in cur.fetchall()], ["id", "note"])

            # pg_tables flags are populated (not blank) for the user table, and
            # reflect reality: no index yet, and DuckDB has no triggers.
            cur.execute(
                "SELECT hasindexes, hastriggers FROM pg_catalog.pg_tables "
                "WHERE tablename='late_arrivals'"
            )
            self.assertEqual(cur.fetchone(), (False, False))

            # Create an index; hasindexes flips true on the next (lazy) scan.
            cur.execute("CREATE INDEX late_idx ON late_arrivals(id)")
            cur.execute(
                "SELECT hasindexes FROM pg_catalog.pg_tables WHERE tablename='late_arrivals'"
            )
            self.assertEqual(cur.fetchone()[0], True)
        conn.close()


if __name__ == "__main__":
    unittest.main()
