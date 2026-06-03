import multiprocessing
import socket
import time
import tempfile
import os
from pathlib import Path
import psycopg
import unittest
from server_readiness import wait_for_catalog, stop_server


def _run_server(db_file: str, port: int, scripts, sqls):
    import sys
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))
    for mod in ["teleduck.server", "teleduck"]:
        if mod in sys.modules:
            del sys.modules[mod]
    from teleduck.server import run_server
    run_server(db_file, port, sql_scripts=scripts, sql=sqls, read_only=False)


class SqlInitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55442
        fd, cls.db_file = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(cls.db_file)

        cls.script1 = tempfile.NamedTemporaryFile(delete=False, suffix=".sql")
        cls.script1.write(b"CREATE TABLE t(v INTEGER);")
        cls.script1.close()
        cls.script2 = tempfile.NamedTemporaryFile(delete=False, suffix=".sql")
        cls.script2.write(b"INSERT INTO t VALUES (1);")
        cls.script2.close()

        cls.proc = multiprocessing.Process(
            target=_run_server,
            args=(cls.db_file, cls.port, [cls.script1.name, cls.script2.name], ["INSERT INTO t VALUES (2);"]),
            daemon=True,
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

        # the socket binds before the init scripts/sql have populated the db,
        # so poll the seeded row count until it settles instead of sleeping.
        waited = wait_for_catalog(
            cls.port,
            "db",
            "SELECT count(*) FROM t",
            2,
        )
        print(f"server ready after {waited:.2f}s")

    @classmethod
    def tearDownClass(cls):
        stop_server(cls.proc)
        # force-kill skips the shutdown checkpoint, so DuckDB may never flush
        # the .db file to disk; tolerate its absence when cleaning up.
        Path(cls.db_file).unlink(missing_ok=True)
        Path(cls.script1.name).unlink(missing_ok=True)
        Path(cls.script2.name).unlink(missing_ok=True)

    def test_scripts_executed(self):
        conn = psycopg.connect(
            f"postgresql://user:123@127.0.0.1:{self.port}/db"
        )
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM t")
            self.assertEqual(cur.fetchone()[0], 2)
        conn.close()


if __name__ == "__main__":
    unittest.main()
