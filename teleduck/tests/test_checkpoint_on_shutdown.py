import multiprocessing
import os
import signal
import socket
import tempfile
import time
import unittest
from pathlib import Path
import duckdb
from server_readiness import wait_for_catalog


def _run_server(db_file, port, sqls):
    """starts a teleduck server over db_file after running the given sql."""
    import sys
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))
    for mod in ["teleduck.server", "teleduck"]:
        if mod in sys.modules:
            del sys.modules[mod]
    from teleduck.server import run_server
    run_server(db_file, port, sql=sqls, read_only=False)


def _wait_for_socket(port):
    """blocks until the server accepts TCP connections, or raises on timeout."""
    for _attempt in range(600):
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return

        time.sleep(0.1)

    raise RuntimeError(f"server never bound port {port}")


class CheckpointOnShutdownTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 55463
        fd, cls.db_file = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(cls.db_file)
        sqls = ["CREATE TABLE t(v INTEGER);", "INSERT INTO t VALUES (1), (2), (3);"]
        cls.proc = multiprocessing.Process(
            target=_run_server, args=(cls.db_file, cls.port, sqls), daemon=True
        )
        cls.proc.start()
        _wait_for_socket(cls.port)
        wait_for_catalog(cls.port, "db", "SELECT count(*) FROM t", 3)

    @classmethod
    def tearDownClass(cls):
        if cls.proc.is_alive():
            cls.proc.kill()
            cls.proc.join()

        Path(cls.db_file).unlink(missing_ok=True)

    def test_sigterm_checkpoints_and_exits(self):
        """
        SIGTERM must trigger riffq's handle_shutdown, which checkpoints DuckDB and
        exits cleanly. previously the python signal handler could not run while
        the main thread was parked in rust, so the server zombied and the data
        was never flushed to disk.
        """
        os.kill(self.proc.pid, signal.SIGTERM)
        self.proc.join(timeout=15)
        self.assertFalse(self.proc.is_alive(), "server zombied after SIGTERM")

        with duckdb.connect(self.db_file, read_only=True) as con:
            persisted = con.execute("SELECT count(*) FROM t").fetchone()[0]

        self.assertEqual(persisted, 3)


if __name__ == "__main__":
    unittest.main()
