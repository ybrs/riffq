import multiprocessing
import socket
import time
import tempfile
import os
from pathlib import Path
import psycopg
import unittest


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
            cls.proc.terminate()
            cls.proc.join()
            raise RuntimeError("Server did not start")

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.join()
        os.unlink(cls.db_file)
        os.unlink(cls.script1.name)
        os.unlink(cls.script2.name)

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
