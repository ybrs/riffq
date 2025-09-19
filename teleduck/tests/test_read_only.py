import unittest
from unittest.mock import patch
from pathlib import Path
import sys

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root / "src"))
from teleduck.server import run_server

class ReadOnlyTest(unittest.TestCase):
    def test_duckdb_connection_read_only(self):
        with patch('duckdb.connect') as mock_connect:
            with patch('teleduck.server.riffq.RiffqServer.start'):
                run_server('db.db', port=1111, read_only=True)
                mock_connect.assert_called_once_with('db.db', read_only=True)

if __name__ == '__main__':
    unittest.main()
