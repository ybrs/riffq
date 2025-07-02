import unittest
from unittest.mock import patch
from click.testing import CliRunner
from pathlib import Path
import sys

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root / "src"))
from teleduck.__main__ import main

class CliTest(unittest.TestCase):
    def test_cli_invokes_run_server(self):
        runner = CliRunner()
        with patch('teleduck.__main__.run_server') as mock_run_server:
            result = runner.invoke(main, ['my.db', '--host', '127.0.0.1', '--port', '9999'])
            self.assertEqual(result.exit_code, 0)
            mock_run_server.assert_called_once_with(
                'my.db',
                9999,
                host='127.0.0.1',
                sql_scripts=[],
                sql=[],
                use_tls=True,
                tls_cert_file=None,
                tls_key_file=None,
                read_only=False,
            )

    def test_cli_sql_options(self):
        runner = CliRunner()
        with patch('teleduck.__main__.run_server') as mock_run_server:
            result = runner.invoke(
                main,
                [
                    'my.db',
                    '--sql-script', 'init.sql',
                    '--sql-script', 'data.sql',
                    '--sql', 'INSERT INTO t VALUES (1)',
                ],
            )
            self.assertEqual(result.exit_code, 0)
            mock_run_server.assert_called_once_with(
                'my.db',
                5433,
                host='127.0.0.1',
                sql_scripts=['init.sql', 'data.sql'],
                sql=['INSERT INTO t VALUES (1)'],
                use_tls=True,
                tls_cert_file=None,
                tls_key_file=None,
                read_only=False,
            )

    def test_cli_tls_options(self):
        runner = CliRunner()
        with patch('teleduck.__main__.run_server') as mock_run_server:
            result = runner.invoke(
                main,
                [
                    'my.db',
                    '--no-use-tls',
                    '--tls-cert-file', 'c.crt',
                    '--tls-key-file', 'k.key',
                ],
            )
            self.assertEqual(result.exit_code, 0)
            mock_run_server.assert_called_once_with(
                'my.db',
                5433,
                host='127.0.0.1',
                sql_scripts=[],
                sql=[],
                use_tls=False,
                tls_cert_file='c.crt',
                tls_key_file='k.key',
                read_only=False,
            )

    def test_cli_read_only_option(self):
        runner = CliRunner()
        with patch('teleduck.__main__.run_server') as mock_run_server:
            result = runner.invoke(main, ['my.db', '--read-only'])
            self.assertEqual(result.exit_code, 0)
            mock_run_server.assert_called_once_with(
                'my.db',
                5433,
                host='127.0.0.1',
                sql_scripts=[],
                sql=[],
                use_tls=True,
                tls_cert_file=None,
                tls_key_file=None,
                read_only=True,
            )

if __name__ == '__main__':
    unittest.main()
