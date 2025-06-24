import unittest
from unittest.mock import patch
from click.testing import CliRunner

from teleduck.__main__ import main

class CliTest(unittest.TestCase):
    def test_cli_invokes_run_server(self):
        runner = CliRunner()
        with patch('teleduck.__main__.run_server') as mock_run_server:
            result = runner.invoke(main, ['my.db', '--host', '127.0.0.1', '--port', '9999'])
            self.assertEqual(result.exit_code, 0)
            mock_run_server.assert_called_once_with(
                'my.db', 9999, host='127.0.0.1', sql_scripts=[], sql=[]
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
            )

if __name__ == '__main__':
    unittest.main()
