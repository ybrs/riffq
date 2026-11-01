"""Optional second JDBC client: SQL Workbench/J driven in batch console mode.

This is the best-effort tier from the plan. It is opt-in -- the jar is only
present after ``setup_toolchain.sh --with-jdbc-tool`` -- so the whole class skips
by default. Its value is exercising a real JDBC tool that issues catalog SQL a
hand-written harness never would, and confirming the data path stays clean under
it (the fixture query log records no errored statement).

SQL Workbench/J connects to riffq and runs ordinary SQL fine. Its metadata
commands (WbList, WbDescribe) go through DatabaseMetaData.getTables, which hits
the same catalog gap the pgjdbc metadata suite documents (an optimizer rule fails
casting 'pg_class' to Int32), so table listing is an expectedFailure here too.
"""
import subprocess
import tempfile
import unittest

from server_case import FixtureServerCase
from toolchain import JAVA, PGJDBC_JAR, SQLWORKBENCH_JAR, require_jdbc, require_jdbc_tool
from jdbc import jdbc_url


@require_jdbc()
@require_jdbc_tool()
class JdbcToolTest(FixtureServerCase):
    """Drive SQL Workbench/J against the fixture and check the data path stays clean."""

    PORT = 55553

    @classmethod
    def setUpClass(cls):
        """Start the fixture server and give the tool an isolated config dir."""
        super().setUpClass()
        cls.config_dir = tempfile.mkdtemp(prefix="riffq_sqlworkbench_")

    def _run_workbench(self, command):
        """Run one batch command through SQL Workbench/J, returning its output.

        Args:
            command: the SQL Workbench command string to execute.

        Returns:
            The tool's combined stdout/stderr as text.
        """
        result = subprocess.run(
            [
                JAVA,
                "-jar",
                SQLWORKBENCH_JAR,
                f"-configDir={self.config_dir}",
                f"-url={jdbc_url(self.PORT)}",
                "-driver=org.postgresql.Driver",
                f"-driverjar={PGJDBC_JAR}",
                "-username=user",
                "-password=secret",
                "-abortOnError=false",
                f"-command={command}",
            ],
            capture_output=True,
            text=True,
        )
        return result.stdout + result.stderr

    def test_tool_connects_and_runs_a_query(self):
        """The tool connects and runs a SELECT with no error and no errored statement."""
        output = self._run_workbench("SELECT id, name FROM public.customers ORDER BY id;")
        self.assertIn("successful", output)
        self.assertIn("executed successfully", output)
        self.assertNotIn("ERROR", output)
        self.assertEqual(self.errored_statements(), [], self.log_tail())

    def test_tool_lists_tables(self):
        """WbList (DatabaseMetaData.getTables) runs without error.

        WbList previously failed with the pg_catalog getTables error; that is now
        fixed, so the tool's table-listing command completes cleanly. (WbList
        reports the connection's current schema, which SQL Workbench/J sets to
        pg_catalog by default, so the fixture table names are not asserted here --
        the closed gap is the error, exercised end to end through a real tool.)
        """
        output = self._run_workbench("WbList;")
        self.assertNotIn("ERROR", output)
        self.assertNotIn("Exception", output)


if __name__ == "__main__":
    unittest.main()
