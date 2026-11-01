"""Shared TestCase base that runs one fixture server per test class.

Every driver suite needs the same lifecycle: start the fixture server on a
class-specific port, wait until its catalog answers, tear it down afterwards,
and read back the JSON-lines query log. That boilerplate lives here so each test
module only declares its port and writes assertions.
"""
import json
import os
import tempfile
import unittest

from fixture_server import start_process
from riffq.testing import stop_server
from toolchain import format_query_log_tail


class FixtureServerCase(unittest.TestCase):
    """Base case that owns a fixture server for the lifetime of the class.

    Subclasses set ``PORT`` to a unique value. The server's query log is written
    to a per-port file so a failing assertion can show which driver statement
    broke.
    """

    # Overridden by each subclass to a port unique across the integration suite.
    PORT = None

    @classmethod
    def setUpClass(cls):
        """Start the fixture server and truncate its query log."""
        if cls.PORT is None:
            raise ValueError(f"{cls.__name__} must set PORT")
        cls.log_path = os.path.join(
            tempfile.gettempdir(), f"riffq_integration_{cls.PORT}.log"
        )
        open(cls.log_path, "w", encoding="utf-8").close()
        cls.server = start_process(cls.PORT, cls.log_path)

    @classmethod
    def tearDownClass(cls):
        """Stop the fixture server."""
        stop_server(cls.server)

    def errored_statements(self):
        """Return the SQL of every data-path statement the server logged as errored.

        The drivers issue their own catalog and handshake SQL; a statement that
        errored means the server could not answer something the driver asked for.
        """
        errored = []
        with open(self.log_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                if entry.get("errored"):
                    errored.append(entry["sql"])
        return errored

    def log_tail(self):
        """Return the tail of the query log, for inclusion in a failure message."""
        return format_query_log_tail(self.log_path)
