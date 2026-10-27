"""The 0.3.0 registration strictness, asserted at the API surface it lives on.

Two behaviors RELEASE_NOTES.md documents as breaking had no direct test:
duplicate register_* calls raising ValueError at CALL time (the docstrings
promise registration-time errors, so a test that only fails at start() would
not hold them to it), and start() emitting a DeprecationWarning when declared
objects run against the in-memory oid registry. Neither needs a running
server: the register_* checks fire on the Server object, and the warning is
provoked in a subprocess with -W error::DeprecationWarning so the warning
itself - not a log line - is what the assertion observes.
"""
import subprocess
import sys
import textwrap
import unittest

import riffq


class DuplicateRegistrationIsRefusedTest(unittest.TestCase):
    """Each register_* kind refuses a duplicate the way PostgreSQL refuses
    CREATE of an existing name, at the moment of registration."""

    def setUp(self):
        """A fresh server per test; nothing is started."""
        self.server = riffq.Server("127.0.0.1:0")

    def test_registering_a_database_twice_raises(self):
        """The second register_database of one name is a ValueError naming it."""
        self.server.register_database("auth")
        with self.assertRaises(ValueError) as refused:
            self.server.register_database("auth")
        self.assertIn('database "auth" already exists', str(refused.exception))

    def test_registering_a_schema_twice_raises(self):
        """The second register_schema of one (database, schema) pair raises."""
        self.server.register_database("auth")
        self.server.register_schema("auth", "app")
        with self.assertRaises(ValueError) as refused:
            self.server.register_schema("auth", "app")
        self.assertIn(
            'schema "app" already exists in database "auth"',
            str(refused.exception),
        )

    def test_the_same_schema_name_under_two_databases_is_not_a_duplicate(self):
        """PostgreSQL's uniqueness key is (database, schema), not the bare
        name - so the same schema name under two databases registers fine."""
        self.server.register_database("auth")
        self.server.register_database("crm")
        self.server.register_schema("auth", "app")
        self.server.register_schema("crm", "app")

    def test_registering_a_table_twice_raises(self):
        """The second register_table of one (db, schema, table) triple raises."""
        column = [{"id": {"type": "int", "nullable": False}}]
        self.server.register_database("auth")
        self.server.register_table("auth", "app", "users", column)
        with self.assertRaises(ValueError) as refused:
            self.server.register_table("auth", "app", "users", column)
        self.assertIn('relation "users" already exists', str(refused.exception))


# Runs in a subprocess with -W error::DeprecationWarning: if start() emits the
# warning it becomes an exception BEFORE the server binds, the script prints
# the marker and exits - no port, no client, no teardown. A quiet start()
# would instead bind and block until the timeout kills it.
WARNING_PROBE = textwrap.dedent(
    """
    import riffq

    server = riffq.Server("127.0.0.1:55491")
    server.register_database("auth")
    server.on_query(lambda sql, callback, **kwargs: callback("OK", is_tag=True))
    try:
        server.start(catalog_emulation=True{extra_start_args})
    except DeprecationWarning as warning:
        print("WARNED:", warning)
    """
)


class InMemoryRegistryFallbackWarnsTest(unittest.TestCase):
    """start() says so - as a real Python DeprecationWarning, not only a log
    line - when declared objects fall back to the in-memory oid registry."""

    def _probe(self, extra_start_args, timeout):
        """Run the probe script with warnings-as-errors and return its output."""
        return subprocess.run(
            [sys.executable, "-W", "error::DeprecationWarning", "-c",
             WARNING_PROBE.format(extra_start_args=extra_start_args)],
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def test_declared_objects_without_a_registry_path_warn(self):
        """No oid_registry_path -> the DeprecationWarning fires, naming the
        remedy, before the server ever binds."""
        probe = self._probe("", timeout=60)
        self.assertIn("WARNED:", probe.stdout, probe.stderr)
        self.assertIn("oid_registry_path", probe.stdout)

    def test_a_registry_path_starts_without_warning(self):
        """With oid_registry_path given there is nothing to warn about: the
        server passes the warning point and binds, so the probe never prints
        the marker and has to be killed by the timeout."""
        import tempfile

        registry_path = tempfile.mktemp(suffix=".redb")
        with self.assertRaises(subprocess.TimeoutExpired) as still_serving:
            self._probe(f', oid_registry_path="{registry_path}"', timeout=15)
        captured = still_serving.exception.stdout or b""
        if isinstance(captured, bytes):
            captured = captured.decode()
        self.assertNotIn("WARNED:", captured)


if __name__ == "__main__":
    unittest.main()
