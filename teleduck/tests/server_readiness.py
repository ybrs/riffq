"""
Readiness helpers for the teleduck server tests. The implementations live in
riffq.testing; this re-exports them and defaults the catalog poll to teleduck's
user:123 credentials, so the tests can keep importing from server_readiness.
"""
from pathlib import Path

from riffq.testing import stop_server, wait_for_catalog as _wait_for_catalog

__all__ = ["stop_server", "wait_for_catalog", "duckdb_database_name"]


def duckdb_database_name(db_file):
    """The database name DuckDB gives the file at `db_file`.

    DuckDB names an attached database after the file stem, and that name is what
    a client must connect as: riffq serves one catalog context per database the
    source reports and refuses any other name. Tests that create the file before
    the server exists cannot ask DuckDB itself, so they derive it here.
    """
    return Path(db_file).stem


def wait_for_catalog(port, database_name, probe_sql, expected_value):
    """Waits for the catalog as teleduck's authenticated user (user:123)."""
    return _wait_for_catalog(
        port, database_name, probe_sql, expected_value, password="123"
    )
