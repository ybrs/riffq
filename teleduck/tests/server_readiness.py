"""
Readiness helpers for the teleduck server tests. The implementations live in
riffq.testing; this re-exports them and defaults the catalog poll to teleduck's
user:123 credentials, so the tests can keep importing from server_readiness.
"""
from riffq.testing import stop_server, wait_for_catalog as _wait_for_catalog

__all__ = ["stop_server", "wait_for_catalog"]


def wait_for_catalog(port, database_name, probe_sql, expected_value):
    """Waits for the catalog as teleduck's authenticated user (user:123)."""
    return _wait_for_catalog(
        port, database_name, probe_sql, expected_value, password="123"
    )
