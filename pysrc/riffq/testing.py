"""
Test helpers for riffq-backed servers. Stop a server subprocess without leaking
a zombie, and wait for a catalog-emulation server to become query-ready instead
of sleeping a fixed amount. Shared by every test suite (tests/, test_concurrency/,
teleduck/tests/), which are separate import roots that all import riffq.
"""
import time
import psycopg


def stop_server(proc, grace=2.0):
    """
    Stops a server child process without leaking a zombie. Sends SIGTERM (riffq
    shuts down gracefully on it), then SIGKILL if the process has not exited
    within the grace period, so a hung server still frees its port for the next
    test.
    """
    proc.terminate()
    proc.join(timeout=grace)

    if proc.is_alive():
        proc.kill()
        proc.join()


def _dsn(port, database, user, password):
    """Builds a postgres DSN, omitting the password section when unset."""
    if password is None:
        return f"postgresql://{user}@127.0.0.1:{port}/{database}"

    return f"postgresql://{user}:{password}@127.0.0.1:{port}/{database}"


def _connect(port, database, user, password):
    """
    Opens a connection to the test server, returning None while the server is
    still starting so the caller can retry. Other errors surface normally.
    """
    try:
        return psycopg.connect(_dsn(port, database, user, password), connect_timeout=2)
    except psycopg.OperationalError:
        return None


def _probe_value(connection, probe_sql):
    """
    Runs probe_sql and returns the first column of the first row, or None when
    the catalog has no answer yet (no rows or a query-side not-ready error).
    """
    with connection.cursor() as cur:
        try:
            cur.execute(probe_sql)
        except psycopg.errors.Error:
            return None

        row = cur.fetchone()

    if row is None:
        return None

    return row[0]


def wait_for_catalog(port, database, probe_sql, expected_value,
                     user="user", password=None, attempts=300):
    """
    Polls the running server until probe_sql returns expected_value, then returns
    the seconds waited. The socket binds before the pg_catalog emulation is
    query-ready, so retry the probe instead of sleeping a fixed amount. Raises
    with full context if the catalog never becomes ready.
    """
    start = time.time()

    for _attempt in range(attempts):
        connection = _connect(port, database, user, password)

        if connection is not None:
            value = _probe_value(connection, probe_sql)
            connection.close()

            if value == expected_value:
                return time.time() - start

        time.sleep(0.1)

    raise RuntimeError(
        f"catalog not ready after {attempts * 0.1:.0f}s. "
        f"port={port}, database='{database}', "
        f"probe='{probe_sql}', expected='{expected_value}'."
    )
