"""
readiness helpers for teleduck server tests. the catalog-emulation server
accepts TCP connections before its pg_catalog view of the DuckDB schema is
query-ready, so a fresh client can race a half-loaded catalog and get empty
results. instead of a fixed sleep, poll the real probe query until it answers.
"""
import time
import psycopg


def stop_server(proc, grace=2.0):
    """
    stops the test server without leaking a zombie. teleduck installs a SIGTERM
    handler for checkpoint-on-shutdown, but the main thread is blocked in the
    rust accept loop so that python handler never runs and terminate() alone
    leaves the daemon child alive holding the port. give SIGTERM a short grace
    period, then force-kill so the next test gets a free port.
    """
    proc.terminate()
    proc.join(timeout=grace)

    if proc.is_alive():
        proc.kill()
        proc.join()


def _connect(port, database_name):
    """
    opens a psycopg connection to the test server. returns None while the
    server is still starting so the caller can retry; any other error is a
    real failure and surfaces with full traceback.
    """
    dsn = f"postgresql://user:123@127.0.0.1:{port}/{database_name}"
    try:
        return psycopg.connect(dsn, connect_timeout=2)
    except psycopg.OperationalError:
        return None


def _probe_value(conn, probe_sql):
    """
    runs probe_sql and returns the first column of the first row, or None when
    the catalog has no answer yet (no rows or a query-side not-ready error).
    """
    with conn.cursor() as cur:
        try:
            cur.execute(probe_sql)
        except psycopg.errors.Error:
            return None

        row = cur.fetchone()

    if row is None:
        return None

    return row[0]


def wait_for_catalog(port, database_name, probe_sql, expected_value, attempts=300):
    """
    polls the running server until probe_sql returns expected_value, then
    returns the seconds waited. attempts*0.1s bounds the wait (default 30s).
    raises with full context if the catalog never becomes ready, so the race
    is surfaced loudly instead of hidden.
    """
    start = time.time()

    for _attempt in range(attempts):
        conn = _connect(port, database_name)

        if conn is not None:
            value = _probe_value(conn, probe_sql)
            conn.close()

            if value == expected_value:
                return time.time() - start

        time.sleep(0.1)

    raise RuntimeError(
        f"catalog not ready after {attempts * 0.1:.0f}s. "
        f"port={port}, database='{database_name}', "
        f"probe='{probe_sql}', expected='{expected_value}'."
    )
