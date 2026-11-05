"""
Test helpers for riffq-backed servers. Stop a server subprocess without leaking
a zombie, and wait for a catalog-emulation server to become query-ready instead
of sleeping a fixed amount. Shared by every test suite (tests/, test_concurrency/,
teleduck/tests/), which are separate import roots that all import riffq.
"""
import multiprocessing
import queue
import socket
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


def _run_target_reporting_errors(target, args, error_queue):
    """
    Runs target(*args), sending a text description of any exception back to the
    parent through error_queue. A target that returns normally sends nothing;
    the parent treats a death with no message as a bare crash and reports the
    exit code instead.
    """
    try:
        target(*args)
    except Exception as exc:
        error_queue.put("{}: {}".format(type(exc).__name__, exc))


def _drain_error_queue(error_queue):
    """Returns the latest message waiting in error_queue, or empty string."""
    latest = ""
    while True:
        try:
            latest = error_queue.get_nowait()
        except queue.Empty:
            return latest


def _is_address_in_use(detail):
    """True when a child-process error reads as a bind collision, not a bug."""
    return "address already in use" in detail.lower()


def launch_server_process(target, port, args=(), host="127.0.0.1",
                          bind_retry_seconds=5.0, startup_timeout=120.0,
                          start_method=None):
    """
    Starts target(port, *args) in a daemon subprocess and waits for it to listen
    on port. Returns the live process, which the caller stops with stop_server.

    A child that dies with EADDRINUSE is restarted while the retry budget lasts:
    the usual cause is a previous test's server between its SIGKILL and the
    kernel freeing the port, which clears within a second or two. A holder that
    outlives the budget is a real conflict (usually a second copy of the suite
    running at once) and raises instead of burning the whole startup timeout.
    A child that dies any other way raises at once with the child's own error,
    so a crash no longer surfaces as a bare timeout. A child that stays alive
    without binding (a slow debug catalog build) gets the full startup timeout.
    A reachable port is re-checked after a short grace: a bind collision kills
    the child within milliseconds of start while the foreign port stays open,
    so returning at once would hand back a doomed process. Pass
    start_method="spawn" for tests that guard process-global state, where
    a forked child would inherit the state under test instead of recreating it.
    """
    if start_method is None:
        process_factory = multiprocessing
    else:
        process_factory = multiprocessing.get_context(start_method)
    error_queue = process_factory.Queue()
    bind_retry_deadline = time.monotonic() + bind_retry_seconds
    startup_deadline = time.monotonic() + startup_timeout

    proc = process_factory.Process(
        target=_run_target_reporting_errors,
        args=(target, (port,) + tuple(args), error_queue),
        daemon=True,
    )
    proc.start()
    while True:
        with socket.socket() as sock:
            reachable = sock.connect_ex((host, port)) == 0
        # An open port plus a live child means our server bound: with
        # SO_REUSEADDR but not SO_REUSEPORT a second bind on a held port fails,
        # so a live child either owns the port or has not bound yet (in which
        # case someone else's server is answering, handled by unique ports).
        # The grace below covers the starting child: a collision kills it within
        # milliseconds while the foreign port stays reachable, so without the
        # re-check we would hand back a process that is already doomed.
        if reachable and proc.is_alive():
            time.sleep(0.5)
            if proc.is_alive():
                return proc
        if not proc.is_alive():
            detail = _drain_error_queue(error_queue)
            if _is_address_in_use(detail) and time.monotonic() < bind_retry_deadline:
                time.sleep(0.2)
                proc = process_factory.Process(
                    target=_run_target_reporting_errors,
                    args=(target, (port,) + tuple(args), error_queue),
                    daemon=True,
                )
                proc.start()
                continue
            exitcode = proc.exitcode
            stop_server(proc)
            if detail:
                raise RuntimeError("server on port {} did not start: {}".format(port, detail))
            raise RuntimeError("server on port {} did not start: child exited with code {}".format(port, exitcode))
        if time.monotonic() >= startup_deadline:
            stop_server(proc)
            raise RuntimeError("server on port {} did not start within {:.0f}s".format(port, startup_timeout))
        time.sleep(0.1)


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
