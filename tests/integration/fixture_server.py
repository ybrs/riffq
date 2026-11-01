"""The riffq server the driver integration tests run against.

A DuckDB in-memory database holds the three fixture tables; riffq answers
pg_catalog / information_schema queries from teleduck's shipped
``DuckdbCatalogSource`` (the same lazy-catalog path a real teleduck deployment
uses), and this module's connection handles the data path -- ordinary SELECTs
plus the SET / SHOW / transaction-control statements the ODBC and JDBC drivers
issue during their connect handshake.

The server is launched in a child process with ``multiprocessing.Process`` and
torn down with ``riffq.testing.stop_server``; tests wait for readiness with
``wait_for_catalog`` rather than sleeping. Two things are specific to these
tests: the server version is pinned (both drivers gate behaviour on it, so
pinning makes their behaviour deterministic), and every statement the data path
receives is appended to a JSON-lines query log with whether it errored. The log
is a test artifact -- it lets a failing assertion name the exact driver query
that broke, which is otherwise very hard to see through a driver -- not a riffq
feature.
"""
import json
import multiprocessing
import re
import threading

import duckdb
import pyarrow as pa
import riffq
from riffq.testing import wait_for_catalog
from teleduck.server import DuckdbCatalogSource

import fixture_dataset

# The pinned wire server_version. Both drivers report and branch on it; a fixed
# value keeps their behaviour independent of riffq's built-in default.
SERVER_VERSION = "17.0"

# The fixture accepts any credentials, but registering an auth handler makes the
# server request a password, so clients must still send one. Tests and the two
# drivers all connect with this password.
FIXTURE_PASSWORD = "secret"

# Set inside the child process by run_server so the connection handler can reach
# the DuckDB connection and the query log without threading them through riffq's
# fixed callback signature.
_duckdb_connection = None
_query_log_path = None
_query_log_lock = threading.Lock()

# Statement prefixes the drivers send during connect/teardown and around
# statements that DuckDB either does not understand or should not run as data.
# Each maps to the PostgreSQL command tag the driver expects back. Order matters:
# "rollback to savepoint" must be matched before the bare "rollback" prefix.
_COMMAND_TAG_PREFIXES = {
    "set ": "SET",
    "begin": "BEGIN",
    "start transaction": "BEGIN",
    "commit": "COMMIT",
    "rollback to": "ROLLBACK",
    "rollback": "ROLLBACK",
    "savepoint": "SAVEPOINT",
    "release": "RELEASE",
    "discard all": "DISCARD ALL",
}


def _placeholder_count(statement):
    """Return the highest ``$n`` placeholder number in a statement, or 0 for none."""
    numbers = [int(match) for match in re.findall(r"\$(\d+)", statement)]
    return max(numbers, default=0)


def _record(sql, errored):
    """Append one statement and its error flag to the query log as a JSON line.

    Serialized by a lock because riffq may dispatch queries from several worker
    threads; the log is read back by tests to identify a failing driver query.
    """
    if _query_log_path is None:
        return
    with _query_log_lock:
        with open(_query_log_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps({"sql": sql, "errored": errored}) + "\n")


class Connection(riffq.BaseConnection):
    """Per-client data path over the fixture's DuckDB connection.

    pg_catalog / information_schema queries never reach here (riffq answers them
    from the lazy catalog source); this handles ordinary SQL plus the handshake
    statements drivers send. Every statement is logged with its error flag.
    """

    def handle_auth(self, user, password, host, database=None, callback=callable):
        """Accept any credentials; authentication is out of scope for these tests."""
        return callback(True)

    def _handle_query(self, sql, callback, **kwargs):
        """Answer one statement: a handshake command tag, or a DuckDB result.

        Handshake statements (SET / BEGIN / COMMIT / ROLLBACK / DISCARD) return
        their PostgreSQL command tag directly. ``SELECT version()`` returns the
        pinned server version. Everything else runs against DuckDB, binding any
        extended-protocol parameter values riffq passes in ``query_args``, and
        streams the resulting Arrow batches back. A DuckDB error is logged and
        re-raised so riffq turns it into a wire error the client sees.

        Args:
            sql: The statement text, with ``$n`` placeholders for any parameters.
            callback: riffq's result callback.
            **kwargs: riffq transport flags; ``query_args`` holds the bound
                parameter values for an extended-protocol query.
        """
        statement = sql.strip().rstrip(";")
        lowered = statement.lower()
        parameters = kwargs.get("query_args") or []

        if lowered == "":
            _record(sql, errored=False)
            return callback("OK", is_tag=True)

        for prefix, tag in _COMMAND_TAG_PREFIXES.items():
            if lowered.startswith(prefix):
                _record(sql, errored=False)
                return callback(tag, is_tag=True)

        if lowered in ("select version()", "select pg_catalog.version()"):
            _record(sql, errored=False)
            version_text = f"PostgreSQL {SERVER_VERSION} on riffq integration fixture"
            reader = self.arrow_batch([pa.array([version_text])], ["version"])
            return self.send_reader(reader, callback)

        try:
            # A fresh cursor is an independent DuckDB session that does not
            # inherit the parent connection's search_path, so set it here; that
            # keeps the catalog source (which also uses cursors) and the data
            # path safely on separate handles while unqualified names still
            # resolve to the fixture schema.
            cursor = _duckdb_connection.cursor()
            cursor.execute(f"SET search_path='{fixture_dataset.SCHEMA_NAME},main'")
            # A prepared-statement describe (psqlodbc's PQdescribePrepared) runs
            # the query with no bound values to learn its result columns; pad any
            # unbound $n placeholders with NULL so DuckDB can plan it (returning
            # the schema and zero rows) instead of failing on a missing parameter.
            placeholders = _placeholder_count(statement)
            if len(parameters) < placeholders:
                parameters = list(parameters) + [None] * (placeholders - len(parameters))
            executed = cursor.execute(statement, parameters) if parameters \
                else cursor.execute(statement)
            # DuckDB's .arrow() yields an object exposing the Arrow C stream
            # interface (a Table or a RecordBatchReader depending on version);
            # send_reader accepts either.
            result = executed.arrow()
            _record(sql, errored=False)
            self.send_reader(result, callback)
        except Exception as exc:
            _record(sql, errored=True)
            # riffq turns this into a wire ErrorResponse the client sees; SQLSTATE
            # 42000 (syntax error or access rule violation) is a sane generic
            # class for a bad data-path statement, and the connection stays open.
            callback(("ERROR", "42000", str(exc)), is_error=True)

    def handle_query(self, sql, callback=callable, **kwargs):
        """Answer a query, re-raising failures so riffq sends the client an error.

        Unlike a production backend, the fixture runs the query inline rather
        than on a worker thread: riffq turns an exception raised here into a wire
        error, which is what the error-handling tests assert. Offloading to the
        executor would swallow that exception in an unretrieved Future and leave
        the client seeing an empty result instead.
        """
        self._handle_query(sql, callback, **kwargs)


def run_server(port, log_path, host="127.0.0.1"):
    """Start the fixture server on ``port``, logging statements to ``log_path``.

    Opens an in-memory DuckDB database, loads the three fixture tables from
    ``fixture_dataset``, sets the DuckDB search path so unqualified table names
    resolve to the ``public`` schema the tables live in, installs teleduck's
    lazy catalog source over that connection, and starts riffq with catalog
    emulation and the pinned server version. Intended as a
    ``multiprocessing.Process`` target.

    Args:
        port: TCP port to listen on.
        log_path: File the data path appends its JSON-lines query log to.
        host: Interface to bind; defaults to loopback.
    """
    global _duckdb_connection, _query_log_path
    _query_log_path = log_path
    _duckdb_connection = duckdb.connect()

    _duckdb_connection.execute(f"CREATE SCHEMA IF NOT EXISTS {fixture_dataset.SCHEMA_NAME}")
    for statement in fixture_dataset.create_table_statements():
        _duckdb_connection.execute(statement)
    for statement in fixture_dataset.insert_statements():
        _duckdb_connection.execute(statement)

    server = riffq.RiffqServer(f"{host}:{port}", connection_cls=Connection)
    server.set_lazy_catalog(DuckdbCatalogSource(_duckdb_connection))
    server.handle_shutdown(_duckdb_connection.close)
    server.start(catalog_emulation=True, server_version=SERVER_VERSION)


def start_process(port, log_path, host="127.0.0.1"):
    """Launch the fixture server in a child process and wait until it is queryable.

    Every integration test class starts the fixture the same way, so the process
    creation and the catalog-readiness wait (the socket binds before the catalog
    emulation can answer, so a fixed sleep would be racy) live here rather than
    being repeated per module.

    Args:
        port: TCP port for the server to listen on.
        log_path: File the server appends its JSON-lines query log to.
        host: Interface to bind; defaults to loopback.

    Returns:
        The started ``multiprocessing.Process``; stop it with
        ``riffq.testing.stop_server``.
    """
    process = multiprocessing.Process(
        target=run_server, args=(port, log_path, host), daemon=True
    )
    process.start()
    # Probe the database the fixture actually serves. The DuckDB source reports
    # exactly one, and a catalog-emulating server refuses any other name, so
    # probing under some other dbname never gets a connection at all.
    wait_for_catalog(
        port,
        fixture_dataset.DATABASE_NAME,
        "SELECT datname FROM pg_catalog.pg_database "
        f"WHERE datname='{fixture_dataset.DATABASE_NAME}'",
        fixture_dataset.DATABASE_NAME,
        password=FIXTURE_PASSWORD,
    )
    return process
