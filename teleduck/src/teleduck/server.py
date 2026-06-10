import atexit
import duckdb
import pyarrow as pa
import riffq
from riffq.helpers import to_arrow
import logging
import threading
from pathlib import Path
from typing import Iterable, Optional
import os
import hashlib

def map_type(data_type: str) -> str:
    dt = data_type.upper()
    if "INT" in dt:
        return "int"
    if any(x in dt for x in ["DOUBLE", "DECIMAL", "REAL", "FLOAT"]):
        return "float"
    if "BOOL" in dt:
        return "bool"
    if dt == "DATE":
        return "date"
    if "TIMESTAMP" in dt or "DATETIME" in dt:
        return "datetime"
    return "str"


def duckdb_type_to_oid(data_type: str) -> int:
    """Map a DuckDB declared type to a PostgreSQL ``pg_type`` OID.

    Used to fill ``pg_attribute.atttypid`` / ``information_schema.columns`` for
    the lazy catalog. Only the common scalar types are distinguished; anything
    unrecognized falls back to ``text`` (25), which is the safest default for
    client type introspection.
    """
    dt = data_type.upper()
    if "BIGINT" in dt or "INT8" in dt or "HUGEINT" in dt or "LONG" in dt:
        return 20  # int8
    if "SMALLINT" in dt or "INT2" in dt or "TINYINT" in dt:
        return 21  # int2
    if "INT" in dt:
        return 23  # int4
    if "BOOL" in dt:
        return 16  # bool
    if "DOUBLE" in dt or "FLOAT8" in dt:
        return 701  # float8
    if "REAL" in dt or "FLOAT" in dt:
        return 700  # float4
    if "DECIMAL" in dt or "NUMERIC" in dt:
        return 1700  # numeric
    if "TIMESTAMP" in dt or "DATETIME" in dt:
        return 1184 if "TZ" in dt else 1114  # timestamptz / timestamp
    if dt == "DATE":
        return 1082  # date
    if "TIME" in dt:
        return 1083  # time
    return 25  # text / varchar / everything else


def _stable_oid(salt: str, *parts: str) -> int:
    """Derive a stable, built-in-clear OID from a namespace `salt` and `parts`.

    The same inputs always yield the same OID (so ``pg_class.oid`` and
    ``pg_attribute.attrelid`` agree across scans and joins resolve), distinct
    object classes use distinct salts to avoid collisions, and the result sits
    well above the built-in OID range and inside the signed-32-bit range that
    the catalog row types use.
    """
    key = "\x00".join((salt,) + parts)
    h = int(hashlib.sha1(key.encode("utf-8")).hexdigest()[:8], 16)
    return 16384 + (h % 2_000_000_000)


class DuckdbCatalogSource:
    """A lazy ``pg_catalog`` source backed by a live DuckDB connection.

    Each method queries DuckDB's own catalog on demand and hands the rows to the
    ``callback``, so the emulated ``pg_catalog`` / ``information_schema`` always
    reflects DuckDB's current schema -- including tables created after the server
    started. Mirrors the Rust ``LazyCatalogSource`` trait one method per level.

    A fresh ``cursor()`` is used per call because the underlying connection is
    shared with the query path and DuckDB connections are not concurrency-safe;
    cursors give an independent, GIL-serialized handle.
    """

    def __init__(self, con):
        self._con = con

    def databases(self, callback):
        rows = self._con.cursor().execute(
            "SELECT database_name FROM duckdb_databases() WHERE internal = false"
        ).fetchall()
        callback(
            [
                {"oid": _stable_oid("db", name), "name": name}
                for (name,) in rows
                if name not in ("system", "temp")
            ]
        )

    def schemas(self, database, callback):
        rows = self._con.cursor().execute(
            "SELECT DISTINCT table_schema FROM information_schema.tables "
            "WHERE table_catalog = ? "
            "AND table_schema NOT IN ('pg_catalog', 'information_schema')",
            (database,),
        ).fetchall()
        callback(
            [
                {"oid": _stable_oid("ns", database, schema), "name": schema}
                for (schema,) in rows
            ]
        )

    def relations(self, database, schema, callback):
        rows = self._con.cursor().execute(
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ?",
            (database, schema),
        ).fetchall()
        # Tables that carry at least one index, so pg_tables.hasindexes is true.
        # DuckDB has no triggers/rules/row-level-security, so those flags stay
        # false (their default), which is truthful rather than blank. DuckDB also
        # has no table ownership, so "owner_oid" is intentionally omitted and
        # pg_tables.tableowner is left blank.
        indexed = {
            name
            for (name,) in self._con.cursor().execute(
                "SELECT DISTINCT table_name FROM duckdb_indexes() "
                "WHERE database_name = ? AND schema_name = ?",
                (database, schema),
            ).fetchall()
        }
        out = []
        for table_name, table_type in rows:
            kind = "view" if (table_type or "").upper().startswith("VIEW") else "table"
            out.append(
                {
                    "oid": _stable_oid("rel", database, schema, table_name),
                    "reltype_oid": _stable_oid("type", database, schema, table_name),
                    "name": table_name,
                    "kind": kind,
                    "has_index": table_name in indexed,
                }
            )
        callback(out)

    def columns(self, database, schema, relation, callback):
        rows = self._con.cursor().execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            (database, schema, relation),
        ).fetchall()
        callback(
            [
                {
                    "name": col_name,
                    "type_oid": duckdb_type_to_oid(data_type),
                    "nullable": str(is_nullable).upper() == "YES",
                }
                for (col_name, data_type, is_nullable) in rows
            ]
        )


class Connection(riffq.BaseConnection):
    def _handle_query(self, sql, callback, **kwargs):
        cur = duckdb_con.cursor()
        sql = sql.strip().split(';')[0]
        if sql == "":
            return callback("OK", is_tag=True)

        if "current_schemas(" in sql:
            batch = self.arrow_batch(
                [pa.array(["main"])],
                ["current_schemas"],
            )
            return self.send_reader(batch, callback)

        # TODO: we have to parse
        if sql.lower().startswith("set "):
            return callback("SET", is_tag=True)

        if sql.lower().startswith("begin"):
            return callback("BEGIN", is_tag=True)
        
        if sql.lower().startswith("commit"):
            return callback("COMMIT", is_tag=True)
        
        if sql.lower().startswith("rollback"):
            return callback("ROLLBACK", is_tag=True)
        
        if sql.lower().startswith("discard all"):
            return callback("DISCARD ALL", is_tag=True)

        if sql == "select pg_catalog.version()":
            batch = self.arrow_batch(
                [pa.array(["PostgreSQL 14.13"])],
                ["version"],
            )
            return self.send_reader(batch, callback)

        if sql == "show datestyle":
            batch = self.arrow_batch(
                [pa.array(["ISO, MDY"])],
                ["datestyle"],
            )
            return self.send_reader(batch, callback)

        if sql == "show transaction isolation level":
            batch = self.arrow_batch(
                [pa.array(["read committed"])],
                ["transaction_isolation"],
            )
            return self.send_reader(batch, callback)

        if sql == "show standard_conforming_strings":
            batch = self.arrow_batch(
                [pa.array(["read committed"])],
                ["transaction_isolation"],
            )
            return self.send_reader(batch, callback)
        
        if sql == "select current_schema()":
            batch = self.arrow_batch(
                [pa.array(["main"])],
                ["current_schema"],
            )
            return self.send_reader(batch, callback)

        try:
            print("sending final query:", sql)
            reader = cur.execute(sql).fetch_record_batch()
            self.send_reader(reader, callback)
        except Exception as exc:
            logging.exception(f"error on executing query -- {sql}")
            batch = self.arrow_batch(
                [pa.array(["ERROR"]), pa.array([str(exc)])],
                ["error", "message"],
            )
            self.send_reader(batch, callback)

    def handle_query(self, sql, callback=callable, **kwargs):
        self.executor.submit(self._handle_query, sql, callback, **kwargs)

    def handle_auth(self, user, password, host, database=None, callback=callable):
        """Handle authentication for incoming connections.

        If the environment variables ``TELEDUCK_USERNAME`` and/or ``TELEDUCK_PASSWORD``
        are set, the given credentials must match them.  Alternatively a hashed
        password can be specified via ``TELEDUCK_PASSWORD_SHA1``.  When none of
        these variables are defined, any credentials are accepted.
        """

        env_user = os.getenv("TELEDUCK_USERNAME")
        env_password = os.getenv("TELEDUCK_PASSWORD")
        env_password_sha1 = os.getenv("TELEDUCK_PASSWORD_SHA1")

        if env_user is None and env_password is None and env_password_sha1 is None:
            return callback(True)

        ok = True
        if env_user is not None:
            ok = ok and user == env_user
        if env_password is not None:
            ok = ok and password == env_password
        if env_password_sha1 is not None:
            hashed = hashlib.sha1(password.encode()).hexdigest()
            ok = ok and hashed == env_password_sha1

        callback(ok)

def run_server(
    db_file: str,
    port: int = 5433,
    host: str = "127.0.0.1",
    sql_scripts: Optional[Iterable[str]] = None,
    sql: Optional[Iterable[str]] = None,
    use_tls: bool = True,
    tls_cert_file: Optional[str] = None,
    tls_key_file: Optional[str] = None,
    read_only: bool = False,
):
    """Start the teleduck server using the given DuckDB database file.

    Parameters
    ----------
    db_file:
        Path to the DuckDB database file.
    port:
        Port to listen on.
    host:
        Host to listen on.
    sql_scripts:
        Iterable of file paths pointing to SQL scripts that will be executed in
        order before the server starts.
    sql:
        Iterable of SQL statements to execute before the server starts.
    use_tls:
        Whether to use TLS for the server.
    tls_cert_file:
        Path to the TLS certificate. If not provided, the built-in certificate
        will be used.
    tls_key_file:
        Path to the TLS key. If not provided, the built-in key will be used.
    read_only:
        Open the DuckDB database in read-only mode.
    """
    global duckdb_con
    duckdb_con = duckdb.connect(db_file, read_only=read_only)

    if not read_only:
        duckdb_con.execute("PRAGMA wal_autocheckpoint='1KB'")

    shutdown_state = {"done": False}

    def _checkpoint_and_close():
        """
        Checkpoints and closes DuckDB exactly once; safe to call repeatedly.
        """
        if shutdown_state["done"]:
            return

        try:
            duckdb_con.execute("CHECKPOINT")
            duckdb_con.close()
            logging.info("Database checkpointed and closed.")
        except Exception as exc:
            logging.error("Checkpoint failed: %s", exc)

        shutdown_state["done"] = True

    # atexit is the fallback; riffq's handle_shutdown (below) is the primary
    # path and fires on SIGINT/SIGTERM from inside the rust runtime.
    atexit.register(_checkpoint_and_close)

    # execute initialization SQL before starting the server
    if sql_scripts:
        for script in sql_scripts:
            with open(script, "r", encoding="utf-8") as file:
                duckdb_con.execute(file.read())

    if sql:
        for statement in sql:
            duckdb_con.execute(statement)

    server = riffq.RiffqServer(f"{host}:{port}", connection_cls=Connection)
    cert_dir = Path(__file__).parent / "certs"
    cert_path = tls_cert_file or str(cert_dir / "server.crt")
    key_path = tls_key_file or str(cert_dir / "server.key")
    if use_tls:
        server.set_tls(cert_path, key_path)


    # Drive pg_catalog lazily from the live DuckDB connection: every catalog
    # scan re-reads DuckDB's schema, so tables created after startup show up
    # without any re-registration. (Replaces the previous eager walk that
    # snapshotted databases/schemas/tables once at boot.)
    server.set_lazy_catalog(DuckdbCatalogSource(duckdb_con))

    # riffq catches SIGINT/SIGTERM inside its tokio runtime and invokes this
    # before start() returns. a python signal.signal handler cannot be used
    # here: start() parks the main thread in rust, so a python-level handler
    # would never be dispatched (it would just leave a zombie holding the port).
    server.handle_shutdown(_checkpoint_and_close)

    server.start(catalog_emulation=True, tls=use_tls)
    
if __name__ == "__main__":
    import click

    @click.command()
    @click.argument("db_file", type=click.Path())
    @click.option("--port", default=5433, show_default=True, help="Port to listen on.")
    @click.option("--use-tls/--no-use-tls", "use_tls", default=True, show_default=True, help="Use TLS for the server")
    @click.option("--tls-cert-file", default=None, type=click.Path(), help="Path to TLS certificate")
    @click.option("--tls-key-file", default=None, type=click.Path(), help="Path to TLS key")
    @click.option("--read-only/--no-read-only", "read_only", default=False, show_default=True, help="Open database in read-only mode")
    def _main(db_file: str, port: int, use_tls: bool, tls_cert_file: str | None, tls_key_file: str | None, read_only: bool):
        run_server(db_file, port, use_tls=use_tls, tls_cert_file=tls_cert_file, tls_key_file=tls_key_file, read_only=read_only)

    _main()
