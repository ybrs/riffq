import duckdb
import pyarrow as pa
import riffq
from riffq.helpers import to_arrow
import logging
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

class Connection(riffq.BaseConnection):
    def _handle_query(self, sql, callback, **kwargs):
        cur = duckdb_con.cursor()
        sql = sql.strip().lower().split(';')[0]
        if sql == "":
            return callback("OK", is_tag=True)

        if "current_schemas(" in sql:
            batch = self.arrow_batch(
                [pa.array(["main"])],
                ["current_schemas"],
            )
            return self.send_reader(batch, callback)

        if sql.startswith("set"):
            return callback("SET", is_tag=True)

        if sql.startswith("begin"):
            return callback("BEGIN", is_tag=True)
        
        if sql.startswith("commit"):
            return callback("COMMIT", is_tag=True)
        
        if sql.startswith("rollback"):
            return callback("ROLLBACK", is_tag=True)
        
        if sql.startswith("discard all"):
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


    def register_schemas_and_tables_in_database(database_name):
        if database_name in ("system", "temp"):
            return

        # duckdb_con.execute(f"use {database_name}")
        
        tbls = duckdb_con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog','information_schema')" \
            "and table_catalog = ?", (database_name,)
        ).fetchall()

        for schema_name, table_name in tbls:
            server._server.register_schema(database_name, schema_name)
            cols_info = duckdb_con.execute(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                "WHERE table_schema=? AND table_name=?",
                (schema_name, table_name),
            ).fetchall()
            columns = []
            for col_name, data_type, is_nullable in cols_info:
                columns.append(
                    {
                        col_name: {
                            "type": map_type(data_type),
                            "nullable": is_nullable.upper() == "YES",
                        }
                    }
                )
            server._server.register_table(database_name, schema_name, table_name, columns)


    databases = duckdb_con.execute(
        "SELECT database_name, path, type FROM duckdb_databases() where internal=false"
    ).fetchall()

    for database_name, path, type in databases:
        server._server.register_database(database_name)
        register_schemas_and_tables_in_database(database_name)
    
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
