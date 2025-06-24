import duckdb
import pyarrow as pa
import riffq
from riffq.helpers import to_arrow
import logging
from pathlib import Path
from typing import Iterable, Optional

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
        # return callback(user == "user" and password == "secret")
        callback(True)

def run_server(
    db_file: str,
    port: int = 5433,
    host: str = "127.0.0.1",
    sql_scripts: Optional[Iterable[str]] = None,
    sql: Optional[Iterable[str]] = None,
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
    """

    global duckdb_con
    duckdb_con = duckdb.connect(db_file)

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
    server.set_tls(str(cert_dir / "server.crt"), str(cert_dir / "server.key"))


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
            print("registering table", schema_name, table_name)
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
        "SELECT database_name, path, type FROM duckdb_databases()"
    ).fetchall()

    for database_name, path, type in databases:
        server._server.register_database(database_name)
        register_schemas_and_tables_in_database(database_name)
    
    server.start(catalog_emulation=True)

if __name__ == "__main__":
    import click

    @click.command()
    @click.argument("db_file", type=click.Path())
    @click.option("--port", default=5433, show_default=True, help="Port to listen on.")
    def _main(db_file: str, port: int):
        run_server(db_file, port)

    _main()
