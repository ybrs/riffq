from .server import run_server
import click

@click.command()
@click.argument("db_file", type=click.Path())
@click.option("--host", default="127.0.0.1", show_default=True, help="Host to listen on")
@click.option("--port", default=5433, show_default=True, help="Port to listen on")
@click.option(
    "--sql-script",
    "sql_scripts",
    multiple=True,
    type=click.Path(),
    help="SQL script file to run before starting the server",
)
@click.option(
    "--sql",
    "sqls",
    multiple=True,
    help="SQL statement to run before starting the server",
)
@click.option("--use-tls/--no-use-tls", "use_tls", default=True, show_default=True, help="Use TLS for the server")
@click.option("--tls-cert-file", default=None, type=click.Path(), help="Path to TLS certificate")
@click.option("--tls-key-file", default=None, type=click.Path(), help="Path to TLS key")
@click.option("--read-only/--no-read-only", "read_only", default=False, show_default=True, help="Open database in read-only mode")
def main(
    db_file: str,
    host: str,
    port: int,
    sql_scripts: tuple[str, ...],
    sqls: tuple[str, ...],
    use_tls: bool,
    tls_cert_file: str | None,
    tls_key_file: str | None,
    read_only: bool,
):
    run_server(
        db_file,
        port,
        host=host,
        sql_scripts=list(sql_scripts),
        sql=list(sqls),
        use_tls=use_tls,
        tls_cert_file=tls_cert_file,
        tls_key_file=tls_key_file,
        read_only=read_only,
    )

if __name__ == "__main__":
    main()
