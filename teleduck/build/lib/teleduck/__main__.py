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
def main(
    db_file: str,
    host: str,
    port: int,
    sql_scripts: tuple[str, ...],
    sqls: tuple[str, ...],
):
    run_server(db_file, port, host=host, sql_scripts=list(sql_scripts), sql=list(sqls))

if __name__ == "__main__":
    main()
