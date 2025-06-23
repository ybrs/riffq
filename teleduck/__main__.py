from .server import run_server
import click

@click.command()
@click.argument("db_file", type=click.Path())
@click.option("--host", default="127.0.0.1", show_default=True, help="Host to listen on")
@click.option("--port", default=5433, show_default=True, help="Port to listen on")
def main(db_file: str, host: str, port: int):
    run_server(db_file, port, host=host)

if __name__ == "__main__":
    main()
