from .server import run_server
import click

@click.command()
@click.argument("db_file", type=click.Path())
@click.option("--port", default=5433, show_default=True, help="Port to listen on")
def main(db_file: str, port: int):
    run_server(db_file, port)

if __name__ == "__main__":
    main()
