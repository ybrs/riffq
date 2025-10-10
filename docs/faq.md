# Frequently Asked Questions

## How do I rebuild the documentation?

Run `make docs` to produce the static site in the `site/` directory. Use `make server-docs` while editing to get live reloads at <http://localhost:8000>.

## Where do the API docs come from?

The API documentation is powered by [mkdocstrings](https://mkdocstrings.github.io/). It inspects the Python code in `pysrc/riffq` and renders docstrings into pages during the build.

## Can I document Rust code too?

MkDocs primarily targets Python, but you can link to Rust documentation hosted elsewhere or generated via `cargo doc`.

## Who maintains the documentation?

The Riffq engineering team owns the documentation. Contributions are welcome—please submit a pull request with your updates.
