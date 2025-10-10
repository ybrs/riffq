# Getting Started

This guide walks you through installing dependencies and bootstrapping a local development environment for Riffq.

## Prerequisites

- Python 3.10+
- Rust toolchain (via [rustup](https://rustup.rs/))
- [Poetry](https://python-poetry.org/) or `pip`
- [MkDocs](https://www.mkdocs.org/) and the documentation dependencies listed in `requirements.txt`

## Installation steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/ybrs/riffq.git
   cd riffq
   ```
2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```
3. **Build the Rust extension for development**
   ```bash
   make dev-build
   ```
4. **Run the test suites**
   ```bash
   make all-tests
   ```

## Working with the project

- Use `cargo test` to execute Rust unit tests.
- Use `python -m unittest` to run Python unit tests.
- Launch the documentation server with `make server-docs` to preview changes live.
- Build the static documentation site with `make docs` before publishing.

## Next steps

Continue with the [API Reference](api-reference.md) to explore the public interfaces, or check the [FAQ](faq.md) for troubleshooting tips.
