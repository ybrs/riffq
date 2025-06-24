"""Teleduck package for serving DuckDB over the PostgreSQL protocol."""

from .server import run_server

__all__ = ["run_server"]

