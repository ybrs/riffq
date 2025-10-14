"""Riffq Python bindings and high-level server interface.

This package exposes a lightweight Python API around the Rust server core
(`Server`) and provides utilities for building custom query backends.

Exports:

- `Server`: Low-level Rust server class (from the compiled extension).
- `BaseConnection`: Abstract base to implement your own connection logic.
- `RiffqServer`: Convenience wrapper that wires callbacks to `Server` and
  manages connection instances.
"""

from ._riffq import Server  # Rust class
from . import connection
from .connection import BaseConnection, RiffqServer

# Re‑exports for a tidy public API
BaseConnection = connection.BaseConnection
RiffqServer = connection.RiffqServer
