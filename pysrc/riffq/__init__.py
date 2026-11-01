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

# Named explicitly so the three imports above read as the package's public
# surface rather than as incidental imports, which is also what stops a linter
# treating them as unused.
__all__ = ["Server", "connection", "BaseConnection", "RiffqServer"]
