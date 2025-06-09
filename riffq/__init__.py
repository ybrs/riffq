from ._riffq import Server  # Rust class
from . import connection 
from .connection import BaseConnection, RiffqServer

BaseConnection = connection.BaseConnection
RiffqServer    = connection.RiffqServer