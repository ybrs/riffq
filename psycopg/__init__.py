import pg8000
from urllib.parse import urlparse

__all__ = ["connect"]

class Cursor:
    def __init__(self, inner):
        self._cur = inner

    def execute(self, query, params=None):
        if params:
            placeholders = []
            for p in params:
                if isinstance(p, str):
                    placeholders.append("'" + p.replace("'", "''") + "'")
                elif p is None:
                    placeholders.append('NULL')
                else:
                    placeholders.append(str(p))
            query = query.replace('%s', '{}').format(*placeholders)
        self._cur.execute(query)

    def fetchone(self):
        return self._cur.fetchone()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

class Connection:
    def __init__(self, **kwargs):
        self._conn = pg8000.connect(**kwargs)

    def cursor(self):
        return Cursor(self._conn.cursor())

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


def connect(dsn):
    if dsn.startswith("postgresql://"):
        u = urlparse(dsn)
        return Connection(user=u.username or '', password=u.password,
                          host=u.hostname or 'localhost', port=u.port or 5432,
                          database=u.path.lstrip('/') or '')
    raise ValueError("DSN string expected")
