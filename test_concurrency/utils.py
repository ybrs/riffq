from sqlalchemy import create_engine, text
import time
from riffq.testing import stop_server  # re-exported for the concurrency tests

__all__ = ["stop_server", "wait_for_server", "ensure_started"]


def ensure_started(proc, port):
    """
    Fails fast if the server process died during startup. The riffq server now
    raises OSError when its port is already taken (e.g. by a real postgres), so
    the child exits; without this check the test would silently connect to the
    stray server on that port and pass against the wrong database.
    """
    if not proc.is_alive():
        raise RuntimeError(
            f"riffq server exited during startup; is port {port} already in use?"
        )


def wait_for_server(port: int = 5433):
    engine = create_engine(f"postgresql://myuser:mypassword@127.0.0.1:{port}/mydb")
    import sqlalchemy.exc
    cnt = 0
    while True:
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM test_concurrency;"))
                print("connected")
                break
        except sqlalchemy.exc.OperationalError:
            print("waiting for server to be online")
            time.sleep(1)
            cnt += 1
        if cnt > 10:
            raise Exception("couldnt spawn the server")
    
    return engine

