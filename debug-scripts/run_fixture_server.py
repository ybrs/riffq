"""Run a riffq fixture server in the foreground until interrupted.

The integration suite starts and stops a fixture server per test class, which
is right for tests but useless when driving a second long-lived server (the
CloudBeaver tier) by hand. This keeps one fixture server up so its port can be
pointed at while the CloudBeaver GraphQL flow is worked out.

Run from the riffq project root:

    venv/bin/python -m claude-scripts.run_fixture_server [port]
"""
import os
import sys
import tempfile
import time

INTEGRATION_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests",
    "integration",
)
sys.path.insert(0, INTEGRATION_DIR)

from fixture_server import start_process  # noqa: E402
from riffq.testing import stop_server  # noqa: E402

DEFAULT_PORT = 55571


def main():
    """Start the fixture server and block until interrupted."""
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    log_path = os.path.join(tempfile.gettempdir(), f"riffq_integration_{port}.log")
    server = start_process(port, log_path)
    print(f"fixture server listening on 127.0.0.1:{port}", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        stop_server(server)


if __name__ == "__main__":
    main()
