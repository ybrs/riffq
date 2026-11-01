"""Start and stop the CloudBeaver server the DBeaver-metadata tier drives.

CloudBeaver is DBeaver's metadata engine without the desktop GUI, so this tier
covers a query surface the other tiers cannot: DBeaver's PostgreSQL plugin does
not read metadata through pgjdbc's DatabaseMetaData, it issues its own SQL
against pg_class, pg_namespace, pg_attribute and friends.

The server is configured entirely from files rather than by scripting its setup
wizard. A fresh workspace is seeded on every start from the two configs in
cloudbeaver/:

- ``cloudbeaver.runtime.conf`` marks initial setup as done. Without it the
  server boots into configuration mode, where it reports no connections and
  refuses to create any, so nothing can be asserted.
- ``initial-data-sources.conf`` defines the connection pointing at the riffq
  fixture, with the fixture's port substituted in, so the connection exists at
  startup under a known id and no admin login is needed. It grants the
  anonymous team access to shared connections, which is what lets an
  unauthenticated GraphQL client use it.

Seeding a new workspace each time keeps runs independent: CloudBeaver persists
connection state and an H2 database inside the workspace, so a reused one would
carry state from a previous run.
"""
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request

from toolchain import CLOUDBEAVER_HOME, CLOUDBEAVER_JAVA_BIN

CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cloudbeaver")
RUNTIME_CONF = os.path.join(CONFIG_DIR, "cloudbeaver.runtime.conf")
DATA_SOURCES_CONF = os.path.join(CONFIG_DIR, "initial-data-sources.conf")

# The connection id defined in initial-data-sources.conf.
CONNECTION_ID = "riffq-fixture"

# Placeholder in initial-data-sources.conf, replaced with the fixture's port so
# the port is defined once by the test rather than duplicated in the config.
PORT_PLACEHOLDER = "__RIFFQ_PORT__"

# CloudBeaver boots a Jetty server, an embedded H2 database and the DBeaver
# platform, which takes appreciably longer than the riffq fixture does.
_STARTUP_TIMEOUT_SECONDS = 180
_POLL_INTERVAL_SECONDS = 2


def _seed_workspace(riffq_port):
    """Replace the server's workspace with a freshly seeded one.

    Args:
        riffq_port: Port of the riffq fixture the connection should target.
    """
    workspace = os.path.join(CLOUDBEAVER_HOME, "workspace")
    shutil.rmtree(workspace, ignore_errors=True)

    data_dir = os.path.join(workspace, ".data")
    global_config = os.path.join(workspace, "GlobalConfiguration", ".dbeaver")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(global_config, exist_ok=True)
    # The launcher only seeds a workspace that has no .metadata directory; the
    # directory is created here so it never overwrites what is written below.
    os.makedirs(os.path.join(workspace, ".metadata"), exist_ok=True)

    shutil.copyfile(
        RUNTIME_CONF, os.path.join(data_dir, ".cloudbeaver.runtime.conf")
    )

    with open(DATA_SOURCES_CONF, "r", encoding="utf-8") as handle:
        data_sources = handle.read().replace(PORT_PLACEHOLDER, str(riffq_port))
    with open(
        os.path.join(global_config, "data-sources.json"), "w", encoding="utf-8"
    ) as handle:
        handle.write(data_sources)


def _wait_until_serving(web_port, process):
    """Block until the server answers on /status, or fail with why it did not.

    Args:
        web_port: The port CloudBeaver serves on.
        process: The server process, checked so a crash is reported as a crash
            rather than as a timeout.

    Raises:
        RuntimeError: If the server exits, or does not serve in time.
    """
    deadline = time.monotonic() + _STARTUP_TIMEOUT_SECONDS
    url = f"http://127.0.0.1:{web_port}/status"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"CloudBeaver exited with code {process.returncode} before serving"
            )
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, OSError, TimeoutError):
            pass
        time.sleep(_POLL_INTERVAL_SECONDS)
    raise RuntimeError(
        f"CloudBeaver did not serve on port {web_port} within "
        f"{_STARTUP_TIMEOUT_SECONDS}s"
    )


def start_server(web_port, riffq_port, log_path):
    """Seed a workspace, launch CloudBeaver, and wait until it serves.

    Args:
        web_port: Port for CloudBeaver's HTTP/GraphQL endpoint.
        riffq_port: Port of the riffq fixture the seeded connection targets.
        log_path: File to write the server's output to; its tail is the first
            thing worth reading when this tier fails.

    Returns:
        The running ``subprocess.Popen``; stop it with ``stop_server``.
    """
    _seed_workspace(riffq_port)

    environment = dict(os.environ)
    # The image bundles its own JRE; using it rather than the toolchain's JDK
    # keeps the server on the runtime it was built and tested against.
    environment["PATH"] = (
        os.path.dirname(CLOUDBEAVER_JAVA_BIN) + os.pathsep + environment.get("PATH", "")
    )
    environment["CLOUDBEAVER_WEB_SERVER_PORT"] = str(web_port)

    log_handle = open(log_path, "w", encoding="utf-8")
    process = subprocess.Popen(
        ["./run-cloudbeaver-server.sh"],
        cwd=CLOUDBEAVER_HOME,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        env=environment,
        start_new_session=True,
    )
    process.log_handle = log_handle
    try:
        _wait_until_serving(web_port, process)
    except Exception:
        stop_server(process)
        raise
    return process


def stop_server(process):
    """Terminate the CloudBeaver server and close its log.

    Args:
        process: The process returned by ``start_server``.
    """
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=30)
    handle = getattr(process, "log_handle", None)
    if handle is not None and not handle.closed:
        handle.close()
