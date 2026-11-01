"""Run the compiled Npgsql harness and return its parsed JSON output.

The .NET tier mirrors the JDBC one: a small program drives the real driver and
prints a JSON document the Python side asserts against. Unlike the Java
harnesses, this one is built ahead of time by setup_toolchain.sh rather than
compiled on demand -- a .NET build pulls in NuGet restore, which must not
happen while tests are running, so the tests only ever execute an already-built
assembly.
"""
import json
import os
import subprocess

from toolchain import DOTNET, DOTNET_HARNESS_DLL

# Keep the CLI silent and self-contained: no telemetry ping, no first-run
# banner, and a package cache inside the toolchain rather than in ~/.nuget.
_HARNESS_ENV = {
    "DOTNET_CLI_TELEMETRY_OPTOUT": "1",
    "DOTNET_NOLOGO": "1",
    "DOTNET_SKIP_FIRST_TIME_EXPERIENCE": "1",
}


def run_npgsql_harness(port, host="127.0.0.1", user="user", password="secret",
                       database="memory"):
    """Run the Npgsql harness against a fixture server, returning its JSON.

    The harness connects with Npgsql's default settings, so opening the
    connection runs its real startup type loading against the catalog.

    Args:
        port: The fixture server's TCP port.
        host: The host to connect to.
        user: Connection user.
        password: Connection password.
        database: Database to connect to.

    Returns:
        The JSON document the harness printed, parsed into Python objects.

    Raises:
        AssertionError: If the harness exits non-zero, with its stderr attached.
        json.JSONDecodeError: If the harness output is not valid JSON.
    """
    environment = dict(os.environ)
    environment.update(_HARNESS_ENV)
    result = subprocess.run(
        [DOTNET, DOTNET_HARNESS_DLL, host, str(port), user, password, database],
        capture_output=True,
        text=True,
        env=environment,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"Npgsql harness exited {result.returncode}:\n"
            f"{result.stderr}\n{result.stdout}"
        )
    return json.loads(result.stdout)
