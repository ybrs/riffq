"""Locate the driver toolchain and gate the integration tests on its presence.

setup_toolchain.sh builds and downloads everything into ``.toolchain/`` next to
this file. This module turns that directory into typed locators (the ODBC
drivers, the JDK, the pgjdbc jar, the psql client, the optional SQL Workbench/J
jar), the runtime environment the ODBC driver manager needs, and ``unittest``
skip decorators whose messages name the exact missing artifact and point at the
setup script. When the toolchain is absent -- as on a bare machine -- those
integration tests skip in one line each and ``make test`` stays green. The psql
tier is the exception: ``require_psql`` raises instead of skipping, because a
green run that never exercised psql's catalog SQL is worse than a loud failure.

Runtime environment note: the pyodbc wheel loads ``libodbc.so.2`` at process
start via the dynamic loader, which reads ``LD_LIBRARY_PATH`` once at startup and
does not re-read it if the variable is changed in-process. So this module cannot
simply set ``os.environ`` before ``import pyodbc``; instead, on import it
re-executes the Python process with our unixODBC library directory on
``LD_LIBRARY_PATH`` (and ``ODBCSYSINI`` / ``ODBCINI`` pointing at our driver
config) when they are not already in effect. ``make integration-test`` sets the
same variables up front, so no re-exec happens there; the re-exec only makes
ad-hoc invocations (running one test module directly) work too.
"""
import os
import sys
import unittest

TOOLCHAIN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".toolchain")

UNIXODBC_LIB = os.path.join(TOOLCHAIN_DIR, "unixodbc", "lib")
PSQLODBC_UNICODE = os.path.join(TOOLCHAIN_DIR, "psqlodbc", "lib", "psqlodbcw.so")
PSQLODBC_ANSI = os.path.join(TOOLCHAIN_DIR, "psqlodbc", "lib", "psqlodbca.so")
ODBC_ETC = os.path.join(TOOLCHAIN_DIR, "etc")
ODBCINST_INI = os.path.join(ODBC_ETC, "odbcinst.ini")

JAVA = os.path.join(TOOLCHAIN_DIR, "jdk", "bin", "java")
JAVAC = os.path.join(TOOLCHAIN_DIR, "jdk", "bin", "javac")
JARS_DIR = os.path.join(TOOLCHAIN_DIR, "jars")
SQLWORKBENCH_JAR = os.path.join(TOOLCHAIN_DIR, "sqlworkbench", "sqlworkbench.jar")

# The .NET SDK and the Npgsql harness setup_toolchain.sh builds with it. The
# tests run the built assembly and never invoke the compiler themselves.
DOTNET = os.path.join(TOOLCHAIN_DIR, "dotnet", "dotnet")
DOTNET_HARNESS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dotnet")
DOTNET_HARNESS_DLL = os.path.join(
    DOTNET_HARNESS_DIR, "bin", "Release", "net9.0", "RiffqNpgsqlHarness.dll"
)

# The psql client, from the PostgreSQL distribution setup_toolchain.sh unpacks.
# The version is pinned because psql's backslash commands are handwritten
# catalog SQL that differs between major releases: the tier asserts the binary
# reports this version so a swapped psql shows up as a version mismatch rather
# than as changed catalog behaviour.
PSQL = os.path.join(TOOLCHAIN_DIR, "postgresql", "bin", "psql")
PSQL_VERSION = "17.4"

# CloudBeaver, unpacked from its container image by setup_toolchain.sh. The
# image bundles the JRE the server runs on, so this tier does not use the
# toolchain's JDK.
CLOUDBEAVER_ROOT = os.path.join(TOOLCHAIN_DIR, "cloudbeaver")
CLOUDBEAVER_HOME = os.path.join(CLOUDBEAVER_ROOT, "opt", "cloudbeaver")
CLOUDBEAVER_LAUNCHER = os.path.join(CLOUDBEAVER_HOME, "run-cloudbeaver-server.sh")
CLOUDBEAVER_JAVA_BIN = os.path.join(
    CLOUDBEAVER_ROOT, "opt", "java", "openjdk", "bin", "java"
)

# The pgjdbc release the suite uses unless a matrix run selects another. It is
# placed under a version-free name so the default path never depends on which
# version is pinned.
PGJDBC_DEFAULT_JAR = os.path.join(JARS_DIR, "postgresql.jar")
PGJDBC_DEFAULT_VERSION = "42.7.13"

# The environment variable a matrix run sets to pick one older release.
PGJDBC_VERSION_ENV = "RIFFQ_PGJDBC_VERSION"

# Older pgjdbc releases the matrix run exercises, newest first. These are the
# versions deployed clients actually carry: 42.7.8 is the release Tableau
# ships, and the 42.2 line is the long-lived branch older BI installations pin.
# The driver picks its catalog SQL from the server version riffq reports, not
# from its own version, so these mostly guard against that branching changing.
PGJDBC_MATRIX_VERSIONS = (PGJDBC_DEFAULT_VERSION, "42.7.8", "42.2.29", "42.2.14")


def pgjdbc_jar_for(version):
    """Return the jar path for one pgjdbc version in the matrix.

    Args:
        version: A version string from PGJDBC_MATRIX_VERSIONS.

    Returns:
        The default jar's path for the default version, otherwise the
        versioned jar setup_toolchain.sh placed alongside it.
    """
    if version == PGJDBC_DEFAULT_VERSION:
        return PGJDBC_DEFAULT_JAR
    return os.path.join(JARS_DIR, f"postgresql-{version}.jar")


def selected_pgjdbc_version():
    """Return the pgjdbc version this run drives, honouring the matrix variable.

    Returns:
        The value of RIFFQ_PGJDBC_VERSION when set, else PGJDBC_DEFAULT_VERSION.

    Raises:
        ValueError: If the variable names a version not in the matrix, which
            would otherwise fail later as a confusing missing-jar error.
    """
    version = os.environ.get(PGJDBC_VERSION_ENV)
    if not version:
        return PGJDBC_DEFAULT_VERSION
    if version not in PGJDBC_MATRIX_VERSIONS:
        raise ValueError(
            f"{PGJDBC_VERSION_ENV}={version} is not one of "
            f"{', '.join(PGJDBC_MATRIX_VERSIONS)}"
        )
    return version


# The jar the harnesses actually run against for this process.
PGJDBC_JAR = pgjdbc_jar_for(selected_pgjdbc_version())

# The odbcinst.ini names of the two driver flavours the setup script registers.
ODBC_UNICODE_DRIVER = "PostgreSQL Unicode"
ODBC_ANSI_DRIVER = "PostgreSQL ANSI"

# The two ODBC driver flavours the tests register and exercise, keyed by the
# name each is registered under in odbcinst.ini.
ODBC_DRIVERS = {
    ODBC_UNICODE_DRIVER: PSQLODBC_UNICODE,
    ODBC_ANSI_DRIVER: PSQLODBC_ANSI,
}

# Marker so the re-exec below happens at most once per process tree.
_REEXEC_MARKER = "RIFFQ_INTEGRATION_ENV_APPLIED"

_SETUP_HINT = "run tests/integration/setup_toolchain.sh"


def has_odbc():
    """Return True when both ODBC drivers and their registration file exist."""
    return (
        os.path.exists(PSQLODBC_UNICODE)
        and os.path.exists(PSQLODBC_ANSI)
        and os.path.exists(ODBCINST_INI)
    )


def has_jdbc():
    """Return True when the JDK and the selected pgjdbc jar are both present."""
    return os.path.exists(JAVAC) and os.path.exists(JAVA) and os.path.exists(PGJDBC_JAR)


def has_jdbc_tool():
    """Return True when the optional SQL Workbench/J jar is present."""
    return os.path.exists(SQLWORKBENCH_JAR)


def has_dotnet():
    """Return True when the .NET runtime and the built Npgsql harness exist."""
    return os.path.exists(DOTNET) and os.path.exists(DOTNET_HARNESS_DLL)


def has_psql():
    """Return True when the pinned psql client is present."""
    return os.path.exists(PSQL)


def has_cloudbeaver():
    """Return True when CloudBeaver's launcher and its bundled JRE exist."""
    return os.path.exists(CLOUDBEAVER_LAUNCHER) and os.path.exists(CLOUDBEAVER_JAVA_BIN)


def require_odbc():
    """Skip decorator for tests needing the ODBC toolchain, naming what is missing."""
    return unittest.skipUnless(
        has_odbc(), f"ODBC toolchain (psqlodbc) not installed; {_SETUP_HINT}"
    )


def require_jdbc():
    """Skip decorator for tests needing the JDBC toolchain, naming what is missing.

    The message names the selected pgjdbc version so a matrix run against a jar
    the setup script has not placed reads as that, not as a missing toolchain.
    """
    return unittest.skipUnless(
        has_jdbc(),
        f"JDBC toolchain (JDK + pgjdbc {selected_pgjdbc_version()}) not "
        f"installed; {_SETUP_HINT}",
    )


def require_dotnet():
    """Skip decorator for tests needing the .NET tier, naming what is missing."""
    return unittest.skipUnless(
        has_dotnet(), f"dotnet SDK or Npgsql harness not built; {_SETUP_HINT}"
    )


def require_cloudbeaver():
    """Skip decorator for the CloudBeaver tier, naming what is missing."""
    return unittest.skipUnless(
        has_cloudbeaver(), f"CloudBeaver not installed; {_SETUP_HINT}"
    )


def require_psql():
    """Return the psql binary's path, raising when the toolchain does not have it.

    Deliberately not a skip decorator like the guards above. psql's backslash
    commands are handwritten catalog SQL -- the queries this project exists to
    answer -- so a machine without psql must fail the tier rather than report
    a green run for a suite that never executed.

    Returns:
        The absolute path of the pinned psql binary.

    Raises:
        FileNotFoundError: If the binary is absent, naming the setup script.
    """
    if not has_psql():
        raise FileNotFoundError(
            f"psql {PSQL_VERSION} not found at {PSQL}; {_SETUP_HINT}"
        )
    return PSQL


def require_jdbc_tool():
    """Skip decorator for the optional SQL Workbench/J tier."""
    return unittest.skipUnless(
        has_jdbc_tool(),
        f"SQL Workbench/J not installed; {_SETUP_HINT} --with-jdbc-tool",
    )


def odbc_connection_string(port, driver="PostgreSQL Unicode", database="memory"):
    """Build a DSN-less ODBC connection string for a fixture server.

    Using a DSN-less string (naming the driver inline) rather than a configured
    data source lets each test class point at its own port without editing a
    shared odbc.ini.

    Args:
        port: The fixture server's TCP port.
        driver: The odbcinst.ini driver name ("PostgreSQL Unicode" or
            "PostgreSQL ANSI").
        database: The database to connect to; defaults to the fixture's.

    Returns:
        An ODBC connection string suitable for pyodbc.connect.
    """
    # BoolsAsChar=0 makes psqlodbc map pg bool to SQL_BIT so pyodbc returns a
    # real Python bool rather than its default '1'/'0' string.
    return (
        f"DRIVER={{{driver}}};SERVER=127.0.0.1;PORT={port};"
        f"DATABASE={database};UID=user;PWD=secret;BoolsAsChar=0"
    )


def connect_odbc(port, driver=ODBC_UNICODE_DRIVER, database="memory"):
    """Open an autocommit pyodbc connection configured for narrow-char decoding.

    pyodbc defaults to fetching character data as wide characters (SQL_C_WCHAR),
    which the narrow psqlodbc ANSI driver cannot produce -- it fails at SQLGetData
    with "Received an unsupported type from Postgres". Setting UTF-8 decoding for
    both SQL_CHAR and SQL_WCHAR makes pyodbc fetch narrow, so the ANSI driver
    reads text correctly; it is harmless for the Unicode driver. This is the
    correct pyodbc configuration for an ANSI ODBC driver, not a server concern.

    Args:
        port: The fixture server's TCP port.
        driver: The odbcinst.ini driver name.
        database: The database to connect to.

    Returns:
        An open, autocommit pyodbc connection.
    """
    import pyodbc

    connection = pyodbc.connect(
        odbc_connection_string(port, driver=driver, database=database), autocommit=True
    )
    connection.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
    connection.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
    return connection


def format_query_log_tail(log_path, limit=15):
    """Return the last ``limit`` lines of the server query log for a failure message.

    The fixture server records each data-path statement and whether it errored;
    showing the tail on an assertion failure names the driver query that broke,
    which is otherwise invisible from the client side.
    """
    try:
        with open(log_path, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except FileNotFoundError:
        return "(no query log)"
    tail = lines[-limit:]
    return "".join(tail).rstrip() or "(query log empty)"


def _reexec_with_runtime_env():
    """Re-run the current process with the ODBC runtime environment applied.

    Prepends our unixODBC lib directory to LD_LIBRARY_PATH so the pyodbc wheel
    resolves our libodbc.so.2, and points ODBCSYSINI / ODBCINI at our driver
    config so no user-level ODBC file is consulted. Does nothing when the ODBC
    toolchain is absent (tests skip) or when the environment is already applied
    -- guarded by a marker variable to avoid an exec loop.
    """
    if os.environ.get(_REEXEC_MARKER):
        return
    if not has_odbc():
        return

    library_path = os.environ.get("LD_LIBRARY_PATH", "")
    already_on_path = UNIXODBC_LIB in library_path.split(os.pathsep)
    if already_on_path:
        # Env was set up front (e.g. by make integration-test); just mark it.
        os.environ[_REEXEC_MARKER] = "1"
        return

    child_env = dict(os.environ)
    child_env[_REEXEC_MARKER] = "1"
    child_env["LD_LIBRARY_PATH"] = (
        UNIXODBC_LIB + (os.pathsep + library_path if library_path else "")
    )
    child_env["ODBCSYSINI"] = ODBC_ETC
    child_env["ODBCINI"] = os.path.join(ODBC_ETC, "odbc.ini")
    # sys.orig_argv preserves the exact original interpreter command (including
    # "-m unittest"), so the re-executed process runs the same thing this one
    # was started with, only with the ODBC runtime environment in place.
    os.execve(sys.executable, [sys.executable] + sys.orig_argv[1:], child_env)


_reexec_with_runtime_env()
