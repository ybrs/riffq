"""Run psql against a fixture server and hand back its rows as Python values.

The other driver tiers reach the server through a driver's metadata API; this
one drives the psql command line instead, because psql's backslash commands are
handwritten catalog SQL (``\\dt`` queries pg_class and pg_namespace, ``\\d+``
adds format_type, pg_get_expr and pg_get_constraintdef, ``\\l`` reads
pg_database) and running them is the most direct check that the emulation
answers what a PostgreSQL client actually asks.

psql is invoked in unaligned tuples-only mode with an explicit field separator,
so this module parses records rather than psql's aligned box output. The box
drawing carries the same data but its column widths and its title lines move
between releases, which would make the assertions test psql's renderer instead
of the catalog.
"""
import os
import subprocess

from toolchain import PSQL_VERSION, require_psql

from fixture_server import FIXTURE_PASSWORD

# The field separator psql puts between columns in unaligned mode. Chosen
# because no value in the fixture dataset or in the catalog metadata the
# backslash commands report contains it, so splitting on it is unambiguous.
FIELD_SEPARATOR = "|"

# The record separator psql puts between rows. Not a newline: an access
# privileges column holds one grant per line, so \l returns values with
# newlines inside them and splitting rows on newlines would tear them apart.
# ASCII 30 is the record separator control character and appears in no catalog
# value.
RECORD_SEPARATOR = "\x1e"

# psql's stand-in for an empty result set from a backslash command. It is
# written to stdout like a row, so the parser has to recognise it rather than
# return it as a one-column record.
NO_RELATIONS_FOUND = "Did not find any relations."

# Every psql run gets this long to finish. A backslash command against the
# fixture answers in well under a second; the timeout only exists so a server
# that stops responding fails the test instead of hanging the suite.
TIMEOUT_SECONDS = 60


class PsqlError(Exception):
    """Raised when psql exits non-zero, carrying the command and its stderr.

    Attributes:
        command: The psql input that failed, as passed to ``run``.
        exit_code: psql's exit status.
        stderr: Everything psql wrote to stderr, which holds the server's
            ErrorResponse text for a failed catalog query.
    """

    def __init__(self, command, exit_code, stderr):
        """Build the exception and its message from the failed psql invocation."""
        super().__init__(
            f"psql exited {exit_code} for {command!r}:\n{stderr.strip()}"
        )
        self.command = command
        self.exit_code = exit_code
        self.stderr = stderr


def psql_version():
    """Return the version string the toolchain's psql binary reports.

    Returns:
        The version as psql prints it, e.g. "17.4".
    """
    binary = require_psql()
    completed = subprocess.run(
        [binary, "--version"],
        capture_output=True,
        text=True,
        check=True,
        timeout=TIMEOUT_SECONDS,
    )
    # psql prints "psql (PostgreSQL) 17.4"; the version is the last field.
    return completed.stdout.strip().split()[-1]


def run(port, command, database="memory", user="user"):
    """Run one psql command against a fixture server and return its output rows.

    Args:
        port: The fixture server's TCP port.
        command: The psql input to run, either a backslash command such as
            ``\\dt`` or a SQL statement.
        database: The database to connect to; defaults to the fixture's.
        user: The user to connect as; the fixture accepts any name.

    Returns:
        A list of rows, each a list of column strings. An empty result -- no
        rows, or psql's "Did not find any relations." -- returns an empty list.

    Raises:
        PsqlError: If psql exits non-zero, which ON_ERROR_STOP makes it do as
            soon as the server rejects any query the command issues.
    """
    binary = require_psql()
    # -X skips ~/.psqlrc so a developer's own settings cannot change what the
    # tier asserts. -A -t -F -R give unaligned, header-free records with
    # explicit field and record separators. ON_ERROR_STOP turns a server error
    # into a non-zero exit, which is the only way a backslash command's failure
    # reaches the caller.
    argv = [
        binary,
        "-X",
        "-A",
        "-t",
        "-F", FIELD_SEPARATOR,
        "-R", RECORD_SEPARATOR,
        "-v", "ON_ERROR_STOP=1",
        "-h", "127.0.0.1",
        "-p", str(port),
        "-U", user,
        "-d", database,
        "-c", command,
    ]
    environment = dict(os.environ)
    environment["PGPASSWORD"] = FIXTURE_PASSWORD
    # psql reads the client encoding and locale from the environment and would
    # otherwise inherit whatever the developer's shell has; pinning them keeps
    # the text it returns identical everywhere.
    environment["PGCLIENTENCODING"] = "UTF8"
    environment["LC_ALL"] = "C"

    completed = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        env=environment,
        timeout=TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        raise PsqlError(command, completed.returncode, completed.stderr)
    return parse_records(completed.stdout)


def parse_records(output):
    """Split psql's unaligned tuples-only output into rows of column strings.

    Args:
        output: Everything psql wrote to stdout.

    Returns:
        A list of rows, each a list of column strings, with psql's
        "Did not find any relations." notice reported as no rows at all.
    """
    # psql ends the last record with a newline rather than a separator, so the
    # trailing newline is stripped before splitting rather than after.
    body = output.rstrip("\n")
    if not body or body == NO_RELATIONS_FOUND:
        return []
    return [record.split(FIELD_SEPARATOR) for record in body.split(RECORD_SEPARATOR)]


def column(rows, index):
    """Return one column of a parsed result, in row order.

    Args:
        rows: Rows as returned by ``run``.
        index: The zero-based column position to pull out.

    Returns:
        A list holding that column's value from every row.
    """
    return [row[index] for row in rows]


def describe_version_mismatch(reported):
    """Return the failure message for a psql that is not the pinned version.

    Args:
        reported: The version the binary printed.

    Returns:
        A message naming both versions and where the pin lives.
    """
    return (
        f"psql reports {reported}, but this tier is pinned to {PSQL_VERSION} "
        "(toolchain.PSQL_VERSION). Backslash commands issue different catalog "
        "SQL per major release, so the expectations below are tied to one "
        "version; re-pin both the artifact in setup_toolchain.sh and "
        "PSQL_VERSION together."
    )
