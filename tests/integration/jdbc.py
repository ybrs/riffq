"""Compile and run the Java harnesses that drive pgjdbc against the fixture.

The two JDBC test modules do not talk to the driver directly; they run a small
Java program (a "harness") that opens a pgjdbc connection, exercises it, and
prints a JSON document the Python side asserts against. This module compiles a
harness once per session (skipping the compile when the .class is newer than its
.java source) and runs it with the pgjdbc jar on the classpath, returning the
parsed JSON.

Compiling and running use different jars on purpose. The harnesses only touch
java.sql interfaces, so they are compiled against the default pgjdbc release and
the resulting .class runs unchanged against any release in the matrix; pinning
the compile jar keeps the .class cache valid across a matrix run instead of
rebuilding, or worse silently reusing, it per version.

No Maven or Gradle: each harness is a single source file compiled with javac.
"""
import json
import os
import subprocess

from toolchain import (
    JAVA,
    JAVAC,
    PGJDBC_DEFAULT_JAR,
    PGJDBC_JAR,
    selected_pgjdbc_version,
)

JAVA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "java")


def jdbc_url(port, database="memory"):
    """Return the pgjdbc URL for a fixture server on the given port."""
    return f"jdbc:postgresql://127.0.0.1:{port}/{database}"


def compile_harness(class_name):
    """Compile ``<class_name>.java`` to a .class if the source is newer.

    Compiles against the default pgjdbc jar rather than the selected one, so a
    matrix run reuses one .class across every version it drives.

    Args:
        class_name: The harness class/file name without extension.

    Raises:
        subprocess.CalledProcessError: If javac reports a compile error.
    """
    source = os.path.join(JAVA_DIR, class_name + ".java")
    compiled = os.path.join(JAVA_DIR, class_name + ".class")
    if os.path.exists(compiled) and os.path.getmtime(compiled) >= os.path.getmtime(source):
        return
    subprocess.run(
        [JAVAC, "-cp", PGJDBC_DEFAULT_JAR, "-d", JAVA_DIR, source],
        check=True,
        capture_output=True,
        text=True,
    )


def run_harness(class_name, port, user="user", password="secret", database="memory"):
    """Compile (if needed) and run a harness, returning its parsed JSON output.

    Args:
        class_name: The harness class/file name without extension.
        port: The fixture server's port.
        user: Connection user.
        password: Connection password.
        database: Database to connect to.

    Returns:
        The JSON document the harness printed, parsed into Python objects.

    Raises:
        AssertionError: If the harness exits non-zero, with its stderr attached.
        json.JSONDecodeError: If the harness output is not valid JSON.
    """
    compile_harness(class_name)
    classpath = os.pathsep.join([PGJDBC_JAR, JAVA_DIR])
    result = subprocess.run(
        [JAVA, "-cp", classpath, class_name, jdbc_url(port, database), user, password],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"{class_name} exited {result.returncode} under pgjdbc "
            f"{selected_pgjdbc_version()}:\n{result.stderr}\n{result.stdout}"
        )
    return json.loads(result.stdout)
