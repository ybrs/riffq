#!/bin/sh
# Verify this repository: format, lint, build, and every test including the
# end-to-end driver tiers.
#
# This is the single definition of what "verified" means here. The pre-commit
# hook calls it rather than keeping its own copy of the pipeline, so a check
# added here is a check the hook enforces, with no second list to keep in step.
#
# Nothing in it is conditional on which files changed: a docs-only edit rebuilds
# the extension and runs the ODBC, JDBC, .NET, CloudBeaver and psql tiers exactly
# as a rewrite of the wire protocol would. Run it by hand any time; it only reads
# the tree, never rewrites it (`cargo fmt --check` reports, the hook is what
# reformats).
#
# Expect roughly half an hour: the pgjdbc matrix alone runs the JDBC tier once
# per pinned driver release. Exits non-zero on the first failure.
set -e

# Put the Rust toolchain on PATH. A hook or cron shell inherits almost no
# environment, and this container keeps the toolchain outside the usual home
# directory, so neither can be assumed present.
ensure_rust_toolchain() {
    if command -v cargo >/dev/null 2>&1; then
        return 0
    fi
    toolchain_bin=/tmp/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin
    if [ ! -x "$toolchain_bin/cargo" ]; then
        echo "run_all_tests: cargo not found on PATH or at $toolchain_bin" >&2
        exit 1
    fi
    PATH="$toolchain_bin:$PATH"
    CARGO_HOME=${CARGO_HOME:-/tmp/.cargo}
    RUSTUP_HOME=${RUSTUP_HOME:-/tmp/.rustup}
    export PATH CARGO_HOME RUSTUP_HOME
}

# Announce a step and time it, so a slow run says which part is slow rather
# than going quiet for minutes.
step() {
    step_name=$1
    shift
    echo
    echo "=== $step_name"
    step_started=$(date +%s)
    "$@"
    echo "--- $step_name ok (`expr $(date +%s) - $step_started`s)"
}

ensure_rust_toolchain
cd "$(dirname "$0")"
repo_root=$(pwd)

PYTHON=venv/bin/python
if [ ! -x "$PYTHON" ]; then
    echo "run_all_tests: $PYTHON missing; this repo expects its venv at venv/" >&2
    exit 1
fi

# maturin refuses to install into a virtualenv it cannot see, and a hook or cron
# shell has nothing activated, so name the venv explicitly.
VIRTUAL_ENV="$repo_root/venv"
export VIRTUAL_ENV

# The driver tiers go through make, whose recipes invoke plain `python`. An
# interactive shell has that only because a venv is activated; a hook or cron
# shell has no `python` at all, just `python3`. Putting the venv's bin directory
# first is what activation would have done, and it keeps the Makefile as the one
# place the ODBC environment is defined.
PATH="$repo_root/venv/bin:$PATH"
export PATH

step "cargo fmt --check" cargo fmt --check
step "cargo clippy (pedantic)" cargo clippy --all-targets -j 6 -- -D warnings -D clippy::pedantic
step "flake8" $PYTHON -m flake8 .

# The Rust unit tests cover the value-encoding and statement-splitting paths
# directly. Nothing above reaches them: the Python suites drive the server over
# the wire, where a panic in the encoder surfaces as a dropped connection rather
# than as a named failing case.
step "cargo test" cargo test -j 6

# The Python tests import the compiled extension, so it is rebuilt every run:
# testing a stale .so reports a green result for code that never executed.
step "maturin develop" venv/bin/maturin develop

step "unittest tests/" $PYTHON -m unittest discover -s tests
step "unittest test_concurrency/" $PYTHON -m unittest discover -s test_concurrency
step "unittest teleduck/tests/" $PYTHON -m unittest discover -s teleduck/tests

# The driver tiers: real unixODBC/psqlodbc, a real JDK with pgjdbc, the .NET SDK
# with Npgsql, a containerised CloudBeaver, and psql running its backslash
# commands (handwritten catalog SQL, not a driver's metadata API).
# `make integration-test` sets
# LD_LIBRARY_PATH before python starts, which is the only moment the dynamic
# loader reads it, so the pyodbc wheel resolves this toolchain's unixODBC rather
# than the system one - which is why this goes through make rather than calling
# unittest directly.
step "driver integration tests" make integration-test

# The JDBC tier once per pinned pgjdbc release: clients ship whichever version
# their vendor bundled, so a catalog change that only works on the newest driver
# has to fail here.
step "pgjdbc version matrix" make integration-matrix

# The psql backslash tier once per pinned psql release. Same argument as the
# pgjdbc matrix, for the client whose queries are handwritten rather than
# generated: psql rewrites its backslash SQL between major versions, so a
# catalog change made for one release can break another on queries nobody here
# wrote.
step "psql version matrix" make psql-matrix

echo
echo "run_all_tests: everything passed"
