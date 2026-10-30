# Contributing to riffq

Thank you for considering a contribution to riffq. This document explains how
to set up your environment, how to verify your change, and what we expect in a
pull request (PR). Please read it before opening a PR.

## What you can contribute

- Bug fixes with a regression test.
- New features with tests and documentation.
- Documentation improvements.
- Additional client/driver compatibility coverage.

If you are unsure whether a larger change fits the project, open an issue
first and describe what you want to do.

## The short version

1. Set up the environment (see below).
2. Enable the pre-commit hook: `git config core.hooksPath hooks`.
3. Make your change, including tests.
4. Run the full verification (`./run_all_tests.sh`) via the hook or by hand.
5. Open a PR only once everything is green.

We do not accept PRs that have not passed the pre-commit hook. See
"Generative AI policy" below: this applies to human-written and AI-assisted
contributions alike.

## Required: the pre-commit hook

Every commit must pass the pre-commit hook in `hooks/pre-commit`. There are
no exceptions for "trivial" changes.

What the hook does:

1. Runs `cargo fmt`. If formatting rewrites the tree, the commit is rejected
   so you can review the reformatting, re-stage, and commit again. The hook
   is the part that reformats; `run_all_tests.sh` itself only checks with
   `cargo fmt --check` and never rewrites.
2. Runs `./run_all_tests.sh` in full. If any step fails, the commit is
   refused.

There is intentionally no logic that skips checks based on which files
changed. A one-line documentation edit rebuilds the extension and runs every
suite, including the ODBC, JDBC, .NET, CloudBeaver, and psql tiers, exactly
like a rewrite of the wire protocol would. The single definition of
"verified" is `run_all_tests.sh`; the hook calls it rather than keeping its
own copy of the pipeline, so a check added there is automatically enforced.

Expect the full run to take roughly half an hour. Most of that is the driver
matrices (the JDBC tier runs once per pinned pgjdbc release, the psql tier
once per pinned psql release).

### Enabling the hook

Git does not run hooks from a tracked directory automatically (if it did,
cloning a repository would execute its author's code before you had read any
of it). Each clone opts in once, from the repository root:

```bash
git config core.hooksPath hooks
```

This is recorded in `.git/config`, survives pulls, and applies to every hook
in the `hooks/` directory.

If you already have hooks in `.git/hooks` that you want to keep, link this
one in instead:

```bash
ln -s ../../hooks/pre-commit .git/hooks/pre-commit
```

Use a symlink rather than a copy, so edits to the tracked file update what
runs.

### Checking that the hook is active

```bash
git config --get core.hooksPath
```

This prints `hooks` when enabled and nothing when not. A commit with the hook
enabled prints its progress (lines like `=== cargo test` and
`--- cargo test ok`); a commit that prints none of that ran no hook.

### Bypassing the hook

`git commit --no-verify` skips the hook. That flag is implemented by git
itself and no hook can disable it. A commit made that way is unverified, and
an unverified PR will not be merged. Please do not use `--no-verify` except
for work-in-progress commits on your own branch that you intend to verify
before opening or undrafting a PR.

## Setting up the environment

The hook assumes a working Rust toolchain, a Python virtualenv at `venv/`,
and the driver-integration toolchain. Set all of this up once; every later
run reuses it.

### 1. Prerequisites

- Python 3.10 or newer (CI uses 3.10; development here uses 3.13).
- A stable Rust toolchain (`cargo`, `rustc`). The hook and
  `run_all_tests.sh` fall back to a vendored toolchain location if `cargo`
  is not on `PATH`, but having Rust installed normally is the supported
  setup.
- A C compiler for building unixODBC from source (part of the integration
  toolchain).
- Network access for downloading the pinned driver artifacts (JDK, .NET SDK,
  pgjdbc jars, PostgreSQL client binaries, CloudBeaver image layers).
  No root access, Docker daemon, or system-wide installs are needed:
  everything goes into gitignored directories inside the checkout.

### 2. Python virtualenv

`run_all_tests.sh` expects the virtualenv at `venv/` in the repository root.
Create it and install the dependencies there:

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/pip install maturin
venv/bin/maturin develop
```

`requirements.txt` covers the runtime and test dependencies (psycopg,
duckdb, pandas, pyarrow, polars, and similar). The compiled extension is
rebuilt with `maturin develop` on every full run so the tests never run
against a stale `.so`.

### 3. Driver-integration toolchain (one time)

The ODBC, JDBC, .NET, CloudBeaver, and psql tiers need real client drivers
that are not committed to the repo. Build and download them once:

```bash
make integration-toolchain
```

This runs `tests/integration/setup_toolchain.sh`, which builds unixODBC and
psqlodbc from pinned sources and downloads the pinned JDK, pgjdbc jars,
.NET SDK, PostgreSQL client binaries, and CloudBeaver image layers into the
gitignored `tests/integration/.toolchain/` directory. Nothing is written
outside `.toolchain/` and the project virtualenv.

The script is idempotent: re-running it after a partial run resumes rather
than rebuilding. Every download is checked against a pinned SHA-256.

To also fetch the optional SQL Workbench/J client used by
`test_jdbc_tool.py`:

```bash
make integration-toolchain ARGS=--with-jdbc-tool
```

Tests whose toolchain piece is missing skip themselves, but the pre-commit
hook runs the full matrices, so set up the complete toolchain before
committing. If a tier skips locally and fails in review, the PR is still
blocked on it.

### 4. Teleduck

The `teleduck/` subpackage is installed and tested as part of the full run
(`pip install -e .` inside `teleduck/` followed by its test suite). You do
not need to install it separately for day-to-day work, but do not be
surprised when the hook does.

## Running the tests

`run_all_tests.sh` is the single definition of "verified". Run it by hand
any time; it only reads the tree, never rewrites it:

```bash
./run_all_tests.sh
```

It runs, in order:

- `cargo fmt --check` (formatting; the hook runs `cargo fmt` first, which
  is the only step allowed to rewrite the tree).
- `cargo clippy --all-targets -- -D warnings -D clippy::pedantic`.
- `flake8 .` (max line length 120, see `.flake8`).
- `cargo test` (Rust unit tests for value encoding and statement
  splitting, which the Python suites cannot reach directly).
- `maturin develop` (rebuild the extension so tests run current code).
- `python -m unittest discover -s tests` (main Python suite).
- `python -m unittest discover -s test_concurrency` (concurrency suite).
- `python -m unittest discover -s teleduck/tests` (teleduck suite).
- `make integration-test` (driver integration: ODBC, JDBC, .NET,
  CloudBeaver, psql tiers).
- `make integration-matrix` (JDBC tier once per pinned pgjdbc release,
  currently 42.7.13, 42.7.8, 42.2.29, 42.2.14).
- `make psql-matrix` (psql backslash tier once per pinned psql release,
  currently 17.4 and 18.4).

Each step is timed and announced (`=== <step>` / `--- <step> ok`), and the
script exits non-zero on the first failure.

For faster iteration you may run individual steps (for example
`venv/bin/python -m unittest discover -s tests` or
`cargo test -j 6`), but a green subset is not a substitute for the full run:
open the PR only after `./run_all_tests.sh` passes end to end. Note that
`make integration-test` and the matrices must go through `make` rather than
calling `unittest` directly, because the Makefile sets `LD_LIBRARY_PATH` and
the ODBC environment before Python starts.

CI (`.github/workflows/ci.yml`) runs the Rust tests, the main Python suite,
the concurrency suite, and the teleduck suite on every push and PR. The
pre-commit hook is stricter than CI: it additionally enforces formatting,
clippy at pedantic level, flake8, and the full driver matrices. Passing CI
alone does not mean the hook will pass.

## Code style

- Rust: `cargo fmt` output, and warning-free `cargo clippy` at pedantic
  level (`-D warnings -D clippy::pedantic`).
- Python: `flake8` clean with max line length 120. Test suites carry client
  SQL captured verbatim, which is why the limit is 120 rather than 79; do
  not reformat captured SQL to fit a shorter limit.
- Plain ASCII text only in code, comments, docstrings, and Markdown: no
  emoji, no status icons, no decorative bullets, no fancy arrows (use `->`),
  no box-drawing characters, and no smart punctuation.
- Write functions and tests that a human can understand: every new
  function, method, and class needs a docstring, and names must describe
  what the code does rather than where it sits in a pipeline.

## What a good PR looks like

- One logical change per PR.
- Tests included: bug fixes add a regression test; features add coverage
  for the new behavior. Do not finish a task without tests.
- Never fail silently: if a query cannot be parsed, return an error to the
  client rather than falling back to a default. Fix failing tests instead
  of skipping or deleting them, and do not remove existing functionality
  unless the PR explicitly proposes to.
- A clear description: what changed, why, and how it was verified (paste
  or describe the `./run_all_tests.sh` result).
- A clean diff: `cargo fmt` applied, no stray files (note that local runs
  can leave artifacts like `duckdata.db*`; do not commit them).

## Generative AI policy

We accept AI-assisted and AI-generated pull requests. Many contributions to
this project are produced with coding assistants, and that is welcome.

The bar is the same regardless of how the code was written:

1. **You must run the pre-commit hook before opening the PR.** AI tooling
   makes it easy to produce a large, plausible-looking diff that fails
   formatting, clippy, flake8, or one of the driver tiers. The hook exists
   for exactly this case. Enable it (`git config core.hooksPath hooks`),
   let it run `./run_all_tests.sh` to completion, and do not open the PR
   until it is green. Do not use `git commit --no-verify` to get an
   unverified change into review.
2. **You are responsible for every line.** Only submit code you understand
   and can explain. Review the full diff yourself, including files the tool
   touched that you did not ask it to touch.
3. **Tests are still required.** If the assistant could not make the tests
   pass, say so in the PR rather than commenting tests out, skipping them,
   or weakening assertions to get green.
4. **Disclose AI assistance briefly.** One line in the PR description is
   enough (for example: "Written with assistance from <tool>"). This helps
   reviewers calibrate; it does not count against the PR.
5. **Keep the diff reviewable.** AI-generated diffs tend to sprawl. Split
   unrelated changes into separate PRs, keep new functions small and
   well-named with docstrings, and follow the style rules above.

A PR that is clearly unverified, unreviewed, or cannot be explained by its
author will be closed, even if the idea behind it is good. A smaller,
verified, understood PR is always preferable to a larger unverified one.
