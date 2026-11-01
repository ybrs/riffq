# claude-scripts

One-off diagnostic scripts from debugging sessions. Each script isolates one
symptom by running the real failing query and dropping one clause at a time,
so a cause is identified rather than guessed at.

They are not part of the test suite, CI, or packaging (`claude-scripts/` is
excluded in `.dockerignore`). Nothing imports them. They are kept because the
driver tiers fail in ways that are hard to re-derive: the exact query text and
the bisection steps are worth more than a summary in a ticket.

## Running

From the repo root:

    venv/bin/python -m claude-scripts.<name>

Each script documents its own question in its docstring.

## Scripts

- `isolate_npgsql_get_tables_query.py` - which clause makes Npgsql's
  `GetSchema("Tables")` return no rows (a `table_type` literal combined with
  a bound parameter).
- `bisect_npgsql_type_query.py` - cuts Npgsql's type-loading query down
  until it plans, to find what breaks it.
- `isolate_correlated_scalar_subquery.py` - which correlated scalar
  subquery shapes plan and execute correctly (checked against an
  equivalent LEFT JOIN).
- `inspect_pg_namespace_duplicates.py` - whether `pg_namespace` carries
  duplicate `public` rows and how far the duplication reaches.
- `trace_public_namespace_origin.py` - which database each duplicate
  `public` row belongs to, so the fix knows one-vs-two databases apart.
- `measure_database_startup_cost.py` - how startup time scales with the
  number of databases (dev vs release, view-creation share).
- `run_fixture_server.py` - keeps one fixture server up on a fixed port
  for driving by hand (e.g. CloudBeaver GraphQL work).

## Adding one

Prefer a script over a long ticket comment when the next person will need the
exact reproduction. Keep it self-contained, state the question in the
docstring, and reference it from the test or `tests/integration/README.md`
entry that records the outcome.
