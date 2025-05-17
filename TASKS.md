# Bug Fix Tasks

Initially the test suite failed because the repository provided a `psycopg` shim
implemented on top of `pg8000`.  The dependency was missing which caused an
`ImportError` during test discovery.

```
$ python -m unittest discover -s tests -v
ImportError: No module named 'pg8000'
```

## Tasks
1. **Remove pg8000 dependency**
   - Delete the custom `psycopg` wrapper and rely on the `psycopg` package from
     `requirements.txt`.
2. **Update tests**
   - Ensure the unit tests import the correct client library.
3. **Automated test run**
   - Run `python -m unittest discover -s tests -v` and verify all tests pass.
