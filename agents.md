# Instructions
- Never fail silently. If tests don't pass fix the test. Never skip the tests.
- Never fail silently. If can't parse the query, return an error. Don't fallback to default or whatever. If there is an error. Return Error to the client.
- Don't remove any existing files unless you are explicitly told. 
- Don't remove any existing feature/functionality unless you are explicitly told.
- Always run tests, if tests fail fix the code. If you can't fix the code, or fail to implement the task, it's fine to fail. You can give up.
- For any given task add test cases. Don't try to finish the task without any tests. 

- For function parameters never use one letter parameters. Always be descriptive.
- Try to keep newly added functions less than 100 lines
- NEVER skip the pre-commit hook. NEVER use `git commit --no-verify` or `git commit -n`, NEVER unset or bypass `core.hooksPath`. Before any commit verify `git config --get core.hooksPath` prints `hooks` and let `hooks/pre-commit` run to completion via a normal `git commit`. A commit made with `--no-verify` is unverified and forbidden.
