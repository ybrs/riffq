# Git hooks

`pre-commit` runs `../run_all_tests.sh` and refuses the commit if anything in it
fails: formatting, clippy at pedantic, flake8, and every test suite. It runs
whatever the change was - a one-line README edit runs the same checks as a
rewrite of the wire protocol.

## Enabling them in a fresh checkout

Git will not run hooks straight from a tracked directory. That is deliberate:
if it did, cloning a repository would execute its author's code before you had
read a line of it. So each clone opts in once, with one command:

    git config core.hooksPath hooks

Run from the repository root. It is recorded in `.git/config`, so it survives
pulls and applies to every hook in this directory, now and later.

If you would rather keep `.git/hooks` in charge - because you have other hooks
there already - link this one into it instead:

    ln -s ../../hooks/pre-commit .git/hooks/pre-commit

A symlink rather than a copy, so editing the tracked file updates what runs.

## Checking it is on

    git config --get core.hooksPath

Prints `hooks` when enabled, nothing when not. A commit with the hook enabled
prints its progress (`=== cargo test`, `--- cargo test ok`); a commit that
prints none of that ran no hook.

## Getting past it

`git commit --no-verify` skips the hook. Git implements that flag itself and no
hook can disable it. A commit made that way has not been verified, which is the
whole of what the flag means here.
