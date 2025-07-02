Teleduck
========

Teleduck exposes a DuckDB database over the PostgreSQL protocol using `riffq`.
Simply run:

```
teleduck mydata.db
```

and connect with any PostgreSQL client.

## Command line options

Teleduck serves the PostgreSQL protocol over TLS by default. The command
line interface accepts the following options:

```
teleduck [OPTIONS] DB_FILE
```

* `--host` – host interface to listen on (default: `127.0.0.1`)
* `--port` – port number (default: `5433`)
* `--sql-script` – path to a SQL script file to execute before the server starts.
  Can be given multiple times to run several scripts in order.
* `--sql` – SQL statement executed before the server starts. Can be specified
  multiple times. Use this to attach other DuckDB databases or run any
  initialisation queries before Teleduck begins accepting connections.
* `--use-tls/--no-use-tls` – enable or disable TLS (default: enabled)
* `--tls-cert-file` – path to a TLS certificate file. If not provided the built
  in certificate is used.
* `--tls-key-file` – path to the TLS private key file. If not provided the built
  in key is used.
* `--read-only/--no-read-only` – open the database in read-only mode
  (default: disabled).

### Generating certificates

You can generate a self‑signed certificate using `openssl`:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout server.key \
  -out server.crt -days 365 -subj "/CN=localhost"
```

Use `--tls-cert-file server.crt --tls-key-file server.key` to provide the files
to Teleduck.

## Authentication

Teleduck accepts any username and password by default.  Set the following
environment variables to require a particular user or password:

* `TELEDUCK_USERNAME` – expected username.
* `TELEDUCK_PASSWORD` – expected password.
* `TELEDUCK_PASSWORD_SHA1` – SHA1 hash of the expected password.

Example of setting credentials:

```bash
export TELEDUCK_USERNAME=myuser
export TELEDUCK_PASSWORD=mypassword
# alternatively use a SHA1 hash
export TELEDUCK_PASSWORD_SHA1=$(echo -n mypassword | sha1sum | awk '{print $1}')
```

If none of these variables are defined Teleduck allows any credentials.
