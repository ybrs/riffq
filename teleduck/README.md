Teleduck
========

Teleduck exposes a DuckDB database over the PostgreSQL protocol using `riffq`.
Simply run:

```
teleduck mydata.db
```

and connect with any PostgreSQL client.

## TLS options

Teleduck serves the PostgreSQL protocol over TLS by default. You can
control TLS behaviour using the following options:

* `--use-tls/--no-use-tls` – enable or disable TLS (default: enabled)
* `--tls-cert-file` – path to a TLS certificate file. If not provided the
  built in certificate is used.
* `--tls-key-file` – path to the TLS private key file. If not provided the
  built in key is used.

## Authentication

Teleduck accepts any username and password by default. Set the following
environment variables to enable credential checks:

* `TELEDUCK_USERNAME` – expected username
* `TELEDUCK_PASSWORD` – expected password
* `TELEDUCK_PASSWORD_SHA1` – SHA1 hash of the expected password

If none of these variables are defined, authentication succeeds for any
credentials.
