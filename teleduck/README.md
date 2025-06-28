Teleduck
========

Teleduck exposes a DuckDB database over the PostgreSQL protocol using `riffq`.
Simply run:

```
teleduck mydata.db
```

and connect with any PostgreSQL client.

### TLS options

Teleduck supports serving over TLS by default. The following command line options
control TLS behaviour:

- `--use-tls / --no-tls` – enable or disable TLS (default: enabled)
- `--tls-cert-file` – path to the certificate file to use
- `--tls-key-file` – path to the key file to use

If no certificate or key is provided, Teleduck uses the default ones shipped in
the package.
