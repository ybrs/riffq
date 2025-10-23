# Release Notes

## release-0.1.9

- Added TLS SNI propagation so `handle_connect` callbacks receive the negotiated server name, enabling per-host routing.
- Upgraded query engine dependencies (`datafusion` 50.2.0 and `pgwire` 0.34) 
- Introduced MkDocs-based documentation scaffolding, GitHub Pages workflow, and new guides covering catalog emulation and Redis as a database example.
- Expanded Python API annotations, docstrings, and supporting helpers to make the connection toolkit clearer to extend.
