test:
	python -m unittest discover -s tests

all-tests:
	maturin develop && \
	python -m unittest discover -s tests && \
	python -m unittest discover -s test_concurrency && \
	cd duckdb_pgcatalog && python test_duckdb_catalog.py

dev-build:
	maturin build --profile=fast -i python3