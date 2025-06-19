test:
	python -m unittest discover -s tests

all-tests:
	python -m unittest discover -s tests && python -m unittest discover -s test_concurrency

dev-build:
	maturin build --profile=fast -i python3