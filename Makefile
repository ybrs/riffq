test:
	python -m unittest discover -s tests

all-tests:
	maturin develop && \
	python -m unittest discover -s tests && \
	python -m unittest discover -s test_concurrency && \
	cd teleduck && pip install -e . && \
	python -m unittest discover -s tests


dev-build:
	maturin build --profile=fast -i python3