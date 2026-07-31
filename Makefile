.PHONY: docs test all-tests integration-toolchain integration-test integration-matrix psql-matrix dev-build docs-serve

# pgjdbc releases integration-matrix drives the JDBC suite against, mirroring
# PGJDBC_MATRIX_VERSIONS in tests/integration/toolchain.py.
PGJDBC_MATRIX_VERSIONS := 42.7.13 42.7.8 42.2.29 42.2.14

# psql releases psql-matrix drives the backslash tier against, mirroring
# PSQL_MATRIX_VERSIONS in tests/integration/toolchain.py.
PSQL_MATRIX_VERSIONS := 17.4 18.4

# Absolute path to the driver-integration toolchain, needed on LD_LIBRARY_PATH
# so the pyodbc wheel resolves our unixODBC before Python starts.
INTEGRATION_DIR := tests/integration
INTEGRATION_TOOLCHAIN := $(abspath $(INTEGRATION_DIR)/.toolchain)

test:
	python -m unittest discover -s tests

# Build/download the ODBC + JDBC driver toolchain the integration tests need.
# Add --with-jdbc-tool as an argument here to also fetch the optional SQL
# Workbench/J client (make integration-toolchain ARGS=--with-jdbc-tool).
integration-toolchain:
	$(INTEGRATION_DIR)/setup_toolchain.sh $(ARGS)

# Run the driver integration tests. The ODBC environment is set before Python
# starts (the dynamic loader reads LD_LIBRARY_PATH only at startup). Tests whose
# toolchain piece is missing skip themselves, so this stays green on a machine
# where integration-toolchain has not been run.
integration-test:
	LD_LIBRARY_PATH=$(INTEGRATION_TOOLCHAIN)/unixodbc/lib \
	ODBCSYSINI=$(INTEGRATION_TOOLCHAIN)/etc \
	ODBCINI=$(INTEGRATION_TOOLCHAIN)/etc/odbc.ini \
	python -m unittest discover -s $(INTEGRATION_DIR) -t $(INTEGRATION_DIR)

# Run the JDBC layer once per pinned pgjdbc release. Clients carry whichever
# version their vendor shipped (Tableau ships 42.7.8; older BI installs pin the
# 42.2 line), so a catalog change that only works on the newest driver has to
# fail here. Runs every version before reporting, so one break does not hide
# the rest.
integration-matrix:
	@failed=""; \
	for version in $(PGJDBC_MATRIX_VERSIONS); do \
		echo "== pgjdbc $$version =="; \
		RIFFQ_PGJDBC_VERSION=$$version \
		LD_LIBRARY_PATH=$(INTEGRATION_TOOLCHAIN)/unixodbc/lib \
		ODBCSYSINI=$(INTEGRATION_TOOLCHAIN)/etc \
		ODBCINI=$(INTEGRATION_TOOLCHAIN)/etc/odbc.ini \
		python -m unittest discover -s $(INTEGRATION_DIR) -t $(INTEGRATION_DIR) -k jdbc \
			|| failed="$$failed $$version"; \
	done; \
	if [ -n "$$failed" ]; then echo "FAILED under pgjdbc:$$failed"; exit 1; fi; \
	echo "all pgjdbc versions passed: $(PGJDBC_MATRIX_VERSIONS)"

# Run the psql backslash tier once per pinned psql release. psql rewrites its
# backslash SQL between major versions, so a catalog change made for one can
# break another on queries nobody here wrote: 18 calls pg_get_constraintdef in
# `\d` and joins pg_available_extensions() in `\dx`, and 17.4 does neither.
# Runs every version before reporting, so one break does not hide the rest.
psql-matrix:
	@failed=""; \
	for version in $(PSQL_MATRIX_VERSIONS); do \
		echo "== psql $$version =="; \
		RIFFQ_PSQL_VERSION=$$version \
		LD_LIBRARY_PATH=$(INTEGRATION_TOOLCHAIN)/unixodbc/lib \
		ODBCSYSINI=$(INTEGRATION_TOOLCHAIN)/etc \
		ODBCINI=$(INTEGRATION_TOOLCHAIN)/etc/odbc.ini \
		python -m unittest discover -s $(INTEGRATION_DIR) -t $(INTEGRATION_DIR) -k psql \
			|| failed="$$failed $$version"; \
	done; \
	if [ -n "$$failed" ]; then echo "FAILED under psql:$$failed"; exit 1; fi; \
	echo "all psql versions passed: $(PSQL_MATRIX_VERSIONS)"

all-tests:
	maturin develop
	python3 -m unittest discover -s tests
	python3 -m unittest discover -s test_concurrency
	cd teleduck && pip install -e . && python3 -m unittest discover -s tests
	$(MAKE) integration-test
	$(MAKE) integration-matrix
	$(MAKE) psql-matrix

dev-build:
	maturin build --profile=fast -i python3

docs:
	mkdocs build

docs-serve:
	mkdocs serve -a 0.0.0.0:8000
