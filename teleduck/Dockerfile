FROM python:3.11-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl && \
    rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH="/usr/local/cargo/bin:${PATH}"

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    chmod -R a+rX /usr/local/rustup /usr/local/cargo

RUN pip install maturin

COPY . /riffq
WORKDIR /riffq
COPY --from=pg_catalog . /pg_catalog
RUN maturin build --profile fast --out /tmp/wheels

FROM python:3.11-slim

RUN pip install duckdb pyarrow click && \
    chmod -R a+rX /usr/local/lib/python3.11/site-packages

COPY --from=builder /tmp/wheels /tmp/wheels
RUN pip install --no-deps /tmp/wheels/*.whl && \
    chmod -R a+rX /usr/local/lib/python3.11/site-packages && \
    rm -rf /tmp/wheels

WORKDIR /app
