FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl && \
    rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH="/usr/local/cargo/bin:${PATH}"

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    chmod -R a+rX /usr/local/rustup /usr/local/cargo

RUN pip install maturin duckdb pyarrow click && \
    chmod -R a+rwX /usr/local/lib/python3.11/site-packages

COPY . /riffq
WORKDIR /riffq
RUN maturin build --profile fast --out /tmp/wheels && \
    pip install --no-deps /tmp/wheels/*.whl && \
    chmod -R a+rX /usr/local/lib/python3.11/site-packages

WORKDIR /app
