"""Measure how server startup time scales with the number of databases.

tests/test_multiple_databases.py carries a 30-second connect budget and a note
that "having multiple databases can take sometime", but a timeout budget is not
a measurement. Whether per-database catalogs are affordable depends on the real
number and on whether it grows per database, so this times it directly.

Each run starts a server registering N databases and measures from process
start to the first successful connection, then repeats to show variance.

Run from the riffq project root:

    venv/bin/python -m claude-scripts.measure_database_startup_cost
"""
import multiprocessing
import socket
import statistics
import sys
import time

# Ports are distinct per run so a lingering socket from a previous server
# cannot make the next run look instant.
BASE_PORT = 55700

# Database counts to measure. 0 means "no register_database call at all", the
# single default context, which is the baseline the others are compared to.
DATABASE_COUNTS = [0, 1, 2, 4]

REPEATS = 2


def _run_server(port, database_count):
    """Start a riffq server registering `database_count` databases.

    A query callback is required before start(); it is never invoked here,
    since only the time to become connectable is being measured.
    """
    import riffq
    import pyarrow as pa

    def handle_query(sql, callback, **kwargs):
        batch = pa.record_batch([pa.array([1], pa.int64())], names=["val"])
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        callback(reader.__arrow_c_stream__())

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    for index in range(database_count):
        server.register_database(f"db{index}")
    server.start(catalog_emulation=True)


def time_startup(port, database_count, timeout=180):
    """Return seconds from process start until the port accepts a connection.

    Args:
        port: Port for this run's server.
        database_count: How many databases to register.
        timeout: Give up after this many seconds.

    Returns:
        Elapsed seconds, or None if the server never became connectable.
    """
    process = multiprocessing.Process(
        target=_run_server, args=(port, database_count), daemon=True
    )
    started = time.monotonic()
    process.start()
    try:
        while time.monotonic() - started < timeout:
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    return time.monotonic() - started
            time.sleep(0.05)
        return None
    finally:
        process.terminate()
        process.join(timeout=30)


def main():
    """Measure startup for each database count and print the results."""
    print(f"{'databases':>10}  {'runs (s)':>22}  {'median':>8}")
    port = BASE_PORT
    for count in DATABASE_COUNTS:
        timings = []
        for _ in range(REPEATS):
            elapsed = time_startup(port, count)
            port += 1
            if elapsed is None:
                print(f"{count:>10}  did not start within the timeout")
                break
            timings.append(elapsed)
        else:
            rendered = ", ".join(f"{t:.2f}" for t in timings)
            print(f"{count:>10}  {rendered:>22}  {statistics.median(timings):>8.2f}")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
