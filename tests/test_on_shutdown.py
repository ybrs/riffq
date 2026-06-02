import multiprocessing
import os
import signal
import socket
import tempfile
import time
import unittest


def _run_server(port, sentinel_path):
    """
    runs a minimal riffq server whose on_shutdown callback writes sentinel_path.
    the sentinel lets the parent process observe that the callback actually ran
    before the server exited.
    """
    import riffq
    from riffq.helpers import to_arrow

    def handle_query(sql, callback, **kwargs):
        callback(to_arrow([{"name": "val", "type": "int"}], [[1]]))

    def on_shutdown():
        with open(sentinel_path, "w", encoding="utf-8") as handle:
            handle.write("shutdown")

    server = riffq.Server(f"127.0.0.1:{port}")
    server.on_query(handle_query)
    server.on_shutdown(on_shutdown)
    server.start()


def _wait_for_socket(port):
    """blocks until the server accepts TCP connections, or raises on timeout."""
    for _attempt in range(300):
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return

        time.sleep(0.1)

    raise RuntimeError(f"server never bound port {port}")


def _wait_for_sentinel(path):
    """waits up to 10s for the on_shutdown callback to create path."""
    for _attempt in range(100):
        if os.path.exists(path):
            return True

        time.sleep(0.1)

    return False


class OnShutdownTest(unittest.TestCase):
    def _run_signal_case(self, port, sig):
        """
        starts a server, delivers sig, and asserts the on_shutdown callback ran
        and the process exited gracefully (no force-kill needed).
        """
        fd, sentinel = tempfile.mkstemp(suffix=".shutdown")
        os.close(fd)
        os.unlink(sentinel)
        proc = multiprocessing.Process(target=_run_server, args=(port, sentinel), daemon=True)
        proc.start()

        try:
            _wait_for_socket(port)
            os.kill(proc.pid, sig)
            ran = _wait_for_sentinel(sentinel)
            proc.join(timeout=10)
            self.assertTrue(ran, "on_shutdown callback did not run")
            self.assertFalse(proc.is_alive(), "server did not exit after signal")
        finally:
            if proc.is_alive():
                proc.kill()
                proc.join()

            if os.path.exists(sentinel):
                os.unlink(sentinel)

    def test_on_shutdown_runs_on_sigterm(self):
        """SIGTERM (kill/docker stop/k8s) triggers the on_shutdown callback."""
        self._run_signal_case(55461, signal.SIGTERM)

    def test_on_shutdown_runs_on_sigint(self):
        """SIGINT (ctrl-c) triggers the on_shutdown callback."""
        self._run_signal_case(55462, signal.SIGINT)


if __name__ == "__main__":
    unittest.main()
