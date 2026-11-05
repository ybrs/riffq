"""Startup helper: EADDRINUSE retries, fast failure on crashes, and timeouts.

launch_server_process owns the start-and-wait loop every server test used to
copy-paste. These tests pin its three behaviors with plain sockets, not riffq
servers, so they run in seconds and fail for exactly one reason each.
"""
import socket
import threading
import time
import unittest

from helpers import launch_server_process, stop_server


def _bind_and_sleep(port):
    """Bind port and hold it briefly, standing in for a real server process."""
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", port))
    sock.listen(1)
    time.sleep(30)
    sock.close()


def _refuse_to_start(port):
    """Die with a bug, not a bind collision: the helper must surface this."""
    raise RuntimeError("boom from target")


def _never_listen(port):
    """Stay alive without binding: the helper must time out waiting."""
    time.sleep(60)


class LaunchServerProcessTest(unittest.TestCase):
    """The shared startup helper retries collisions but not crashes."""

    def test_bind_collision_retries_until_port_frees(self):
        """A port held at first attempt binds once the holder lets go."""
        port = 55511
        holder = socket.socket()
        holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        holder.bind(("127.0.0.1", port))
        holder.listen(1)
        timer = threading.Timer(1.0, holder.close)
        timer.start()
        try:
            proc = launch_server_process(_bind_and_sleep, port)
        finally:
            timer.join()
        try:
            self.assertTrue(proc.is_alive())
        finally:
            stop_server(proc)

    def test_non_bind_crash_fails_fast_with_its_error(self):
        """A target that raises does not burn the startup timeout."""
        with self.assertRaises(RuntimeError) as ctx:
            launch_server_process(_refuse_to_start, 55512)
        self.assertIn("boom from target", str(ctx.exception))

    def test_silent_target_times_out(self):
        """A live target that never binds raises after the startup timeout."""
        with self.assertRaises(RuntimeError) as ctx:
            launch_server_process(_never_listen, 55513, startup_timeout=3)
        self.assertIn("did not start within", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
