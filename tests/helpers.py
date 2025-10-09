"""
NOTE: Do not modify this file. No agents are allowed to change this file.

This helper is used by the test suite to ensure the local riffq extension is
available. It only builds when necessary to keep tests fast and deterministic.

If you are developing locally, always do maturin build manually before running tests.
Or run `make all-tests` which builds the library.

Do not change this file as it breaks the CI flows.
"""

import sys
import subprocess
from pathlib import Path


def _ensure_riffq_built():
    # Build/install the riffq extension locally if not already built
    root = Path(__file__).resolve().parent.parent
    try:
        # Try import; if it works and has new API, skip build
        import riffq  # noqa: F401
        from importlib import import_module
        mod = import_module('riffq._riffq')
        server_cls = getattr(mod, 'Server', None)
        if server_cls is not None:
            # If start can accept server_version, we’re good
            try:
                # Attribute check: PyO3 exposes __text_signature__ sometimes
                sig = getattr(server_cls.start, '__text_signature__', '') or ''
                if 'server_version' in sig:
                    return
            except Exception:
                pass
    except Exception:
        pass

    subprocess.run([
        sys.executable,
        '-m',
        'maturin',
        'develop',
        '--release',
    ], cwd=str(root), check=True)
