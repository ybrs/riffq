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
