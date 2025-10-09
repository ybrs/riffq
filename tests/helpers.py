import sys
import subprocess
from pathlib import Path


def _ensure_riffq_built():
    # Always build/install the riffq extension locally to ensure latest code is tested
    root = Path(__file__).resolve().parent.parent
    subprocess.run(
        [sys.executable, '-m', 'maturin', 'develop', '--release'],
        cwd=str(root), check=True
    )
