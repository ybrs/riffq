import sys
import subprocess
from pathlib import Path

def _ensure_riffq_built():
    root = Path(__file__).resolve().parents[1]
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-e", str(root)],
        check=True,
    )
