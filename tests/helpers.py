import sys
import subprocess
from pathlib import Path

def _ensure_riffq_built():
    subprocess.call([sys.executable, "-m", "pip", "uninstall", "-y", "riffq"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        subprocess.check_call(["maturin", "build", "--release", "-q"])
        wheel = next(Path("target/wheels").glob("riffq-*.whl"))
        subprocess.check_call([sys.executable, "-m", "pip", "install", str(wheel)])
    except Exception as exc:
        raise RuntimeError(f"riffq build failed: {exc}")
