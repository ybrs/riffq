import unittest
from pathlib import Path
import importlib.util


def load_tests_from_file(path: Path) -> unittest.TestSuite:
    spec = importlib.util.spec_from_file_location("example_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)  # type: ignore
    return unittest.defaultTestLoader.loadTestsFromModule(module)


if __name__ == "__main__":
    test_file = Path(__file__).with_name("test_psql_redis.py")
    suite = load_tests_from_file(test_file)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)

