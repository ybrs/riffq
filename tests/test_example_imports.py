"""The documented examples import against the current riffq API.

example/lazy_catalog.py is what README.md and docs/catalog.md point a new
lazy-source user at, and nothing in the test suite exercised it -- so when
release-0.3.0 removed riffq.helpers.stable_oid, the example kept importing it
and died with ImportError on startup, unnoticed by a fully green gate. An
import is not a run, but it is exactly the failure class that slipped through:
an example referencing API that no longer exists.
"""
import importlib
import sys
import unittest
from pathlib import Path

EXAMPLE_DIR = Path(__file__).resolve().parents[1] / "example"


class ExampleImportTest(unittest.TestCase):
    """Each documented, import-safe example module loads cleanly."""

    def test_the_lazy_catalog_example_imports(self):
        """The module loads (its server starts only under __main__), and its
        oid registry answers get-or-allocate the way the module documents."""
        sys.path.insert(0, str(EXAMPLE_DIR))
        try:
            module = importlib.import_module("lazy_catalog")
            first = module.oid_for("rel", "appdb", "public", "users")
            self.assertEqual(module.oid_for("rel", "appdb", "public", "users"), first)
            self.assertGreaterEqual(first, 16384, "clear of the built-in range")
        finally:
            sys.path.remove(str(EXAMPLE_DIR))


if __name__ == "__main__":
    unittest.main()
