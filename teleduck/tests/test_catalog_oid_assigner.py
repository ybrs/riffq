"""teleduck's oid assigner: deterministic, unique, and safe under threads.

The assigner is the one place teleduck invents identifiers, and it is shared:
riffq builds per-database contexts concurrently, and every context's catalog
scans resolve the same "db" keys through one DuckdbCatalogSource instance. An
unlocked check-then-act in resolve() let two racing threads assign X and X+1
to the same object -- found by review, reproduced empirically, and fixed with
a lock. The concurrency test here is that reproduction, kept as the
regression guard; the rest pins the derivation properties the docstring
promises (determinism, class separation, the built-in floor, and the
probe-on-collision fallback).
"""
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from teleduck.server import _CatalogOidAssigner, _FIRST_USER_OID  # noqa: E402


class CatalogOidAssignerTest(unittest.TestCase):
    """Determinism, uniqueness, the floor, and the collision probe."""

    def test_the_same_key_always_resolves_to_the_same_oid(self):
        """Repeated resolutions agree, and a second instance derives the same
        number for an uncollided key -- the property catalog joins rely on."""
        first = _CatalogOidAssigner()
        oid = first.resolve("rel", "db", "s", "t")
        self.assertEqual(first.resolve("rel", "db", "s", "t"), oid)
        self.assertEqual(_CatalogOidAssigner().resolve("rel", "db", "s", "t"), oid)

    def test_distinct_classes_and_names_get_distinct_oids(self):
        """Salts separate object classes and names separate objects, so a
        database, a schema, and a table sharing a name never share a number."""
        assigner = _CatalogOidAssigner()
        oids = {
            assigner.resolve("db", "orders"),
            assigner.resolve("ns", "orders"),
            assigner.resolve("rel", "orders"),
            assigner.resolve("rel", "customers"),
        }
        self.assertEqual(len(oids), 4)

    def test_every_oid_sits_at_or_above_the_builtin_floor(self):
        """Derived oids stay clear of PostgreSQL's reserved built-in range."""
        assigner = _CatalogOidAssigner()
        for salt, parts in [("db", ("a",)), ("rel", ("a", "b", "c"))]:
            self.assertGreaterEqual(assigner.resolve(salt, *parts), _FIRST_USER_OID)

    def test_a_derivation_landing_on_a_taken_oid_probes_to_the_next_free_one(self):
        """The collision fallback: a fresh key whose hash is already taken gets
        the next free number instead of a silent duplicate.

        Forced rather than searched for: the first key's derived oid is marked
        taken under a second key by patching the derivation, so the probe path
        runs deterministically.
        """
        assigner = _CatalogOidAssigner()
        collided = assigner.resolve("rel", "db", "s", "first")

        original_derive = assigner._derive
        assigner._derive = lambda key: collided
        try:
            probed = assigner.resolve("rel", "db", "s", "second")
        finally:
            assigner._derive = original_derive

        self.assertEqual(probed, collided + 1, "the next free number, exactly")
        # And both assignments stay stable afterwards.
        self.assertEqual(assigner.resolve("rel", "db", "s", "first"), collided)
        self.assertEqual(assigner.resolve("rel", "db", "s", "second"), probed)

    def test_concurrent_first_resolutions_of_one_key_agree(self):
        """The regression guard for the review finding: racing threads must
        never split one object across two oids.

        Before the lock, two threads passing the check-then-act together
        assigned X and X+1 to the same key; with a tight switch interval the
        unlocked code failed this test in a few dozen of 3000 trials. Each
        trial races a FRESH assigner so the first resolution -- the vulnerable
        window -- is what is exercised, on several keys to widen the window.
        """
        keys = [("db", f"database_{n}") for n in range(8)]
        original_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            for _ in range(300):
                assigner = _CatalogOidAssigner()
                barrier = threading.Barrier(4)
                answers = []

                def race(results=answers, gate=barrier, subject=assigner):
                    """Resolve every key once, in step with the other threads."""
                    gate.wait()
                    results.append([subject.resolve(*key) for key in keys])

                threads = [threading.Thread(target=race) for _ in range(4)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

                for other in answers[1:]:
                    self.assertEqual(
                        other,
                        answers[0],
                        "every thread must see one oid per object",
                    )
        finally:
            sys.setswitchinterval(original_interval)


if __name__ == "__main__":
    unittest.main()
