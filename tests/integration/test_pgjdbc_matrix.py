"""Tests for the pgjdbc version selection the matrix run drives.

``make integration-matrix`` runs the JDBC layer once per pinned pgjdbc release
by setting ``RIFFQ_PGJDBC_VERSION``. These tests cover the selection logic
itself -- which jar a version resolves to, and that a typo fails loudly instead
of silently skipping the whole JDBC layer as an absent toolchain.

The jar-presence test needs the toolchain and skips without it; the pure
selection tests run anywhere, so a bare machine still checks them.
"""
import os
import unittest
from unittest import mock

import toolchain
from toolchain import require_jdbc


class PgjdbcVersionSelectionTest(unittest.TestCase):
    """RIFFQ_PGJDBC_VERSION resolves to the right jar, or fails loudly."""

    def test_default_version_when_variable_is_unset(self):
        """With no variable set, the run uses the default pinned release."""
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                toolchain.selected_pgjdbc_version(), toolchain.PGJDBC_DEFAULT_VERSION
            )

    def test_default_version_when_variable_is_empty(self):
        """An empty variable is treated as unset, not as an unknown version."""
        with mock.patch.dict(os.environ, {toolchain.PGJDBC_VERSION_ENV: ""}):
            self.assertEqual(
                toolchain.selected_pgjdbc_version(), toolchain.PGJDBC_DEFAULT_VERSION
            )

    def test_matrix_version_is_selected(self):
        """A version from the matrix is returned as given."""
        with mock.patch.dict(os.environ, {toolchain.PGJDBC_VERSION_ENV: "42.2.14"}):
            self.assertEqual(toolchain.selected_pgjdbc_version(), "42.2.14")

    def test_unknown_version_raises_naming_the_choices(self):
        """A version outside the matrix raises, listing the valid versions.

        Without this the run would fall through to a missing jar and skip the
        JDBC layer, reporting success while testing nothing.
        """
        with mock.patch.dict(os.environ, {toolchain.PGJDBC_VERSION_ENV: "41.0.0"}):
            with self.assertRaises(ValueError) as caught:
                toolchain.selected_pgjdbc_version()
        self.assertIn("41.0.0", str(caught.exception))
        self.assertIn(toolchain.PGJDBC_DEFAULT_VERSION, str(caught.exception))

    def test_default_version_maps_to_the_unversioned_jar(self):
        """The default release resolves to postgresql.jar, not a versioned name."""
        self.assertEqual(
            toolchain.pgjdbc_jar_for(toolchain.PGJDBC_DEFAULT_VERSION),
            toolchain.PGJDBC_DEFAULT_JAR,
        )

    def test_older_version_maps_to_its_versioned_jar(self):
        """An older release resolves to the versioned jar the setup script places."""
        self.assertEqual(
            os.path.basename(toolchain.pgjdbc_jar_for("42.7.8")),
            "postgresql-42.7.8.jar",
        )

    def test_default_version_is_in_the_matrix(self):
        """The default release is itself part of the matrix, so it is covered too."""
        self.assertIn(
            toolchain.PGJDBC_DEFAULT_VERSION, toolchain.PGJDBC_MATRIX_VERSIONS
        )


@require_jdbc()
class PgjdbcMatrixJarsTest(unittest.TestCase):
    """Every jar the matrix names was actually placed by the setup script."""

    def test_every_matrix_version_has_its_jar(self):
        """A missing jar means the matrix run would skip that version silently."""
        missing = [
            version
            for version in toolchain.PGJDBC_MATRIX_VERSIONS
            if not os.path.exists(toolchain.pgjdbc_jar_for(version))
        ]
        self.assertEqual(missing, [], f"re-run setup_toolchain.sh; missing: {missing}")


if __name__ == "__main__":
    unittest.main()
