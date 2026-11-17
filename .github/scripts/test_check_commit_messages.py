#!/usr/bin/env python3

import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parent / "check-commit-messages.py"
spec = importlib.util.spec_from_file_location("check_commit_messages", SCRIPT)
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


def violations(subject, allow_conventional_commits=True):
    return check.get_subject_violations(
        "commit", f"{subject}\n\nBody.\n", allow_conventional_commits
    )


class TestSubjects(unittest.TestCase):
    def test_upstream_style_unchanged(self):
        for allow in (False, True):
            with self.subTest(allow_conventional_commits=allow):
                self.assertEqual(violations("Add ch_multimatchanyindex", allow), [])
                self.assertEqual(violations("Remove plaintext passwords from the file cache", allow), [])
                [violation] = violations("add something", allow)
                self.assertTrue(violation.starts_with_lowercase)
                [violation] = violations("Add something.", allow)
                self.assertTrue(violation.ends_with_period)
                [violation] = violations("Added something", allow)
                self.assertEqual(violation.suggested_imperative, "Add")
                [violation] = violations("A" * 61, allow)
                self.assertEqual(violation.length, 61)
                self.assertFalse(violation.conventional)
                self.assertEqual(violations("A" * 60, allow), [])

    def test_conventional_subjects_pass(self):
        for subject in (
            "feat(hogql): add ch_multimatchanyindex",
            "fix(data-imports): dedupe case-colliding CDC source columns",
            "chore(mcp): require team review for exec and server instructions",
            "feat!: drop the legacy endpoint",
            "refactor(api)!: Rename the session handler",
            "build: bump airlift to 400",
        ):
            with self.subTest(subject=subject):
                self.assertEqual(violations(subject), [])

    def test_conventional_subjects_rejected_without_flag(self):
        [violation] = violations("feat(hogql): add ch_multimatchanyindex", False)
        self.assertTrue(violation.starts_with_lowercase)

    def test_conventional_summary_rules(self):
        [violation] = violations("fix(hogql): handle empty arrays.")
        self.assertTrue(violation.ends_with_period)
        self.assertFalse(violation.starts_with_lowercase)

        [violation] = violations("fix(hogql): fixed empty arrays")
        self.assertEqual(violation.suggested_imperative, "Fix")

        self.assertEqual(violations("refactor(data-warehouse): " + "a" * 60), [])
        [violation] = violations("refactor(data-warehouse): " + "a" * 61)
        self.assertEqual(violation.length, 61)
        self.assertTrue(violation.conventional)

    def test_malformed_prefixes_use_upstream_rules(self):
        for subject in (
            "wip: add something",
            "feature(hogql): add something",
            "feat(): add something",
            "feat(hogql):add something",
            "feat(hogql):  add something",
            "feat (hogql): add something",
        ):
            with self.subTest(subject=subject):
                [violation] = violations(subject)
                self.assertTrue(violation.starts_with_lowercase)
                self.assertFalse(violation.conventional)


if __name__ == "__main__":
    unittest.main()
