#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import unittest

from version_compat_metadata import latest_release_version, validate_observation


def valid_observation() -> dict[str, object]:
    return {
        "schema_version": 1,
        "repository": "xlsynth/xlsynth-crate",
        "observed_at_utc": "2026-09-03T22:34:04Z",
        "head_ref": "main",
        "head_commit": "a" * 40,
        "head_committed_at_utc": "2026-09-03T17:53:02Z",
        "latest_crate_version": "0.67.1",
        "latest_release_tag": "v0.67.1",
        "latest_release_commit": "b" * 40,
        "latest_release_committed_at_utc": "2026-09-03T16:00:00Z",
        "comparison_status": "ahead",
        "commits_ahead": 2,
        "commits_behind": 0,
    }


class VersionCompatMetadataTest(unittest.TestCase):
    def test_latest_release_uses_publication_order_for_backport(self) -> None:
        versions = {
            "0.68.0": {"crate_release_datetime": "2026-08-28 17:52:54 PDT"},
            "0.67.1": {"crate_release_datetime": "2026-09-03 09:00:00 PDT"},
        }

        self.assertEqual(latest_release_version(versions), "0.67.1")

    def test_latest_release_uses_numeric_version_as_datetime_tiebreak(self) -> None:
        versions = {
            "0.67.1": {"crate_release_datetime": "2026-09-03 09:00:00 PDT"},
            "0.68.0": {"crate_release_datetime": "2026-09-03 09:00:00 PDT"},
        }

        self.assertEqual(latest_release_version(versions), "0.68.0")

    def test_validate_observation_accepts_consistent_metadata(self) -> None:
        validate_observation(
            valid_observation(),
            repository="xlsynth/xlsynth-crate",
            latest_crate_version="0.67.1",
        )

    def test_validate_observation_rejects_latest_release_mismatch(self) -> None:
        observation = valid_observation()
        observation["latest_crate_version"] = "0.68.0"

        with self.assertRaisesRegex(ValueError, "publication-latest"):
            validate_observation(
                observation,
                repository="xlsynth/xlsynth-crate",
                latest_crate_version="0.67.1",
            )

    def test_validate_observation_rejects_malformed_commit(self) -> None:
        observation = valid_observation()
        observation["head_commit"] = "not-a-commit"

        with self.assertRaisesRegex(ValueError, "head_commit"):
            validate_observation(
                observation,
                repository="xlsynth/xlsynth-crate",
                latest_crate_version="0.67.1",
            )

    def test_validate_observation_rejects_inconsistent_comparison(self) -> None:
        observation = valid_observation()
        observation["comparison_status"] = "diverged"

        with self.assertRaisesRegex(ValueError, "comparison_status"):
            validate_observation(
                observation,
                repository="xlsynth/xlsynth-crate",
                latest_crate_version="0.67.1",
            )


if __name__ == "__main__":
    unittest.main()
