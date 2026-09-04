#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import unittest

from version_compat_metadata import latest_release_version, validate_observation


def valid_observation() -> dict[str, object]:
    return {
        "schema_version": 2,
        "repository": "xlsynth/xlsynth-crate",
        "version_compat_sha256": "c" * 64,
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


def validate(
    observation: object, *, expected_remote_observation: object | None = None
) -> None:
    validate_observation(
        observation,
        repository="xlsynth/xlsynth-crate",
        latest_crate_version="0.67.1",
        version_compat_sha256="c" * 64,
        expected_remote_observation=(
            valid_observation()
            if expected_remote_observation is None
            else expected_remote_observation
        ),
    )


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
        validate(valid_observation())

    def test_validate_observation_rejects_latest_release_mismatch(self) -> None:
        observation = valid_observation()
        observation["latest_crate_version"] = "0.68.0"

        with self.assertRaisesRegex(ValueError, "publication-latest"):
            validate(observation)

    def test_validate_observation_rejects_compatibility_digest_mismatch(self) -> None:
        observation = valid_observation()
        observation["version_compat_sha256"] = "d" * 64

        with self.assertRaisesRegex(ValueError, "compatibility map"):
            validate(observation)

    def test_validate_observation_rejects_malformed_commit(self) -> None:
        observation = valid_observation()
        observation["head_commit"] = "not-a-commit"

        with self.assertRaisesRegex(ValueError, "head_commit"):
            validate(observation)

    def test_validate_observation_rejects_inconsistent_comparison(self) -> None:
        observation = valid_observation()
        observation["comparison_status"] = "diverged"

        with self.assertRaisesRegex(ValueError, "comparison_status"):
            validate(observation)

    def test_validate_observation_rejects_different_resolved_head(self) -> None:
        expected = valid_observation()
        expected["head_commit"] = "c" * 40
        with self.assertRaisesRegex(ValueError, "head_commit.*remote repository state"):
            validate(valid_observation(), expected_remote_observation=expected)

    def test_validate_observation_rejects_fabricated_remote_fields(self) -> None:
        for field, value in [
            ("latest_release_commit", "0" * 40),
            ("head_committed_at_utc", "2000-01-01T00:00:00Z"),
            ("latest_release_committed_at_utc", "2000-01-01T00:00:00Z"),
            ("commits_ahead", 999_999),
        ]:
            with self.subTest(field=field):
                observation = valid_observation()
                observation[field] = value
                with self.assertRaisesRegex(
                    ValueError, f"{field}.*remote repository state"
                ):
                    validate(observation)

    def test_validate_observation_rejects_date_only_timestamp(self) -> None:
        observation = valid_observation()
        observation["observed_at_utc"] = "2026-09-03Z"

        with self.assertRaisesRegex(ValueError, "RFC 3339"):
            validate(observation)

    def test_validate_observation_rejects_observation_before_commit(self) -> None:
        observation = valid_observation()
        observation["observed_at_utc"] = "2026-09-03T15:00:00Z"

        with self.assertRaisesRegex(ValueError, "must not predate"):
            validate(observation)

    def test_validate_observation_rejects_identical_with_distinct_commits(self) -> None:
        observation = valid_observation()
        observation["comparison_status"] = "identical"
        observation["commits_ahead"] = 0
        observation["latest_release_commit"] = "b" * 40

        with self.assertRaisesRegex(ValueError, "commit equality"):
            validate(observation)

    def test_validate_observation_rejects_nonzero_distance_for_same_commit(self) -> None:
        observation = valid_observation()
        observation["latest_release_commit"] = observation["head_commit"]

        with self.assertRaisesRegex(ValueError, "commit equality"):
            validate(observation)


if __name__ == "__main__":
    unittest.main()
