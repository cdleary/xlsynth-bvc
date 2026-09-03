#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""Select and validate checked-in xlsynth-crate publication metadata."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sys
from typing import Any


_TIMEZONE_OFFSETS = {
    "UTC": 0,
    "GMT": 0,
    "PST": -8 * 60 * 60,
    "PDT": -7 * 60 * 60,
    "MST": -7 * 60 * 60,
    "MDT": -6 * 60 * 60,
    "CST": -6 * 60 * 60,
    "CDT": -5 * 60 * 60,
    "EST": -5 * 60 * 60,
    "EDT": -4 * 60 * 60,
}
_OBSERVATION_FIELDS = {
    "schema_version",
    "repository",
    "observed_at_utc",
    "head_ref",
    "head_commit",
    "head_committed_at_utc",
    "latest_crate_version",
    "latest_release_tag",
    "latest_release_commit",
    "latest_release_committed_at_utc",
    "comparison_status",
    "commits_ahead",
    "commits_behind",
}


def _version_key(value: str) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in value.split("."))
    except ValueError as error:
        raise ValueError(f"invalid numeric crate version: {value!r}") from error


def _release_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("crate_release_datetime must be a string")
    try:
        naive_text, abbreviation = value.rsplit(" ", 1)
        offset = _TIMEZONE_OFFSETS[abbreviation]
        naive = datetime.strptime(naive_text, "%Y-%m-%d %H:%M:%S")
    except (KeyError, ValueError) as error:
        raise ValueError(f"invalid crate_release_datetime: {value!r}") from error
    return naive.replace(tzinfo=timezone(timedelta(seconds=offset)))


def latest_release_version(versions: Any) -> str:
    if not isinstance(versions, dict) or not versions:
        raise ValueError("version compatibility map must be a non-empty object")

    def ordering(item: tuple[str, Any]) -> tuple[datetime, tuple[int, ...]]:
        version, metadata = item
        if not isinstance(metadata, dict):
            raise ValueError(f"metadata for crate version {version!r} must be an object")
        return _release_datetime(metadata.get("crate_release_datetime")), _version_key(version)

    return max(versions.items(), key=ordering)[0]


def _require_utc_timestamp(observation: dict[str, Any], field: str) -> datetime:
    value = observation.get(field)
    if (
        not isinstance(value, str)
        or re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", value
        )
        is None
    ):
        raise ValueError(f"{field} must be an RFC 3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{field} must be an RFC 3339 UTC timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must be an RFC 3339 UTC timestamp")
    return parsed


def _require_nonnegative_integer(observation: dict[str, Any], field: str) -> int:
    value = observation.get(field)
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def validate_observation(
    observation: Any,
    *,
    repository: str,
    latest_crate_version: str,
    expected_head_commit: str,
) -> None:
    if not isinstance(observation, dict):
        raise ValueError("repository observation must be an object")
    unknown = set(observation) - _OBSERVATION_FIELDS
    missing = _OBSERVATION_FIELDS - set(observation)
    if unknown or missing:
        raise ValueError(
            f"repository observation fields differ: missing={sorted(missing)} unknown={sorted(unknown)}"
        )
    if type(observation["schema_version"]) is not int or observation["schema_version"] != 1:
        raise ValueError("schema_version must be 1")
    if observation["repository"] != repository:
        raise ValueError(f"repository must be {repository!r}")
    if observation["head_ref"] != "main":
        raise ValueError("head_ref must be 'main'")
    if observation["latest_crate_version"] != latest_crate_version:
        raise ValueError(
            "latest_crate_version does not match the publication-latest compatibility entry"
        )
    if observation["latest_release_tag"] != f"v{latest_crate_version}":
        raise ValueError("latest_release_tag does not match latest_crate_version")
    for field in ("head_commit", "latest_release_commit"):
        value = observation[field]
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{40}", value) is None:
            raise ValueError(f"{field} must be a lowercase 40-character Git commit")
    if re.fullmatch(r"[0-9a-f]{40}", expected_head_commit) is None:
        raise ValueError("expected_head_commit must be a lowercase 40-character Git commit")
    if observation["head_commit"] != expected_head_commit:
        raise ValueError("head_commit does not match the resolved upstream head")
    observed_at = _require_utc_timestamp(observation, "observed_at_utc")
    head_committed_at = _require_utc_timestamp(observation, "head_committed_at_utc")
    release_committed_at = _require_utc_timestamp(
        observation, "latest_release_committed_at_utc"
    )
    if observed_at < head_committed_at or observed_at < release_committed_at:
        raise ValueError("observed_at_utc must not predate either observed commit")
    ahead = _require_nonnegative_integer(observation, "commits_ahead")
    behind = _require_nonnegative_integer(observation, "commits_behind")
    expected_status = (
        "identical"
        if ahead == 0 and behind == 0
        else "ahead"
        if ahead > 0 and behind == 0
        else "behind"
        if ahead == 0 and behind > 0
        else "diverged"
    )
    if observation["comparison_status"] != expected_status:
        raise ValueError(
            f"comparison_status must be {expected_status!r} for ahead={ahead} behind={behind}"
        )
    commits_match = observation["head_commit"] == observation["latest_release_commit"]
    zero_distance = ahead == 0 and behind == 0
    if commits_match != zero_distance:
        raise ValueError("commit equality must agree with zero ahead/behind distance")


def _load_json(path: str) -> Any:
    with Path(path).open(encoding="utf-8") as input_file:
        return json.load(input_file)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    latest_parser = subparsers.add_parser("latest-release")
    latest_parser.add_argument("compatibility_json")
    validate_parser = subparsers.add_parser("validate-observation")
    validate_parser.add_argument("observation_json")
    validate_parser.add_argument("repository")
    validate_parser.add_argument("latest_crate_version")
    validate_parser.add_argument("expected_head_commit")
    args = parser.parse_args()
    try:
        if args.command == "latest-release":
            print(latest_release_version(_load_json(args.compatibility_json)))
        else:
            validate_observation(
                _load_json(args.observation_json),
                repository=args.repository,
                latest_crate_version=args.latest_crate_version,
                expected_head_commit=args.expected_head_commit,
            )
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
