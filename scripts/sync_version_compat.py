#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""Refresh checked-in xlsynth-crate release and repository metadata."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Any
import urllib.error
import urllib.request

from version_compat_metadata import latest_release_version, validate_observation


REPOSITORY = "xlsynth/xlsynth-crate"
GITHUB_API = f"https://api.github.com/repos/{REPOSITORY}"
REPO_ROOT = Path(__file__).resolve().parent.parent
COMPAT_PATH = REPO_ROOT / "third_party/xlsynth-crate/generated_version_compat.json"
OBSERVATION_PATH = (
    REPO_ROOT / "third_party/xlsynth-crate/repository_head_observation.json"
)


def _fetch(url: str, *, token: str | None, attempts: int = 3) -> bytes:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "xlsynth-bvc-version-sync",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
            if attempt == attempts:
                raise
            time.sleep(1)
    raise AssertionError("unreachable")


def _fetch_json(url: str, *, token: str | None) -> dict[str, Any]:
    value = json.loads(_fetch(url, token=token))
    if not isinstance(value, dict):
        raise ValueError(f"GitHub response from {url} is not an object")
    return value


def _commit_sha(response: dict[str, Any], *, source: str) -> str:
    sha = response.get("sha")
    if (
        not isinstance(sha, str)
        or len(sha) != 40
        or any(character not in "0123456789abcdef" for character in sha)
    ):
        raise ValueError(f"{source} has an invalid commit sha")
    return sha


def _commit_date(response: dict[str, Any], *, source: str) -> str:
    try:
        value = response["commit"]["committer"]["date"]
    except (KeyError, TypeError) as error:
        raise ValueError(f"{source} has no committer date") from error
    if not isinstance(value, str):
        raise ValueError(f"{source} has an invalid committer date")
    return value


def _nonnegative_integer(response: dict[str, Any], field: str) -> int:
    value = response.get(field)
    if type(value) is not int or value < 0:
        raise ValueError(f"GitHub comparison {field} is not a non-negative integer")
    return value


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _encode_json(value: Any) -> bytes:
    return (json.dumps(value, indent=2) + "\n").encode()


def _load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as input_file:
        return json.load(input_file)


def _write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output_file:
            output_file.write(data)
            output_file.flush()
            os.fsync(output_file.fileno())
        os.chmod(temporary_path, 0o644)
        os.replace(temporary_path, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary_path.unlink(missing_ok=True)


def _remote_state(token: str | None) -> tuple[bytes, str, dict[str, Any]]:
    head = _fetch_json(f"{GITHUB_API}/commits/main", token=token)
    head_commit = _commit_sha(head, source="GitHub main commit response")
    compat_url = (
        f"https://raw.githubusercontent.com/{REPOSITORY}/{head_commit}/"
        "generated_version_compat.json"
    )
    compat_bytes = _fetch(compat_url, token=token)
    compat = json.loads(compat_bytes)
    latest_version = latest_release_version(compat)
    latest_tag = f"v{latest_version}"
    release = _fetch_json(f"{GITHUB_API}/commits/{latest_tag}", token=token)
    release_commit = _commit_sha(release, source=f"GitHub {latest_tag} commit response")
    comparison = _fetch_json(
        f"{GITHUB_API}/compare/{release_commit}...{head_commit}", token=token
    )
    observation = {
        "schema_version": 2,
        "repository": REPOSITORY,
        "version_compat_sha256": _sha256(compat_bytes),
        "observed_at_utc": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "head_ref": "main",
        "head_commit": head_commit,
        "head_committed_at_utc": _commit_date(
            head, source="GitHub main commit response"
        ),
        "latest_crate_version": latest_version,
        "latest_release_tag": latest_tag,
        "latest_release_commit": release_commit,
        "latest_release_committed_at_utc": _commit_date(
            release, source=f"GitHub {latest_tag} commit response"
        ),
        "comparison_status": comparison.get("status"),
        "commits_ahead": _nonnegative_integer(comparison, "ahead_by"),
        "commits_behind": _nonnegative_integer(comparison, "behind_by"),
    }
    validate_observation(
        observation,
        repository=REPOSITORY,
        latest_crate_version=latest_version,
        version_compat_sha256=_sha256(compat_bytes),
        expected_remote_observation=observation,
    )
    return compat_bytes, latest_version, observation


def _local_observation_is_current(
    *,
    remote_observation: dict[str, Any],
    latest_version: str,
    compat_sha256: str,
) -> tuple[bool, str]:
    if not OBSERVATION_PATH.is_file():
        return False, f"repository head observation is missing: {OBSERVATION_PATH}"
    try:
        validate_observation(
            _load_json(OBSERVATION_PATH),
            repository=REPOSITORY,
            latest_crate_version=latest_version,
            version_compat_sha256=compat_sha256,
            expected_remote_observation=remote_observation,
        )
    except (OSError, json.JSONDecodeError, ValueError) as error:
        return False, str(error)
    return True, ""


def run(*, check: bool, quiet: bool) -> int:
    compat_bytes, latest_version, remote_observation = _remote_state(
        os.environ.get("GITHUB_TOKEN")
    )
    remote_sha = _sha256(compat_bytes)
    local_bytes = COMPAT_PATH.read_bytes() if COMPAT_PATH.is_file() else None
    compat_is_current = local_bytes == compat_bytes
    observation_is_current, observation_error = _local_observation_is_current(
        remote_observation=remote_observation,
        latest_version=latest_version,
        compat_sha256=remote_sha,
    )

    if check:
        if compat_is_current and observation_is_current:
            if not quiet:
                print(f"version compatibility metadata is up to date ({remote_sha})")
            return 0
        if not quiet:
            if not compat_is_current:
                local_sha = _sha256(local_bytes) if local_bytes is not None else "missing"
                print(
                    "version compatibility JSON is out of date: "
                    f"local={local_sha} remote={remote_sha}",
                    file=sys.stderr,
                )
            if not observation_is_current:
                print(
                    f"repository head observation is invalid: {observation_error}",
                    file=sys.stderr,
                )
        return 1

    if compat_is_current and observation_is_current:
        if not quiet:
            print(f"version compatibility metadata already up to date ({remote_sha})")
        return 0

    # Each rename is atomic. The observation's compatibility digest makes the pair
    # fail closed if the process stops between these two replacements.
    if not compat_is_current:
        _write_atomic(COMPAT_PATH, compat_bytes)
    _write_atomic(OBSERVATION_PATH, _encode_json(remote_observation))
    if not quiet:
        print(f"recorded compatibility map {remote_sha}")
        print(
            f"recorded {REPOSITORY} main relative to v{latest_version} in {OBSERVATION_PATH}"
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check", action="store_true", help="fail if local metadata is stale"
    )
    mode.add_argument(
        "--update",
        action="store_true",
        help="update local metadata if stale (the default)",
    )
    parser.add_argument("--quiet", action="store_true", help="suppress success output")
    args = parser.parse_args()
    try:
        return run(check=args.check, quiet=args.quiet)
    except (
        OSError,
        json.JSONDecodeError,
        ValueError,
        urllib.error.URLError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
