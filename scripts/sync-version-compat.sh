#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/sync-version-compat.sh [--check] [--update] [--quiet]

Syncs third_party/xlsynth-crate/generated_version_compat.json from:
  https://raw.githubusercontent.com/xlsynth/xlsynth-crate/main/generated_version_compat.json

Modes:
  --update  Update local file if remote differs (default).
  --check   Exit non-zero if local file is out of date.
  --quiet   Suppress success output.
EOF
}

mode="update"
quiet="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) mode="check" ;;
    --update) mode="update" ;;
    --quiet) quiet="true" ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
output_path="${repo_root}/third_party/xlsynth-crate/generated_version_compat.json"
observation_path="${repo_root}/third_party/xlsynth-crate/repository_head_observation.json"
source_url="https://raw.githubusercontent.com/xlsynth/xlsynth-crate/main/generated_version_compat.json"
repository="xlsynth/xlsynth-crate"
repository_api="https://api.github.com/repos/${repository}"

if ! command -v curl >/dev/null 2>&1; then
  echo "error: curl is required but was not found in PATH" >&2
  exit 2
fi
if ! command -v sha256sum >/dev/null 2>&1; then
  echo "error: sha256sum is required but was not found in PATH" >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required but was not found in PATH" >&2
  exit 2
fi

tmp="$(mktemp "${TMPDIR:-/tmp}/xlsynth-version-compat.XXXXXX")"
observation_tmp="$(mktemp "${TMPDIR:-/tmp}/xlsynth-repository-observation.XXXXXX")"
head_tmp="$(mktemp "${TMPDIR:-/tmp}/xlsynth-repository-head.XXXXXX")"
release_tmp="$(mktemp "${TMPDIR:-/tmp}/xlsynth-repository-release.XXXXXX")"
compare_tmp="$(mktemp "${TMPDIR:-/tmp}/xlsynth-repository-compare.XXXXXX")"
cleanup() {
  rm -f "${tmp}" "${observation_tmp}" "${head_tmp}" "${release_tmp}" "${compare_tmp}"
}
trap cleanup EXIT

curl -fsSL --retry 3 --retry-delay 1 "${source_url}" -o "${tmp}"

if command -v jq >/dev/null 2>&1; then
  jq empty "${tmp}" >/dev/null
fi

remote_sha="$(sha256sum "${tmp}" | awk '{print $1}')"
local_sha=""
same="false"
if [[ -f "${output_path}" ]]; then
  local_sha="$(sha256sum "${output_path}" | awk '{print $1}')"
  if cmp -s "${output_path}" "${tmp}"; then
    same="true"
  fi
fi

if [[ "${mode}" == "check" ]]; then
  if [[ "${same}" == "true" && -f "${observation_path}" ]]; then
    if [[ "${quiet}" != "true" ]]; then
      echo "version compat JSON is up to date (${remote_sha})"
    fi
    exit 0
  fi
  if [[ "${quiet}" != "true" ]]; then
    if [[ -z "${local_sha}" ]]; then
      echo "version compat JSON is missing; expected sha256 ${remote_sha}" >&2
    else
      echo "version compat JSON is out of date: local=${local_sha} remote=${remote_sha}" >&2
    fi
    if [[ ! -f "${observation_path}" ]]; then
      echo "repository head observation is missing: ${observation_path}" >&2
    fi
  fi
  exit 1
fi

if [[ "${same}" == "true" && -f "${observation_path}" ]]; then
  if [[ "${quiet}" != "true" ]]; then
    echo "version compat JSON already up to date (${remote_sha})"
  fi
  exit 0
fi

latest_crate_version="$(python3 -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as f:
    versions = json.load(f)
if not versions:
    raise SystemExit("version compatibility map is empty")
print(max(versions, key=lambda value: tuple(int(part) for part in value.split("."))))
' "${tmp}")"
latest_release_tag="v${latest_crate_version}"

github_headers=(
  -H "Accept: application/vnd.github+json"
  -H "X-GitHub-Api-Version: 2022-11-28"
  -H "User-Agent: xlsynth-bvc-version-sync"
)
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  github_headers+=( -H "Authorization: Bearer ${GITHUB_TOKEN}" )
fi
curl -fsSL --retry 3 --retry-delay 1 "${github_headers[@]}" "${repository_api}/commits/main" -o "${head_tmp}"
curl -fsSL --retry 3 --retry-delay 1 "${github_headers[@]}" "${repository_api}/commits/${latest_release_tag}" -o "${release_tmp}"
curl -fsSL --retry 3 --retry-delay 1 "${github_headers[@]}" "${repository_api}/compare/${latest_release_tag}...main" -o "${compare_tmp}"

python3 - "${head_tmp}" "${release_tmp}" "${compare_tmp}" "${observation_tmp}" "${repository}" "${latest_crate_version}" "${latest_release_tag}" <<'PY'
from datetime import datetime, timezone
import json
import sys

head_path, release_path, compare_path, output_path, repository, version, tag = sys.argv[1:]
with open(head_path, encoding="utf-8") as f:
    head = json.load(f)
with open(release_path, encoding="utf-8") as f:
    release = json.load(f)
with open(compare_path, encoding="utf-8") as f:
    comparison = json.load(f)
observation = {
    "schema_version": 1,
    "repository": repository,
    "observed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    "head_ref": "main",
    "head_commit": head["sha"],
    "head_committed_at_utc": head["commit"]["committer"]["date"],
    "latest_crate_version": version,
    "latest_release_tag": tag,
    "latest_release_commit": release["sha"],
    "latest_release_committed_at_utc": release["commit"]["committer"]["date"],
    "comparison_status": comparison["status"],
    "commits_ahead": comparison["ahead_by"],
    "commits_behind": comparison["behind_by"],
}
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(observation, f, indent=2)
    f.write("\n")
PY

mkdir -p "$(dirname "${output_path}")"
if [[ "${same}" != "true" ]]; then
  cp "${tmp}" "${output_path}"
fi
cp "${observation_tmp}" "${observation_path}"
if [[ "${quiet}" != "true" ]]; then
  if [[ -z "${local_sha}" ]]; then
    echo "wrote version compat JSON (${remote_sha}) to ${output_path}"
  elif [[ "${same}" == "true" ]]; then
    echo "version compat JSON already up to date (${remote_sha})"
  else
    echo "updated version compat JSON: ${local_sha} -> ${remote_sha}"
  fi
  echo "recorded ${repository} main relative to ${latest_release_tag} in ${observation_path}"
fi
