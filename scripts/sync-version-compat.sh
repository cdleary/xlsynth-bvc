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
source_url="https://raw.githubusercontent.com/xlsynth/xlsynth-crate/main/generated_version_compat.json"

if ! command -v curl >/dev/null 2>&1; then
  echo "error: curl is required but was not found in PATH" >&2
  exit 2
fi
if ! command -v sha256sum >/dev/null 2>&1; then
  echo "error: sha256sum is required but was not found in PATH" >&2
  exit 2
fi

tmp="$(mktemp "${TMPDIR:-/tmp}/xlsynth-version-compat.XXXXXX")"
cleanup() {
  rm -f "${tmp}"
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
  if [[ "${same}" == "true" ]]; then
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
  fi
  exit 1
fi

if [[ "${same}" == "true" ]]; then
  if [[ "${quiet}" != "true" ]]; then
    echo "version compat JSON already up to date (${remote_sha})"
  fi
  exit 0
fi

mkdir -p "$(dirname "${output_path}")"
cp "${tmp}" "${output_path}"
if [[ "${quiet}" != "true" ]]; then
  if [[ -z "${local_sha}" ]]; then
    echo "wrote version compat JSON (${remote_sha}) to ${output_path}"
  else
    echo "updated version compat JSON: ${local_sha} -> ${remote_sha}"
  fi
fi
