#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bvc_bin="${BVC_BIN:-${repo_root}/target/release/xlsynth_bvc}"
store_dir="${BVC_STORE_DIR:-${repo_root}/bvc-artifacts}"
sled_db="${BVC_SLED_DB:-${store_dir}/artifacts.sled}"
work_dir="${BVC_WORK_DIR:-${store_dir}/publication-work}"
publish_root="${BVC_PUBLISH_ROOT:-${store_dir}/published-site}"
base_url="${BVC_BASE_URL:-/xlsynth-bvc/}"
workers="${BVC_WORKERS:-4}"
max_versions="${BVC_MAX_VERSIONS:-1}"

cd "${repo_root}"
"${bvc_bin}" --store-dir "${store_dir}" --artifacts-via-sled "${sled_db}" refresh-version-compat

mapfile -t pending_versions < <(
  "${bvc_bin}" --store-dir "${store_dir}" --artifacts-via-sled "${sled_db}" \
    list-pending-campaign-versions | tail -n "${max_versions}"
)

for crate_version in "${pending_versions[@]}"; do
  "${bvc_bin}" --store-dir "${store_dir}" --artifacts-via-sled "${sled_db}" \
    coordinate-release \
    --crate-version "${crate_version}" \
    --work-dir "${work_dir}" \
    --publish-root "${publish_root}" \
    --base-url "${base_url}" \
    --workers "${workers}"
done
