SHELL := /usr/bin/env bash

.PHONY: help test \
	check-ir-fn-corpus-structural-freshness \
	populate-ir-fn-corpus-structural \
	refresh-ir-fn-corpus-structural \
	analyze-sled-space \
	build-static-snapshot \
	verify-static-snapshot

LOCAL_SLED_DIR ?= $(CURDIR)/artifacts.sled
LOCAL_STORE_DIR ?= bvc-artifacts
SLED_SPACE_TOP ?= 25
SLED_SPACE_SAMPLE ?= 40
LOCAL_SNAPSHOT_DIR ?= $(CURDIR)/snapshot
LOCAL_SNAPSHOT_SKIP_REBUILD_WEB_INDICES ?= 0

help: ## Show available make targets and xlsynth_bvc top-level subcommands
	@echo "Make targets:"; \
	awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_.-]+:.*## / { printf "  %-44s %s\n", $$1, $$2 }' Makefile
	@echo
	@echo "xlsynth_bvc top-level subcommands:"; \
	awk 'BEGIN { in_enum = 0 } \
	function camel_to_kebab(s, out, i, c, prev) { \
	  out = ""; \
	  prev = ""; \
	  for (i = 1; i <= length(s); i++) { \
	    c = substr(s, i, 1); \
	    if (i > 1 && c ~ /[A-Z]/ && prev ~ /[a-z0-9]/) out = out "-"; \
	    out = out tolower(c); \
	    prev = c; \
	  } \
	  return out; \
	} \
	/^[[:space:]]*pub enum TopCommand/ { in_enum = 1; next } \
	in_enum && /^}/ { in_enum = 0 } \
	in_enum { \
	  line = $$0; \
	  gsub(/^[[:space:]]+/, "", line); \
	  if (line ~ /^[A-Za-z][A-Za-z0-9_]*[[:space:]]*[{,]/) { \
	    sub(/[[:space:]].*$$/, "", line); \
	    sub(/[,{}].*$$/, "", line); \
	    print "  " camel_to_kebab(line); \
	  } \
	}' src/cli.rs

test: ## Run cargo tests
	cargo test

check-ir-fn-corpus-structural-freshness: ## Check local structural corpus index freshness
	@set -euo pipefail; \
	cargo run --release --bin xlsynth_bvc -- \
	  --store-dir "$(LOCAL_STORE_DIR)" \
	  --artifacts-via-sled "$(LOCAL_SLED_DIR)" \
	  check-ir-fn-corpus-structural-freshness

populate-ir-fn-corpus-structural: ## Populate local structural IR corpus index in sled
	@set -euo pipefail; \
	cargo run --release --bin xlsynth_bvc -- \
	  --store-dir "$(LOCAL_STORE_DIR)" \
	  --artifacts-via-sled "$(LOCAL_SLED_DIR)" \
	  populate-ir-fn-corpus-structural

refresh-ir-fn-corpus-structural: populate-ir-fn-corpus-structural check-ir-fn-corpus-structural-freshness ## Rebuild and verify local structural IR corpus index

analyze-sled-space: ## Print local sled space breakdown JSON (set SLED_SPACE_TOP/SLED_SPACE_SAMPLE)
	@set -euo pipefail; \
	cargo run --release --bin xlsynth_bvc -- \
	  --store-dir "$(LOCAL_STORE_DIR)" \
	  --artifacts-via-sled "$(LOCAL_SLED_DIR)" \
	  analyze-sled-space \
	  --top "$(SLED_SPACE_TOP)" \
	  --sample "$(SLED_SPACE_SAMPLE)"

build-static-snapshot: ## Build a static snapshot directory from local sled web indices
	@set -euo pipefail; \
	extra_args=(); \
	if [[ "$(LOCAL_SNAPSHOT_SKIP_REBUILD_WEB_INDICES)" == "1" ]]; then \
	  extra_args+=(--skip-rebuild-web-indices); \
	fi; \
	cargo run --release --bin xlsynth_bvc -- \
	  --store-dir "$(LOCAL_STORE_DIR)" \
	  --artifacts-via-sled "$(LOCAL_SLED_DIR)" \
	  build-static-snapshot \
	  --out-dir "$(LOCAL_SNAPSHOT_DIR)" \
	  --overwrite \
	  "$${extra_args[@]}"

verify-static-snapshot: ## Verify static snapshot integrity checksums and manifest
	@set -euo pipefail; \
	cargo run --release --bin xlsynth_bvc -- \
	  --store-dir "$(LOCAL_STORE_DIR)" \
	  --artifacts-via-sled "$(LOCAL_SLED_DIR)" \
	  verify-static-snapshot \
	  --snapshot-dir "$(LOCAL_SNAPSHOT_DIR)"
