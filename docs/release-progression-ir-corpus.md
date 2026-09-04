<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fixed IR release-progression corpus

The release-progression chart deliberately uses a pinned benchmark cohort, not the functions enumerated by each xlsynth release. Its canonical manifest is `src/site_assets/release_progression_ir_hashes.txt`: 187 sorted, unique whole-function structural hashes. `src/site.rs` pins both the count and the domain-separated manifest digest, so changing the cohort is an explicit versioned decision.

## Origin

The cohort was captured on 2026-09-03 from a production static-site snapshot whose `catalog.json` SHA-256 is `5a99a8efcc222687995a12ef0fe1a7f70dfd5ab87812b3c895df3c5943aaeeb2`. The snapshot's `ir-fn-corpus-ir.v1.json` descriptor SHA-256 is `d08e478594386e648347757ac9bef9d5d652d71a9752c479848b54ebc3bc0aba`; its paired `ir-fn-corpus-g8r-abc-vs-codegen-yosys-abc.v1.json` descriptor SHA-256 is `cd778dec73387f41ddf137616a3ad2fced7290f488fb86a215b93a3633e59246`.

It is the common set of exact whole-function structural hashes with paired G8r and Yosys/ABC measurements in both of these complete generations:

- xlsynth crate `0.66.0`, DSO `0.54.7`: 187 artifacts
- xlsynth crate `0.68.0`, DSO `0.54.7`: 187 artifacts

The two sets were identical. Requiring each indexed IR action to equal its source IR action excludes generated k3 cones and MFFCs. The corresponding canonical IR packages were materialized one function per file, named `<structural-hash>.ir`, and are the inputs to historical backtests. The checked-in manifest's raw SHA-256 is `bd6a384406f764baebe95089b792fe483ef84f0a8eda3ee8ea8047767a77ce38`; its code-pinned domain-separated digest is `a70a2e38b978d07b8bfc642f7a7cd6806a35bfa4de52f8c9919cd880057e2f77`.

## Reproducing the manifest

Given the verified snapshot site directory, this command reproduces the manifest byte-for-byte:

```bash
snapshot_site=/path/to/verified-snapshot/site
manifest_out=/tmp/release_progression_ir_hashes.txt
jq -s -r '
  [.[].entries[]
   | select(.crate_version == "0.66.0" or .crate_version == "0.68.0")
   | select(.g8r.source_structural_hash == .yosys_abc.source_structural_hash)
   | select(.g8r.ir_action_id == .g8r.source_ir_action_id)
   | select(.yosys_abc.ir_action_id == .yosys_abc.source_ir_action_id)
   | {crate_version, hash: .g8r.source_structural_hash}]
  | group_by(.hash)
  | map(select(([.[].crate_version] | unique | length) == 2))
  | .[] | .[0].hash
' "$snapshot_site"/data/ir-fn-corpus-ir.v1/by-hash-prefix/*.json \
  | sort -u > "$manifest_out"
test "$(wc -l < "$manifest_out")" -eq 187
sha256sum "$manifest_out"
```

This command then materializes the exact single-top packages from the `0.68.0` copy of each identical whole-function entry:

```bash
corpus_out=/tmp/release-progression-ir-corpus
mkdir -p "$corpus_out"
jq -s -r --rawfile manifest "$manifest_out" '
  ($manifest | split("\n") | map(select(length == 64))) as $wanted
  | [.[].entries[]
     | select(.crate_version == "0.68.0")
     | select(.g8r.ir_action_id == .g8r.source_ir_action_id)
     | select(.g8r.source_structural_hash as $hash | $wanted | index($hash))]
  | unique_by(.g8r.source_structural_hash)
  | .[]
  | {hash: .g8r.source_structural_hash, ir_text: .g8r.ir_text}
  | @base64
' "$snapshot_site"/data/ir-fn-corpus-ir.v1/by-hash-prefix/*.json \
  | while IFS= read -r row; do
      hash=$(printf '%s' "$row" | base64 --decode | jq -r .hash)
      ir_text=$(printf '%s' "$row" | base64 --decode | jq -r .ir_text)
      printf 'package fixed_%s\n\n%s\n' "$hash" "$ir_text" > "$corpus_out/$hash.ir"
    done
```

The materialized directory contains 187 files. Hashing each file with its hash-only filename in sorted order produces `983fab9ccbfc6d6cb3ce112215730203b0c7731514281ebd4610a61cd82fc6a6`. Run the `g8r-vs-yabc-aig-diff` corpus recipe against that directory for every historical crate/DSO pair.

The checked-in manifest is the benchmark identity. Do not replace it merely because a newer release adds or removes input functions; create and review a new cohort version when intentionally changing the benchmark.
