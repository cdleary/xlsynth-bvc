<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fixed IR release-progression corpus

The release-progression chart deliberately uses a pinned benchmark cohort, not the functions enumerated by each xlsynth release. Its canonical manifest is `src/site_assets/release_progression_ir_hashes.txt`: 187 sorted, unique whole-function structural hashes. `src/site.rs` pins both the count and the domain-separated manifest digest, so changing the cohort is an explicit versioned decision.

## Origin

The cohort was captured on 2026-09-03 from the production structural IR corpus snapshot. It is the common set of exact whole-function structural hashes with paired G8r and Yosys/ABC measurements in both of these complete generations:

- xlsynth crate `0.66.0`, DSO `0.54.7`: 187 artifacts
- xlsynth crate `0.68.0`, DSO `0.54.7`: 187 artifacts

The two sets were identical. Generated k3 cones and MFFCs were excluded. The corresponding canonical IR packages were materialized one function per file, named `<structural-hash>.ir`, and are the inputs to historical backtests.

## Reproducing the manifest

Use the paired whole-function dataset and IR-text index from the same publication snapshot:

1. Select samples from the two source generations above.
2. Exclude tops beginning with `__k3_cone_` or `__mffc_`.
3. Take the intersection of their `structural_hash` fields, lowercase it, sort it, and require exactly 187 unique SHA-256 values.
4. Resolve each hash through the snapshot IR-text index and materialize one single-top package named `<hash>.ir`.
5. Run the `g8r-vs-yabc-aig-diff` corpus recipe against that directory for every historical crate/DSO pair.

The checked-in manifest is the benchmark identity. Do not replace it merely because a newer release adds or removes input functions; create and review a new cohort version when intentionally changing the benchmark.
