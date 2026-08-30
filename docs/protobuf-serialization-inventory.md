# Protobuf Serialization Inventory

Status: Milestone 0 inventory, 2026-08-28.

This document assigns every current serde/JSON family to a protobuf target or an
explicit JSON boundary. The goal is not to remove JSON support from external
interfaces; it is to prevent JSON-shaped values and duplicate serde domain
models from becoming canonical inside Rust.

## Canonical core records

| Current location and types | Current encoding/use | Protobuf target | Milestone |
| --- | --- | --- | --- |
| `model.rs`: `DriverRuntimeSpec`, `YosysRuntimeSpec`, `G8rLoweringMode`, `ArtifactType`, `ArtifactRef`, `ScriptRef` | embedded in action/provenance JSON | `common.proto` | 1 |
| `model.rs`: `ActionSpec` | action fingerprint and all queue/provenance JSON | `action.proto` | 1 |
| `model.rs`: `OutputFile`, `CommandTrace`, `Provenance`, `SuggestedAction`, `ActionOutcome`, `DslxFnDiscovery`, `DslxImportContext` | sled/materialized JSON and in-memory dynamic details | `provenance.proto` with typed action-detail oneofs | 2 |
| `model.rs`: `QueueItem`, `QueueRunning`, `QueueDone`, `QueueFailed`, `QueueCanceled` | queue filesystem JSON and sled failures | `queue.proto` | 2 |
| `service/runtime_docker.rs`: release-cache manifest and setup lock | cache-control protobuf files | `common.proto`: `DriverReleaseCacheManifest` and `DriverReleaseCacheSetupLock` | 2 |
| `model.rs`: k-bool and MFFC corpus manifests/entries | generated action payload JSON | `corpus.proto` | 3 |
| `model.rs`: structural manifest/group/member/origin | sled web-index JSON | `structural_index.proto` | 3 |
| `corpus.rs`: IR-directory manifest, sample record, joined row, summary | JSON/JSONL is both resume state and public export | `corpus.proto`; JSON/JSONL/CSV become projections | 3 |
| `snapshot.rs`: static snapshot manifest and dataset entry | JSON snapshot authority | `publication.proto`; JSON becomes projection | 5 |

## Derived datasets and reports

| Current location and types | Current encoding/use | Protobuf target | Milestone |
| --- | --- | --- | --- |
| `view.rs`: version cards, failure rows, stdlib trends/timelines, comparison samples, graph nodes/edges, structural rows | web-index JSON or HTML input | `web_dataset.proto` | 3 |
| `query.rs`: corpus delta/index files, version summary index, trend/comparison/timeline/graph index files and states | sled `web_index_bytes` JSON | `web_dataset.proto` | 3 |
| `query/corpus_structural.rs`: archive cache metadata | web-index JSON | `structural_index.proto` / `publication.proto` | 3/5 |
| `model.rs`: enqueue, audit, discovery, structural population/freshness, and query summaries | Rust serde values printed by CLI | `operation_report.proto` | 3/4 |
| `store.rs`, `queue.rs`, `ops.rs`, `sled_space.rs`: compaction, compression, pruning, repair, worker, and size reports | CLI JSON | `operation_report.proto` | 3/4 |
| campaign/run completion and analysis data not yet present | none | `campaign.proto`, `run.proto`, `analysis.proto` | 4 |

## JSON ingress adapters that remain

These formats are owned by another tool/service or are explicit public request
boundaries. Their serde types should live in or move toward narrow adapter
modules.

| Current location | Input | Eventual adapter |
| --- | --- | --- |
| `versioning.rs` | upstream `generated_version_compat.json` and GitHub release API | `adapters/version_compat_json.rs`, `adapters/github_json.rs` |
| `model.rs`: `VersionCompatEntry`, `GithubRelease`, `GithubReleaseAsset` | external HTTP JSON | same adapters |
| `executor.rs`: DSLX function discovery JSONL | `xlsynth-driver dslx-list-fns` | `adapters/driver_json.rs` |
| `executor.rs`: raw bool-cone and MFFC manifest rows | `xlsynth-driver` JSONL | `adapters/driver_json.rs` |
| `executor.rs` / `service/core.rs`: structural-hash response | `xlsynth-driver` JSON | `adapters/driver_json.rs` |
| `executor.rs`, `query.rs`, `corpus.rs`, `service/structural_index.rs`: AIG stats JSON | driver-produced evidence artifact | `adapters/driver_json.rs`; convert parsed metrics to protobuf |
| `service/runtime_docker.rs` and `scripts/persistent_runner_worker.py` | private host/Python runner request, result, heartbeat, and capability JSON | subprocess boundary; schema-versioned and identity-checked JSON remains |
| `web/types.rs` | Axum query/form extraction | HTTP boundary; serde remains |
| `web/routes_api.rs` | JSON-RPC requests and responses | HTTP boundary; serde remains until the static site removes runtime API dependence |

Raw JSON evidence produced by external tools may remain an action output.
Queries and Rust transformations must consume a validated protobuf
interpretation rather than passing around `serde_json::Value`. Where useful,
an action emits both the untouched external evidence and a canonical protobuf
companion artifact.

## JSON/publication egress that remains

| Current location | Output | Rule |
| --- | --- | --- |
| `app.rs` | CLI JSON summaries | serialize/project from protobuf report messages |
| `corpus.rs` | public manifest JSON, samples/joined JSONL, CSV | generate from protobuf corpus messages |
| `web/render.rs` | Plotly/browser data embedded in HTML | publication boundary; generate from protobuf web datasets |
| `web/routes_api.rs` | live JSON-RPC | boundary only; static site must not depend on it |
| `snapshot.rs` | browser/public manifest JSON | projection from canonical publication protobuf |
| future `publication/json.rs` | sharded static web JSON | the only authoritative web JSON projection layer |

## Dynamic `serde_json::Value` removal targets

The following are not approved long-term core uses:

- `Provenance.details`
- executor/service `details` maps
- structural-index extraction from arbitrary provenance JSON
- generic numeric-leaf traversal used as an internal metric model
- Rust-generated AIG-stat-diff JSON as the only canonical output
- JSON fingerprints for actions, structural manifests, or snapshots

Typed protobuf action-detail messages replace provenance maps. Typed metric and
finding messages replace numeric-leaf walking. Domain-separated normalized
protobuf replaces JSON fingerprints.

## Approved end-state serde/JSON zones

After Milestones 2 and 3, production `serde_json` imports should be limited to:

- `src/adapters/*_json.rs`
- `src/publication/json.rs`
- HTTP request/response boundary modules under `src/web/`
- CLI projection code
- focused tests and fixtures

A repository check will enforce this allowlist after the core and derived
conversions are complete.

## Immediate ownership

- Milestone 1 owns `common.proto`, `action.proto`, validation, and action ID.
- Milestone 2 owns `provenance.proto`, `queue.proto`, runner/runtime records,
  sled values, and queue files.
- Milestone 3 owns corpus, structural, web-dataset, and operation-report
  messages.
- Milestones 4-6 own campaign/run/analysis/publication messages and JSON
  projections.
