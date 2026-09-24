# Repository guidance

## Data boundaries

Use typed protobuf messages as the canonical persisted and cross-stage representation whenever a
schema exists or a new durable pipeline boundary is introduced. JSON, JSONL, and CSV are allowed
for external-tool inputs, CLI/HTTP adapters, operational corpus exports, and the final static web
projection; they are not internal sources of truth. Validate boundary JSON and convert it to a
protobuf before passing data to another stage. Do not hand-merge or hand-edit JSON artifacts to
move results between computation and publication.

## Repository hygiene

Do not commit operator names, hostnames, machine-specific hardware details, or absolute paths
derived from an individual's environment, including captured home, worktree, and ephemeral
temporary-directory paths. Keep host-specific tuning in local configuration or operator-local
skills. In documentation and examples, use repository-relative paths, named inputs, or clearly
portable placeholders. Deliberately synthetic paths used by leak-prevention tests are allowed when
they are clearly fictional and remain test-only.
