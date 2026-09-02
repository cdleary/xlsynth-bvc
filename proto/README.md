# xlsynth-bvc Protobuf Conventions

The schemas under this directory are the canonical model for Rust-side
xlsynth-bvc data. JSON is restricted to explicit external-tool, HTTP, CLI, and
static-publication boundaries.

## Package and file layout

- The initial package is `xlsynth.bvc.v1`.
- Files are grouped by domain rather than by storage tree.
- A package-version change is distinct from an operational record-version or
  action-identity-version change.
- Generated Rust is produced by the pinned `prost-build` and vendored
  `protoc` dependencies in `Cargo.lock`.

## Field evolution

- Never renumber or reuse a published field number.
- Mark removed field numbers and enum values `reserved`.
- Every enum defines an `UNSPECIFIED = 0` value.
- Validation rejects unspecified or unknown enum values when a concrete value
  is required.
- Use `optional` only when absence is semantically different from a scalar
  default.
- Older binaries must reject store formats or operational schema versions they
  cannot safely rewrite.

The initial deployment is a fresh protobuf-only store. Compatibility with the
pre-protobuf JSON store and its action IDs is intentionally not implemented.

## Validation

Generated protobuf structs may represent incomplete states. Every message that
enters action identity, sled, queue storage, or publication must pass an
explicit validator.

At the public snapshot/site/publication boundary, successful decoding is not
sufficient: snapshot manifests, site manifests, run/finding records, immutable
published catalogs, and current pointers must equal the canonical re-encoding
of the validated message. This prevents unknown or duplicate wire fields from
carrying unreviewed bytes into a statically served artifact.

Validators enforce at least:

- SHA-256 and action ID byte lengths
- normalized version spelling
- normalized relative paths
- required message presence
- non-empty semantic identifiers
- concrete, known enum values
- action-specific numeric constraints

Core logic should construct validated messages through helpers rather than
assembling generated structs ad hoc.

## Digests and identifiers

- Digests and action IDs are 32-byte protobuf `bytes` values.
- Lowercase hexadecimal is a display/boundary representation only.
- Hash inputs use domain separation.
- Wall-clock time, host paths, process IDs, and machine names do not enter
  content identities.
- An absent `ComboVerilogToYosysAbcAigAction.frontend` is the canonical
  builtin-Yosys encoding. Slang is explicit and carries its full source commit,
  which must match `YosysRuntimeSpec.slang_commit`; the runtime fingerprint and
  Docker build therefore bind the same revision that participates in the action
  ID and provenance.

Action identity has a separate, documented canonical encoding contract in
`docs/action-id-v2.md`.

## Versions

`DsoVersion.value` and `CrateVersion.value` contain normalized versions
without a leading `v`. Human-facing output adds explicit `dso:v...` or
`crate:v...` labels.

## Paths

`NormalizedRelpath.value`:

- is UTF-8
- uses `/` separators
- is relative
- is non-empty
- contains no empty, `.`, or `..` component
- has no leading or trailing slash

Absolute build-machine paths are operational data and must not enter action
identity or public provenance.

## Collections and maps

Identity messages do not use protobuf maps or floating-point values. Repeated
identity fields are sorted during normalization when their source order is not
semantic.

Non-identity schemas should still prefer explicitly ordered repeated entry
messages when stable ordering improves reproducibility. The code generator is
configured to use `BTreeMap` for any maps that are introduced.

## Dynamic data

Do not use `google.protobuf.Struct`, `google.protobuf.Any`, or an untyped
key/value bag for core action or provenance data. Use typed messages and
`oneof` fields.

## Boundary JSON

External JSON is parsed by narrow adapter modules and immediately converted to
protobuf. Static website JSON and CLI JSON are projections from protobuf
messages; they are not authoritative persisted models.

Browser catalogs and publication pointers are deny-unknown typed projections.
Their raw bytes must equal the canonical Rust encoding during verification;
semantic deserialization alone is insufficient.

## Regeneration and verification

A schema change must pass:

1. protobuf code generation
2. descriptor-set generation
3. Rust compile and round-trip tests
4. message validation tests
5. action-ID golden tests when identity messages are touched
6. a clean regeneration check in CI
