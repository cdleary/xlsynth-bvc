# Action ID V2 Canonical Encoding

## Purpose

Action IDs identify the complete semantic input to an xlsynth-bvc action. The
protobuf deployment intentionally does not preserve the earlier JSON-derived
IDs.

## Definition

For a validated and normalized protobuf `ActionSpec`:

```text
fingerprint = ActionFingerprint {
  identity_schema_version: 2,
  action: normalized_action_spec,
}

action_id_bytes =
  SHA256(
    UTF8("xlsynth-bvc/action/v2\\0") ||
    PROST_ENCODE(fingerprint)
  )
```

The `\\0` shown above is one zero byte, not two display characters. The
lowercase 64-character hexadecimal action ID is a boundary rendering of the
32-byte digest.

## Canonicalization contract

Before encoding:

1. Strip one leading `v` from DSO and crate versions.
2. Validate the remaining version spelling.
3. Normalize repository-relative paths to `/` separators.
4. Reject absolute paths, empty components, `.`, and `..`.
5. Reject empty identifiers such as function names, platform names, image
   references, delay models, and output formats.
6. Decode hexadecimal input digests/action IDs to exactly 32 bytes.
7. Map every semantic enum stored on the wire to a concrete nonzero protobuf
   value. The absent `ComboVerilogToYosysAbcAig.frontend` submessage is the
   one canonical exception: it preserves the historical builtin frontend
   encoding; slang uses an explicit enum and full source revision.
8. Reject an absent action `oneof` or absent required submessage.
9. Require driver runtimes to carry both a 32-byte immutable OCI image ID and
   the 32-byte digest of their canonical release-cache input manifest.
10. Sort repeated fields when their order is declared non-semantic. The initial
   action schema contains no repeated identity fields.
11. Do not include timestamps, output data, host paths, queue priority, worker
    identity, or publication metadata.

The normalized message is constructed from scratch. Unknown wire fields from an
input message are not carried into identity.

## Encoding constraints

Identity messages:

- contain no protobuf maps
- contain no floating-point fields
- do not use `Any` or `Struct`
- depend on presence only where absence is semantic
- use stable field numbers and oneof tags

`PROST_ENCODE` means `prost::Message::encode_to_vec` using the generated
types and dependency versions pinned by `Cargo.lock`. Protocol Buffers does
not promise a universal canonical byte encoding across arbitrary
implementations, so xlsynth-bvc pins this narrower implementation contract and
protects it with golden byte vectors.

## Change policy

A change may retain action-ID V2 only if all existing golden protobuf bytes and
digests remain unchanged. Adding a new action oneof variant or a new optional
field that is absent in existing actions may retain V2 after golden tests pass.

Any intentional byte change for an existing semantic action requires:

- a new domain string such as `xlsynth-bvc/action/v3\\0`
- a new identity schema version
- new golden vectors
- a clean new action lineage

Before the initial protobuf deployment, this repository may intentionally update
the V2 schema and golden vectors because no compatible V2 store exists. Once a
V2 store is deployed, the version-bump rule above applies.

The initial deployment is on a fresh store, so there is no V1 migration or
fallback path.

## Golden tests

Tests must include:

- at least one golden vector for every action kind
- exact encoded `ActionFingerprint` bytes
- exact SHA-256 action ID
- field-mutation sensitivity for every semantic field
- normalization equivalence where multiple accepted input spellings normalize
  to the same value
- rejection of malformed digests, versions, paths, enum values, and missing
  submessages
