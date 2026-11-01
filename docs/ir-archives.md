<!-- SPDX-License-Identifier: Apache-2.0 -->

# Portable IR corpus archives

`build-static-site` automatically emits a human-readable `ir/` download page,
the machine-readable `ir/index.pb`, and exactly 256
`ir/00.tar.zst` through `ir/ff.tar.zst` archives for a complete corpus site or
all-versions dashboard. The dashboard's root download combines full-corpus
inputs with whole-function and MFFC roots from its admitted, typed progression
run evidence. A standalone corpus site's download covers its full-corpus inputs.
The embedded `corpus/` site retains its
independently verified full-corpus-only download.

The dashboard and corpus explorer link to the download page. It shows the roots
per corpus, deduplicated IR-file count, compressed download size, import instructions,
and individual shard links with sizes and SHA-256 checksums. The archive list is
static HTML and works without JavaScript; a small script fills the current site
root into the example import command. The protobuf index remains directly
downloadable from the page. Both page metadata and archive bytes are verified
against the same canonical index, including for the embedded corpus-only download.

Each exact IR byte string is stored once per download, regardless of how many
releases, top functions, or populations reference it. The bucket is the first
two hexadecimal characters of the IR's **content SHA-256**, not its structural
hash or generating action ID. Tar members are named `<content-sha256>.ir`.
The protobuf index records pack lengths/checksums, individual IR lengths/hashes,
and each root's corpus, portable relative path, and selected top function.

Archives have sorted members, zero timestamps and ownership IDs, and fixed
permissions. They do not contain machine paths, original execution commands,
hostnames, or evaluation stores. Empty buckets are included. Builds fail if an
archive exceeds the 25 MiB hosting limit; there is no automatic subdivision.
Expanded buckets are bounded at 256 MiB and the total reconstructed input files
at 1 GiB for safe imports, including repeated references to shared blobs.

## Download and seed a cache

Use an immutable published site URL, or a newly generated local site:

```sh
cargo run --release --bin xlsynth_bvc -- \
  --store-dir ../ir-cache/artifacts --artifacts-via-sled ../ir-cache/artifacts.sled \
  import-ir-archives \
  --url https://bvc.xlsynth.org/sites/SITE_ID/ \
  --out-dir ../downloaded-corpus

# Alternatively, replace --url with --site-dir ../rendered-site.
```

The destination must not already exist or overlap the source site, checkout,
store, or artifact database. All archives and roots are validated before any
IR import actions are created. Imports reject mismatched lengths/checksums,
unknown protobuf fields, unsafe paths, links, extra tar members, undeclared
functions, and oversized content. Downloads have bounded reads and timeouts;
HTTP redirects are not followed. A failed import never replaces an existing
output directory. Successfully committed IR roots remain reusable if a later
local write fails; retry into a new destination to reuse those cache entries.

The output retains `ir/` downloads and reconstructs
`inputs/<corpus>/<original-relative-path>`. For example, evaluate the frozen
full corpus with `run-ir-dir-corpus --input-dir ../downloaded-corpus/inputs/full-corpus`
and the usual explicit tool versions. Fixed cohorts use
`inputs/whole-functions-v1` or `inputs/mffc-v1` and their registered scheduling
policies. Only roots actually present in the publication are included; check
the pinned population identity before treating a directory as cohort-complete.

`roots.json` is an operator-facing projection listing every root and its
`import_action_id`, for commands such as `run ir-to-g8r-aig --ir-action-id ID`.
Use the same `--store-dir` and `--artifacts-via-sled` when running these actions.
The retained protobuf index is canonical; the JSON listing is not imported or
used as evidence. `run-ir-dir-corpus` instead owns a separate store under its
output workspace: it can replay the downloaded input files, but does not
implicitly share the cache seeded by this command.

Cache seeding uses the existing canonical `ImportIrPackageFile` action. Exact IR
bytes and top function reproduce that root identity, so downstream action IDs
can match when tool/runtime/options also match. No original crate-generation
pipeline is required. Imported IR alone does **not** claim g8r, ABC, or any
original DSLX-to-IR action has completed. This does not download evaluated
results or synthesize anything.

## Publication integrity

Archive schemas are compiled separately from the evaluation-store descriptor;
this feature does not migrate or restart evaluation stores. The site verifier
reconstructs both the index and deterministic packs from the same validated
publication inputs as the charts. Editing an archive and updating its file
checksum is insufficient to make an altered publication pass verification.
