// SPDX-License-Identifier: Apache-2.0

use super::*;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ComparisonShardKind {
    Samples,
    G8rPoints,
    YosysPoints,
}

fn comparison_source_schema(index_key: &str) -> Option<u32> {
    match index_key {
        crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME => {
            Some(crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION)
        }
        crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME => {
            Some(crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_SCHEMA_VERSION)
        }
        _ => None,
    }
}

fn comparison_shard_kind_slug(kind: ComparisonShardKind) -> &'static str {
    match kind {
        ComparisonShardKind::Samples => "samples-by-hash-prefix",
        ComparisonShardKind::G8rPoints => "g8r-points-by-hash-prefix",
        ComparisonShardKind::YosysPoints => "yosys-points-by-hash-prefix",
    }
}

fn comparison_shard_key(
    source_index_key: &str,
    kind: ComparisonShardKind,
    prefix: &str,
) -> Result<String> {
    let stem = source_index_key
        .strip_suffix(".json")
        .with_context(|| format!("comparison source key lacks .json suffix: {source_index_key}"))?;
    Ok(format!(
        "{stem}/{}/{prefix}.json",
        comparison_shard_kind_slug(kind)
    ))
}

fn comparison_shard_key_parts(
    index_key: &str,
) -> Option<(&'static str, ComparisonShardKind, &str)> {
    for source_key in [
        crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
        crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
    ] {
        let stem = source_key.strip_suffix(".json").expect("constant is JSON");
        for kind in [
            ComparisonShardKind::Samples,
            ComparisonShardKind::G8rPoints,
            ComparisonShardKind::YosysPoints,
        ] {
            let namespace = format!("{stem}/{}/", comparison_shard_kind_slug(kind));
            let Some(prefix) = index_key
                .strip_prefix(&namespace)
                .and_then(|suffix| suffix.strip_suffix(".json"))
            else {
                continue;
            };
            if is_lower_hex(prefix, STATIC_COMPARISON_SHARD_PREFIX_HEX_CHARS as usize) {
                return Some((source_key, kind, prefix));
            }
        }
    }
    None
}

fn structural_shard_key(prefix: &str) -> String {
    format!("{STATIC_STRUCTURAL_SHARD_NAMESPACE}/{prefix}.json")
}

fn is_structural_manifest_source(index_key: &str) -> bool {
    index_key == crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY
}

fn structural_shard_prefix(index_key: &str) -> Option<&str> {
    let prefix = index_key
        .strip_prefix(STATIC_STRUCTURAL_SHARD_NAMESPACE)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .and_then(|suffix| suffix.strip_suffix(".json"))?;
    is_lower_hex(prefix, STATIC_STRUCTURAL_SHARD_PREFIX_HEX_CHARS as usize).then_some(prefix)
}

fn structural_group_hash(index_key: &str) -> Option<&str> {
    let namespace = format!(
        "{}/by-hash/",
        crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE
    );
    let suffix = index_key.strip_prefix(&namespace)?;
    let mut parts = suffix.split('/');
    let (Some(first), Some(second), Some(filename), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    let hash = filename.strip_suffix(".json")?;
    (is_lower_hex(hash, 64) && first == &hash[..2] && second == &hash[2..4]).then_some(hash)
}

fn structural_group_key(hash: &str) -> String {
    format!(
        "{}/{}",
        crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE,
        crate::service::hash_group_relpath(hash)
    )
}

fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hash_prefix<'a>(hash: &'a str, chars: u8, field: &str) -> Result<&'a str> {
    let chars = chars as usize;
    if !is_lower_hex(hash, 64) {
        bail!("{field} is not a canonical lowercase SHA-256 digest: {hash}");
    }
    Ok(&hash[..chars])
}

fn sample_hash(sample: &StdlibG8rVsYosysSample) -> &str {
    sample
        .structural_hash
        .as_deref()
        .unwrap_or(&sample.ir_action_id)
}

fn write_browser_dataset(
    out_dir: &Path,
    logical_key: &str,
    bytes: &[u8],
) -> Result<BrowserDataset> {
    let url = format!("data/{logical_key}");
    write_file(out_dir, &url, bytes)?;
    Ok(BrowserDataset {
        logical_key: logical_key.to_string(),
        url,
        bytes: bytes.len() as u64,
        sha256: sha256_hex(bytes),
    })
}

fn read_snapshot_json(
    snapshot_dir: &Path,
    entry: &crate::snapshot::StaticSnapshotDatasetFile,
) -> Result<Vec<u8>> {
    let source = snapshot_dir.join(&entry.relpath);
    let bytes = fs::read(&source)
        .with_context(|| format!("reading snapshot dataset: {}", source.display()))?;
    if bytes.len() as u64 != entry.bytes || sha256_hex(&bytes) != entry.sha256 {
        bail!(
            "snapshot dataset changed after verification: {}",
            entry.relpath
        );
    }
    Ok(bytes)
}

fn summary_for_dataset(
    prefix: String,
    dataset: &BrowserDataset,
    row_count: usize,
) -> StaticComparisonShardSummary {
    StaticComparisonShardSummary {
        prefix,
        index_key: dataset.logical_key.clone(),
        row_count,
        bytes: dataset.bytes,
        sha256: dataset.sha256.clone(),
    }
}

fn build_comparison_datasets(
    snapshot_dir: &Path,
    out_dir: &Path,
    entry: &crate::snapshot::StaticSnapshotDatasetFile,
) -> Result<Vec<BrowserDataset>> {
    let source_bytes = read_snapshot_json(snapshot_dir, entry)?;
    let mut source: ComparisonSourceIndex = serde_json::from_slice(&source_bytes)
        .with_context(|| format!("decoding comparison source {}", entry.index_key))?;
    let expected_schema = comparison_source_schema(&entry.index_key)
        .with_context(|| format!("unsupported comparison source key: {}", entry.index_key))?;
    if source.schema_version != expected_schema {
        bail!(
            "comparison source schema mismatch for {}: expected={} actual={}",
            entry.index_key,
            expected_schema,
            source.schema_version
        );
    }
    let canonical_source =
        serde_json::to_vec(&source).context("canonicalizing comparison source")?;
    if canonical_source != source_bytes {
        bail!(
            "comparison source representation is not exactly understood: {}",
            entry.index_key
        );
    }

    let samples = std::mem::take(&mut source.dataset.samples);
    let sample_count = samples.len();
    let mut sample_groups: BTreeMap<String, Vec<StaticComparisonSampleRow>> = BTreeMap::new();
    for (source_ordinal, sample) in samples.into_iter().enumerate() {
        let prefix = hash_prefix(
            sample_hash(&sample),
            STATIC_COMPARISON_SHARD_PREFIX_HEX_CHARS,
            "comparison sample shard hash",
        )?;
        sample_groups
            .entry(prefix.to_string())
            .or_default()
            .push(StaticComparisonSampleRow {
                source_ordinal,
                sample,
            });
    }

    let mut datasets = Vec::new();
    let mut sample_shards = Vec::new();
    for (prefix, rows) in sample_groups {
        let row_count = rows.len();
        let key = comparison_shard_key(&entry.index_key, ComparisonShardKind::Samples, &prefix)?;
        let bytes = serde_json::to_vec(&StaticComparisonSampleShard {
            schema_version: STATIC_COMPARISON_SHARD_SCHEMA_VERSION,
            prefix: prefix.clone(),
            rows,
        })
        .context("serializing static comparison sample shard")?;
        let dataset = write_browser_dataset(out_dir, &key, &bytes)?;
        sample_shards.push(summary_for_dataset(prefix, &dataset, row_count));
        datasets.push(dataset);
    }

    let mut g8r_groups: BTreeMap<String, Vec<StaticComparisonEntityRow>> = BTreeMap::new();
    for (source_ordinal, entity) in source.g8r_points.iter().cloned().enumerate() {
        let prefix = hash_prefix(
            &entity.structural_hash,
            STATIC_COMPARISON_SHARD_PREFIX_HEX_CHARS,
            "G8r entity shard hash",
        )?;
        g8r_groups
            .entry(prefix.to_string())
            .or_default()
            .push(StaticComparisonEntityRow {
                source_ordinal,
                entity,
            });
    }
    let mut g8r_point_shards = Vec::new();
    for (prefix, rows) in g8r_groups {
        let row_count = rows.len();
        let key = comparison_shard_key(&entry.index_key, ComparisonShardKind::G8rPoints, &prefix)?;
        let bytes = serde_json::to_vec(&StaticComparisonEntityShard {
            schema_version: STATIC_COMPARISON_SHARD_SCHEMA_VERSION,
            prefix: prefix.clone(),
            rows,
        })
        .context("serializing static comparison G8r point shard")?;
        let dataset = write_browser_dataset(out_dir, &key, &bytes)?;
        g8r_point_shards.push(summary_for_dataset(prefix, &dataset, row_count));
        datasets.push(dataset);
    }

    let mut yosys_groups: BTreeMap<String, Vec<StaticComparisonEntityRow>> = BTreeMap::new();
    for (source_ordinal, entity) in source.yosys_points.iter().cloned().enumerate() {
        let prefix = hash_prefix(
            &entity.structural_hash,
            STATIC_COMPARISON_SHARD_PREFIX_HEX_CHARS,
            "Yosys entity shard hash",
        )?;
        yosys_groups
            .entry(prefix.to_string())
            .or_default()
            .push(StaticComparisonEntityRow {
                source_ordinal,
                entity,
            });
    }
    let mut yosys_point_shards = Vec::new();
    for (prefix, rows) in yosys_groups {
        let row_count = rows.len();
        let key =
            comparison_shard_key(&entry.index_key, ComparisonShardKind::YosysPoints, &prefix)?;
        let bytes = serde_json::to_vec(&StaticComparisonEntityShard {
            schema_version: STATIC_COMPARISON_SHARD_SCHEMA_VERSION,
            prefix: prefix.clone(),
            rows,
        })
        .context("serializing static comparison Yosys point shard")?;
        let dataset = write_browser_dataset(out_dir, &key, &bytes)?;
        yosys_point_shards.push(summary_for_dataset(prefix, &dataset, row_count));
        datasets.push(dataset);
    }

    let manifest = StaticComparisonManifest {
        schema_version: STATIC_COMPARISON_SHARD_SCHEMA_VERSION,
        generated_utc: source.generated_utc,
        source: StaticComparisonSource {
            logical_key: entry.index_key.clone(),
            schema_version: source.schema_version,
            bytes: entry.bytes,
            sha256: entry.sha256.clone(),
        },
        shard_prefix_hex_chars: STATIC_COMPARISON_SHARD_PREFIX_HEX_CHARS,
        sample_count,
        dataset: source.dataset,
        sample_shards,
        g8r_point_shards,
        yosys_point_shards,
    };
    let manifest_bytes =
        serde_json::to_vec(&manifest).context("serializing static comparison manifest")?;
    datasets.push(write_browser_dataset(
        out_dir,
        &entry.index_key,
        &manifest_bytes,
    )?);
    Ok(datasets)
}

fn build_structural_datasets(
    snapshot_dir: &Path,
    out_dir: &Path,
    entries: &[crate::snapshot::StaticSnapshotDatasetFile],
) -> Result<Vec<BrowserDataset>> {
    let Some(source_manifest_entry) = entries
        .iter()
        .find(|entry| is_structural_manifest_source(&entry.index_key))
    else {
        if entries
            .iter()
            .any(|entry| structural_group_hash(&entry.index_key).is_some())
        {
            bail!("structural groups exist without their source manifest");
        }
        return Ok(Vec::new());
    };
    let source_manifest_bytes = read_snapshot_json(snapshot_dir, source_manifest_entry)?;
    let source_manifest: crate::model::IrFnCorpusStructuralManifest =
        serde_json::from_slice(&source_manifest_bytes)
            .context("decoding structural source manifest")?;
    let canonical_source = crate::query::canonicalize_public_web_index_json(
        &source_manifest_entry.index_key,
        &source_manifest_bytes,
    )?;
    if canonical_source != source_manifest_bytes {
        bail!("structural source manifest is not canonical");
    }

    let mut groups_by_prefix: BTreeMap<String, Vec<crate::model::IrFnCorpusStructuralGroupFile>> =
        BTreeMap::new();
    for entry in entries {
        let Some(hash) = structural_group_hash(&entry.index_key) else {
            continue;
        };
        let bytes = read_snapshot_json(snapshot_dir, entry)?;
        let group: crate::model::IrFnCorpusStructuralGroupFile = serde_json::from_slice(&bytes)
            .with_context(|| format!("decoding structural group {}", entry.index_key))?;
        if group.structural_hash != hash || serde_json::to_vec_pretty(&group)? != bytes {
            bail!(
                "structural group source is not canonical for {}",
                entry.index_key
            );
        }
        groups_by_prefix
            .entry(hash[..STATIC_STRUCTURAL_SHARD_PREFIX_HEX_CHARS as usize].to_string())
            .or_default()
            .push(group);
    }

    let mut datasets = Vec::new();
    let mut shard_summaries = Vec::new();
    for (prefix, mut groups) in groups_by_prefix {
        groups.sort_by(|left, right| left.structural_hash.cmp(&right.structural_hash));
        let group_count = groups.len();
        let member_count = groups.iter().map(|group| group.members.len()).sum();
        let key = structural_shard_key(&prefix);
        let bytes = serde_json::to_vec(&StaticStructuralShard {
            schema_version: STATIC_STRUCTURAL_SHARD_SCHEMA_VERSION,
            prefix: prefix.clone(),
            groups,
        })
        .context("serializing static structural shard")?;
        let dataset = write_browser_dataset(out_dir, &key, &bytes)?;
        shard_summaries.push(StaticStructuralShardSummary {
            prefix,
            index_key: key,
            group_count,
            member_count,
            bytes: dataset.bytes,
            sha256: dataset.sha256.clone(),
        });
        datasets.push(dataset);
    }
    let static_manifest = StaticStructuralManifest {
        schema_version: STATIC_STRUCTURAL_SHARD_SCHEMA_VERSION,
        source: StaticStructuralSource {
            logical_key: source_manifest_entry.index_key.clone(),
            bytes: source_manifest_entry.bytes,
            sha256: source_manifest_entry.sha256.clone(),
            manifest: source_manifest,
        },
        shard_prefix_hex_chars: STATIC_STRUCTURAL_SHARD_PREFIX_HEX_CHARS,
        shards: shard_summaries,
    };
    let manifest_bytes =
        serde_json::to_vec(&static_manifest).context("serializing static structural manifest")?;
    datasets.push(write_browser_dataset(
        out_dir,
        &source_manifest_entry.index_key,
        &manifest_bytes,
    )?);
    Ok(datasets)
}

pub(super) fn build_static_site_datasets(
    snapshot_dir: &Path,
    out_dir: &Path,
    snapshot: &crate::snapshot::StaticSnapshotManifest,
) -> Result<Vec<BrowserDataset>> {
    let mut datasets = Vec::new();
    for entry in &snapshot.dataset_files {
        if !entry.relpath.ends_with(".json") {
            continue;
        }
        if comparison_source_schema(&entry.index_key).is_some()
            || is_structural_manifest_source(&entry.index_key)
            || structural_group_hash(&entry.index_key).is_some()
        {
            continue;
        }
        let bytes = read_snapshot_json(snapshot_dir, entry)?;
        datasets.push(write_browser_dataset(out_dir, &entry.index_key, &bytes)?);
    }
    for entry in &snapshot.dataset_files {
        if comparison_source_schema(&entry.index_key).is_some() {
            datasets.extend(build_comparison_datasets(snapshot_dir, out_dir, entry)?);
        }
    }
    datasets.extend(build_structural_datasets(
        snapshot_dir,
        out_dir,
        &snapshot.dataset_files,
    )?);
    datasets.sort_by(|left, right| left.logical_key.cmp(&right.logical_key));
    Ok(datasets)
}

fn canonicalize_typed<T>(bytes: &[u8], validate: impl FnOnce(&T) -> Result<()>) -> Result<Vec<u8>>
where
    T: DeserializeOwned + Serialize,
{
    let value: serde_json::Value = serde_json::from_slice(bytes).context("decoding site JSON")?;
    let typed: T = serde_json::from_value(value.clone()).context("decoding typed site JSON")?;
    let projected = serde_json::to_value(&typed).context("projecting typed site JSON")?;
    if projected != value {
        bail!("site JSON contains values outside its typed schema");
    }
    validate(&typed)?;
    serde_json::to_vec(&typed).context("canonicalizing typed site JSON")
}

fn validate_digest(field: &str, value: &str) -> Result<()> {
    if !is_lower_hex(value, 64) {
        bail!("{field} is not a canonical SHA-256 digest");
    }
    Ok(())
}

fn validate_shard_summaries(
    source_key: &str,
    kind: ComparisonShardKind,
    summaries: &[StaticComparisonShardSummary],
) -> Result<usize> {
    let mut previous = None;
    let mut total = 0_usize;
    for summary in summaries {
        if summary.row_count == 0
            || !is_lower_hex(
                &summary.prefix,
                STATIC_COMPARISON_SHARD_PREFIX_HEX_CHARS as usize,
            )
            || previous
                .as_ref()
                .is_some_and(|value| value >= &summary.prefix)
            || summary.index_key != comparison_shard_key(source_key, kind, &summary.prefix)?
        {
            bail!("comparison shard summary is invalid or unsorted");
        }
        validate_digest("comparison shard summary sha256", &summary.sha256)?;
        previous = Some(summary.prefix.clone());
        total += summary.row_count;
    }
    Ok(total)
}

fn validate_comparison_manifest(
    index_key: &str,
    manifest: &StaticComparisonManifest,
) -> Result<()> {
    let expected_source_schema = comparison_source_schema(index_key)
        .with_context(|| format!("not a comparison manifest key: {index_key}"))?;
    if manifest.schema_version != STATIC_COMPARISON_SHARD_SCHEMA_VERSION
        || manifest.shard_prefix_hex_chars != STATIC_COMPARISON_SHARD_PREFIX_HEX_CHARS
        || manifest.source.logical_key != index_key
        || manifest.source.schema_version != expected_source_schema
        || !manifest.dataset.samples.is_empty()
    {
        bail!("static comparison manifest header is invalid");
    }
    validate_digest("comparison source sha256", &manifest.source.sha256)?;
    crate::query::validate_comparison_dataset("static_comparison.dataset", &manifest.dataset)?;
    if validate_shard_summaries(
        index_key,
        ComparisonShardKind::Samples,
        &manifest.sample_shards,
    )? != manifest.sample_count
    {
        bail!("comparison manifest sample count does not match its shards");
    }
    validate_shard_summaries(
        index_key,
        ComparisonShardKind::G8rPoints,
        &manifest.g8r_point_shards,
    )?;
    validate_shard_summaries(
        index_key,
        ComparisonShardKind::YosysPoints,
        &manifest.yosys_point_shards,
    )?;
    Ok(())
}

fn validate_sample_shard(prefix: &str, shard: &StaticComparisonSampleShard) -> Result<()> {
    if shard.schema_version != STATIC_COMPARISON_SHARD_SCHEMA_VERSION
        || shard.prefix != prefix
        || shard.rows.is_empty()
    {
        bail!("static comparison sample shard header is invalid");
    }
    let mut ordinals = BTreeSet::new();
    for row in &shard.rows {
        if !ordinals.insert(row.source_ordinal)
            || hash_prefix(
                sample_hash(&row.sample),
                STATIC_COMPARISON_SHARD_PREFIX_HEX_CHARS,
                "comparison sample shard hash",
            )? != prefix
        {
            bail!("comparison sample shard row is duplicated or misplaced");
        }
        crate::query::validate_comparison_sample("static_comparison.sample", &row.sample)?;
    }
    Ok(())
}

fn validate_entity_shard(prefix: &str, shard: &StaticComparisonEntityShard) -> Result<()> {
    if shard.schema_version != STATIC_COMPARISON_SHARD_SCHEMA_VERSION
        || shard.prefix != prefix
        || shard.rows.is_empty()
    {
        bail!("static comparison entity shard header is invalid");
    }
    let mut ordinals = BTreeSet::new();
    for row in &shard.rows {
        if !ordinals.insert(row.source_ordinal)
            || hash_prefix(
                &row.entity.structural_hash,
                STATIC_COMPARISON_SHARD_PREFIX_HEX_CHARS,
                "comparison entity shard hash",
            )? != prefix
            || row.entity.crate_version != row.entity.point.crate_version
        {
            bail!("comparison entity shard row is duplicated, misplaced, or inconsistent");
        }
    }
    Ok(())
}

fn validate_structural_shard(prefix: &str, shard: &StaticStructuralShard) -> Result<()> {
    if shard.schema_version != STATIC_STRUCTURAL_SHARD_SCHEMA_VERSION
        || shard.prefix != prefix
        || shard.groups.is_empty()
    {
        bail!("static structural shard header is invalid");
    }
    let mut previous = None;
    for group in &shard.groups {
        if hash_prefix(
            &group.structural_hash,
            STATIC_STRUCTURAL_SHARD_PREFIX_HEX_CHARS,
            "structural group hash",
        )? != prefix
            || previous
                .as_ref()
                .is_some_and(|value| value >= &group.structural_hash)
        {
            bail!("static structural shard groups are misplaced or unsorted");
        }
        let group_key = structural_group_key(&group.structural_hash);
        let group_bytes = serde_json::to_vec_pretty(group)?;
        let canonical = crate::query::canonicalize_public_web_index_json(&group_key, &group_bytes)?;
        if canonical != group_bytes {
            bail!("static structural shard contains a noncanonical group");
        }
        previous = Some(group.structural_hash.clone());
    }
    Ok(())
}

fn validate_structural_manifest(manifest: &StaticStructuralManifest) -> Result<()> {
    if manifest.schema_version != STATIC_STRUCTURAL_SHARD_SCHEMA_VERSION
        || manifest.shard_prefix_hex_chars != STATIC_STRUCTURAL_SHARD_PREFIX_HEX_CHARS
        || !is_structural_manifest_source(&manifest.source.logical_key)
    {
        bail!("static structural manifest header is invalid");
    }
    validate_digest("structural source sha256", &manifest.source.sha256)?;
    let source_bytes = serde_json::to_vec_pretty(&manifest.source.manifest)
        .context("serializing structural source manifest")?;
    let canonical_source = crate::query::canonicalize_public_web_index_json(
        &manifest.source.logical_key,
        &source_bytes,
    )?;
    if canonical_source != source_bytes
        || source_bytes.len() as u64 != manifest.source.bytes
        || sha256_hex(&source_bytes) != manifest.source.sha256
    {
        bail!("static structural source commitment is invalid");
    }

    let mut expected_counts = BTreeMap::<String, (usize, usize)>::new();
    for group in &manifest.source.manifest.groups {
        let prefix = hash_prefix(
            &group.structural_hash,
            STATIC_STRUCTURAL_SHARD_PREFIX_HEX_CHARS,
            "structural manifest group hash",
        )?;
        let counts = expected_counts.entry(prefix.to_string()).or_default();
        counts.0 += 1;
        counts.1 += group.member_count;
    }
    if manifest.shards.len() != expected_counts.len() {
        bail!("static structural manifest shard count is invalid");
    }
    let mut previous = None;
    for summary in &manifest.shards {
        let expected = expected_counts
            .get(&summary.prefix)
            .with_context(|| format!("unexpected structural shard prefix {}", summary.prefix))?;
        if !is_lower_hex(
            &summary.prefix,
            STATIC_STRUCTURAL_SHARD_PREFIX_HEX_CHARS as usize,
        ) || previous
            .as_ref()
            .is_some_and(|value| value >= &summary.prefix)
            || summary.index_key != structural_shard_key(&summary.prefix)
            || (summary.group_count, summary.member_count) != *expected
            || summary.group_count == 0
        {
            bail!("static structural shard summary is invalid or unsorted");
        }
        validate_digest("structural shard summary sha256", &summary.sha256)?;
        previous = Some(summary.prefix.clone());
    }
    Ok(())
}

pub(super) fn should_include_static_site_dataset_key(index_key: &str) -> bool {
    should_include_snapshot_index_key(index_key)
        || comparison_shard_key_parts(index_key).is_some()
        || structural_shard_prefix(index_key).is_some()
}

pub(super) fn canonicalize_static_site_dataset_json(
    index_key: &str,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    if comparison_source_schema(index_key).is_some() {
        return canonicalize_typed::<StaticComparisonManifest>(bytes, |manifest| {
            validate_comparison_manifest(index_key, manifest)
        });
    }
    if let Some((_source_key, kind, prefix)) = comparison_shard_key_parts(index_key) {
        return match kind {
            ComparisonShardKind::Samples => {
                canonicalize_typed::<StaticComparisonSampleShard>(bytes, |shard| {
                    validate_sample_shard(prefix, shard)
                })
            }
            ComparisonShardKind::G8rPoints | ComparisonShardKind::YosysPoints => {
                canonicalize_typed::<StaticComparisonEntityShard>(bytes, |shard| {
                    validate_entity_shard(prefix, shard)
                })
            }
        };
    }
    if let Some(prefix) = structural_shard_prefix(index_key) {
        return canonicalize_typed::<StaticStructuralShard>(bytes, |shard| {
            validate_structural_shard(prefix, shard)
        });
    }
    if is_structural_manifest_source(index_key) {
        return canonicalize_typed::<StaticStructuralManifest>(bytes, |manifest| {
            validate_structural_manifest(manifest)
        });
    }
    crate::query::canonicalize_public_web_index_json(index_key, bytes)
}

fn catalog_dataset<'a>(
    catalog_by_key: &'a BTreeMap<&str, &BrowserDataset>,
    key: &str,
) -> Result<&'a BrowserDataset> {
    catalog_by_key
        .get(key)
        .copied()
        .with_context(|| format!("site catalog is missing projected dataset {key}"))
}

fn read_catalog_dataset(site_dir: &Path, dataset: &BrowserDataset) -> Result<Vec<u8>> {
    let bytes = fs::read(site_dir.join(&dataset.url))
        .with_context(|| format!("reading site dataset {}", dataset.logical_key))?;
    if bytes.len() as u64 != dataset.bytes || sha256_hex(&bytes) != dataset.sha256 {
        bail!("site catalog metadata mismatch for {}", dataset.logical_key);
    }
    Ok(bytes)
}

fn rows_in_source_order<T>(rows: BTreeMap<usize, T>, field: &str) -> Result<Vec<T>> {
    for (expected, actual) in rows.keys().copied().enumerate() {
        if expected != actual {
            bail!("{field} source ordinals are not an exact zero-based sequence");
        }
    }
    Ok(rows.into_values().collect())
}

fn verify_comparison_projection(
    site_dir: &Path,
    source_entry: &crate::snapshot::StaticSnapshotDatasetFile,
    catalog_by_key: &BTreeMap<&str, &BrowserDataset>,
    expected_keys: &mut BTreeSet<String>,
) -> Result<()> {
    let root_dataset = catalog_dataset(catalog_by_key, &source_entry.index_key)?;
    expected_keys.insert(source_entry.index_key.clone());
    let root_bytes = read_catalog_dataset(site_dir, root_dataset)?;
    let manifest: StaticComparisonManifest = serde_json::from_slice(&root_bytes)
        .with_context(|| format!("decoding comparison manifest {}", source_entry.index_key))?;
    validate_comparison_manifest(&source_entry.index_key, &manifest)?;
    if manifest.source.bytes != source_entry.bytes
        || manifest.source.sha256 != source_entry.sha256
        || manifest.source.logical_key != source_entry.index_key
    {
        bail!("comparison manifest source commitment does not match snapshot");
    }

    let mut sample_rows = BTreeMap::new();
    for summary in &manifest.sample_shards {
        expected_keys.insert(summary.index_key.clone());
        let dataset = catalog_dataset(catalog_by_key, &summary.index_key)?;
        if dataset.bytes != summary.bytes || dataset.sha256 != summary.sha256 {
            bail!("comparison sample shard metadata disagrees with manifest");
        }
        let bytes = read_catalog_dataset(site_dir, dataset)?;
        let shard: StaticComparisonSampleShard = serde_json::from_slice(&bytes)?;
        validate_sample_shard(&summary.prefix, &shard)?;
        if shard.rows.len() != summary.row_count {
            bail!("comparison sample shard row count mismatch");
        }
        for row in shard.rows {
            if sample_rows.insert(row.source_ordinal, row.sample).is_some() {
                bail!("duplicate comparison sample source ordinal");
            }
        }
    }

    let mut g8r_rows = BTreeMap::new();
    for summary in &manifest.g8r_point_shards {
        expected_keys.insert(summary.index_key.clone());
        let dataset = catalog_dataset(catalog_by_key, &summary.index_key)?;
        if dataset.bytes != summary.bytes || dataset.sha256 != summary.sha256 {
            bail!("comparison G8r shard metadata disagrees with manifest");
        }
        let bytes = read_catalog_dataset(site_dir, dataset)?;
        let shard: StaticComparisonEntityShard = serde_json::from_slice(&bytes)?;
        validate_entity_shard(&summary.prefix, &shard)?;
        if shard.rows.len() != summary.row_count {
            bail!("comparison G8r shard row count mismatch");
        }
        for row in shard.rows {
            if g8r_rows.insert(row.source_ordinal, row.entity).is_some() {
                bail!("duplicate comparison G8r source ordinal");
            }
        }
    }

    let mut yosys_rows = BTreeMap::new();
    for summary in &manifest.yosys_point_shards {
        expected_keys.insert(summary.index_key.clone());
        let dataset = catalog_dataset(catalog_by_key, &summary.index_key)?;
        if dataset.bytes != summary.bytes || dataset.sha256 != summary.sha256 {
            bail!("comparison Yosys shard metadata disagrees with manifest");
        }
        let bytes = read_catalog_dataset(site_dir, dataset)?;
        let shard: StaticComparisonEntityShard = serde_json::from_slice(&bytes)?;
        validate_entity_shard(&summary.prefix, &shard)?;
        if shard.rows.len() != summary.row_count {
            bail!("comparison Yosys shard row count mismatch");
        }
        for row in shard.rows {
            if yosys_rows.insert(row.source_ordinal, row.entity).is_some() {
                bail!("duplicate comparison Yosys source ordinal");
            }
        }
    }

    let samples = rows_in_source_order(sample_rows, "comparison samples")?;
    if samples.len() != manifest.sample_count {
        bail!("comparison projection sample count mismatch");
    }
    let mut dataset = manifest.dataset.clone();
    dataset.samples = samples;
    let reconstructed = ComparisonSourceIndex {
        schema_version: manifest.source.schema_version,
        generated_utc: manifest.generated_utc,
        dataset,
        g8r_points: rows_in_source_order(g8r_rows, "comparison G8r points")?,
        yosys_points: rows_in_source_order(yosys_rows, "comparison Yosys points")?,
    };
    let reconstructed_bytes =
        serde_json::to_vec(&reconstructed).context("reconstructing comparison source")?;
    let canonical = crate::query::canonicalize_public_web_index_json(
        &source_entry.index_key,
        &reconstructed_bytes,
    )?;
    if canonical != reconstructed_bytes
        || reconstructed_bytes.len() as u64 != source_entry.bytes
        || sha256_hex(&reconstructed_bytes) != source_entry.sha256
    {
        bail!(
            "sharded comparison projection does not reconstruct snapshot source {}",
            source_entry.index_key
        );
    }
    Ok(())
}

fn verify_structural_projection(
    site_dir: &Path,
    source_by_key: &BTreeMap<&str, &crate::snapshot::StaticSnapshotDatasetFile>,
    catalog_by_key: &BTreeMap<&str, &BrowserDataset>,
    expected_keys: &mut BTreeSet<String>,
) -> Result<()> {
    let source_manifest_entry = source_by_key
        .get(crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY)
        .copied();
    let source_group_keys = source_by_key
        .keys()
        .filter(|key| structural_group_hash(key).is_some())
        .map(|key| (*key).to_string())
        .collect::<BTreeSet<_>>();
    let Some(source_manifest_entry) = source_manifest_entry else {
        if !source_group_keys.is_empty() {
            bail!("snapshot structural groups exist without their manifest");
        }
        return Ok(());
    };
    let static_manifest_dataset = catalog_dataset(
        catalog_by_key,
        crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY,
    )?;
    expected_keys.insert(crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY.to_string());
    let static_manifest_bytes = read_catalog_dataset(site_dir, static_manifest_dataset)?;
    let static_manifest: StaticStructuralManifest = serde_json::from_slice(&static_manifest_bytes)
        .context("decoding static structural manifest")?;
    validate_structural_manifest(&static_manifest)?;
    if static_manifest.source.logical_key != source_manifest_entry.index_key
        || static_manifest.source.bytes != source_manifest_entry.bytes
        || static_manifest.source.sha256 != source_manifest_entry.sha256
    {
        bail!("static structural manifest source does not match snapshot");
    }

    let mut reconstructed_group_keys = BTreeSet::new();
    for summary in &static_manifest.shards {
        let prefix = &summary.prefix;
        let dataset = catalog_dataset(catalog_by_key, &summary.index_key)?;
        expected_keys.insert(summary.index_key.clone());
        if dataset.bytes != summary.bytes || dataset.sha256 != summary.sha256 {
            bail!("structural shard metadata disagrees with manifest");
        }
        let bytes = read_catalog_dataset(site_dir, dataset)?;
        let shard: StaticStructuralShard = serde_json::from_slice(&bytes)?;
        validate_structural_shard(prefix, &shard)?;
        if shard.groups.len() != summary.group_count
            || shard
                .groups
                .iter()
                .map(|group| group.members.len())
                .sum::<usize>()
                != summary.member_count
        {
            bail!("structural shard counts disagree with manifest");
        }
        for group in shard.groups {
            let source_key = structural_group_key(&group.structural_hash);
            if !reconstructed_group_keys.insert(source_key.clone()) {
                bail!("duplicate structural group in static shards: {source_key}");
            }
            let source_entry = source_by_key
                .get(source_key.as_str())
                .copied()
                .with_context(|| {
                    format!("static structural shard has unknown group {source_key}")
                })?;
            let group_bytes = serde_json::to_vec_pretty(&group)?;
            if group_bytes.len() as u64 != source_entry.bytes
                || sha256_hex(&group_bytes) != source_entry.sha256
            {
                bail!("static structural group does not match snapshot source {source_key}");
            }
        }
    }
    if reconstructed_group_keys != source_group_keys {
        bail!("static structural shards do not exactly cover snapshot groups");
    }
    Ok(())
}

pub(super) fn verify_static_site_dataset_projection(
    site_dir: &Path,
    catalog: &BrowserCatalog,
    snapshot: &crate::snapshot::StaticSnapshotManifest,
) -> Result<()> {
    let source_by_key = snapshot
        .dataset_files
        .iter()
        .filter(|entry| entry.relpath.ends_with(".json"))
        .map(|entry| (entry.index_key.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let catalog_by_key = catalog
        .datasets
        .iter()
        .map(|dataset| (dataset.logical_key.as_str(), dataset))
        .collect::<BTreeMap<_, _>>();
    if catalog_by_key.len() != catalog.datasets.len() {
        bail!("site catalog contains duplicate dataset keys");
    }

    let mut expected_keys = BTreeSet::new();
    for (key, source_entry) in &source_by_key {
        if comparison_source_schema(key).is_some()
            || is_structural_manifest_source(key)
            || structural_group_hash(key).is_some()
        {
            continue;
        }
        expected_keys.insert((*key).to_string());
        let dataset = catalog_dataset(&catalog_by_key, key)?;
        let bytes = read_catalog_dataset(site_dir, dataset)?;
        if dataset.bytes != source_entry.bytes
            || dataset.sha256 != source_entry.sha256
            || bytes.len() as u64 != source_entry.bytes
            || sha256_hex(&bytes) != source_entry.sha256
        {
            bail!("ordinary site dataset does not match snapshot source: {key}");
        }
    }
    for source_entry in source_by_key.values() {
        if comparison_source_schema(&source_entry.index_key).is_some() {
            verify_comparison_projection(
                site_dir,
                source_entry,
                &catalog_by_key,
                &mut expected_keys,
            )?;
        }
    }
    verify_structural_projection(
        site_dir,
        &source_by_key,
        &catalog_by_key,
        &mut expected_keys,
    )?;

    let actual_keys = catalog_by_key
        .keys()
        .map(|key| (*key).to_string())
        .collect::<BTreeSet<_>>();
    if actual_keys != expected_keys {
        let unexpected = actual_keys.difference(&expected_keys).collect::<Vec<_>>();
        let missing = expected_keys.difference(&actual_keys).collect::<Vec<_>>();
        bail!(
            "site dataset projection key closure mismatch: unexpected={unexpected:?} missing={missing:?}"
        );
    }
    Ok(())
}
