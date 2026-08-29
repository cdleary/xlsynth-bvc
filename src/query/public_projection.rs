// SPDX-License-Identifier: Apache-2.0

use super::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::path::{Component, Path};

const MAX_PUBLIC_LABEL_BYTES: usize = 512;
const MAX_PUBLIC_SIGNATURE_BYTES: usize = 4096;

fn validate_safe_public_text(field: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty() || value.trim() != value || value.len() > max_bytes {
        bail!("{field} must be nonempty, trimmed, and at most {max_bytes} bytes");
    }
    if value.chars().any(char::is_control) || value.contains('\\') {
        bail!("{field} contains a control character or backslash");
    }
    let bytes = value.as_bytes();
    let windows_absolute = bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\');
    if value.starts_with('/') || value.starts_with("//") || windows_absolute {
        bail!("{field} must not contain an absolute host path");
    }
    Ok(())
}

fn validate_slug(field: &str, value: &str) -> Result<()> {
    validate_safe_public_text(field, value, 128)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-.".contains(&byte))
    {
        bail!("{field} must be a lowercase public slug");
    }
    Ok(())
}

fn validate_version(field: &str, value: &str) -> Result<()> {
    validate_safe_public_text(field, value, 128)?;
    let normalized = value.strip_prefix('v').unwrap_or(value);
    if !normalized.contains('.')
        || !normalized
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+'))
    {
        bail!("{field} is not a public version identifier");
    }
    Ok(())
}

fn validate_hex_digest(field: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{field} must be a lowercase 64-character hex digest");
    }
    Ok(())
}

fn validate_relative_path(field: &str, value: &str) -> Result<()> {
    validate_safe_public_text(field, value, MAX_PUBLIC_LABEL_BYTES)?;
    for component in Path::new(value).components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("{field} must be a source-relative path")
            }
        }
    }
    Ok(())
}

fn validate_unique_strings<'a>(
    field: &str,
    values: impl IntoIterator<Item = &'a String>,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            bail!("{field} contains a duplicate value");
        }
    }
    Ok(())
}

fn canonicalize_typed<T>(
    index_key: &str,
    bytes: &[u8],
    pretty: bool,
    require_canonical_input_bytes: bool,
    validate: impl FnOnce(&T) -> Result<()>,
) -> Result<Vec<u8>>
where
    T: DeserializeOwned + Serialize,
{
    let input_value: Value = serde_json::from_slice(bytes)
        .with_context(|| format!("parsing public dataset JSON for {index_key}"))?;
    let decoded: T = serde_json::from_value(input_value.clone()).map_err(|_| {
        anyhow::anyhow!("public dataset {index_key} does not match its typed schema")
    })?;
    let typed_value = serde_json::to_value(&decoded)
        .with_context(|| format!("projecting typed public dataset for {index_key}"))?;
    if input_value != typed_value {
        bail!(
            "public dataset {index_key} contains unknown, omitted, or non-canonical typed fields"
        );
    }
    validate(&decoded).with_context(|| format!("validating public dataset {index_key}"))?;
    let canonical = if pretty {
        serde_json::to_vec_pretty(&decoded)
    } else {
        serde_json::to_vec(&decoded)
    }
    .with_context(|| format!("canonically encoding public dataset {index_key}"))?;
    if require_canonical_input_bytes && bytes != canonical {
        bail!("public dataset {index_key} bytes are not in canonical encoding");
    }
    Ok(canonical)
}

fn validate_enumeration_status(status: &StdlibEnumerationStatusView) -> Result<()> {
    let counters_are_zero = status.scanned_files == 0
        && status.failed_files == 0
        && status.concrete_functions == 0
        && status.suggested_actions == 0;
    match status.reason {
        StdlibEnumerationReason::CompatibilityMapMissing
        | StdlibEnumerationReason::RuntimeUnavailable
        | StdlibEnumerationReason::RootIdentityUnavailable
        | StdlibEnumerationReason::ProvenanceUnavailable => {
            if status.state != StdlibEnumerationState::Unknown || !counters_are_zero {
                bail!("unknown enumeration status has inconsistent reason or counters");
            }
        }
        StdlibEnumerationReason::RootNotMaterialized => {
            if status.state != StdlibEnumerationState::Missing || !counters_are_zero {
                bail!("missing enumeration status has inconsistent reason or counters");
            }
        }
        StdlibEnumerationReason::DiscoveryFailed | StdlibEnumerationReason::DiscoveryEmpty => {
            if status.state != StdlibEnumerationState::Failed || !counters_are_zero {
                bail!("failed enumeration status has inconsistent reason or counters");
            }
        }
        StdlibEnumerationReason::DiscoveryMetadataMissing => {
            if status.state != StdlibEnumerationState::Partial
                || status.suggested_actions == 0
                || status.scanned_files != 0
                || status.failed_files != 0
                || status.concrete_functions != 0
            {
                bail!("metadata-missing enumeration status has inconsistent counters");
            }
        }
        StdlibEnumerationReason::DiscoveryCounts => {
            if status.failed_files > status.scanned_files {
                bail!("enumeration failed_files exceeds scanned_files");
            }
            let no_outputs = status.concrete_functions == 0 && status.suggested_actions == 0;
            let expected = if (status.scanned_files > 0
                && status.failed_files == status.scanned_files)
                || (status.failed_files > 0 && no_outputs)
            {
                StdlibEnumerationState::Failed
            } else if status.failed_files > 0 || no_outputs {
                StdlibEnumerationState::Partial
            } else {
                StdlibEnumerationState::Ok
            };
            if status.state != expected {
                bail!("enumeration state does not match its structured counters");
            }
        }
    }
    Ok(())
}

fn validate_versions_summary(index: &VersionsSummaryIndexFile) -> Result<()> {
    if index.schema_version != crate::WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION {
        bail!("versions summary schema version mismatch");
    }
    validate_unique_strings(
        "versions_summary.cards.crate_version",
        index.report.cards.iter().map(|card| &card.crate_version),
    )?;
    for card in &index.report.cards {
        validate_version("version_card.crate_version", &card.crate_version)?;
        if let Some(released) = &card.crate_release_datetime
            && crate::versioning::parse_compat_release_datetime_utc(released).is_none()
        {
            bail!("version_card.crate_release_datetime is invalid");
        }
        validate_unique_strings("version_card.dso_versions", &card.dso_versions)?;
        for version in &card.dso_versions {
            validate_version("version_card.dso_version", version)?;
        }
        validate_enumeration_status(&card.stdlib_enumeration)?;
        if card.failed_total != card.failures.len() {
            bail!("version_card.failed_total does not match failures");
        }
        let mut observed_by_kind: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
        for failure in &card.failures {
            validate_hex_digest("version_card.failure.action_id", &failure.action_id)?;
            validate_slug("version_card.failure.action_kind", &failure.action_kind)?;
            if let Some(version) = &failure.dso_version {
                validate_version("version_card.failure.dso_version", version)?;
            }
            let counts = observed_by_kind
                .entry(failure.action_kind.as_str())
                .or_default();
            counts.0 += 1;
            if failure.failure_class == PublicFailureClass::Timeout {
                counts.1 += 1;
            }
        }
        let mut declared_by_kind = BTreeMap::new();
        for kind in &card.failed_by_kind {
            validate_slug("version_card.failed_kind.kind", &kind.kind)?;
            if kind.timeout_count > kind.count
                || declared_by_kind
                    .insert(kind.kind.as_str(), (kind.count, kind.timeout_count))
                    .is_some()
            {
                bail!("version_card.failed_by_kind is inconsistent");
            }
        }
        if declared_by_kind != observed_by_kind {
            bail!("version_card.failed_by_kind does not match failures");
        }
    }
    for action in &index.report.unattributed_actions {
        validate_hex_digest("unattributed_action.action_id", &action.action_id)?;
        validate_slug("unattributed_action.action_kind", &action.action_kind)?;
        if let Some(version) = &action.dso_version {
            validate_version("unattributed_action.dso_version", version)?;
        }
    }
    Ok(())
}

fn validate_trend_point(field: &str, point: &StdlibFnTrendPoint) -> Result<()> {
    validate_version(&format!("{field}.crate_version"), &point.crate_version)?;
    validate_version(&format!("{field}.dso_version"), &point.dso_version)?;
    validate_hex_digest(&format!("{field}.stats_action_id"), &point.stats_action_id)?;
    if point.and_nodes < 0.0 || point.depth < 0.0 {
        bail!("{field} contains negative AIG metrics");
    }
    Ok(())
}

fn validate_trend_index(
    index: &StdlibFnsTrendIndexFile,
    expected_kind: StdlibTrendKind,
    expected_fraig: bool,
) -> Result<()> {
    if index.schema_version != crate::WEB_STDLIB_FNS_TREND_INDEX_SCHEMA_VERSION
        || index.dataset.kind != expected_kind
        || index.dataset.fraig != expected_fraig
    {
        bail!("stdlib trend schema or key-specific payload mismatch");
    }
    validate_unique_strings("stdlib_trend.crate_versions", &index.dataset.crate_versions)?;
    for version in &index.dataset.crate_versions {
        validate_version("stdlib_trend.crate_version", version)?;
    }
    validate_unique_strings(
        "stdlib_trend.available_files",
        &index.dataset.available_files,
    )?;
    for path in &index.dataset.available_files {
        validate_relative_path("stdlib_trend.available_file", path)?;
    }
    if let Some(selected) = &index.dataset.selected_file
        && !index.dataset.available_files.contains(selected)
    {
        bail!("stdlib trend selected_file is not available");
    }
    let mut total_points = 0_usize;
    for series in &index.dataset.series {
        validate_safe_public_text(
            "stdlib_trend.fn_key",
            &series.fn_key,
            MAX_PUBLIC_LABEL_BYTES,
        )?;
        for point in &series.points {
            validate_trend_point("stdlib_trend.point", point)?;
        }
        total_points += series.points.len();
    }
    if total_points != index.dataset.total_points {
        bail!("stdlib trend total_points is inconsistent");
    }
    Ok(())
}

fn validate_comparison_sample(field: &str, sample: &StdlibG8rVsYosysSample) -> Result<()> {
    validate_safe_public_text(
        &format!("{field}.fn_key"),
        &sample.fn_key,
        MAX_PUBLIC_LABEL_BYTES,
    )?;
    validate_version(&format!("{field}.crate_version"), &sample.crate_version)?;
    validate_version(&format!("{field}.dso_version"), &sample.dso_version)?;
    for (name, action_id) in [
        ("ir_action_id", &sample.ir_action_id),
        ("g8r_stats_action_id", &sample.g8r_stats_action_id),
        (
            "yosys_abc_stats_action_id",
            &sample.yosys_abc_stats_action_id,
        ),
    ] {
        validate_hex_digest(&format!("{field}.{name}"), action_id)?;
    }
    if let Some(ir_top) = &sample.ir_top {
        validate_safe_public_text(&format!("{field}.ir_top"), ir_top, MAX_PUBLIC_LABEL_BYTES)?;
    }
    if let Some(hash) = &sample.structural_hash {
        validate_hex_digest(&format!("{field}.structural_hash"), hash)?;
    }
    if [
        sample.g8r_nodes,
        sample.g8r_levels,
        sample.yosys_abc_nodes,
        sample.yosys_abc_levels,
        sample.g8r_product,
        sample.yosys_abc_product,
    ]
    .into_iter()
    .any(|value| value < 0.0)
    {
        bail!("{field} contains negative size/depth/product metrics");
    }
    Ok(())
}

fn validate_comparison_dataset(field: &str, dataset: &StdlibG8rVsYosysDataset) -> Result<()> {
    if dataset.min_ir_nodes > dataset.max_ir_nodes {
        bail!("{field} has inverted IR-node bounds");
    }
    validate_unique_strings(
        &format!("{field}.available_crate_versions"),
        &dataset.available_crate_versions,
    )?;
    for version in &dataset.available_crate_versions {
        validate_version(&format!("{field}.crate_version"), version)?;
    }
    for sample in &dataset.samples {
        validate_comparison_sample(&format!("{field}.sample"), sample)?;
        if !dataset
            .available_crate_versions
            .contains(&sample.crate_version)
        {
            bail!("{field} sample references an unavailable crate version");
        }
    }
    Ok(())
}

fn validate_stdlib_comparison_index(
    index: &StdlibG8rVsYosysIndexFile,
    expected_fraig: bool,
) -> Result<()> {
    if index.schema_version != crate::WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION
        || index.dataset.fraig != expected_fraig
    {
        bail!("stdlib comparison schema or key-specific fraig mismatch");
    }
    validate_comparison_dataset("stdlib_comparison", &index.dataset)
}

fn validate_aig_stats_point(field: &str, point: &StdlibAigStatsPoint) -> Result<()> {
    validate_safe_public_text(
        &format!("{field}.fn_key"),
        &point.fn_key,
        MAX_PUBLIC_LABEL_BYTES,
    )?;
    validate_hex_digest(&format!("{field}.ir_action_id"), &point.ir_action_id)?;
    validate_hex_digest(&format!("{field}.stats_action_id"), &point.stats_action_id)?;
    if let Some(ir_top) = &point.ir_top {
        validate_safe_public_text(&format!("{field}.ir_top"), ir_top, MAX_PUBLIC_LABEL_BYTES)?;
    }
    validate_version(&format!("{field}.crate_version"), &point.crate_version)?;
    validate_version(&format!("{field}.dso_version"), &point.dso_version)?;
    if point.and_nodes < 0.0 || point.depth < 0.0 {
        bail!("{field} contains negative AIG metrics");
    }
    Ok(())
}

fn validate_corpus_index(index: &IrFnCorpusG8rVsYosysIndexFile, schema: u32) -> Result<()> {
    if index.schema_version != schema {
        bail!("IR corpus comparison schema version mismatch");
    }
    validate_comparison_dataset("ir_corpus_comparison", &index.dataset)?;
    for (field, points) in [
        ("ir_corpus.g8r_points", &index.g8r_points),
        ("ir_corpus.yosys_points", &index.yosys_points),
    ] {
        for entity in points {
            validate_hex_digest(&format!("{field}.structural_hash"), &entity.structural_hash)?;
            validate_version(&format!("{field}.crate_version"), &entity.crate_version)?;
            if entity.crate_version != entity.point.crate_version {
                bail!("{field} wrapper crate version does not match point");
            }
            validate_aig_stats_point(field, &entity.point)?;
        }
    }
    Ok(())
}

fn validate_timeline_index(index: &StdlibFnTimelineIndexFile) -> Result<()> {
    if index.schema_version != crate::WEB_STDLIB_FN_TIMELINE_INDEX_SCHEMA_VERSION {
        bail!("stdlib timeline schema version mismatch");
    }
    validate_unique_strings("stdlib_timeline.available_files", &index.available_files)?;
    for file in &index.available_files {
        validate_relative_path("stdlib_timeline.available_file", file)?;
    }
    if index.functions_by_file.len() != index.available_files.len() {
        bail!("stdlib timeline functions_by_file does not cover available_files exactly");
    }
    for (file, functions) in &index.functions_by_file {
        validate_relative_path("stdlib_timeline.functions_by_file.key", file)?;
        if !index.available_files.contains(file) {
            bail!("stdlib timeline functions_by_file has unknown file");
        }
        validate_unique_strings("stdlib_timeline.functions", functions)?;
        for function in functions {
            validate_safe_public_text(
                "stdlib_timeline.function",
                function,
                MAX_PUBLIC_LABEL_BYTES,
            )?;
        }
    }
    for (fn_key, entry) in &index.entries_by_fn_key {
        validate_relative_path("stdlib_timeline.entry.dslx_file", &entry.dslx_file)?;
        validate_safe_public_text(
            "stdlib_timeline.entry.dslx_fn_name",
            &entry.dslx_fn_name,
            MAX_PUBLIC_LABEL_BYTES,
        )?;
        if fn_key != &format!("{}::{}", entry.dslx_file, entry.dslx_fn_name) {
            bail!("stdlib timeline entry key does not match file/function");
        }
        for (field, points) in [
            ("g8r_fraig_false", &entry.g8r_fraig_false_by_crate),
            ("g8r_fraig_true", &entry.g8r_fraig_true_by_crate),
            ("yosys", &entry.yosys_by_crate),
        ] {
            for (crate_version, point) in points {
                if crate_version != &point.crate_version {
                    bail!("stdlib timeline {field} map key does not match point version");
                }
                validate_trend_point(&format!("stdlib_timeline.{field}"), point)?;
            }
        }
        for (delay_model, points) in &entry.delay_by_model_and_crate {
            validate_slug("stdlib_timeline.delay_model", delay_model)?;
            for (crate_version, point) in points {
                if crate_version != &point.crate_version || point.delay_ps < 0.0 {
                    bail!("stdlib timeline delay map is inconsistent");
                }
                validate_version("stdlib_timeline.delay.crate_version", &point.crate_version)?;
                validate_version("stdlib_timeline.delay.dso_version", &point.dso_version)?;
                validate_hex_digest("stdlib_timeline.delay.action_id", &point.action_id)?;
            }
        }
    }
    Ok(())
}

fn validate_structural_manifest(manifest: &IrFnCorpusStructuralManifest) -> Result<()> {
    if manifest.schema_version != crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION
        || manifest.distinct_structural_hashes != manifest.groups.len()
    {
        bail!("structural manifest schema or distinct group count mismatch");
    }
    if let Some(hash) = &manifest.source_action_set_sha256 {
        validate_hex_digest("structural_manifest.source_action_set_sha256", hash)?;
    }
    let mut total_members = 0_usize;
    for group in &manifest.groups {
        validate_hex_digest(
            "structural_manifest.group.structural_hash",
            &group.structural_hash,
        )?;
        validate_hex_digest(
            "structural_manifest.group.content_sha256",
            &group.content_sha256,
        )?;
        validate_relative_path("structural_manifest.group.relpath", &group.relpath)?;
        if group.relpath != crate::service::hash_group_relpath(&group.structural_hash)
            || group.member_count == 0
        {
            bail!("structural manifest group path or member count is invalid");
        }
        total_members += group.member_count;
    }
    if total_members != manifest.indexed_actions {
        bail!("structural manifest indexed_actions does not match group members");
    }
    Ok(())
}

fn validate_structural_group(group: &IrFnCorpusStructuralGroupFile) -> Result<()> {
    if group.schema_version != crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION
        || group.members.is_empty()
    {
        bail!("structural group schema version or members are invalid");
    }
    validate_hex_digest("structural_group.structural_hash", &group.structural_hash)?;
    for member in &group.members {
        validate_hex_digest(
            "structural_member.opt_ir_action_id",
            &member.opt_ir_action_id,
        )?;
        validate_hex_digest(
            "structural_member.source_ir_action_id",
            &member.source_ir_action_id,
        )?;
        validate_safe_public_text(
            "structural_member.ir_top",
            &member.ir_top,
            MAX_PUBLIC_LABEL_BYTES,
        )?;
        if let Some(signature) = &member.ir_fn_signature {
            validate_safe_public_text(
                "structural_member.ir_fn_signature",
                signature,
                MAX_PUBLIC_SIGNATURE_BYTES,
            )?;
        }
        validate_version("structural_member.crate_version", &member.crate_version)?;
        validate_version("structural_member.dso_version", &member.dso_version)?;
        validate_hex_digest(
            "structural_member.output_artifact.action_id",
            &member.output_artifact.action_id,
        )?;
        validate_relative_path(
            "structural_member.output_artifact.relpath",
            &member.output_artifact.relpath,
        )?;
        if member.output_artifact.action_id != member.opt_ir_action_id
            || member.output_artifact.artifact_type != ArtifactType::IrPackageFile
        {
            bail!("structural member output artifact does not match its producer");
        }
        validate_hex_digest(
            "structural_member.output_file_sha256",
            &member.output_file_sha256,
        )?;
        if !matches!(
            member.hash_source.as_str(),
            "dependency_hint" | "recomputed" | "k_bool_cone_manifest"
        ) {
            bail!("structural member hash_source is not a public vocabulary value");
        }
        for action_id in &member.hash_hint_source_action_ids {
            validate_hex_digest("structural_member.hash_hint_source_action_id", action_id)?;
        }
        if let Some(origin) = &member.dslx_origin {
            validate_hex_digest(
                "structural_member.dslx_origin.action_id",
                &origin.dslx_subtree_action_id,
            )?;
            validate_relative_path("structural_member.dslx_origin.dslx_file", &origin.dslx_file)?;
            validate_safe_public_text(
                "structural_member.dslx_origin.dslx_fn_name",
                &origin.dslx_fn_name,
                MAX_PUBLIC_LABEL_BYTES,
            )?;
        }
        if !matches!(
            member.producer_action_kind.as_deref(),
            Some("driver_ir_to_opt" | "ir_fn_to_k_bool_cone_corpus")
        ) {
            bail!("structural member producer_action_kind is invalid");
        }
    }
    Ok(())
}

pub(crate) fn canonicalize_public_web_index_json(index_key: &str, bytes: &[u8]) -> Result<Vec<u8>> {
    match index_key {
        crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME => {
            canonicalize_typed::<VersionsSummaryIndexFile>(
                index_key,
                bytes,
                false,
                false,
                validate_versions_summary,
            )
        }
        crate::WEB_STDLIB_FNS_TREND_G8R_FRAIG_FALSE_INDEX_FILENAME => {
            canonicalize_typed::<StdlibFnsTrendIndexFile>(index_key, bytes, false, false, |index| {
                validate_trend_index(index, StdlibTrendKind::G8r, false)
            })
        }
        crate::WEB_STDLIB_FNS_TREND_YOSYS_ABC_INDEX_FILENAME => {
            canonicalize_typed::<StdlibFnsTrendIndexFile>(index_key, bytes, false, false, |index| {
                validate_trend_index(index, StdlibTrendKind::YosysAbc, false)
            })
        }
        crate::WEB_STDLIB_FN_TIMELINE_INDEX_FILENAME => {
            canonicalize_typed::<StdlibFnTimelineIndexFile>(
                index_key,
                bytes,
                false,
                false,
                validate_timeline_index,
            )
        }
        crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME => {
            canonicalize_typed::<StdlibG8rVsYosysIndexFile>(
                index_key,
                bytes,
                false,
                false,
                |index| validate_stdlib_comparison_index(index, false),
            )
        }
        crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_TRUE_INDEX_FILENAME => {
            canonicalize_typed::<StdlibG8rVsYosysIndexFile>(
                index_key,
                bytes,
                false,
                false,
                |index| validate_stdlib_comparison_index(index, true),
            )
        }
        crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME => {
            canonicalize_typed::<IrFnCorpusG8rVsYosysIndexFile>(
                index_key,
                bytes,
                false,
                false,
                |index| {
                    validate_corpus_index(
                        index,
                        crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
                    )
                },
            )
        }
        crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME => {
            canonicalize_typed::<IrFnCorpusG8rVsYosysIndexFile>(
                index_key,
                bytes,
                false,
                false,
                |index| {
                    validate_corpus_index(
                        index,
                        crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_SCHEMA_VERSION,
                    )
                },
            )
        }
        crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY => {
            canonicalize_typed::<IrFnCorpusStructuralManifest>(
                index_key,
                bytes,
                true,
                true,
                validate_structural_manifest,
            )
        }
        key if key.starts_with(&format!(
            "{}/by-hash/",
            crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE
        )) =>
        {
            canonicalize_typed::<IrFnCorpusStructuralGroupFile>(
                index_key,
                bytes,
                true,
                true,
                |group| {
                    validate_structural_group(group)?;
                    let expected_key = format!(
                        "{}/{}",
                        crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE,
                        crate::service::hash_group_relpath(&group.structural_hash)
                    );
                    if index_key != expected_key {
                        bail!("structural group key does not match its payload hash");
                    }
                    Ok(())
                },
            )
        }
        _ => bail!("index key is not an allowlisted public JSON schema: {index_key}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn generated_utc() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 29, 12, 0, 0)
            .single()
            .unwrap()
    }

    fn empty_versions_value() -> Value {
        json!({
            "schema_version": crate::WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION,
            "generated_utc": "2026-08-29T12:00:00Z",
            "report": {"cards": [], "unattributed_actions": []}
        })
    }

    fn empty_trend_value(kind: &str) -> Value {
        json!({
            "schema_version": crate::WEB_STDLIB_FNS_TREND_INDEX_SCHEMA_VERSION,
            "generated_utc": "2026-08-29T12:00:00Z",
            "dataset": {
                "kind": kind,
                "fraig": false,
                "crate_versions": [],
                "series": [],
                "total_points": 0,
                "available_files": [],
                "selected_file": null
            }
        })
    }

    fn empty_comparison_value(schema_version: u32, fraig: bool, corpus: bool) -> Value {
        let mut value = json!({
            "schema_version": schema_version,
            "generated_utc": "2026-08-29T12:00:00Z",
            "dataset": {
                "fraig": fraig,
                "samples": [],
                "min_ir_nodes": 0,
                "max_ir_nodes": 0,
                "g8r_only_count": 0,
                "yosys_only_count": 0,
                "available_crate_versions": []
            }
        });
        if corpus {
            value["g8r_points"] = json!([]);
            value["yosys_points"] = json!([]);
        }
        value
    }

    fn structural_group_fixture() -> (String, Vec<u8>) {
        let structural_hash = "7".repeat(64);
        let opt_ir_action_id = "1".repeat(64);
        let group = IrFnCorpusStructuralGroupFile {
            schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
            structural_hash: structural_hash.clone(),
            members: vec![IrFnCorpusStructuralMember {
                opt_ir_action_id: opt_ir_action_id.clone(),
                source_ir_action_id: "2".repeat(64),
                ir_top: "__sample".to_string(),
                ir_fn_signature: Some("fn __sample(x: bits[1]) -> bits[1]".to_string()),
                ir_op_count: Some(1),
                crate_version: "0.35.0".to_string(),
                dso_version: "0.35.0".to_string(),
                created_utc: generated_utc(),
                output_artifact: ArtifactRef {
                    action_id: opt_ir_action_id,
                    artifact_type: ArtifactType::IrPackageFile,
                    relpath: "payload/result.ir".to_string(),
                },
                output_file_sha256: "3".repeat(64),
                output_file_bytes: 42,
                hash_source: "dependency_hint".to_string(),
                hash_hint_source_action_ids: vec!["4".repeat(64)],
                dslx_origin: None,
                producer_action_kind: Some("driver_ir_to_opt".to_string()),
            }],
        };
        let key = format!(
            "{}/{}",
            crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE,
            crate::service::hash_group_relpath(&structural_hash)
        );
        (key, serde_json::to_vec_pretty(&group).unwrap())
    }

    #[test]
    fn every_allowlisted_public_key_has_a_typed_canonical_schema() {
        let timeline = json!({
            "schema_version": crate::WEB_STDLIB_FN_TIMELINE_INDEX_SCHEMA_VERSION,
            "generated_utc": "2026-08-29T12:00:00Z",
            "available_files": [],
            "functions_by_file": {},
            "entries_by_fn_key": {}
        });
        let manifest = IrFnCorpusStructuralManifest {
            schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
            generated_utc: generated_utc(),
            recompute_missing_hashes: false,
            total_actions_scanned: 0,
            total_driver_ir_to_opt_actions: 0,
            total_ir_fn_to_k_bool_cone_corpus_actions: 0,
            indexed_actions: 0,
            indexed_k_bool_cone_members: 0,
            distinct_structural_hashes: 0,
            hash_from_dependency_hint_count: 0,
            hash_recomputed_count: 0,
            hash_hint_conflict_count: 0,
            skipped_missing_output_count: 0,
            skipped_missing_ir_top_count: 0,
            skipped_missing_hash_hint_count: 0,
            skipped_hash_error_count: 0,
            skipped_k_bool_cone_manifest_errors: 0,
            skipped_k_bool_cone_empty_count: 0,
            source_action_set_sha256: Some("5".repeat(64)),
            groups: vec![],
        };
        let (group_key, group_bytes) = structural_group_fixture();
        let mut cases = vec![
            (
                crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME.to_string(),
                serde_json::to_vec(&empty_versions_value()).unwrap(),
            ),
            (
                crate::WEB_STDLIB_FNS_TREND_G8R_FRAIG_FALSE_INDEX_FILENAME.to_string(),
                serde_json::to_vec(&empty_trend_value("g8r")).unwrap(),
            ),
            (
                crate::WEB_STDLIB_FNS_TREND_YOSYS_ABC_INDEX_FILENAME.to_string(),
                serde_json::to_vec(&empty_trend_value("yosys_abc")).unwrap(),
            ),
            (
                crate::WEB_STDLIB_FN_TIMELINE_INDEX_FILENAME.to_string(),
                serde_json::to_vec(&timeline).unwrap(),
            ),
            (
                crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME.to_string(),
                serde_json::to_vec(&empty_comparison_value(
                    crate::WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
                    false,
                    false,
                ))
                .unwrap(),
            ),
            (
                crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_TRUE_INDEX_FILENAME.to_string(),
                serde_json::to_vec(&empty_comparison_value(
                    crate::WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
                    true,
                    false,
                ))
                .unwrap(),
            ),
            (
                crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME.to_string(),
                serde_json::to_vec(&empty_comparison_value(
                    crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
                    false,
                    true,
                ))
                .unwrap(),
            ),
            (
                crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME.to_string(),
                serde_json::to_vec(&empty_comparison_value(
                    crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_SCHEMA_VERSION,
                    false,
                    true,
                ))
                .unwrap(),
            ),
            (
                crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY.to_string(),
                serde_json::to_vec_pretty(&manifest).unwrap(),
            ),
        ];
        cases.push((group_key, group_bytes));

        assert_eq!(cases.len(), 10);
        for (key, bytes) in cases {
            let canonical = canonicalize_public_web_index_json(&key, &bytes)
                .unwrap_or_else(|error| panic!("{key}: {error:#}"));
            assert!(!canonical.is_empty(), "{key}");
        }
    }

    #[test]
    fn public_projection_rejects_unknown_private_text_and_absolute_paths() {
        let marker = "/srv/build/secrets/credential.txt";
        let mut versions = empty_versions_value();
        versions["report"]["cards"] = json!([{
            "crate_version": "0.35.0",
            "crate_release_datetime": null,
            "total_materialized": 1,
            "failed_total": 0,
            "dso_versions": ["0.35.0"],
            "stdlib_enumeration": {
                "state": "failed",
                "reason": "discovery_failed",
                "scanned_files": 0,
                "failed_files": 0,
                "concrete_functions": 0,
                "suggested_actions": 0,
                "summary": marker
            },
            "failed_by_kind": [],
            "failures": []
        }]);
        let error = canonicalize_public_web_index_json(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &serde_json::to_vec(&versions).unwrap(),
        )
        .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("unknown, omitted, or non-canonical"));
        assert!(!error.contains(marker));

        let mut versions = empty_versions_value();
        versions["report"]["cards"] = json!([{
            "crate_version": "0.35.0",
            "crate_release_datetime": null,
            "total_materialized": 1,
            "failed_total": 0,
            "dso_versions": ["0.35.0"],
            "stdlib_enumeration": {
                "state": marker,
                "reason": "discovery_failed",
                "scanned_files": 0,
                "failed_files": 0,
                "concrete_functions": 0,
                "suggested_actions": 0
            },
            "failed_by_kind": [],
            "failures": []
        }]);
        let error = canonicalize_public_web_index_json(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &serde_json::to_vec(&versions).unwrap(),
        )
        .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("does not match its typed schema"));
        assert!(!error.contains(marker));

        let mut trend = empty_trend_value("g8r");
        trend["dataset"]["available_files"] = json!([marker]);
        trend["dataset"]["selected_file"] = json!(marker);
        let error = canonicalize_public_web_index_json(
            crate::WEB_STDLIB_FNS_TREND_G8R_FRAIG_FALSE_INDEX_FILENAME,
            &serde_json::to_vec(&trend).unwrap(),
        )
        .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("absolute host path"));
        assert!(!error.contains(marker));
    }

    #[test]
    fn public_projection_rejects_key_payload_mismatch() {
        let bytes = serde_json::to_vec(&empty_comparison_value(
            crate::WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
            true,
            false,
        ))
        .unwrap();
        let error = canonicalize_public_web_index_json(
            crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME,
            &bytes,
        )
        .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("key-specific fraig mismatch"));

        let (group_key, group_bytes) = structural_group_fixture();
        let wrong_group_key = group_key.replace("/77/77/", "/00/00/");
        let error = canonicalize_public_web_index_json(&wrong_group_key, &group_bytes).unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("key does not match its payload hash"));
    }
}
