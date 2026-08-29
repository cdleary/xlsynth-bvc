// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use prost::Message;
use serde_json::{Map, Value, json};

use crate::model;
use crate::proto::action::{
    action_id_to_hex, action_id_to_proto, action_spec_from_proto, action_spec_to_proto,
    digest_from_hex, digest_to_hex, driver_runtime_from_proto, driver_runtime_to_proto,
    script_ref_from_proto, script_ref_to_proto, yosys_runtime_from_proto, yosys_runtime_to_proto,
};
use crate::proto::queue::{
    artifact_ref_from_proto, artifact_ref_to_proto, timestamp_from_proto, timestamp_to_proto,
};
use crate::proto::v1 as pb;

pub(crate) const PROVENANCE_RECORD_VERSION: u32 = 1;

fn required<'a, T>(value: &'a Option<T>, field: &str) -> Result<&'a T> {
    value
        .as_ref()
        .with_context(|| format!("missing required protobuf field {field}"))
}

fn object(value: &Value) -> Result<&Map<String, Value>> {
    value
        .as_object()
        .context("provenance details must be a JSON object at the executor adapter boundary")
}

fn string(map: &Map<String, Value>, key: &str) -> Option<String> {
    map.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn bool_value(map: &Map<String, Value>, key: &str) -> Option<bool> {
    map.get(key).and_then(Value::as_bool)
}

fn u64_value(map: &Map<String, Value>, key: &str) -> Option<u64> {
    map.get(key).and_then(Value::as_u64)
}

fn relpath(value: impl Into<String>) -> pb::NormalizedRelpath {
    pb::NormalizedRelpath {
        value: value.into(),
    }
}

fn dso_version(value: &str) -> pb::DsoVersion {
    pb::DsoVersion {
        value: value.strip_prefix('v').unwrap_or(value).to_string(),
    }
}

fn diagnostics(map: &Map<String, Value>) -> Vec<pb::ExecutionDiagnostic> {
    map.iter()
        .filter(|(key, _)| {
            key.ends_with("_error") || key.ends_with("_missing") || key.ends_with("_warning")
        })
        .map(|(code, value)| pb::ExecutionDiagnostic {
            code: code.clone(),
            message: value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string()),
        })
        .collect()
}

fn runtime_from_json(value: &Value, field: &str) -> Result<model::DriverRuntimeSpec> {
    serde_json::from_value(value.clone()).with_context(|| format!("decoding {field}"))
}

fn yosys_runtime_from_json(value: &Value, field: &str) -> Result<model::YosysRuntimeSpec> {
    serde_json::from_value(value.clone()).with_context(|| format!("decoding {field}"))
}

fn script_ref_from_json(value: &Value, field: &str) -> Result<model::ScriptRef> {
    serde_json::from_value(value.clone()).with_context(|| format!("decoding {field}"))
}

fn optional_digest(map: &Map<String, Value>, key: &str) -> Result<Option<pb::Sha256Digest>> {
    string(map, key)
        .map(|value| digest_from_hex(&value, &format!("details.{key}")))
        .transpose()
}

fn common_driver_details(
    map: &Map<String, Value>,
    version: &str,
    runtime: &model::DriverRuntimeSpec,
) -> Result<pb::CommonDriverActionDetails> {
    let runtime = match map.get("driver_runtime") {
        Some(value) => runtime_from_json(value, "details.driver_runtime")?,
        None => runtime.clone(),
    };
    let semantic_cache_hit = map
        .get("semantic_cache_hit")
        .and_then(Value::as_object)
        .and_then(|hit| string(hit, "from_action_id"))
        .map(|action_id| {
            Ok::<_, anyhow::Error>(pb::SemanticCacheHitDetails {
                from_action_id: Some(action_id_to_proto(
                    &action_id,
                    "details.semantic_cache_hit.from_action_id",
                )?),
            })
        })
        .transpose()?;
    let driver_ir_aig_equiv_runtime = map
        .get("driver_ir_aig_equiv_runtime")
        .map(|value| runtime_from_json(value, "details.driver_ir_aig_equiv_runtime"))
        .transpose()?
        .map(|value| driver_runtime_to_proto(&value, "details.driver_ir_aig_equiv_runtime"))
        .transpose()?;
    Ok(pb::CommonDriverActionDetails {
        driver_runtime: Some(driver_runtime_to_proto(&runtime, "details.driver_runtime")?),
        dso_version: Some(dso_version(
            &string(map, "xlsynth_version").unwrap_or_else(|| version.to_string()),
        )),
        crate_version_label: string(map, "crate_version_label"),
        dso_version_label: string(map, "dso_version_label"),
        ir_top: string(map, "ir_top"),
        driver_subcommand: string(map, "driver_subcommand"),
        input_ir_fn_structural_hash: optional_digest(map, "input_ir_fn_structural_hash")?,
        output_ir_fn_structural_hash: optional_digest(map, "output_ir_fn_structural_hash")?,
        output_ir_op_count: u64_value(map, "output_ir_op_count"),
        output_manifest_relpath: string(map, "output_manifest_relpath").map(relpath),
        semantic_cache_hit,
        synthetic: bool_value(map, "synthetic"),
        test_label: string(map, "test").or_else(|| string(map, "test_label")),
        driver_ir_aig_equiv_runtime,
        input_ir_fn_structural_hash_source: string(map, "input_ir_fn_structural_hash_source"),
        ir_top_explicit: bool_value(map, "ir_top_explicit"),
        output_ir_relpath: string(map, "output_ir_relpath").map(relpath),
        diagnostics: diagnostics(map),
    })
}

fn discovery_details(value: &Value) -> Result<pb::DslxFunctionDiscoveryDetails> {
    let map = object(value)?;
    let source_runtime_value = map
        .get("driver_runtime")
        .context("discovery.driver_runtime is required")?;
    let enumeration_runtime_value = map
        .get("enumeration_runtime")
        .context("discovery.enumeration_runtime is required")?;
    let source_runtime = runtime_from_json(source_runtime_value, "discovery.driver_runtime")?;
    let enumeration_runtime =
        runtime_from_json(enumeration_runtime_value, "discovery.enumeration_runtime")?;
    let failed_dslx_files = map
        .get("failed_dslx_files")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(relpath)
        .collect();
    Ok(pb::DslxFunctionDiscoveryDetails {
        source_runtime: Some(driver_runtime_to_proto(
            &source_runtime,
            "discovery.driver_runtime",
        )?),
        enumeration_runtime: Some(driver_runtime_to_proto(
            &enumeration_runtime,
            "discovery.enumeration_runtime",
        )?),
        enumeration_runtime_dso_version: string(map, "enumeration_runtime_xlsynth_version")
            .map(|value| dso_version(&value)),
        enumeration_runtime_overrides_source: bool_value(
            map,
            "enumeration_runtime_overrides_source",
        )
        .unwrap_or(false),
        dslx_path: string(map, "dslx_path").unwrap_or_default(),
        dslx_stdlib_path: string(map, "dslx_stdlib_path").unwrap_or_default(),
        stdlib_source: string(map, "stdlib_source").unwrap_or_default(),
        scanned_dslx_files: u64_value(map, "scanned_dslx_files").unwrap_or(0),
        listed_functions: u64_value(map, "listed_functions").unwrap_or(0),
        concrete_functions: u64_value(map, "concrete_functions").unwrap_or(0),
        failed_dslx_files,
        suggested_actions: u64_value(map, "suggested_actions").unwrap_or(0),
    })
}

fn discovery_details_from_proto(
    value: &pb::DslxFunctionDiscoveryDetails,
    source_dso_version: &str,
    field: &str,
) -> Result<Value> {
    let source_runtime = driver_runtime_from_proto(
        required(&value.source_runtime, &format!("{field}.source_runtime"))?,
        &format!("{field}.source_runtime"),
    )?;
    let enumeration_runtime = driver_runtime_from_proto(
        required(
            &value.enumeration_runtime,
            &format!("{field}.enumeration_runtime"),
        )?,
        &format!("{field}.enumeration_runtime"),
    )?;
    let enumeration_dso = required(
        &value.enumeration_runtime_dso_version,
        &format!("{field}.enumeration_runtime_dso_version"),
    )?;
    if enumeration_dso.value.is_empty() {
        bail!("{field}.enumeration_runtime_dso_version must not be empty");
    }
    let enumeration_dso_version = format!("v{}", enumeration_dso.value);
    let failed_dslx_files = value
        .failed_dslx_files
        .iter()
        .enumerate()
        .map(|(index, path)| {
            let path_field = format!("{field}.failed_dslx_files[{index}]");
            if path.value.is_empty()
                || path.value.starts_with('/')
                || path.value.contains('\\')
                || path
                    .value
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == "..")
            {
                bail!("{path_field} must be a normalized relative path");
            }
            Ok(path.value.clone())
        })
        .collect::<Result<Vec<_>>>()?;
    let failed_dslx_files_count = failed_dslx_files.len() as u64;
    let crate_version_label =
        crate::versioning::version_label("crate", &source_runtime.driver_version);
    let dso_version_label = crate::versioning::version_label("dso", source_dso_version);
    let enumeration_runtime_dso_version_label =
        crate::versioning::version_label("dso", &enumeration_dso_version);
    Ok(json!({
        "driver_runtime": source_runtime,
        "enumeration_runtime": enumeration_runtime,
        "enumeration_runtime_xlsynth_version": enumeration_dso_version,
        "enumeration_runtime_overrides_source": value.enumeration_runtime_overrides_source,
        "crate_version_label": crate_version_label,
        "dso_version_label": dso_version_label,
        "enumeration_runtime_dso_version_label": enumeration_runtime_dso_version_label,
        "dslx_path": value.dslx_path,
        "dslx_stdlib_path": value.dslx_stdlib_path,
        "stdlib_source": value.stdlib_source,
        "scanned_dslx_files": value.scanned_dslx_files,
        "listed_functions": value.listed_functions,
        "concrete_functions": value.concrete_functions,
        "failed_dslx_files_count": failed_dslx_files_count,
        "failed_dslx_files": failed_dslx_files,
        "suggested_actions": value.suggested_actions,
    }))
}

fn details_to_proto(action: &model::ActionSpec, details: &Value) -> Result<pb::ActionDetails> {
    use model::ActionSpec as M;
    use pb::action_details::Kind;

    let map = object(details)?;
    let kind = match action {
        M::ImportIrPackageFile {
            source_sha256,
            top_fn_name,
        } => Kind::ImportIrPackageFile(pb::ImportIrPackageFileDetails {
            source_path: string(map, "source_path"),
            source_sha256: Some(digest_from_hex(source_sha256, "details.source_sha256")?),
            top_fn_name: top_fn_name.clone(),
            import_kind: string(map, "import_kind").unwrap_or_else(|| "local_ir_file".into()),
        }),
        M::DownloadAndExtractXlsynthReleaseStdlibTarball { .. } => {
            let download = map.get("download").and_then(Value::as_object);
            let discovery = map
                .get("dslx_list_fns_discovery")
                .map(discovery_details)
                .transpose()?;
            Kind::DownloadReleaseStdlib(pb::DownloadReleaseStdlibDetails {
                tarball_url: download
                    .and_then(|v| string(v, "tarball_url"))
                    .unwrap_or_default(),
                sha256_url: download
                    .and_then(|v| string(v, "sha256_url"))
                    .unwrap_or_default(),
                expected_sha256: download
                    .and_then(|v| string(v, "expected_sha256"))
                    .map(|v| digest_from_hex(&v, "details.download.expected_sha256"))
                    .transpose()?,
                actual_sha256: download
                    .and_then(|v| string(v, "actual_sha256"))
                    .map(|v| digest_from_hex(&v, "details.download.actual_sha256"))
                    .transpose()?,
                function_discovery: discovery,
                function_discovery_error: string(map, "dslx_list_fns_discovery_error"),
            })
        }
        M::DownloadAndExtractXlsynthSourceSubtree { subtree, .. } => {
            let download = map.get("download").and_then(Value::as_object);
            Kind::DownloadSourceSubtree(pb::DownloadSourceSubtreeDetails {
                source_archive_url: download
                    .and_then(|v| string(v, "source_archive_url"))
                    .unwrap_or_default(),
                source_archive_sha256: download
                    .and_then(|v| string(v, "source_archive_sha256"))
                    .map(|v| digest_from_hex(&v, "details.download.source_archive_sha256"))
                    .transpose()?,
                subtree: Some(relpath(
                    string(map, "subtree").unwrap_or_else(|| subtree.clone()),
                )),
                extracted_file_count: u64_value(map, "extracted_file_count").unwrap_or(0),
                function_discovery: map
                    .get("dslx_list_fns_discovery")
                    .map(discovery_details)
                    .transpose()?,
                function_discovery_error: string(map, "dslx_list_fns_discovery_error"),
            })
        }
        M::DriverDslxFnToIr {
            dslx_file,
            dslx_fn_name,
            version,
            runtime,
            ..
        } => Kind::DriverDslxFnToIr(pb::DriverDslxFnToIrDetails {
            common: Some(common_driver_details(map, version, runtime)?),
            dslx_file: Some(relpath(dslx_file.clone())),
            dslx_fn_name: dslx_fn_name.clone(),
            dslx_path: string(map, "dslx_path").unwrap_or_default(),
            dslx_stdlib_path: string(map, "dslx_stdlib_path").unwrap_or_default(),
            stdlib_source: string(map, "stdlib_source").unwrap_or_default(),
        }),
        M::DriverIrToOpt {
            version, runtime, ..
        } => Kind::DriverIrToOpt(pb::DriverIrToOptDetails {
            common: Some(common_driver_details(map, version, runtime)?),
            driver_ir_aig_equiv_supported: bool_value(map, "driver_ir_aig_equiv_supported"),
            driver_ir_aig_equiv_quarantined: bool_value(map, "driver_ir_aig_equiv_quarantined"),
            driver_ir_aig_equiv_mode: string(map, "driver_ir_aig_equiv_mode"),
            driver_ir_aig_equiv_error: string(map, "driver_ir_aig_equiv_probe_error")
                .or_else(|| string(map, "driver_ir_aig_equiv_suggestions_error")),
            mffc_suggestion_eligible: bool_value(map, "mffc_suggestion_eligible").unwrap_or(false),
            mffc_suggestion_reason: string(map, "mffc_suggestion_reason").unwrap_or_default(),
            mffc_min_internal_non_literal: u64_value(map, "mffc_min_internal_non_literal")
                .unwrap_or(0),
        }),
        M::DriverIrToDelayInfo {
            delay_model,
            output_format,
            version,
            runtime,
            ..
        } => {
            let urls = map
                .get("delay_info_proto_schema_urls")
                .and_then(Value::as_object);
            Kind::DriverIrToDelayInfo(pb::DriverIrToDelayInfoDetails {
                common: Some(common_driver_details(map, version, runtime)?),
                delay_model: string(map, "delay_model").unwrap_or_else(|| delay_model.clone()),
                output_format: string(map, "output_format")
                    .unwrap_or_else(|| output_format.clone()),
                delay_info_textproto: string(map, "delay_info_textproto"),
                parse_error: string(map, "parse_error"),
                delay_info_proto_schema_url: urls
                    .and_then(|v| string(v, "delay_info_proto"))
                    .unwrap_or_default(),
                op_proto_schema_url: urls.and_then(|v| string(v, "op_proto")).unwrap_or_default(),
            })
        }
        M::DriverIrEquiv {
            version, runtime, ..
        } => Kind::DriverIrEquiv(pb::DriverIrEquivDetails {
            common: Some(common_driver_details(map, version, runtime)?),
            mode: string(map, "driver_subcommand").unwrap_or_else(|| "ir-equiv".into()),
            timeout_seconds: u64_value(map, "timeout_secs").unwrap_or(0),
            return_code: map
                .get("return_code")
                .and_then(Value::as_i64)
                .and_then(|v| i32::try_from(v).ok()),
            stdout_tail: string(map, "stdout_tail"),
            stderr_tail: string(map, "stderr_tail"),
        }),
        M::DriverIrAigEquiv {
            version, runtime, ..
        } => Kind::DriverIrAigEquiv(pb::DriverIrAigEquivDetails {
            common: Some(common_driver_details(map, version, runtime)?),
            mode: string(map, "driver_ir_aig_equiv_mode")
                .or_else(|| string(map, "driver_subcommand"))
                .unwrap_or_else(|| "aig2ir_then_ir_equiv".into()),
            timeout_seconds: u64_value(map, "timeout_secs").unwrap_or(0),
            return_code: map
                .get("return_code")
                .and_then(Value::as_i64)
                .and_then(|v| i32::try_from(v).ok()),
            stdout_tail: string(map, "stdout_tail"),
            stderr_tail: string(map, "stderr_tail"),
        }),
        M::DriverIrToG8rAig {
            fraig,
            lowering_mode,
            version,
            runtime,
            ..
        } => {
            let (batch_size, member_index) = map
                .get("batch_execution")
                .and_then(Value::as_object)
                .map(|batch| {
                    (
                        u64_value(batch, "batch_size"),
                        u64_value(batch, "member_index"),
                    )
                })
                .unwrap_or((None, None));
            Kind::DriverIrToG8rAig(pb::DriverIrToG8rAigDetails {
                common: Some(common_driver_details(map, version, runtime)?),
                fraig: bool_value(map, "fraig").unwrap_or(*fraig),
                lowering_mode: match lowering_mode {
                    model::G8rLoweringMode::Default => pb::G8rLoweringMode::Default as i32,
                    model::G8rLoweringMode::FrontendNoPrepRewrite => {
                        pb::G8rLoweringMode::FrontendNoPrepRewrite as i32
                    }
                },
                driver_ir2g8r_cli_mode: string(map, "driver_ir2g8r_cli_mode"),
                driver_ir2g8r_passed_top: bool_value(map, "driver_ir2g8r_passed_top"),
                driver_ir_aig_equiv_supported: bool_value(map, "driver_ir_aig_equiv_supported"),
                driver_ir_aig_equiv_quarantined: bool_value(map, "driver_ir_aig_equiv_quarantined"),
                driver_ir_aig_equiv_mode: string(map, "driver_ir_aig_equiv_mode"),
                legacy_g8r_stats_relpath: string(map, "legacy_g8r_stats_relpath").map(relpath),
                batch_size,
                member_index,
                prepared_ir_supported: bool_value(map, "driver_ir2g8r_prepared_ir_supported"),
                prepared_ir_relpath: string(map, "driver_ir2g8r_prepared_ir_relpath").map(relpath),
                prepared_ir_reason: string(map, "driver_ir2g8r_prepared_ir_reason"),
                output_kind: string(map, "driver_ir2g8r_output_kind"),
                top_ignored_reason: string(map, "driver_ir2g8r_top_ignored_reason"),
                lowering_mode_flags: string(map, "g8r_lowering_mode_flags").unwrap_or_default(),
            })
        }
        M::IrFnToCombinationalVerilog {
            use_system_verilog,
            version,
            runtime,
            ..
        } => Kind::IrFnToCombinationalVerilog(pb::IrFnToCombinationalVerilogDetails {
            common: Some(common_driver_details(map, version, runtime)?),
            use_system_verilog: bool_value(map, "use_system_verilog")
                .unwrap_or(*use_system_verilog),
            driver_ir2combo_top_rewritten: bool_value(map, "driver_ir2combo_top_rewritten")
                .unwrap_or(false),
        }),
        M::IrFnToKBoolConeCorpus {
            k,
            max_ir_ops,
            version,
            runtime,
            ..
        } => Kind::IrFnToKBoolConeCorpus(pb::IrFnToKBoolConeCorpusDetails {
            common: Some(common_driver_details(map, version, runtime)?),
            k: u64_value(map, "k")
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(*k),
            max_ir_ops: u64_value(map, "max_ir_ops").or(*max_ir_ops),
            total_manifest_rows: u64_value(map, "total_manifest_rows").unwrap_or(0),
            emitted_cone_files: u64_value(map, "emitted_cone_files").unwrap_or(0),
            deduped_unique_cones: u64_value(map, "deduped_unique_cones").unwrap_or(0),
            cone_entry_count: u64_value(map, "k_bool_cone_entry_count").unwrap_or(0),
            text_id_validation: string(map, "text_id_validation").unwrap_or_default(),
            suggested_actions: u64_value(map, "suggested_actions").unwrap_or(0),
            filtered_out_ir_op_count: u64_value(map, "filtered_out_ir_op_count"),
            output_ir_relpath: string(map, "output_ir_relpath").map(relpath),
        }),
        M::IrFnToMffcCorpus {
            max_mffcs,
            min_internal_non_literal,
            max_frontier_non_literal,
            version,
            runtime,
            ..
        } => Kind::IrFnToMffcCorpus(pb::IrFnToMffcCorpusDetails {
            common: Some(common_driver_details(map, version, runtime)?),
            max_mffcs: u64_value(map, "max_mffcs").or(*max_mffcs),
            min_internal_non_literal: u64_value(map, "min_internal_non_literal")
                .unwrap_or(*min_internal_non_literal),
            max_frontier_non_literal: u64_value(map, "max_frontier_non_literal")
                .or(*max_frontier_non_literal),
            total_manifest_rows: u64_value(map, "total_manifest_rows").unwrap_or(0),
            emitted_mffc_files: u64_value(map, "emitted_mffc_files").unwrap_or(0),
            deduped_unique_mffcs: u64_value(map, "deduped_unique_mffcs").unwrap_or(0),
            mffc_entry_count: u64_value(map, "mffc_entry_count").unwrap_or(0),
            suggested_actions: u64_value(map, "suggested_actions").unwrap_or(0),
        }),
        M::ComboVerilogToYosysAbcAig {
            verilog_top_module_name,
            yosys_script_ref,
            runtime,
            ..
        } => {
            let runtime = map
                .get("yosys_runtime")
                .map(|value| yosys_runtime_from_json(value, "details.yosys_runtime"))
                .transpose()?
                .unwrap_or_else(|| runtime.clone());
            let script_ref = map
                .get("yosys_script_ref")
                .map(|value| script_ref_from_json(value, "details.yosys_script_ref"))
                .transpose()?
                .unwrap_or_else(|| yosys_script_ref.clone());
            Kind::ComboVerilogToYosysAbcAig(pb::ComboVerilogToYosysAbcAigDetails {
                yosys_runtime: Some(yosys_runtime_to_proto(&runtime, "details.yosys_runtime")?),
                yosys_script_ref: Some(script_ref_to_proto(
                    &script_ref,
                    "details.yosys_script_ref",
                )?),
                verilog_top_module_name: string(map, "verilog_top_module_name")
                    .or_else(|| verilog_top_module_name.clone()),
                flow: string(map, "flow").unwrap_or_default(),
            })
        }
        M::AigToYosysAbcAig {
            yosys_script_ref,
            runtime,
            ..
        } => {
            let runtime = map
                .get("yosys_runtime")
                .map(|value| yosys_runtime_from_json(value, "details.yosys_runtime"))
                .transpose()?
                .unwrap_or_else(|| runtime.clone());
            let script_ref = map
                .get("yosys_script_ref")
                .map(|value| script_ref_from_json(value, "details.yosys_script_ref"))
                .transpose()?
                .unwrap_or_else(|| yosys_script_ref.clone());
            Kind::AigToYosysAbcAig(pb::AigToYosysAbcAigDetails {
                yosys_runtime: Some(yosys_runtime_to_proto(&runtime, "details.yosys_runtime")?),
                yosys_script_ref: Some(script_ref_to_proto(
                    &script_ref,
                    "details.yosys_script_ref",
                )?),
                flow: string(map, "flow").unwrap_or_default(),
            })
        }
        M::DriverAigToStats {
            version, runtime, ..
        } => {
            let legacy = map.get("legacy_g8r_source").and_then(Value::as_object);
            let legacy_source = legacy
                .and_then(|legacy| string(legacy, "ir_action_id").map(|id| (legacy, id)))
                .map(|(legacy, ir_action_id)| {
                    let source_runtime = legacy
                        .get("driver_runtime")
                        .map(|v| runtime_from_json(v, "details.legacy_g8r_source.driver_runtime"))
                        .transpose()?;
                    Ok::<_, anyhow::Error>(pb::LegacyG8rStatsSourceDetails {
                        ir_action_id: Some(action_id_to_proto(
                            &ir_action_id,
                            "details.legacy_g8r_source.ir_action_id",
                        )?),
                        fraig: bool_value(legacy, "fraig").unwrap_or(false),
                        source_runtime: source_runtime
                            .as_ref()
                            .map(|v| {
                                driver_runtime_to_proto(
                                    v,
                                    "details.legacy_g8r_source.driver_runtime",
                                )
                            })
                            .transpose()?,
                        source_dso_version: string(legacy, "xlsynth_version")
                            .map(|v| dso_version(&v)),
                    })
                })
                .transpose()?;
            let source_aig_driver_runtime = map
                .get("source_aig_driver_runtime")
                .map(|v| runtime_from_json(v, "details.source_aig_driver_runtime"))
                .transpose()?
                .map(|v| driver_runtime_to_proto(&v, "details.source_aig_driver_runtime"))
                .transpose()?;
            Kind::DriverAigToStats(pb::DriverAigToStatsDetails {
                common: Some(common_driver_details(map, version, runtime)?),
                runtime_dso_version: Some(dso_version(
                    &string(map, "runtime_xlsynth_version").unwrap_or_else(|| version.to_string()),
                )),
                aig_stats_mode: string(map, "aig_stats_mode").unwrap_or_default(),
                legacy_g8r_source: legacy_source,
                legacy_g8r_stats_relpath: string(map, "legacy_g8r_stats_relpath").map(relpath),
                source_aig_driver_runtime,
                source_aig_crate_version_label: string(map, "source_aig_crate_version_label"),
                source_aig_dso_version_label: string(map, "source_aig_dso_version_label"),
                runtime_xlsynth_version: string(map, "runtime_xlsynth_version"),
                stats_runtime_crate_version_label: string(map, "stats_runtime_crate_version_label"),
                stats_runtime_dso_version_label: string(map, "stats_runtime_dso_version_label"),
            })
        }
        M::AigStatDiff {
            opt_ir_action_id,
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
        } => Kind::AigStatDiff(pb::AigStatDiffDetails {
            opt_ir_action_id: Some(action_id_to_proto(
                opt_ir_action_id,
                "details.opt_ir_action_id",
            )?),
            g8r_aig_stats_action_id: Some(action_id_to_proto(
                g8r_aig_stats_action_id,
                "details.g8r_aig_stats_action_id",
            )?),
            yosys_abc_aig_stats_action_id: Some(action_id_to_proto(
                yosys_abc_aig_stats_action_id,
                "details.yosys_abc_aig_stats_action_id",
            )?),
            numeric_delta_count: u64_value(map, "numeric_delta_count").unwrap_or(0),
        }),
    };
    Ok(pb::ActionDetails { kind: Some(kind) })
}

fn insert_common_json(
    map: &mut Map<String, Value>,
    value: &pb::CommonDriverActionDetails,
) -> Result<()> {
    if let Some(runtime) = &value.driver_runtime {
        map.insert(
            "driver_runtime".into(),
            json!(driver_runtime_from_proto(
                runtime,
                "details.driver_runtime"
            )?),
        );
    }
    if let Some(version) = &value.dso_version {
        map.insert(
            "xlsynth_version".into(),
            json!(format!("v{}", version.value)),
        );
    }
    for (key, value) in [
        ("crate_version_label", &value.crate_version_label),
        ("dso_version_label", &value.dso_version_label),
        ("ir_top", &value.ir_top),
        ("driver_subcommand", &value.driver_subcommand),
        (
            "input_ir_fn_structural_hash_source",
            &value.input_ir_fn_structural_hash_source,
        ),
        ("test_label", &value.test_label),
    ] {
        if let Some(value) = value {
            map.insert(key.into(), json!(value));
        }
    }
    if let Some(hash) = &value.input_ir_fn_structural_hash {
        map.insert(
            "input_ir_fn_structural_hash".into(),
            json!(digest_to_hex(hash, "details.input_ir_fn_structural_hash")?),
        );
    }
    if let Some(hash) = &value.output_ir_fn_structural_hash {
        map.insert(
            "output_ir_fn_structural_hash".into(),
            json!(digest_to_hex(hash, "details.output_ir_fn_structural_hash")?),
        );
    }
    if let Some(count) = value.output_ir_op_count {
        map.insert("output_ir_op_count".into(), json!(count));
    }
    if let Some(path) = &value.output_manifest_relpath {
        map.insert("output_manifest_relpath".into(), json!(path.value));
    }
    if let Some(path) = &value.output_ir_relpath {
        map.insert("output_ir_relpath".into(), json!(path.value));
    }
    if let Some(hit) = &value.semantic_cache_hit {
        map.insert(
            "semantic_cache_hit".into(),
            json!({
                "from_action_id": action_id_to_hex(
                    required(&hit.from_action_id, "details.semantic_cache_hit.from_action_id")?,
                    "details.semantic_cache_hit.from_action_id",
                )?,
                "key": "ir_fn_structural_hash",
            }),
        );
    }
    if let Some(value) = value.synthetic {
        map.insert("synthetic".into(), json!(value));
    }
    if let Some(value) = value.ir_top_explicit {
        map.insert("ir_top_explicit".into(), json!(value));
    }
    if let Some(runtime) = &value.driver_ir_aig_equiv_runtime {
        map.insert(
            "driver_ir_aig_equiv_runtime".into(),
            json!(driver_runtime_from_proto(
                runtime,
                "details.driver_ir_aig_equiv_runtime",
            )?),
        );
    }
    for diagnostic in &value.diagnostics {
        if diagnostic.code.is_empty() {
            bail!("details diagnostic code must not be empty");
        }
        map.insert(diagnostic.code.clone(), json!(diagnostic.message));
    }
    Ok(())
}

fn details_from_proto(action: &model::ActionSpec, value: &pb::ActionDetails) -> Result<Value> {
    use model::ActionSpec as M;
    use pb::action_details::Kind;
    let kind = required(&value.kind, "provenance.details.kind")?;
    let mut map = Map::new();
    match (action, kind) {
        (M::ImportIrPackageFile { .. }, Kind::ImportIrPackageFile(details)) => {
            if let Some(value) = &details.source_path {
                map.insert("source_path".into(), json!(value));
            }
            map.insert("import_kind".into(), json!(details.import_kind));
            if let Some(hash) = &details.source_sha256 {
                map.insert(
                    "source_sha256".into(),
                    json!(digest_to_hex(hash, "details.source_sha256")?),
                );
            }
            if let Some(top) = &details.top_fn_name {
                map.insert("ir_top".into(), json!(top));
            }
        }
        (
            M::DownloadAndExtractXlsynthReleaseStdlibTarball { version, .. },
            Kind::DownloadReleaseStdlib(details),
        ) => {
            let mut download = Map::new();
            download.insert("tarball_url".into(), json!(details.tarball_url));
            download.insert("sha256_url".into(), json!(details.sha256_url));
            if let Some(hash) = &details.expected_sha256 {
                download.insert(
                    "expected_sha256".into(),
                    json!(digest_to_hex(hash, "details.download.expected_sha256")?),
                );
            }
            if let Some(hash) = &details.actual_sha256 {
                download.insert(
                    "actual_sha256".into(),
                    json!(digest_to_hex(hash, "details.download.actual_sha256")?),
                );
            }
            map.insert("download".into(), Value::Object(download));
            if let Some(discovery) = &details.function_discovery {
                map.insert(
                    "dslx_list_fns_discovery".into(),
                    discovery_details_from_proto(discovery, version, "details.function_discovery")?,
                );
            }
            if let Some(error) = &details.function_discovery_error {
                map.insert("dslx_list_fns_discovery_error".into(), json!(error));
            }
        }
        (
            M::DownloadAndExtractXlsynthSourceSubtree { version, .. },
            Kind::DownloadSourceSubtree(details),
        ) => {
            let mut download = Map::new();
            download.insert(
                "source_archive_url".into(),
                json!(details.source_archive_url),
            );
            if let Some(hash) = &details.source_archive_sha256 {
                download.insert(
                    "source_archive_sha256".into(),
                    json!(digest_to_hex(
                        hash,
                        "details.download.source_archive_sha256"
                    )?),
                );
            }
            map.insert("download".into(), Value::Object(download));
            if let Some(subtree) = &details.subtree {
                map.insert("subtree".into(), json!(subtree.value));
            }
            map.insert(
                "extracted_file_count".into(),
                json!(details.extracted_file_count),
            );
            if let Some(discovery) = &details.function_discovery {
                map.insert(
                    "dslx_list_fns_discovery".into(),
                    discovery_details_from_proto(discovery, version, "details.function_discovery")?,
                );
            }
            if let Some(error) = &details.function_discovery_error {
                map.insert("dslx_list_fns_discovery_error".into(), json!(error));
            }
        }
        (M::DriverDslxFnToIr { .. }, Kind::DriverDslxFnToIr(details)) => {
            if let Some(common) = &details.common {
                insert_common_json(&mut map, common)?;
            }
            if let Some(path) = &details.dslx_file {
                map.insert("dslx_file".into(), json!(path.value));
            }
            map.insert("dslx_fn_name".into(), json!(details.dslx_fn_name));
            map.insert("dslx_path".into(), json!(details.dslx_path));
            map.insert("dslx_stdlib_path".into(), json!(details.dslx_stdlib_path));
            map.insert("stdlib_source".into(), json!(details.stdlib_source));
        }
        (M::DriverIrToOpt { .. }, Kind::DriverIrToOpt(details)) => {
            if let Some(common) = &details.common {
                insert_common_json(&mut map, common)?;
            }
            if let Some(value) = details.driver_ir_aig_equiv_supported {
                map.insert("driver_ir_aig_equiv_supported".into(), json!(value));
            }
            if let Some(value) = details.driver_ir_aig_equiv_quarantined {
                map.insert("driver_ir_aig_equiv_quarantined".into(), json!(value));
            }
            if let Some(value) = &details.driver_ir_aig_equiv_mode {
                map.insert("driver_ir_aig_equiv_mode".into(), json!(value));
            }
            map.insert(
                "mffc_suggestion_eligible".into(),
                json!(details.mffc_suggestion_eligible),
            );
            map.insert(
                "mffc_suggestion_reason".into(),
                json!(details.mffc_suggestion_reason),
            );
            map.insert(
                "mffc_min_internal_non_literal".into(),
                json!(details.mffc_min_internal_non_literal),
            );
        }
        (M::DriverIrToDelayInfo { .. }, Kind::DriverIrToDelayInfo(details)) => {
            if let Some(common) = &details.common {
                insert_common_json(&mut map, common)?;
            }
            map.insert("delay_model".into(), json!(details.delay_model));
            map.insert("output_format".into(), json!(details.output_format));
            map.insert(
                "delay_info_proto_schema_urls".into(),
                json!({
                    "delay_info_proto": details.delay_info_proto_schema_url,
                    "op_proto": details.op_proto_schema_url,
                }),
            );
        }
        (M::DriverIrEquiv { .. }, Kind::DriverIrEquiv(details)) => {
            if let Some(common) = &details.common {
                insert_common_json(&mut map, common)?;
            }
            map.insert("timeout_secs".into(), json!(details.timeout_seconds));
        }
        (M::DriverIrAigEquiv { .. }, Kind::DriverIrAigEquiv(details)) => {
            if let Some(common) = &details.common {
                insert_common_json(&mut map, common)?;
            }
            map.insert("driver_ir_aig_equiv_mode".into(), json!(details.mode));
            map.insert("timeout_secs".into(), json!(details.timeout_seconds));
        }
        (M::DriverIrToG8rAig { .. }, Kind::DriverIrToG8rAig(details)) => {
            if let Some(common) = &details.common {
                insert_common_json(&mut map, common)?;
            }
            map.insert("fraig".into(), json!(details.fraig));
            map.insert(
                "g8r_lowering_mode_flags".into(),
                json!(details.lowering_mode_flags),
            );
            if let Some(value) = &details.driver_ir2g8r_cli_mode {
                map.insert("driver_ir2g8r_cli_mode".into(), json!(value));
            }
            if let Some(value) = details.driver_ir2g8r_passed_top {
                map.insert("driver_ir2g8r_passed_top".into(), json!(value));
            }
            if let Some(value) = details.driver_ir_aig_equiv_supported {
                map.insert("driver_ir_aig_equiv_supported".into(), json!(value));
            }
            if let Some(value) = details.driver_ir_aig_equiv_quarantined {
                map.insert("driver_ir_aig_equiv_quarantined".into(), json!(value));
            }
            if let Some(value) = &details.driver_ir_aig_equiv_mode {
                map.insert("driver_ir_aig_equiv_mode".into(), json!(value));
            }
            if let Some(value) = &details.legacy_g8r_stats_relpath {
                map.insert("legacy_g8r_stats_relpath".into(), json!(value.value));
            }
            if let (Some(batch_size), Some(member_index)) =
                (details.batch_size, details.member_index)
            {
                map.insert(
                    "batch_execution".into(),
                    json!({
                        "kind": "driver_ir_to_g8r_aig",
                        "batch_size": batch_size,
                        "member_index": member_index,
                    }),
                );
            }
            if let Some(value) = details.prepared_ir_supported {
                map.insert("driver_ir2g8r_prepared_ir_supported".into(), json!(value));
            }
            if let Some(value) = &details.prepared_ir_relpath {
                map.insert(
                    "driver_ir2g8r_prepared_ir_relpath".into(),
                    json!(value.value),
                );
            }
            if let Some(value) = &details.prepared_ir_reason {
                map.insert("driver_ir2g8r_prepared_ir_reason".into(), json!(value));
            }
            if let Some(value) = &details.output_kind {
                map.insert("driver_ir2g8r_output_kind".into(), json!(value));
            }
            if let Some(value) = &details.top_ignored_reason {
                map.insert("driver_ir2g8r_top_ignored_reason".into(), json!(value));
            }
        }
        (M::IrFnToCombinationalVerilog { .. }, Kind::IrFnToCombinationalVerilog(details)) => {
            if let Some(common) = &details.common {
                insert_common_json(&mut map, common)?;
            }
            map.insert(
                "use_system_verilog".into(),
                json!(details.use_system_verilog),
            );
            map.insert(
                "driver_ir2combo_top_rewritten".into(),
                json!(details.driver_ir2combo_top_rewritten),
            );
        }
        (M::IrFnToKBoolConeCorpus { .. }, Kind::IrFnToKBoolConeCorpus(details)) => {
            if let Some(common) = &details.common {
                insert_common_json(&mut map, common)?;
            }
            map.insert("k".into(), json!(details.k));
            if let Some(value) = details.max_ir_ops {
                map.insert("max_ir_ops".into(), json!(value));
            }
            map.insert(
                "total_manifest_rows".into(),
                json!(details.total_manifest_rows),
            );
            map.insert(
                "emitted_cone_files".into(),
                json!(details.emitted_cone_files),
            );
            map.insert(
                "deduped_unique_cones".into(),
                json!(details.deduped_unique_cones),
            );
            map.insert(
                "k_bool_cone_entry_count".into(),
                json!(details.cone_entry_count),
            );
            map.insert(
                "text_id_validation".into(),
                json!(details.text_id_validation),
            );
            map.insert("suggested_actions".into(), json!(details.suggested_actions));
        }
        (M::IrFnToMffcCorpus { .. }, Kind::IrFnToMffcCorpus(details)) => {
            if let Some(common) = &details.common {
                insert_common_json(&mut map, common)?;
            }
            if let Some(value) = details.max_mffcs {
                map.insert("max_mffcs".into(), json!(value));
            }
            map.insert(
                "min_internal_non_literal".into(),
                json!(details.min_internal_non_literal),
            );
            if let Some(value) = details.max_frontier_non_literal {
                map.insert("max_frontier_non_literal".into(), json!(value));
            }
            map.insert(
                "total_manifest_rows".into(),
                json!(details.total_manifest_rows),
            );
            map.insert(
                "emitted_mffc_files".into(),
                json!(details.emitted_mffc_files),
            );
            map.insert(
                "deduped_unique_mffcs".into(),
                json!(details.deduped_unique_mffcs),
            );
            map.insert("mffc_entry_count".into(), json!(details.mffc_entry_count));
            map.insert("suggested_actions".into(), json!(details.suggested_actions));
        }
        (M::ComboVerilogToYosysAbcAig { .. }, Kind::ComboVerilogToYosysAbcAig(details)) => {
            if let Some(runtime) = &details.yosys_runtime {
                map.insert(
                    "yosys_runtime".into(),
                    json!(yosys_runtime_from_proto(runtime, "details.yosys_runtime")?),
                );
            }
            if let Some(script) = &details.yosys_script_ref {
                map.insert(
                    "yosys_script_ref".into(),
                    json!(script_ref_from_proto(script, "details.yosys_script_ref")?),
                );
            }
            if let Some(value) = &details.verilog_top_module_name {
                map.insert("verilog_top_module_name".into(), json!(value));
            }
            map.insert("flow".into(), json!(details.flow));
        }
        (M::AigToYosysAbcAig { .. }, Kind::AigToYosysAbcAig(details)) => {
            if let Some(runtime) = &details.yosys_runtime {
                map.insert(
                    "yosys_runtime".into(),
                    json!(yosys_runtime_from_proto(runtime, "details.yosys_runtime")?),
                );
            }
            if let Some(script) = &details.yosys_script_ref {
                map.insert(
                    "yosys_script_ref".into(),
                    json!(script_ref_from_proto(script, "details.yosys_script_ref")?),
                );
            }
            map.insert("flow".into(), json!(details.flow));
        }
        (M::DriverAigToStats { .. }, Kind::DriverAigToStats(details)) => {
            if let Some(common) = &details.common {
                insert_common_json(&mut map, common)?;
            }
            map.insert("aig_stats_mode".into(), json!(details.aig_stats_mode));
            if let Some(path) = &details.legacy_g8r_stats_relpath {
                map.insert("legacy_g8r_stats_relpath".into(), json!(path.value));
            }
            if let Some(runtime) = &details.source_aig_driver_runtime {
                map.insert(
                    "source_aig_driver_runtime".into(),
                    json!(driver_runtime_from_proto(
                        runtime,
                        "details.source_aig_driver_runtime",
                    )?),
                );
            }
            for (key, value) in [
                (
                    "source_aig_crate_version_label",
                    &details.source_aig_crate_version_label,
                ),
                (
                    "source_aig_dso_version_label",
                    &details.source_aig_dso_version_label,
                ),
                ("runtime_xlsynth_version", &details.runtime_xlsynth_version),
                (
                    "stats_runtime_crate_version_label",
                    &details.stats_runtime_crate_version_label,
                ),
                (
                    "stats_runtime_dso_version_label",
                    &details.stats_runtime_dso_version_label,
                ),
            ] {
                if let Some(value) = value {
                    map.insert(key.into(), json!(value));
                }
            }
        }
        (M::AigStatDiff { .. }, Kind::AigStatDiff(details)) => {
            for (key, value) in [
                ("opt_ir_action_id", &details.opt_ir_action_id),
                ("g8r_aig_stats_action_id", &details.g8r_aig_stats_action_id),
                (
                    "yosys_abc_aig_stats_action_id",
                    &details.yosys_abc_aig_stats_action_id,
                ),
            ] {
                map.insert(
                    key.into(),
                    json!(action_id_to_hex(
                        required(value, &format!("details.{key}"))?,
                        &format!("details.{key}"),
                    )?),
                );
            }
            map.insert(
                "numeric_delta_count".into(),
                json!(details.numeric_delta_count),
            );
        }
        _ => bail!("provenance action/details oneof kinds do not match"),
    }
    Ok(Value::Object(map))
}

fn validate_action_id_matches(
    action_id: &str,
    action: &model::ActionSpec,
    field: &str,
) -> Result<()> {
    let expected = crate::proto::compute_model_action_id_v2(action)?.to_hex();
    if action_id != expected {
        bail!(
            "{field} does not match the V2 action fingerprint: expected {expected}, got {action_id}"
        );
    }
    Ok(())
}

pub(crate) fn encode_provenance(value: &model::Provenance) -> Result<Vec<u8>> {
    if value.schema_version != PROVENANCE_RECORD_VERSION {
        bail!(
            "provenance has unsupported record version {}; expected {}",
            value.schema_version,
            PROVENANCE_RECORD_VERSION
        );
    }
    validate_action_id_matches(&value.action_id, &value.action, "provenance.action_id")?;
    if value.output_artifact.action_id != value.action_id {
        bail!("provenance.output_artifact.action_id must match provenance.action_id");
    }
    let mut output_paths = BTreeSet::new();
    let output_files = value
        .output_files
        .iter()
        .map(|file| {
            if file.path.is_empty()
                || file.path.starts_with('/')
                || file
                    .path
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == "..")
            {
                bail!("output file path is not normalized: {:?}", file.path);
            }
            if !output_paths.insert(file.path.clone()) {
                bail!("duplicate output file path: {}", file.path);
            }
            Ok(pb::OutputFile {
                path: Some(relpath(file.path.clone())),
                bytes: file.bytes,
                sha256: Some(digest_from_hex(&file.sha256, "output_file.sha256")?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let suggested_next_actions = value
        .suggested_next_actions
        .iter()
        .map(|suggestion| {
            validate_action_id_matches(
                &suggestion.action_id,
                &suggestion.action,
                "suggested_action.action_id",
            )?;
            Ok(pb::SuggestedAction {
                reason: suggestion.reason.clone(),
                action_id: Some(action_id_to_proto(
                    &suggestion.action_id,
                    "suggested_action.action_id",
                )?),
                action: Some(action_spec_to_proto(&suggestion.action)?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(pb::Provenance {
        record_version: PROVENANCE_RECORD_VERSION,
        action_id: Some(action_id_to_proto(
            &value.action_id,
            "provenance.action_id",
        )?),
        created_at: Some(timestamp_to_proto(&value.created_utc)),
        action: Some(action_spec_to_proto(&value.action)?),
        dependencies: value
            .dependencies
            .iter()
            .map(artifact_ref_to_proto)
            .collect::<Result<Vec<_>>>()?,
        output_artifact: Some(artifact_ref_to_proto(&value.output_artifact)?),
        output_files,
        commands: value
            .commands
            .iter()
            .map(|command| pb::CommandTrace {
                argv: command.argv.clone(),
                exit_code: command.exit_code,
            })
            .collect(),
        details: Some(details_to_proto(&value.action, &value.details)?),
        suggested_next_actions,
    }
    .encode_to_vec())
}

pub(crate) fn decode_provenance(bytes: &[u8]) -> Result<model::Provenance> {
    let value = pb::Provenance::decode(bytes).context("decoding Provenance")?;
    if value.record_version != PROVENANCE_RECORD_VERSION {
        bail!(
            "provenance has unsupported record version {}; expected {}",
            value.record_version,
            PROVENANCE_RECORD_VERSION
        );
    }
    let action_id = action_id_to_hex(
        required(&value.action_id, "provenance.action_id")?,
        "provenance.action_id",
    )?;
    let action = action_spec_from_proto(required(&value.action, "provenance.action")?)?;
    validate_action_id_matches(&action_id, &action, "provenance.action_id")?;
    let output_artifact = artifact_ref_from_proto(required(
        &value.output_artifact,
        "provenance.output_artifact",
    )?)?;
    if output_artifact.action_id != action_id {
        bail!("provenance.output_artifact.action_id must match provenance.action_id");
    }
    let mut output_paths = BTreeSet::new();
    let output_files = value
        .output_files
        .iter()
        .map(|file| {
            let path = required(&file.path, "output_file.path")?.value.clone();
            if path.is_empty()
                || path.starts_with('/')
                || path
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == "..")
            {
                bail!("output file path is not normalized: {path:?}");
            }
            if !output_paths.insert(path.clone()) {
                bail!("duplicate output file path: {path}");
            }
            Ok(model::OutputFile {
                path,
                bytes: file.bytes,
                sha256: digest_to_hex(
                    required(&file.sha256, "output_file.sha256")?,
                    "output_file.sha256",
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let suggested_next_actions = value
        .suggested_next_actions
        .iter()
        .map(|suggestion| {
            let suggestion_action =
                action_spec_from_proto(required(&suggestion.action, "suggested_action.action")?)?;
            let suggestion_action_id = action_id_to_hex(
                required(&suggestion.action_id, "suggested_action.action_id")?,
                "suggested_action.action_id",
            )?;
            validate_action_id_matches(
                &suggestion_action_id,
                &suggestion_action,
                "suggested_action.action_id",
            )?;
            Ok(model::SuggestedAction {
                reason: suggestion.reason.clone(),
                action_id: suggestion_action_id,
                action: suggestion_action,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(model::Provenance {
        schema_version: value.record_version,
        action_id,
        created_utc: timestamp_from_proto(&value.created_at, "provenance.created_at")?,
        details: details_from_proto(&action, required(&value.details, "provenance.details")?)?,
        action,
        dependencies: value
            .dependencies
            .iter()
            .map(artifact_ref_from_proto)
            .collect::<Result<Vec<_>>>()?,
        output_artifact,
        output_files,
        commands: value
            .commands
            .into_iter()
            .map(|command| model::CommandTrace {
                argv: command.argv,
                exit_code: command.exit_code,
            })
            .collect(),
        suggested_next_actions,
    })
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    use super::*;

    fn sample_provenance() -> model::Provenance {
        let action = model::ActionSpec::ImportIrPackageFile {
            source_sha256: "11".repeat(32),
            top_fn_name: Some("main".into()),
        };
        let action_id = crate::proto::compute_model_action_id_v2(&action)
            .expect("action id")
            .to_hex();
        model::Provenance {
            schema_version: PROVENANCE_RECORD_VERSION,
            action_id: action_id.clone(),
            created_utc: Utc.with_ymd_and_hms(2026, 8, 28, 1, 2, 3).unwrap(),
            action,
            dependencies: Vec::new(),
            output_artifact: model::ArtifactRef {
                action_id,
                artifact_type: model::ArtifactType::IrPackageFile,
                relpath: "payload/package.ir".into(),
            },
            output_files: vec![model::OutputFile {
                path: "package.ir".into(),
                bytes: 3,
                sha256: "22".repeat(32),
            }],
            commands: vec![model::CommandTrace {
                argv: vec!["xlsynth-driver".into(), "dslx2ir".into()],
                exit_code: 0,
            }],
            details: json!({"source_path": "/input/package.ir", "import_kind": "local_ir_file"}),
            suggested_next_actions: Vec::new(),
        }
    }

    fn sample_discovery_details(failed_file: bool) -> Value {
        let source_runtime = model::DriverRuntimeSpec {
            driver_version: "0.47.0".into(),
            release_platform: "ubuntu2004".into(),
            docker_image: "xlsynth-bvc-driver:0.47.0".into(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".into(),
        };
        let enumeration_runtime = model::DriverRuntimeSpec {
            driver_version: "0.48.0".into(),
            release_platform: "ubuntu2004".into(),
            docker_image: "xlsynth-bvc-driver:0.48.0".into(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".into(),
        };
        let failed_dslx_files = if failed_file {
            vec!["xls/dslx/stdlib/bad.x".to_string()]
        } else {
            Vec::new()
        };
        json!({
            "driver_runtime": source_runtime,
            "enumeration_runtime": enumeration_runtime,
            "enumeration_runtime_xlsynth_version": "v0.0.200",
            "enumeration_runtime_overrides_source": true,
            "crate_version_label": "crate:v0.47.0",
            "dso_version_label": "dso:v0.0.199",
            "enumeration_runtime_dso_version_label": "dso:v0.0.200",
            "dslx_path": "/inputs/subtree",
            "dslx_stdlib_path": "/inputs/subtree/xls/dslx/stdlib",
            "stdlib_source": "downloaded",
            "scanned_dslx_files": 12,
            "listed_functions": 8,
            "concrete_functions": 7,
            "failed_dslx_files_count": failed_dslx_files.len(),
            "failed_dslx_files": failed_dslx_files,
            "suggested_actions": 7,
        })
    }

    fn sample_download_provenance(action: model::ActionSpec, details: Value) -> model::Provenance {
        let action_id = crate::proto::compute_model_action_id_v2(&action)
            .expect("action id")
            .to_hex();
        model::Provenance {
            schema_version: PROVENANCE_RECORD_VERSION,
            action_id: action_id.clone(),
            created_utc: Utc.with_ymd_and_hms(2026, 8, 28, 1, 2, 3).unwrap(),
            action,
            dependencies: Vec::new(),
            output_artifact: model::ArtifactRef {
                action_id,
                artifact_type: model::ArtifactType::DslxFileSubtree,
                relpath: "payload".into(),
            },
            output_files: Vec::new(),
            commands: Vec::new(),
            details,
            suggested_next_actions: Vec::new(),
        }
    }

    #[test]
    fn download_discovery_details_round_trip_for_both_root_actions() {
        let stdlib_details = json!({
            "download": {
                "tarball_url": "https://example.test/dslx_stdlib.tar.gz",
                "sha256_url": "https://example.test/dslx_stdlib.tar.gz.sha256",
                "expected_sha256": "33".repeat(32),
                "actual_sha256": "33".repeat(32),
            },
            "dslx_list_fns_discovery": sample_discovery_details(false),
        });
        let subtree_details = json!({
            "download": {
                "source_archive_url": "https://example.test/source.tar.gz",
                "source_archive_sha256": "44".repeat(32),
            },
            "subtree": "xls/dslx/stdlib",
            "extracted_file_count": 42,
            "dslx_list_fns_discovery": sample_discovery_details(true),
        });
        let cases = [
            (
                sample_download_provenance(
                    model::ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                        version: "v0.0.199".into(),
                        discovery_runtime: None,
                    },
                    stdlib_details,
                ),
                "ok",
            ),
            (
                sample_download_provenance(
                    model::ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
                        version: "v0.0.199".into(),
                        subtree: "xls/dslx/stdlib".into(),
                        discovery_runtime: None,
                    },
                    subtree_details,
                ),
                "partial",
            ),
        ];
        for (original, expected_badge) in cases {
            let encoded = encode_provenance(&original).expect("encode discovery provenance");
            let decoded = decode_provenance(&encoded).expect("decode discovery provenance");
            assert_eq!(decoded.details, original.details);
            assert_eq!(
                crate::query::stdlib_enumeration_status_from_provenance(&decoded).badge_label(),
                expected_badge
            );
        }
    }

    #[test]
    fn provenance_round_trips_binary_proto() {
        let original = sample_provenance();
        let encoded = encode_provenance(&original).expect("encode");
        assert_ne!(encoded.first(), Some(&b'{'));
        let decoded = decode_provenance(&encoded).expect("decode");
        assert_eq!(decoded.action_id, original.action_id);
        assert_eq!(
            decoded.output_files[0].sha256,
            original.output_files[0].sha256
        );
        assert_eq!(decoded.details["import_kind"], "local_ir_file");
    }

    #[test]
    fn provenance_rejects_wrong_action_id_and_truncation() {
        let mut value = sample_provenance();
        value.action_id = "00".repeat(32);
        assert!(encode_provenance(&value).is_err());
        assert!(decode_provenance(&[0x0a, 0xff]).is_err());
    }

    #[test]
    fn provenance_rejects_wrong_version_and_missing_details() {
        let encoded = encode_provenance(&sample_provenance()).expect("encode");
        let mut wire = pb::Provenance::decode(encoded.as_slice()).expect("decode wire");
        wire.record_version = 99;
        assert!(decode_provenance(&wire.encode_to_vec()).is_err());
        wire.record_version = PROVENANCE_RECORD_VERSION;
        wire.details = None;
        assert!(decode_provenance(&wire.encode_to_vec()).is_err());
    }
}
