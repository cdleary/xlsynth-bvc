// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use prost::Message;
use sha2::{Digest, Sha256};

use crate::model;
use crate::proto::v1 as pb;

pub(crate) const ACTION_IDENTITY_SCHEMA_VERSION: u32 = 2;
const ACTION_ID_V2_DOMAIN: &[u8] = b"xlsynth-bvc/action/v2\0";
const DIGEST_BYTE_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedActionSpec(pb::ActionSpec);

impl ValidatedActionSpec {
    pub(crate) fn as_proto(&self) -> &pb::ActionSpec {
        &self.0
    }
}

impl TryFrom<pb::ActionSpec> for ValidatedActionSpec {
    type Error = anyhow::Error;

    fn try_from(value: pb::ActionSpec) -> Result<Self> {
        validate_action_spec(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&model::ActionSpec> for ValidatedActionSpec {
    type Error = anyhow::Error;

    fn try_from(value: &model::ActionSpec) -> Result<Self> {
        let action = action_spec_from_model(value)?;
        Self::try_from(action)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ActionIdV2([u8; DIGEST_BYTE_LEN]);

impl ActionIdV2 {
    pub(crate) fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

pub(crate) fn action_fingerprint_bytes(action: &ValidatedActionSpec) -> Vec<u8> {
    pb::ActionFingerprint {
        identity_schema_version: ACTION_IDENTITY_SCHEMA_VERSION,
        action: Some(action.as_proto().clone()),
    }
    .encode_to_vec()
}

pub(crate) fn compute_action_id_v2(action: &ValidatedActionSpec) -> ActionIdV2 {
    let fingerprint = action_fingerprint_bytes(action);
    let mut hasher = Sha256::new();
    hasher.update(ACTION_ID_V2_DOMAIN);
    hasher.update(fingerprint);
    ActionIdV2(hasher.finalize().into())
}

pub(crate) fn compute_model_action_id_v2(action: &model::ActionSpec) -> Result<ActionIdV2> {
    let validated = ValidatedActionSpec::try_from(action)?;
    Ok(compute_action_id_v2(&validated))
}

fn required<'a, T>(value: &'a Option<T>, field: &str) -> Result<&'a T> {
    value
        .as_ref()
        .with_context(|| format!("missing required protobuf field {field}"))
}

fn validate_nonempty(value: &str, field: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    if value.trim() != value {
        bail!("{field} must not have leading or trailing whitespace");
    }
    if value.contains('\0') {
        bail!("{field} must not contain NUL");
    }
    Ok(())
}

fn validate_optional_identifier(value: &Option<String>, field: &str) -> Result<()> {
    if let Some(value) = value {
        validate_nonempty(value, field)?;
    }
    Ok(())
}

fn normalize_version(value: &str, field: &str) -> Result<String> {
    validate_nonempty(value, field)?;
    let normalized = value.strip_prefix('v').unwrap_or(value);
    let (base, suffix) = match normalized.split_once('-') {
        Some((base, suffix)) => (base, Some(suffix)),
        None => (normalized, None),
    };
    let parts: Vec<&str> = base.split('.').collect();
    if parts.len() != 3
        || parts
            .iter()
            .any(|part| part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()))
    {
        bail!("{field} must be a numeric X.Y.Z version, got {value:?}");
    }
    if let Some(suffix) = suffix
        && (suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()))
    {
        bail!("{field} suffix must be numeric, got {value:?}");
    }
    Ok(normalized.to_string())
}

fn validate_canonical_version(value: &str, field: &str) -> Result<()> {
    let normalized = normalize_version(value, field)?;
    if normalized != value {
        bail!("{field} must omit the leading v, got {value:?}");
    }
    Ok(())
}

fn normalize_relpath(value: &str, field: &str) -> Result<String> {
    validate_nonempty(value, field)?;
    let normalized = value.replace('\\', "/");
    if normalized.starts_with('/')
        || normalized.ends_with('/')
        || normalized.as_bytes().get(1) == Some(&b':')
    {
        bail!("{field} must be a relative normalized path, got {value:?}");
    }
    if normalized
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        bail!("{field} contains an empty, dot, or parent component: {value:?}");
    }
    Ok(normalized)
}

fn validate_canonical_relpath(value: &pb::NormalizedRelpath, field: &str) -> Result<()> {
    let normalized = normalize_relpath(&value.value, field)?;
    if normalized != value.value {
        bail!(
            "{field} must use normalized / separators, got {:?}",
            value.value
        );
    }
    Ok(())
}

pub(crate) fn digest_from_hex(value: &str, field: &str) -> Result<pb::Sha256Digest> {
    let bytes = hex::decode(value).with_context(|| format!("decoding {field} as hexadecimal"))?;
    if bytes.len() != DIGEST_BYTE_LEN {
        bail!(
            "{field} must decode to {DIGEST_BYTE_LEN} bytes, got {}",
            bytes.len()
        );
    }
    Ok(pb::Sha256Digest { value: bytes })
}

fn action_id_from_hex(value: &str, field: &str) -> Result<pb::ActionId> {
    let bytes = hex::decode(value).with_context(|| format!("decoding {field} as hexadecimal"))?;
    if bytes.len() != DIGEST_BYTE_LEN {
        bail!(
            "{field} must decode to {DIGEST_BYTE_LEN} bytes, got {}",
            bytes.len()
        );
    }
    Ok(pb::ActionId { value: bytes })
}

fn validate_digest(value: &pb::Sha256Digest, field: &str) -> Result<()> {
    if value.value.len() != DIGEST_BYTE_LEN {
        bail!(
            "{field} must contain {DIGEST_BYTE_LEN} bytes, got {}",
            value.value.len()
        );
    }
    Ok(())
}

fn validate_action_id(value: &pb::ActionId, field: &str) -> Result<()> {
    if value.value.len() != DIGEST_BYTE_LEN {
        bail!(
            "{field} must contain {DIGEST_BYTE_LEN} bytes, got {}",
            value.value.len()
        );
    }
    Ok(())
}

fn dso_version(value: &str, field: &str) -> Result<pb::DsoVersion> {
    Ok(pb::DsoVersion {
        value: normalize_version(value, field)?,
    })
}

fn crate_version(value: &str, field: &str) -> Result<pb::CrateVersion> {
    Ok(pb::CrateVersion {
        value: normalize_version(value, field)?,
    })
}

fn relpath(value: &str, field: &str) -> Result<pb::NormalizedRelpath> {
    Ok(pb::NormalizedRelpath {
        value: normalize_relpath(value, field)?,
    })
}

pub(crate) fn driver_runtime_to_proto(
    value: &model::DriverRuntimeSpec,
    field: &str,
) -> Result<pb::DriverRuntimeSpec> {
    Ok(pb::DriverRuntimeSpec {
        driver_crate_version: Some(crate_version(
            &value.driver_version,
            &format!("{field}.driver_crate_version"),
        )?),
        release_platform: value.release_platform.clone(),
        docker_image: value.docker_image.clone(),
        dockerfile: Some(relpath(&value.dockerfile, &format!("{field}.dockerfile"))?),
        dockerfile_sha256: Some(digest_from_hex(
            &value.dockerfile_sha256,
            &format!("{field}.dockerfile_sha256"),
        )?),
        docker_image_id: Some(digest_from_hex(
            &value.docker_image_id,
            &format!("{field}.docker_image_id"),
        )?),
    })
}

pub(crate) fn yosys_runtime_to_proto(
    value: &model::YosysRuntimeSpec,
    field: &str,
) -> Result<pb::YosysRuntimeSpec> {
    Ok(pb::YosysRuntimeSpec {
        docker_image: value.docker_image.clone(),
        dockerfile: Some(relpath(&value.dockerfile, &format!("{field}.dockerfile"))?),
        upstream_commit: value.upstream_commit.clone(),
        dockerfile_sha256: Some(digest_from_hex(
            &value.dockerfile_sha256,
            &format!("{field}.dockerfile_sha256"),
        )?),
        docker_image_id: Some(digest_from_hex(
            &value.docker_image_id,
            &format!("{field}.docker_image_id"),
        )?),
    })
}

pub(crate) fn script_ref_to_proto(value: &model::ScriptRef, field: &str) -> Result<pb::ScriptRef> {
    Ok(pb::ScriptRef {
        path: Some(relpath(&value.path, &format!("{field}.path"))?),
        sha256: Some(digest_from_hex(&value.sha256, &format!("{field}.sha256"))?),
    })
}

fn validate_driver_runtime(value: &pb::DriverRuntimeSpec, field: &str) -> Result<()> {
    let crate_version = required(
        &value.driver_crate_version,
        &format!("{field}.driver_crate_version"),
    )?;
    validate_canonical_version(
        &crate_version.value,
        &format!("{field}.driver_crate_version.value"),
    )?;
    validate_nonempty(
        &value.release_platform,
        &format!("{field}.release_platform"),
    )?;
    validate_nonempty(&value.docker_image, &format!("{field}.docker_image"))?;
    validate_canonical_relpath(
        required(&value.dockerfile, &format!("{field}.dockerfile"))?,
        &format!("{field}.dockerfile.value"),
    )?;
    validate_digest(
        required(
            &value.dockerfile_sha256,
            &format!("{field}.dockerfile_sha256"),
        )?,
        &format!("{field}.dockerfile_sha256"),
    )?;
    validate_digest(
        required(&value.docker_image_id, &format!("{field}.docker_image_id"))?,
        &format!("{field}.docker_image_id"),
    )
}

fn validate_yosys_runtime(value: &pb::YosysRuntimeSpec, field: &str) -> Result<()> {
    validate_nonempty(&value.docker_image, &format!("{field}.docker_image"))?;
    validate_canonical_relpath(
        required(&value.dockerfile, &format!("{field}.dockerfile"))?,
        &format!("{field}.dockerfile.value"),
    )?;
    validate_digest(
        required(
            &value.dockerfile_sha256,
            &format!("{field}.dockerfile_sha256"),
        )?,
        &format!("{field}.dockerfile_sha256"),
    )?;
    validate_digest(
        required(&value.docker_image_id, &format!("{field}.docker_image_id"))?,
        &format!("{field}.docker_image_id"),
    )?;
    let commit = required(&value.upstream_commit, &format!("{field}.upstream_commit"))?;
    if commit.len() != 40 || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{field}.upstream_commit must be a full 40-character hexadecimal commit");
    }
    Ok(())
}

fn validate_dso_version(value: &Option<pb::DsoVersion>, field: &str) -> Result<()> {
    let value = required(value, field)?;
    validate_canonical_version(&value.value, &format!("{field}.value"))
}

fn validate_script_ref(value: &pb::ScriptRef, field: &str) -> Result<()> {
    validate_canonical_relpath(
        required(&value.path, &format!("{field}.path"))?,
        &format!("{field}.path.value"),
    )?;
    validate_digest(
        required(&value.sha256, &format!("{field}.sha256"))?,
        &format!("{field}.sha256.value"),
    )
}

pub(crate) fn validate_action_spec(action: &pb::ActionSpec) -> Result<()> {
    use pb::action_spec::Kind;

    let kind = action
        .kind
        .as_ref()
        .context("ActionSpec.kind must be present")?;
    match kind {
        Kind::ImportIrPackageFile(value) => {
            validate_digest(
                required(&value.source_sha256, "import_ir_package_file.source_sha256")?,
                "import_ir_package_file.source_sha256.value",
            )?;
            validate_optional_identifier(&value.top_fn_name, "import_ir_package_file.top_fn_name")
        }
        Kind::DownloadAndExtractXlsynthReleaseStdlibTarball(value) => {
            validate_dso_version(&value.dso_version, "download_release_stdlib.dso_version")?;
            validate_digest(
                required(
                    &value.stdlib_tarball_sha256,
                    "download_release_stdlib.stdlib_tarball_sha256",
                )?,
                "download_release_stdlib.stdlib_tarball_sha256.value",
            )?;
            if let Some(runtime) = &value.discovery_runtime {
                validate_driver_runtime(runtime, "download_release_stdlib.discovery_runtime")?;
            }
            Ok(())
        }
        Kind::DownloadAndExtractXlsynthSourceSubtree(value) => {
            validate_dso_version(&value.dso_version, "download_source_subtree.dso_version")?;
            if value.source_commit.len() != 40
                || !value
                    .source_commit
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                bail!(
                    "download_source_subtree.source_commit must be a full 40-character hexadecimal commit"
                );
            }
            validate_canonical_relpath(
                required(&value.subtree, "download_source_subtree.subtree")?,
                "download_source_subtree.subtree.value",
            )?;
            if let Some(runtime) = &value.discovery_runtime {
                validate_driver_runtime(runtime, "download_source_subtree.discovery_runtime")?;
            }
            Ok(())
        }
        Kind::DriverDslxFnToIr(value) => {
            validate_action_id(
                required(
                    &value.dslx_subtree_action_id,
                    "driver_dslx_fn_to_ir.dslx_subtree_action_id",
                )?,
                "driver_dslx_fn_to_ir.dslx_subtree_action_id.value",
            )?;
            validate_canonical_relpath(
                required(&value.dslx_file, "driver_dslx_fn_to_ir.dslx_file")?,
                "driver_dslx_fn_to_ir.dslx_file.value",
            )?;
            validate_nonempty(&value.dslx_fn_name, "driver_dslx_fn_to_ir.dslx_fn_name")?;
            validate_dso_version(&value.dso_version, "driver_dslx_fn_to_ir.dso_version")?;
            validate_driver_runtime(
                required(&value.runtime, "driver_dslx_fn_to_ir.runtime")?,
                "driver_dslx_fn_to_ir.runtime",
            )
        }
        Kind::DriverIrToOpt(value) => validate_ir_action_common(
            &value.ir_action_id,
            &value.top_fn_name,
            &value.dso_version,
            &value.runtime,
            "driver_ir_to_opt",
        ),
        Kind::DriverIrToDelayInfo(value) => {
            validate_ir_action_common(
                &value.ir_action_id,
                &value.top_fn_name,
                &value.dso_version,
                &value.runtime,
                "driver_ir_to_delay_info",
            )?;
            validate_nonempty(&value.delay_model, "driver_ir_to_delay_info.delay_model")?;
            validate_nonempty(
                &value.output_format,
                "driver_ir_to_delay_info.output_format",
            )
        }
        Kind::DriverIrEquiv(value) => {
            validate_action_id(
                required(&value.lhs_ir_action_id, "driver_ir_equiv.lhs_ir_action_id")?,
                "driver_ir_equiv.lhs_ir_action_id.value",
            )?;
            validate_action_id(
                required(&value.rhs_ir_action_id, "driver_ir_equiv.rhs_ir_action_id")?,
                "driver_ir_equiv.rhs_ir_action_id.value",
            )?;
            validate_optional_identifier(&value.top_fn_name, "driver_ir_equiv.top_fn_name")?;
            validate_dso_version(&value.dso_version, "driver_ir_equiv.dso_version")?;
            validate_driver_runtime(
                required(&value.runtime, "driver_ir_equiv.runtime")?,
                "driver_ir_equiv.runtime",
            )
        }
        Kind::DriverIrAigEquiv(value) => {
            validate_action_id(
                required(&value.ir_action_id, "driver_ir_aig_equiv.ir_action_id")?,
                "driver_ir_aig_equiv.ir_action_id.value",
            )?;
            validate_action_id(
                required(&value.aig_action_id, "driver_ir_aig_equiv.aig_action_id")?,
                "driver_ir_aig_equiv.aig_action_id.value",
            )?;
            validate_optional_identifier(&value.top_fn_name, "driver_ir_aig_equiv.top_fn_name")?;
            validate_dso_version(&value.dso_version, "driver_ir_aig_equiv.dso_version")?;
            validate_driver_runtime(
                required(&value.runtime, "driver_ir_aig_equiv.runtime")?,
                "driver_ir_aig_equiv.runtime",
            )
        }
        Kind::DriverIrToG8rAig(value) => {
            validate_ir_action_common(
                &value.ir_action_id,
                &value.top_fn_name,
                &value.dso_version,
                &value.runtime,
                "driver_ir_to_g8r_aig",
            )?;
            match pb::G8rLoweringMode::try_from(value.lowering_mode) {
                Ok(pb::G8rLoweringMode::Default)
                | Ok(pb::G8rLoweringMode::FrontendNoPrepRewrite) => Ok(()),
                Ok(pb::G8rLoweringMode::Unspecified) => {
                    bail!("driver_ir_to_g8r_aig.lowering_mode must be specified")
                }
                Err(_) => bail!(
                    "driver_ir_to_g8r_aig.lowering_mode is unknown: {}",
                    value.lowering_mode
                ),
            }
        }
        Kind::IrFnToCombinationalVerilog(value) => validate_ir_action_common(
            &value.ir_action_id,
            &value.top_fn_name,
            &value.dso_version,
            &value.runtime,
            "ir_fn_to_combinational_verilog",
        ),
        Kind::IrFnToKBoolConeCorpus(value) => {
            validate_ir_action_common(
                &value.ir_action_id,
                &value.top_fn_name,
                &value.dso_version,
                &value.runtime,
                "ir_fn_to_k_bool_cone_corpus",
            )?;
            if value.k == 0 {
                bail!("ir_fn_to_k_bool_cone_corpus.k must be greater than zero");
            }
            Ok(())
        }
        Kind::IrFnToMffcCorpus(value) => validate_ir_action_common(
            &value.ir_action_id,
            &value.top_fn_name,
            &value.dso_version,
            &value.runtime,
            "ir_fn_to_mffc_corpus",
        ),
        Kind::ComboVerilogToYosysAbcAig(value) => {
            validate_action_id(
                required(
                    &value.verilog_action_id,
                    "combo_verilog_to_yosys_abc_aig.verilog_action_id",
                )?,
                "combo_verilog_to_yosys_abc_aig.verilog_action_id.value",
            )?;
            validate_optional_identifier(
                &value.verilog_top_module_name,
                "combo_verilog_to_yosys_abc_aig.verilog_top_module_name",
            )?;
            validate_script_ref(
                required(
                    &value.yosys_script_ref,
                    "combo_verilog_to_yosys_abc_aig.yosys_script_ref",
                )?,
                "combo_verilog_to_yosys_abc_aig.yosys_script_ref",
            )?;
            validate_yosys_runtime(
                required(&value.runtime, "combo_verilog_to_yosys_abc_aig.runtime")?,
                "combo_verilog_to_yosys_abc_aig.runtime",
            )
        }
        Kind::AigToYosysAbcAig(value) => {
            validate_action_id(
                required(&value.aig_action_id, "aig_to_yosys_abc_aig.aig_action_id")?,
                "aig_to_yosys_abc_aig.aig_action_id.value",
            )?;
            validate_script_ref(
                required(
                    &value.yosys_script_ref,
                    "aig_to_yosys_abc_aig.yosys_script_ref",
                )?,
                "aig_to_yosys_abc_aig.yosys_script_ref",
            )?;
            validate_yosys_runtime(
                required(&value.runtime, "aig_to_yosys_abc_aig.runtime")?,
                "aig_to_yosys_abc_aig.runtime",
            )
        }
        Kind::DriverAigToStats(value) => {
            validate_action_id(
                required(&value.aig_action_id, "driver_aig_to_stats.aig_action_id")?,
                "driver_aig_to_stats.aig_action_id.value",
            )?;
            validate_dso_version(&value.dso_version, "driver_aig_to_stats.dso_version")?;
            validate_driver_runtime(
                required(&value.runtime, "driver_aig_to_stats.runtime")?,
                "driver_aig_to_stats.runtime",
            )
        }
        Kind::AigStatDiff(value) => {
            validate_action_id(
                required(&value.opt_ir_action_id, "aig_stat_diff.opt_ir_action_id")?,
                "aig_stat_diff.opt_ir_action_id.value",
            )?;
            validate_action_id(
                required(
                    &value.g8r_aig_stats_action_id,
                    "aig_stat_diff.g8r_aig_stats_action_id",
                )?,
                "aig_stat_diff.g8r_aig_stats_action_id.value",
            )?;
            validate_action_id(
                required(
                    &value.yosys_abc_aig_stats_action_id,
                    "aig_stat_diff.yosys_abc_aig_stats_action_id",
                )?,
                "aig_stat_diff.yosys_abc_aig_stats_action_id.value",
            )
        }
    }
}

fn validate_ir_action_common(
    ir_action_id: &Option<pb::ActionId>,
    top_fn_name: &Option<String>,
    dso_version: &Option<pb::DsoVersion>,
    runtime: &Option<pb::DriverRuntimeSpec>,
    field: &str,
) -> Result<()> {
    validate_action_id(
        required(ir_action_id, &format!("{field}.ir_action_id"))?,
        &format!("{field}.ir_action_id.value"),
    )?;
    validate_optional_identifier(top_fn_name, &format!("{field}.top_fn_name"))?;
    validate_dso_version(dso_version, &format!("{field}.dso_version"))?;
    validate_driver_runtime(
        required(runtime, &format!("{field}.runtime"))?,
        &format!("{field}.runtime"),
    )
}

fn action_spec_from_model(action: &model::ActionSpec) -> Result<pb::ActionSpec> {
    use model::ActionSpec as M;
    use pb::action_spec::Kind;

    let kind = match action {
        M::ImportIrPackageFile {
            source_sha256,
            top_fn_name,
        } => Kind::ImportIrPackageFile(pb::ImportIrPackageFileAction {
            source_sha256: Some(digest_from_hex(
                source_sha256,
                "import_ir_package_file.source_sha256",
            )?),
            top_fn_name: top_fn_name.clone(),
        }),
        M::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version,
            discovery_runtime,
            stdlib_tarball_sha256,
        } => Kind::DownloadAndExtractXlsynthReleaseStdlibTarball(
            pb::DownloadAndExtractXlsynthReleaseStdlibTarballAction {
                dso_version: Some(dso_version(version, "download_release_stdlib.dso_version")?),
                discovery_runtime: discovery_runtime
                    .as_ref()
                    .map(|runtime| {
                        driver_runtime_to_proto(
                            runtime,
                            "download_release_stdlib.discovery_runtime",
                        )
                    })
                    .transpose()?,
                stdlib_tarball_sha256: Some(digest_from_hex(
                    stdlib_tarball_sha256,
                    "download_release_stdlib.stdlib_tarball_sha256",
                )?),
            },
        ),
        M::DownloadAndExtractXlsynthSourceSubtree {
            version,
            subtree,
            discovery_runtime,
            source_commit,
        } => Kind::DownloadAndExtractXlsynthSourceSubtree(
            pb::DownloadAndExtractXlsynthSourceSubtreeAction {
                dso_version: Some(dso_version(version, "download_source_subtree.dso_version")?),
                subtree: Some(relpath(subtree, "download_source_subtree.subtree")?),
                discovery_runtime: discovery_runtime
                    .as_ref()
                    .map(|runtime| {
                        driver_runtime_to_proto(
                            runtime,
                            "download_source_subtree.discovery_runtime",
                        )
                    })
                    .transpose()?,
                source_commit: source_commit.clone(),
            },
        ),
        M::DriverDslxFnToIr {
            dslx_subtree_action_id,
            dslx_file,
            dslx_fn_name,
            version,
            runtime,
        } => Kind::DriverDslxFnToIr(pb::DriverDslxFnToIrAction {
            dslx_subtree_action_id: Some(action_id_from_hex(
                dslx_subtree_action_id,
                "driver_dslx_fn_to_ir.dslx_subtree_action_id",
            )?),
            dslx_file: Some(relpath(dslx_file, "driver_dslx_fn_to_ir.dslx_file")?),
            dslx_fn_name: dslx_fn_name.clone(),
            dso_version: Some(dso_version(version, "driver_dslx_fn_to_ir.dso_version")?),
            runtime: Some(driver_runtime_to_proto(
                runtime,
                "driver_dslx_fn_to_ir.runtime",
            )?),
        }),
        M::DriverIrToOpt {
            ir_action_id,
            top_fn_name,
            version,
            runtime,
        } => Kind::DriverIrToOpt(pb::DriverIrToOptAction {
            ir_action_id: Some(action_id_from_hex(
                ir_action_id,
                "driver_ir_to_opt.ir_action_id",
            )?),
            top_fn_name: top_fn_name.clone(),
            dso_version: Some(dso_version(version, "driver_ir_to_opt.dso_version")?),
            runtime: Some(driver_runtime_to_proto(
                runtime,
                "driver_ir_to_opt.runtime",
            )?),
        }),
        M::DriverIrToDelayInfo {
            ir_action_id,
            top_fn_name,
            delay_model,
            output_format,
            version,
            runtime,
        } => Kind::DriverIrToDelayInfo(pb::DriverIrToDelayInfoAction {
            ir_action_id: Some(action_id_from_hex(
                ir_action_id,
                "driver_ir_to_delay_info.ir_action_id",
            )?),
            top_fn_name: top_fn_name.clone(),
            delay_model: delay_model.clone(),
            output_format: output_format.clone(),
            dso_version: Some(dso_version(version, "driver_ir_to_delay_info.dso_version")?),
            runtime: Some(driver_runtime_to_proto(
                runtime,
                "driver_ir_to_delay_info.runtime",
            )?),
        }),
        M::DriverIrEquiv {
            lhs_ir_action_id,
            rhs_ir_action_id,
            top_fn_name,
            version,
            runtime,
        } => Kind::DriverIrEquiv(pb::DriverIrEquivAction {
            lhs_ir_action_id: Some(action_id_from_hex(
                lhs_ir_action_id,
                "driver_ir_equiv.lhs_ir_action_id",
            )?),
            rhs_ir_action_id: Some(action_id_from_hex(
                rhs_ir_action_id,
                "driver_ir_equiv.rhs_ir_action_id",
            )?),
            top_fn_name: top_fn_name.clone(),
            dso_version: Some(dso_version(version, "driver_ir_equiv.dso_version")?),
            runtime: Some(driver_runtime_to_proto(runtime, "driver_ir_equiv.runtime")?),
        }),
        M::DriverIrAigEquiv {
            ir_action_id,
            aig_action_id,
            top_fn_name,
            version,
            runtime,
        } => Kind::DriverIrAigEquiv(pb::DriverIrAigEquivAction {
            ir_action_id: Some(action_id_from_hex(
                ir_action_id,
                "driver_ir_aig_equiv.ir_action_id",
            )?),
            aig_action_id: Some(action_id_from_hex(
                aig_action_id,
                "driver_ir_aig_equiv.aig_action_id",
            )?),
            top_fn_name: top_fn_name.clone(),
            dso_version: Some(dso_version(version, "driver_ir_aig_equiv.dso_version")?),
            runtime: Some(driver_runtime_to_proto(
                runtime,
                "driver_ir_aig_equiv.runtime",
            )?),
        }),
        M::DriverIrToG8rAig {
            ir_action_id,
            top_fn_name,
            fraig,
            lowering_mode,
            version,
            runtime,
        } => Kind::DriverIrToG8rAig(pb::DriverIrToG8rAigAction {
            ir_action_id: Some(action_id_from_hex(
                ir_action_id,
                "driver_ir_to_g8r_aig.ir_action_id",
            )?),
            top_fn_name: top_fn_name.clone(),
            fraig: *fraig,
            lowering_mode: match lowering_mode {
                model::G8rLoweringMode::Default => pb::G8rLoweringMode::Default as i32,
                model::G8rLoweringMode::FrontendNoPrepRewrite => {
                    pb::G8rLoweringMode::FrontendNoPrepRewrite as i32
                }
            },
            dso_version: Some(dso_version(version, "driver_ir_to_g8r_aig.dso_version")?),
            runtime: Some(driver_runtime_to_proto(
                runtime,
                "driver_ir_to_g8r_aig.runtime",
            )?),
        }),
        M::IrFnToCombinationalVerilog {
            ir_action_id,
            top_fn_name,
            use_system_verilog,
            version,
            runtime,
        } => Kind::IrFnToCombinationalVerilog(pb::IrFnToCombinationalVerilogAction {
            ir_action_id: Some(action_id_from_hex(
                ir_action_id,
                "ir_fn_to_combinational_verilog.ir_action_id",
            )?),
            top_fn_name: top_fn_name.clone(),
            use_system_verilog: *use_system_verilog,
            dso_version: Some(dso_version(
                version,
                "ir_fn_to_combinational_verilog.dso_version",
            )?),
            runtime: Some(driver_runtime_to_proto(
                runtime,
                "ir_fn_to_combinational_verilog.runtime",
            )?),
        }),
        M::IrFnToKBoolConeCorpus {
            ir_action_id,
            top_fn_name,
            k,
            max_ir_ops,
            version,
            runtime,
        } => Kind::IrFnToKBoolConeCorpus(pb::IrFnToKBoolConeCorpusAction {
            ir_action_id: Some(action_id_from_hex(
                ir_action_id,
                "ir_fn_to_k_bool_cone_corpus.ir_action_id",
            )?),
            top_fn_name: top_fn_name.clone(),
            k: *k,
            max_ir_ops: *max_ir_ops,
            dso_version: Some(dso_version(
                version,
                "ir_fn_to_k_bool_cone_corpus.dso_version",
            )?),
            runtime: Some(driver_runtime_to_proto(
                runtime,
                "ir_fn_to_k_bool_cone_corpus.runtime",
            )?),
        }),
        M::IrFnToMffcCorpus {
            ir_action_id,
            top_fn_name,
            max_mffcs,
            min_internal_non_literal,
            max_frontier_non_literal,
            version,
            runtime,
        } => Kind::IrFnToMffcCorpus(pb::IrFnToMffcCorpusAction {
            ir_action_id: Some(action_id_from_hex(
                ir_action_id,
                "ir_fn_to_mffc_corpus.ir_action_id",
            )?),
            top_fn_name: top_fn_name.clone(),
            max_mffcs: *max_mffcs,
            min_internal_non_literal: *min_internal_non_literal,
            max_frontier_non_literal: *max_frontier_non_literal,
            dso_version: Some(dso_version(version, "ir_fn_to_mffc_corpus.dso_version")?),
            runtime: Some(driver_runtime_to_proto(
                runtime,
                "ir_fn_to_mffc_corpus.runtime",
            )?),
        }),
        M::ComboVerilogToYosysAbcAig {
            verilog_action_id,
            verilog_top_module_name,
            yosys_script_ref,
            runtime,
        } => Kind::ComboVerilogToYosysAbcAig(pb::ComboVerilogToYosysAbcAigAction {
            verilog_action_id: Some(action_id_from_hex(
                verilog_action_id,
                "combo_verilog_to_yosys_abc_aig.verilog_action_id",
            )?),
            verilog_top_module_name: verilog_top_module_name.clone(),
            yosys_script_ref: Some(script_ref_to_proto(
                yosys_script_ref,
                "combo_verilog_to_yosys_abc_aig.yosys_script_ref",
            )?),
            runtime: Some(yosys_runtime_to_proto(
                runtime,
                "combo_verilog_to_yosys_abc_aig.runtime",
            )?),
        }),
        M::AigToYosysAbcAig {
            aig_action_id,
            yosys_script_ref,
            runtime,
        } => Kind::AigToYosysAbcAig(pb::AigToYosysAbcAigAction {
            aig_action_id: Some(action_id_from_hex(
                aig_action_id,
                "aig_to_yosys_abc_aig.aig_action_id",
            )?),
            yosys_script_ref: Some(script_ref_to_proto(
                yosys_script_ref,
                "aig_to_yosys_abc_aig.yosys_script_ref",
            )?),
            runtime: Some(yosys_runtime_to_proto(
                runtime,
                "aig_to_yosys_abc_aig.runtime",
            )?),
        }),
        M::DriverAigToStats {
            aig_action_id,
            version,
            runtime,
        } => Kind::DriverAigToStats(pb::DriverAigToStatsAction {
            aig_action_id: Some(action_id_from_hex(
                aig_action_id,
                "driver_aig_to_stats.aig_action_id",
            )?),
            dso_version: Some(dso_version(version, "driver_aig_to_stats.dso_version")?),
            runtime: Some(driver_runtime_to_proto(
                runtime,
                "driver_aig_to_stats.runtime",
            )?),
        }),
        M::AigStatDiff {
            opt_ir_action_id,
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
        } => Kind::AigStatDiff(pb::AigStatDiffAction {
            opt_ir_action_id: Some(action_id_from_hex(
                opt_ir_action_id,
                "aig_stat_diff.opt_ir_action_id",
            )?),
            g8r_aig_stats_action_id: Some(action_id_from_hex(
                g8r_aig_stats_action_id,
                "aig_stat_diff.g8r_aig_stats_action_id",
            )?),
            yosys_abc_aig_stats_action_id: Some(action_id_from_hex(
                yosys_abc_aig_stats_action_id,
                "aig_stat_diff.yosys_abc_aig_stats_action_id",
            )?),
        }),
    };
    Ok(pb::ActionSpec { kind: Some(kind) })
}

pub(crate) fn action_spec_to_proto(action: &model::ActionSpec) -> Result<pb::ActionSpec> {
    let action = action_spec_from_model(action)?;
    validate_action_spec(&action)?;
    Ok(action)
}

pub(crate) fn action_id_to_proto(value: &str, field: &str) -> Result<pb::ActionId> {
    action_id_from_hex(value, field)
}

pub(crate) fn action_id_to_hex(value: &pb::ActionId, field: &str) -> Result<String> {
    validate_action_id(value, field)?;
    Ok(hex::encode(&value.value))
}

pub(crate) fn digest_to_hex(value: &pb::Sha256Digest, field: &str) -> Result<String> {
    validate_digest(value, field)?;
    Ok(hex::encode(&value.value))
}

fn version_value(value: &Option<pb::DsoVersion>, field: &str) -> Result<String> {
    validate_dso_version(value, field)?;
    Ok(format!("v{}", required(value, field)?.value))
}

fn relpath_value(value: &Option<pb::NormalizedRelpath>, field: &str) -> Result<String> {
    let value = required(value, field)?;
    validate_canonical_relpath(value, field)?;
    Ok(value.value.clone())
}

pub(crate) fn driver_runtime_from_proto(
    value: &pb::DriverRuntimeSpec,
    field: &str,
) -> Result<model::DriverRuntimeSpec> {
    validate_driver_runtime(value, field)?;
    Ok(model::DriverRuntimeSpec {
        driver_version: required(
            &value.driver_crate_version,
            &format!("{field}.driver_crate_version"),
        )?
        .value
        .clone(),
        release_platform: value.release_platform.clone(),
        docker_image: value.docker_image.clone(),
        dockerfile: relpath_value(&value.dockerfile, &format!("{field}.dockerfile"))?,
        dockerfile_sha256: digest_to_hex(
            required(
                &value.dockerfile_sha256,
                &format!("{field}.dockerfile_sha256"),
            )?,
            &format!("{field}.dockerfile_sha256"),
        )?,
        docker_image_id: digest_to_hex(
            required(&value.docker_image_id, &format!("{field}.docker_image_id"))?,
            &format!("{field}.docker_image_id"),
        )?,
    })
}

pub(crate) fn yosys_runtime_from_proto(
    value: &pb::YosysRuntimeSpec,
    field: &str,
) -> Result<model::YosysRuntimeSpec> {
    validate_yosys_runtime(value, field)?;
    Ok(model::YosysRuntimeSpec {
        docker_image: value.docker_image.clone(),
        dockerfile: relpath_value(&value.dockerfile, &format!("{field}.dockerfile"))?,
        upstream_commit: value.upstream_commit.clone(),
        dockerfile_sha256: digest_to_hex(
            required(
                &value.dockerfile_sha256,
                &format!("{field}.dockerfile_sha256"),
            )?,
            &format!("{field}.dockerfile_sha256"),
        )?,
        docker_image_id: digest_to_hex(
            required(&value.docker_image_id, &format!("{field}.docker_image_id"))?,
            &format!("{field}.docker_image_id"),
        )?,
    })
}

pub(crate) fn script_ref_from_proto(
    value: &pb::ScriptRef,
    field: &str,
) -> Result<model::ScriptRef> {
    validate_script_ref(value, field)?;
    Ok(model::ScriptRef {
        path: relpath_value(&value.path, &format!("{field}.path"))?,
        sha256: digest_to_hex(
            required(&value.sha256, &format!("{field}.sha256"))?,
            &format!("{field}.sha256"),
        )?,
    })
}

pub(crate) fn action_spec_from_proto(action: &pb::ActionSpec) -> Result<model::ActionSpec> {
    use model::ActionSpec as M;
    use pb::action_spec::Kind;

    validate_action_spec(action)?;
    let kind = required(&action.kind, "action.kind")?;
    Ok(match kind {
        Kind::ImportIrPackageFile(value) => M::ImportIrPackageFile {
            source_sha256: digest_to_hex(
                required(&value.source_sha256, "import_ir_package_file.source_sha256")?,
                "import_ir_package_file.source_sha256",
            )?,
            top_fn_name: value.top_fn_name.clone(),
        },
        Kind::DownloadAndExtractXlsynthReleaseStdlibTarball(value) => {
            M::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: version_value(&value.dso_version, "download_release_stdlib.dso_version")?,
                discovery_runtime: value
                    .discovery_runtime
                    .as_ref()
                    .map(|v| {
                        driver_runtime_from_proto(v, "download_release_stdlib.discovery_runtime")
                    })
                    .transpose()?,
                stdlib_tarball_sha256: digest_to_hex(
                    required(
                        &value.stdlib_tarball_sha256,
                        "download_release_stdlib.stdlib_tarball_sha256",
                    )?,
                    "download_release_stdlib.stdlib_tarball_sha256",
                )?,
            }
        }
        Kind::DownloadAndExtractXlsynthSourceSubtree(value) => {
            M::DownloadAndExtractXlsynthSourceSubtree {
                version: version_value(&value.dso_version, "download_source_subtree.dso_version")?,
                subtree: relpath_value(&value.subtree, "download_source_subtree.subtree")?,
                discovery_runtime: value
                    .discovery_runtime
                    .as_ref()
                    .map(|v| {
                        driver_runtime_from_proto(v, "download_source_subtree.discovery_runtime")
                    })
                    .transpose()?,
                source_commit: value.source_commit.clone(),
            }
        }
        Kind::DriverDslxFnToIr(value) => M::DriverDslxFnToIr {
            dslx_subtree_action_id: action_id_to_hex(
                required(
                    &value.dslx_subtree_action_id,
                    "driver_dslx_fn_to_ir.dslx_subtree_action_id",
                )?,
                "driver_dslx_fn_to_ir.dslx_subtree_action_id",
            )?,
            dslx_file: relpath_value(&value.dslx_file, "driver_dslx_fn_to_ir.dslx_file")?,
            dslx_fn_name: value.dslx_fn_name.clone(),
            version: version_value(&value.dso_version, "driver_dslx_fn_to_ir.dso_version")?,
            runtime: driver_runtime_from_proto(
                required(&value.runtime, "driver_dslx_fn_to_ir.runtime")?,
                "driver_dslx_fn_to_ir.runtime",
            )?,
        },
        Kind::DriverIrToOpt(value) => M::DriverIrToOpt {
            ir_action_id: action_id_to_hex(
                required(&value.ir_action_id, "driver_ir_to_opt.ir_action_id")?,
                "driver_ir_to_opt.ir_action_id",
            )?,
            top_fn_name: value.top_fn_name.clone(),
            version: version_value(&value.dso_version, "driver_ir_to_opt.dso_version")?,
            runtime: driver_runtime_from_proto(
                required(&value.runtime, "driver_ir_to_opt.runtime")?,
                "driver_ir_to_opt.runtime",
            )?,
        },
        Kind::DriverIrToDelayInfo(value) => M::DriverIrToDelayInfo {
            ir_action_id: action_id_to_hex(
                required(&value.ir_action_id, "driver_ir_to_delay_info.ir_action_id")?,
                "driver_ir_to_delay_info.ir_action_id",
            )?,
            top_fn_name: value.top_fn_name.clone(),
            delay_model: value.delay_model.clone(),
            output_format: value.output_format.clone(),
            version: version_value(&value.dso_version, "driver_ir_to_delay_info.dso_version")?,
            runtime: driver_runtime_from_proto(
                required(&value.runtime, "driver_ir_to_delay_info.runtime")?,
                "driver_ir_to_delay_info.runtime",
            )?,
        },
        Kind::DriverIrEquiv(value) => M::DriverIrEquiv {
            lhs_ir_action_id: action_id_to_hex(
                required(&value.lhs_ir_action_id, "driver_ir_equiv.lhs_ir_action_id")?,
                "driver_ir_equiv.lhs_ir_action_id",
            )?,
            rhs_ir_action_id: action_id_to_hex(
                required(&value.rhs_ir_action_id, "driver_ir_equiv.rhs_ir_action_id")?,
                "driver_ir_equiv.rhs_ir_action_id",
            )?,
            top_fn_name: value.top_fn_name.clone(),
            version: version_value(&value.dso_version, "driver_ir_equiv.dso_version")?,
            runtime: driver_runtime_from_proto(
                required(&value.runtime, "driver_ir_equiv.runtime")?,
                "driver_ir_equiv.runtime",
            )?,
        },
        Kind::DriverIrAigEquiv(value) => M::DriverIrAigEquiv {
            ir_action_id: action_id_to_hex(
                required(&value.ir_action_id, "driver_ir_aig_equiv.ir_action_id")?,
                "driver_ir_aig_equiv.ir_action_id",
            )?,
            aig_action_id: action_id_to_hex(
                required(&value.aig_action_id, "driver_ir_aig_equiv.aig_action_id")?,
                "driver_ir_aig_equiv.aig_action_id",
            )?,
            top_fn_name: value.top_fn_name.clone(),
            version: version_value(&value.dso_version, "driver_ir_aig_equiv.dso_version")?,
            runtime: driver_runtime_from_proto(
                required(&value.runtime, "driver_ir_aig_equiv.runtime")?,
                "driver_ir_aig_equiv.runtime",
            )?,
        },
        Kind::DriverIrToG8rAig(value) => M::DriverIrToG8rAig {
            ir_action_id: action_id_to_hex(
                required(&value.ir_action_id, "driver_ir_to_g8r_aig.ir_action_id")?,
                "driver_ir_to_g8r_aig.ir_action_id",
            )?,
            top_fn_name: value.top_fn_name.clone(),
            fraig: value.fraig,
            lowering_mode: match pb::G8rLoweringMode::try_from(value.lowering_mode)? {
                pb::G8rLoweringMode::Default => model::G8rLoweringMode::Default,
                pb::G8rLoweringMode::FrontendNoPrepRewrite => {
                    model::G8rLoweringMode::FrontendNoPrepRewrite
                }
                pb::G8rLoweringMode::Unspecified => {
                    bail!("driver_ir_to_g8r_aig lowering mode unspecified")
                }
            },
            version: version_value(&value.dso_version, "driver_ir_to_g8r_aig.dso_version")?,
            runtime: driver_runtime_from_proto(
                required(&value.runtime, "driver_ir_to_g8r_aig.runtime")?,
                "driver_ir_to_g8r_aig.runtime",
            )?,
        },
        Kind::IrFnToCombinationalVerilog(value) => M::IrFnToCombinationalVerilog {
            ir_action_id: action_id_to_hex(
                required(
                    &value.ir_action_id,
                    "ir_fn_to_combinational_verilog.ir_action_id",
                )?,
                "ir_fn_to_combinational_verilog.ir_action_id",
            )?,
            top_fn_name: value.top_fn_name.clone(),
            use_system_verilog: value.use_system_verilog,
            version: version_value(
                &value.dso_version,
                "ir_fn_to_combinational_verilog.dso_version",
            )?,
            runtime: driver_runtime_from_proto(
                required(&value.runtime, "ir_fn_to_combinational_verilog.runtime")?,
                "ir_fn_to_combinational_verilog.runtime",
            )?,
        },
        Kind::IrFnToKBoolConeCorpus(value) => M::IrFnToKBoolConeCorpus {
            ir_action_id: action_id_to_hex(
                required(
                    &value.ir_action_id,
                    "ir_fn_to_k_bool_cone_corpus.ir_action_id",
                )?,
                "ir_fn_to_k_bool_cone_corpus.ir_action_id",
            )?,
            top_fn_name: value.top_fn_name.clone(),
            k: value.k,
            max_ir_ops: value.max_ir_ops,
            version: version_value(
                &value.dso_version,
                "ir_fn_to_k_bool_cone_corpus.dso_version",
            )?,
            runtime: driver_runtime_from_proto(
                required(&value.runtime, "ir_fn_to_k_bool_cone_corpus.runtime")?,
                "ir_fn_to_k_bool_cone_corpus.runtime",
            )?,
        },
        Kind::IrFnToMffcCorpus(value) => M::IrFnToMffcCorpus {
            ir_action_id: action_id_to_hex(
                required(&value.ir_action_id, "ir_fn_to_mffc_corpus.ir_action_id")?,
                "ir_fn_to_mffc_corpus.ir_action_id",
            )?,
            top_fn_name: value.top_fn_name.clone(),
            max_mffcs: value.max_mffcs,
            min_internal_non_literal: value.min_internal_non_literal,
            max_frontier_non_literal: value.max_frontier_non_literal,
            version: version_value(&value.dso_version, "ir_fn_to_mffc_corpus.dso_version")?,
            runtime: driver_runtime_from_proto(
                required(&value.runtime, "ir_fn_to_mffc_corpus.runtime")?,
                "ir_fn_to_mffc_corpus.runtime",
            )?,
        },
        Kind::ComboVerilogToYosysAbcAig(value) => M::ComboVerilogToYosysAbcAig {
            verilog_action_id: action_id_to_hex(
                required(
                    &value.verilog_action_id,
                    "combo_verilog_to_yosys_abc_aig.verilog_action_id",
                )?,
                "combo_verilog_to_yosys_abc_aig.verilog_action_id",
            )?,
            verilog_top_module_name: value.verilog_top_module_name.clone(),
            yosys_script_ref: script_ref_from_proto(
                required(
                    &value.yosys_script_ref,
                    "combo_verilog_to_yosys_abc_aig.yosys_script_ref",
                )?,
                "combo_verilog_to_yosys_abc_aig.yosys_script_ref",
            )?,
            runtime: yosys_runtime_from_proto(
                required(&value.runtime, "combo_verilog_to_yosys_abc_aig.runtime")?,
                "combo_verilog_to_yosys_abc_aig.runtime",
            )?,
        },
        Kind::AigToYosysAbcAig(value) => M::AigToYosysAbcAig {
            aig_action_id: action_id_to_hex(
                required(&value.aig_action_id, "aig_to_yosys_abc_aig.aig_action_id")?,
                "aig_to_yosys_abc_aig.aig_action_id",
            )?,
            yosys_script_ref: script_ref_from_proto(
                required(
                    &value.yosys_script_ref,
                    "aig_to_yosys_abc_aig.yosys_script_ref",
                )?,
                "aig_to_yosys_abc_aig.yosys_script_ref",
            )?,
            runtime: yosys_runtime_from_proto(
                required(&value.runtime, "aig_to_yosys_abc_aig.runtime")?,
                "aig_to_yosys_abc_aig.runtime",
            )?,
        },
        Kind::DriverAigToStats(value) => M::DriverAigToStats {
            aig_action_id: action_id_to_hex(
                required(&value.aig_action_id, "driver_aig_to_stats.aig_action_id")?,
                "driver_aig_to_stats.aig_action_id",
            )?,
            version: version_value(&value.dso_version, "driver_aig_to_stats.dso_version")?,
            runtime: driver_runtime_from_proto(
                required(&value.runtime, "driver_aig_to_stats.runtime")?,
                "driver_aig_to_stats.runtime",
            )?,
        },
        Kind::AigStatDiff(value) => M::AigStatDiff {
            opt_ir_action_id: action_id_to_hex(
                required(&value.opt_ir_action_id, "aig_stat_diff.opt_ir_action_id")?,
                "aig_stat_diff.opt_ir_action_id",
            )?,
            g8r_aig_stats_action_id: action_id_to_hex(
                required(
                    &value.g8r_aig_stats_action_id,
                    "aig_stat_diff.g8r_aig_stats_action_id",
                )?,
                "aig_stat_diff.g8r_aig_stats_action_id",
            )?,
            yosys_abc_aig_stats_action_id: action_id_to_hex(
                required(
                    &value.yosys_abc_aig_stats_action_id,
                    "aig_stat_diff.yosys_abc_aig_stats_action_id",
                )?,
                "aig_stat_diff.yosys_abc_aig_stats_action_id",
            )?,
        },
    })
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    fn driver_runtime_fixture() -> model::DriverRuntimeSpec {
        model::DriverRuntimeSpec {
            driver_version: "v0.47.0".to_string(),
            release_platform: "ubuntu2004".to_string(),
            docker_image: "xlsynth-bvc-driver:0.47.0".to_string(),
            dockerfile: "docker\\xlsynth-driver.Dockerfile".to_string(),
            dockerfile_sha256: "d".repeat(64),
            docker_image_id: "e".repeat(64),
        }
    }

    fn yosys_runtime_fixture() -> model::YosysRuntimeSpec {
        model::YosysRuntimeSpec {
            docker_image: "xlsynth-bvc-yosys-abc:test".to_string(),
            dockerfile: "docker/yosys-abc.Dockerfile".to_string(),
            docker_image_id: "e".repeat(64),
            dockerfile_sha256: "d".repeat(64),
            upstream_commit: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
        }
    }

    fn id(byte: u8) -> String {
        hex::encode([byte; DIGEST_BYTE_LEN])
    }

    fn script_ref_fixture() -> model::ScriptRef {
        model::ScriptRef {
            path: "flows/yosys_to_aig.ys".to_string(),
            sha256: id(0xa5),
        }
    }

    fn sample_actions() -> Vec<(&'static str, model::ActionSpec)> {
        let runtime = driver_runtime_fixture();
        let yosys = yosys_runtime_fixture();
        let script = script_ref_fixture();
        vec![
            (
                "import_ir_package_file",
                model::ActionSpec::ImportIrPackageFile {
                    source_sha256: id(0x01),
                    top_fn_name: Some("main".to_string()),
                },
            ),
            (
                "download_release_stdlib",
                model::ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                    version: "v0.47.0".to_string(),
                    discovery_runtime: Some(runtime.clone()),
                    stdlib_tarball_sha256: "11".repeat(32),
                },
            ),
            (
                "download_source_subtree",
                model::ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
                    version: "v0.47.0".to_string(),
                    subtree: "xls\\modules/add_dual_path".to_string(),
                    discovery_runtime: Some(runtime.clone()),
                    source_commit: "2".repeat(40),
                },
            ),
            (
                "driver_dslx_fn_to_ir",
                model::ActionSpec::DriverDslxFnToIr {
                    dslx_subtree_action_id: id(0x02),
                    dslx_file: "xls/dslx/stdlib/math.x".to_string(),
                    dslx_fn_name: "add".to_string(),
                    version: "v0.47.0".to_string(),
                    runtime: runtime.clone(),
                },
            ),
            (
                "driver_ir_to_opt",
                model::ActionSpec::DriverIrToOpt {
                    ir_action_id: id(0x03),
                    top_fn_name: Some("main".to_string()),
                    version: "v0.47.0".to_string(),
                    runtime: runtime.clone(),
                },
            ),
            (
                "driver_ir_to_delay_info",
                model::ActionSpec::DriverIrToDelayInfo {
                    ir_action_id: id(0x04),
                    top_fn_name: Some("main".to_string()),
                    delay_model: "asap7".to_string(),
                    output_format: "textproto_v1".to_string(),
                    version: "v0.47.0".to_string(),
                    runtime: runtime.clone(),
                },
            ),
            (
                "driver_ir_equiv",
                model::ActionSpec::DriverIrEquiv {
                    lhs_ir_action_id: id(0x05),
                    rhs_ir_action_id: id(0x06),
                    top_fn_name: Some("main".to_string()),
                    version: "v0.47.0".to_string(),
                    runtime: runtime.clone(),
                },
            ),
            (
                "driver_ir_aig_equiv",
                model::ActionSpec::DriverIrAigEquiv {
                    ir_action_id: id(0x07),
                    aig_action_id: id(0x08),
                    top_fn_name: Some("main".to_string()),
                    version: "v0.47.0".to_string(),
                    runtime: runtime.clone(),
                },
            ),
            (
                "driver_ir_to_g8r_aig",
                model::ActionSpec::DriverIrToG8rAig {
                    ir_action_id: id(0x09),
                    top_fn_name: Some("main".to_string()),
                    fraig: true,
                    lowering_mode: model::G8rLoweringMode::FrontendNoPrepRewrite,
                    version: "v0.47.0".to_string(),
                    runtime: runtime.clone(),
                },
            ),
            (
                "ir_fn_to_combinational_verilog",
                model::ActionSpec::IrFnToCombinationalVerilog {
                    ir_action_id: id(0x0a),
                    top_fn_name: Some("main".to_string()),
                    use_system_verilog: true,
                    version: "v0.47.0".to_string(),
                    runtime: runtime.clone(),
                },
            ),
            (
                "ir_fn_to_k_bool_cone_corpus",
                model::ActionSpec::IrFnToKBoolConeCorpus {
                    ir_action_id: id(0x0b),
                    top_fn_name: Some("main".to_string()),
                    k: 3,
                    max_ir_ops: Some(16),
                    version: "v0.47.0".to_string(),
                    runtime: runtime.clone(),
                },
            ),
            (
                "ir_fn_to_mffc_corpus",
                model::ActionSpec::IrFnToMffcCorpus {
                    ir_action_id: id(0x0c),
                    top_fn_name: Some("main".to_string()),
                    max_mffcs: Some(32),
                    min_internal_non_literal: 4,
                    max_frontier_non_literal: Some(8),
                    version: "v0.47.0".to_string(),
                    runtime: runtime.clone(),
                },
            ),
            (
                "combo_verilog_to_yosys_abc_aig",
                model::ActionSpec::ComboVerilogToYosysAbcAig {
                    verilog_action_id: id(0x0d),
                    verilog_top_module_name: Some("main".to_string()),
                    yosys_script_ref: script.clone(),
                    runtime: yosys.clone(),
                },
            ),
            (
                "aig_to_yosys_abc_aig",
                model::ActionSpec::AigToYosysAbcAig {
                    aig_action_id: id(0x0e),
                    yosys_script_ref: script,
                    runtime: yosys,
                },
            ),
            (
                "driver_aig_to_stats",
                model::ActionSpec::DriverAigToStats {
                    aig_action_id: id(0x0f),
                    version: "v0.47.0".to_string(),
                    runtime,
                },
            ),
            (
                "aig_stat_diff",
                model::ActionSpec::AigStatDiff {
                    opt_ir_action_id: id(0x10),
                    g8r_aig_stats_action_id: id(0x11),
                    yosys_abc_aig_stats_action_id: id(0x12),
                },
            ),
        ]
    }

    #[test]
    fn every_model_action_converts_validates_and_round_trips() {
        for (name, action) in sample_actions() {
            let validated =
                ValidatedActionSpec::try_from(&action).unwrap_or_else(|e| panic!("{name}: {e:#}"));
            let bytes = validated.as_proto().encode_to_vec();
            let decoded = pb::ActionSpec::decode(bytes.as_slice())
                .unwrap_or_else(|e| panic!("{name}: decode: {e}"));
            let decoded =
                ValidatedActionSpec::try_from(decoded).unwrap_or_else(|e| panic!("{name}: {e:#}"));
            assert_eq!(decoded, validated, "{name}");
        }
    }

    #[test]
    fn model_conversion_normalizes_versions_and_paths() {
        let action = model::ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
            version: "v0.47.0".to_string(),
            subtree: "xls\\modules\\add_dual_path".to_string(),
            discovery_runtime: Some(driver_runtime_fixture()),
            source_commit: "2".repeat(40),
        };
        let validated = ValidatedActionSpec::try_from(&action).expect("convert");
        let pb::action_spec::Kind::DownloadAndExtractXlsynthSourceSubtree(value) =
            validated.as_proto().kind.as_ref().expect("kind")
        else {
            panic!("unexpected kind");
        };
        assert_eq!(value.dso_version.as_ref().expect("version").value, "0.47.0");
        assert_eq!(
            value.subtree.as_ref().expect("subtree").value,
            "xls/modules/add_dual_path"
        );
        assert_eq!(
            value
                .discovery_runtime
                .as_ref()
                .expect("runtime")
                .dockerfile
                .as_ref()
                .expect("dockerfile")
                .value,
            "docker/xlsynth-driver.Dockerfile"
        );
    }

    #[test]
    fn validator_rejects_missing_kind_and_bad_digest() {
        assert!(ValidatedActionSpec::try_from(pb::ActionSpec { kind: None }).is_err());
        let action = pb::ActionSpec {
            kind: Some(pb::action_spec::Kind::ImportIrPackageFile(
                pb::ImportIrPackageFileAction {
                    source_sha256: Some(pb::Sha256Digest { value: vec![0; 31] }),
                    top_fn_name: None,
                },
            )),
        };
        assert!(ValidatedActionSpec::try_from(action).is_err());
    }

    #[test]
    fn action_id_changes_when_a_semantic_field_changes() {
        let mut actions = sample_actions();
        let (_, mut action) = actions.remove(4);
        let first = compute_model_action_id_v2(&action).expect("first");
        let model::ActionSpec::DriverIrToOpt { top_fn_name, .. } = &mut action else {
            panic!("expected DriverIrToOpt");
        };
        *top_fn_name = Some("different".to_string());
        let second = compute_model_action_id_v2(&action).expect("second");
        assert_ne!(first, second);
    }

    #[test]
    fn download_action_ids_bind_locked_upstream_inputs() {
        let mut actions = sample_actions();
        let (_, mut stdlib) = actions.remove(1);
        let original = compute_model_action_id_v2(&stdlib).expect("stdlib original");
        let model::ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            stdlib_tarball_sha256,
            ..
        } = &mut stdlib
        else {
            panic!("expected stdlib action");
        };
        *stdlib_tarball_sha256 = "12".repeat(32);
        assert_ne!(
            original,
            compute_model_action_id_v2(&stdlib).expect("stdlib changed")
        );

        let (_, mut source) = actions.remove(1);
        let original = compute_model_action_id_v2(&source).expect("source original");
        let model::ActionSpec::DownloadAndExtractXlsynthSourceSubtree { source_commit, .. } =
            &mut source
        else {
            panic!("expected source action");
        };
        *source_commit = "3".repeat(40);
        assert_ne!(
            original,
            compute_model_action_id_v2(&source).expect("source changed")
        );
    }

    #[test]
    fn action_id_v2_golden_vectors() {
        const GOLDENS: &[(&str, &str)] = &[
            (
                "import_ir_package_file",
                "878a4a6e69dcf7015929939f114e15599f4ffb6734d98bc905372cf472bc268f",
            ),
            (
                "download_release_stdlib",
                "bbb811263b1f0eaacbc8e73e2a4f4541f40997ae1dbc1b2f9a80e838f0a1ec46",
            ),
            (
                "download_source_subtree",
                "5780c8111ecca48fff5a94cdcea4fef3c217c6be498efc6d44dffe4a42212f43",
            ),
            (
                "driver_dslx_fn_to_ir",
                "94b5be97aa7d94eab0b667a18966197613659ae60684a87cc0bff6435781c090",
            ),
            (
                "driver_ir_to_opt",
                "5280e35e6c308aa7eb2b1e84cb5288e4be8716ae9d4c99620f70072725b79b5a",
            ),
            (
                "driver_ir_to_delay_info",
                "ea3c7e9ce3f90e56944f474e3ef118145ed6b8a54c57d4d9be2a9c08e65fac4a",
            ),
            (
                "driver_ir_equiv",
                "9698f2b6f55256979f18a7a20039e5fdab2cb934807457b12450f85f3bb05de3",
            ),
            (
                "driver_ir_aig_equiv",
                "7c40a11f0cf4e656478638e67a709173f91bce1bc76c1d469e2a3aad7e6c7342",
            ),
            (
                "driver_ir_to_g8r_aig",
                "ee01588da3728a59adb579b40ca93c2ab32d51b2c9574234d7ac305d5de50197",
            ),
            (
                "ir_fn_to_combinational_verilog",
                "261ff5eaf4accd0916af607589a252261086f559c1f74d71d246c1bae815109f",
            ),
            (
                "ir_fn_to_k_bool_cone_corpus",
                "7e5a82ad03ed7d1cd3522e70deb5d41355ef8f2cd99d16ad8bdb7b021c707472",
            ),
            (
                "ir_fn_to_mffc_corpus",
                "276f3116d83ac579674993a220808a722c1e9217836cfd4f88a75b9fd59c7770",
            ),
            (
                "combo_verilog_to_yosys_abc_aig",
                "737ba20f9a6d538bcbe7e045311d0b7b2db19aba5094925b7ed41c00f3dc24d2",
            ),
            (
                "aig_to_yosys_abc_aig",
                "1848d068c136ac475078693f709f4f3e980cd4a1782543860e7816be86da8da8",
            ),
            (
                "driver_aig_to_stats",
                "a58fe5c2547923a48ddcabc1ea7819c88ed7c2e74e37302df2887d97e80d71e8",
            ),
            (
                "aig_stat_diff",
                "665f87891654fae846b91bc460ed97edd948d0f308ef2b73cb89e3026db2ad68",
            ),
        ];
        let actual = sample_actions()
            .into_iter()
            .map(|(name, action)| {
                let validated = ValidatedActionSpec::try_from(&action).expect("validate");
                let action_id_hex = compute_action_id_v2(&validated).to_hex();
                assert!(!action_fingerprint_bytes(&validated).is_empty());
                (name, action_id_hex)
            })
            .collect::<Vec<_>>();

        assert_eq!(actual.len(), GOLDENS.len());
        for ((actual_name, actual_id), (name, id)) in actual.iter().zip(GOLDENS) {
            assert_eq!(actual_name, name);
            assert_eq!(actual_id, id);
        }
    }
}
