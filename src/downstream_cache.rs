// SPDX-License-Identifier: Apache-2.0

//! Cross-store reuse of exact downstream computations, retaining current action identities.
use anyhow::{Context, Result, bail};
use prost::Message;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::executor::compute_action_id;
use crate::model::{ActionSpec, ArtifactType, Provenance};
use crate::proto::{self, v1};
use crate::service::{resolve_script_ref, sha256_file};
use crate::store::ArtifactStore;

pub(crate) mod pb {
    include!(concat!(env!("OUT_DIR"), "/xlsynth.bvc.cache.v1.rs"));
}
const REVISION: u32 = 1;
static NONCE: AtomicU64 = AtomicU64::new(0);

fn parent(action: &ActionSpec) -> Option<(&str, ArtifactType, &'static str)> {
    match action {
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id, ..
        } => Some((verilog_action_id, ArtifactType::VerilogFile, "result.aig")),
        ActionSpec::AigToYosysAbcAig { aig_action_id, .. } => {
            Some((aig_action_id, ArtifactType::AigFile, "result.aig"))
        }
        ActionSpec::DriverAigToStats { aig_action_id, .. } => {
            Some((aig_action_id, ArtifactType::AigFile, "stats.json"))
        }
        _ => None,
    }
}

fn declared_output(provenance: &Provenance) -> Result<&crate::model::OutputFile> {
    if provenance.output_artifact.action_id != provenance.action_id {
        bail!("cache provenance output action mismatch");
    }
    let relative = provenance
        .output_artifact
        .relpath
        .strip_prefix("payload/")
        .context("cache input/output must be a payload file")?;
    provenance
        .output_files
        .iter()
        .find(|file| file.path == relative)
        .context("cache provenance does not declare its output")
}

fn key(action: &ActionSpec, input: &Provenance) -> Result<pb::ExecutionKey> {
    let (id, expected_type, _) = parent(action).context("unsupported downstream action")?;
    if id != input.action_id
        || compute_action_id(&input.action)? != id
        || input.output_artifact.artifact_type != expected_type
    {
        bail!("cache input provenance does not match action dependency");
    }
    let output = declared_output(input)?;
    let stage = match proto::action_spec_to_proto(action)?
        .kind
        .context("missing action kind")?
    {
        v1::action_spec::Kind::ComboVerilogToYosysAbcAig(a) => {
            pb::execution_key::Stage::VerilogToAig(pb::VerilogToAig {
                runtime: a.runtime,
                script: a.yosys_script_ref,
                frontend: a.frontend,
                top: a.verilog_top_module_name,
                system_verilog: input.output_artifact.relpath.ends_with(".sv"),
            })
        }
        v1::action_spec::Kind::AigToYosysAbcAig(a) => {
            pb::execution_key::Stage::AigToAig(pb::AigToAig {
                runtime: a.runtime,
                script: a.yosys_script_ref,
            })
        }
        v1::action_spec::Kind::DriverAigToStats(a) => {
            pb::execution_key::Stage::AigToStats(pb::AigToStats {
                runtime: a.runtime,
                dso_version: a.dso_version,
            })
        }
        _ => bail!("unsupported cache action"),
    };
    Ok(pb::ExecutionKey {
        execution_revision: REVISION,
        input_sha256: Some(proto::digest_from_hex(&output.sha256, "cache input")?),
        stage: Some(stage),
    })
}

fn request(
    store: &ArtifactStore,
    repo: &Path,
    action: &ActionSpec,
) -> Result<Option<(pb::ExecutionKey, Provenance)>> {
    let Some((id, _, _)) = parent(action) else {
        return Ok(None);
    };
    let input = store.load_provenance(id)?;
    let key = key(action, &input)?;
    let path = store.resolve_artifact_ref_path(&input.output_artifact);
    let declared = declared_output(&input)?;
    if fs::metadata(&path)?.len() != declared.bytes || sha256_file(&path)? != declared.sha256 {
        bail!("cache input bytes disagree with canonical provenance");
    }
    match action {
        ActionSpec::ComboVerilogToYosysAbcAig {
            yosys_script_ref, ..
        }
        | ActionSpec::AigToYosysAbcAig {
            yosys_script_ref, ..
        } => {
            resolve_script_ref(repo, yosys_script_ref)?;
        }
        ActionSpec::DriverAigToStats { .. } => {
            // Legacy sidecars and IR-based stats are not functions of AIG bytes.
            if crate::query::ensure_aiger_input_header(&path).is_err() {
                return Ok(None);
            }
        }
        _ => unreachable!(),
    }
    Ok(Some((key, input)))
}

fn entry_path(root: &Path, key: &pb::ExecutionKey) -> PathBuf {
    let digest = hex::encode(Sha256::digest(key.encode_to_vec()));
    root.join("v1")
        .join(&digest[..2])
        .join(format!("{digest}.pb"))
}

fn validated(entry: &pb::Entry, expected: &pb::ExecutionKey) -> Result<Provenance> {
    if entry.key.as_ref() != Some(expected) || expected.execution_revision != REVISION {
        bail!("downstream cache key mismatch");
    }
    let source = proto::decode_provenance(
        &entry
            .source
            .as_ref()
            .context("missing source provenance")?
            .encode_to_vec(),
    )?;
    let input = proto::decode_provenance(
        &entry
            .input
            .as_ref()
            .context("missing input provenance")?
            .encode_to_vec(),
    )?;
    if key(&source.action, &input)? != *expected
        || source.commands.is_empty()
        || source.commands.iter().any(|command| command.exit_code != 0)
        || source.dependencies.len() != 1
        || !source.dependencies.iter().all(|dep| {
            dep.action_id == input.output_artifact.action_id
                && dep.artifact_type == input.output_artifact.artifact_type
                && dep.relpath == input.output_artifact.relpath
        })
    {
        bail!("downstream cache provenance does not establish this computation");
    }
    let (_, _, output_name) = parent(&source.action).context("unsupported cache source")?;
    let expected_type = if output_name == "stats.json" {
        ArtifactType::AigStatsFile
    } else {
        ArtifactType::AigFile
    };
    if source.output_artifact.relpath != format!("payload/{output_name}")
        || source.output_artifact.artifact_type != expected_type
    {
        bail!("downstream cache output type/path mismatch");
    }
    if output_name == "stats.json"
        && source
            .details
            .get("aig_stats_mode")
            .and_then(|value| value.as_str())
            != Some("aiger_aig_stats")
    {
        bail!("legacy statistics are not eligible for downstream reuse");
    }
    let output = declared_output(&source)?;
    if output.bytes != entry.output.len() as u64
        || output.sha256 != hex::encode(Sha256::digest(&entry.output))
    {
        bail!("downstream cache output hash/size mismatch");
    }
    Ok(source)
}

pub(crate) fn restore(
    store: &ArtifactStore,
    repo: &Path,
    action: &ActionSpec,
    payload: &Path,
) -> Result<bool> {
    let Some(root) = &store.downstream_cache_dir else {
        return Ok(false);
    };
    let Some((key, _)) = request(store, repo, action)? else {
        return Ok(false);
    };
    let path = entry_path(root, &key);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).context("reading downstream cache"),
    };
    // A corrupt/incompatible cache is a miss, never an accepted result.
    let verified = pb::Entry::decode(bytes.as_slice())
        .map_err(anyhow::Error::from)
        .and_then(|entry| validated(&entry, &key).map(|source| (entry, source)));
    let (entry, source) = match verified {
        Ok(value) => value,
        Err(error) => {
            log::warn!("ignoring invalid downstream cache entry: {error:#}");
            return Ok(false);
        }
    };
    fs::write(payload.join(parent(action).unwrap().2), &entry.output)?;
    let receipt = pb::ReuseReceipt {
        key: Some(key),
        source_action_id: Some(proto::action_id_to_proto(
            &source.action_id,
            "cache source",
        )?),
        output_sha256: Some(proto::digest_from_hex(
            &declared_output(&source)?.sha256,
            "cache output",
        )?),
        source: entry.source,
        input: entry.input,
    };
    fs::write(payload.join("downstream-reuse.pb"), receipt.encode_to_vec())?;
    log::info!("downstream_cache.hit source_action={}", source.action_id);
    Ok(true)
}

fn write_entry(root: &Path, entry: &pb::Entry) -> Result<()> {
    let key = entry.key.as_ref().context("cache entry lacks key")?;
    validated(entry, key)?;
    let destination = entry_path(root, key);
    let directory = destination.parent().unwrap();
    fs::create_dir_all(directory)?;
    let temporary = directory.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        NONCE.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&entry.encode_to_vec())?;
        drop(file);
        // One self-contained protobuf: readers never see a partial payload.
        // This cache is disposable, not the canonical store: avoid an fsync per
        // entry. After power loss, missing/corrupt entries are verified misses.
        fs::rename(&temporary, &destination)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn publish(store: &ArtifactStore, repo: &Path, root: &Path, source: &Provenance) -> Result<bool> {
    // Keep the original computation as the witness, not a chain of reused actions.
    if source.commands.is_empty() {
        return Ok(false);
    }
    if matches!(source.action, ActionSpec::DriverAigToStats { .. })
        && source
            .details
            .get("aig_stats_mode")
            .and_then(|value| value.as_str())
            != Some("aiger_aig_stats")
    {
        return Ok(false);
    }
    let Some((key, input)) = request(store, repo, &source.action)? else {
        return Ok(false);
    };
    let entry = pb::Entry {
        key: Some(key),
        source: Some(v1::Provenance::decode(
            proto::encode_provenance(source)?.as_slice(),
        )?),
        input: Some(v1::Provenance::decode(
            proto::encode_provenance(&input)?.as_slice(),
        )?),
        output: fs::read(store.resolve_artifact_ref_path(&source.output_artifact))?,
    };
    write_entry(root, &entry)?;
    Ok(true)
}

pub(crate) fn publish_completed(store: &ArtifactStore, repo: &Path, source: &Provenance) {
    if let Some(root) = &store.downstream_cache_dir {
        if let Err(error) = publish(store, repo, root, source) {
            log::warn!("downstream cache publication failed: {error:#}");
        }
    }
}

pub(crate) fn seed(
    store: &ArtifactStore,
    repo: &Path,
    root: &Path,
    action_ids: &[String],
) -> Result<usize> {
    let mut count = 0;
    if !action_ids.is_empty() {
        for id in action_ids {
            if publish(store, repo, root, &store.load_provenance(id)?)? {
                count += 1;
            }
        }
        return Ok(count);
    }
    store.for_each_provenance(|source| {
        if publish(store, repo, root, &source)? {
            count += 1;
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;
    use serde_json::json;

    #[test]
    fn cache_schema_is_not_part_of_canonical_store_descriptor() {
        let descriptor =
            prost_types::FileDescriptorSet::decode(crate::proto::FILE_DESCRIPTOR_SET).unwrap();
        assert!(
            descriptor
                .file
                .iter()
                .all(|file| file.package.as_deref() != Some("xlsynth.bvc.cache.v1"))
        );
    }

    fn driver(version: &str) -> DriverRuntimeSpec {
        DriverRuntimeSpec {
            driver_version: version.into(),
            source_revision: None,
            release_platform: "ubuntu2004".into(),
            docker_image: format!("test-driver:{version}"),
            dockerfile: "docker/xlsynth-driver.Dockerfile".into(),
            dockerfile_sha256: "d".repeat(64),
            docker_image_id: "e".repeat(64),
            release_cache_input_sha256: "f".repeat(64),
        }
    }
    fn yosys() -> YosysRuntimeSpec {
        YosysRuntimeSpec {
            docker_image: "test-yosys".into(),
            dockerfile: "docker/yosys-abc.Dockerfile".into(),
            dockerfile_sha256: "c".repeat(64),
            docker_image_id: "b".repeat(64),
            upstream_commit: Some("a".repeat(40)),
            slang_commit: None,
        }
    }
    fn script() -> ScriptRef {
        ScriptRef {
            path: "flows/yosys_to_aig.ys".into(),
            sha256: sha256_file(
                &Path::new(env!("CARGO_MANIFEST_DIR")).join("flows/yosys_to_aig.ys"),
            )
            .unwrap(),
        }
    }
    fn input_action(version: &str, verilog: bool) -> ActionSpec {
        if verilog {
            ActionSpec::IrFnToCombinationalVerilog {
                ir_action_id: "a".repeat(64),
                top_fn_name: Some("top".into()),
                use_system_verilog: false,
                version: "v0.59.0".into(),
                runtime: driver(version),
            }
        } else {
            ActionSpec::DriverIrToG8rAig {
                ir_action_id: "a".repeat(64),
                top_fn_name: Some("top".into()),
                fraig: false,
                lowering_mode: G8rLoweringMode::FrontendNoPrepRewrite,
                execution_recipe_revision:
                    crate::versioning::driver_ir2g8r_execution_recipe_revision(version),
                version: "v0.59.0".into(),
                runtime: driver(version),
            }
        }
    }
    fn stage(
        store: &ArtifactStore,
        action: ActionSpec,
        kind: ArtifactType,
        leaf: &str,
        bytes: &[u8],
        deps: Vec<ArtifactRef>,
        details: serde_json::Value,
    ) -> Provenance {
        let id = compute_action_id(&action).unwrap();
        let staging = crate::executor::make_staging_dir(store, &id).unwrap();
        fs::create_dir(staging.join("payload")).unwrap();
        fs::write(staging.join("payload").join(leaf), bytes).unwrap();
        let provenance = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: id.clone(),
            created_utc: chrono::Utc::now(),
            action,
            dependencies: deps,
            output_artifact: ArtifactRef {
                action_id: id.clone(),
                artifact_type: kind,
                relpath: format!("payload/{leaf}"),
            },
            output_files: vec![OutputFile {
                path: leaf.into(),
                bytes: bytes.len() as u64,
                sha256: hex::encode(Sha256::digest(bytes)),
            }],
            commands: vec![CommandTrace {
                argv: vec!["synthetic-test-computation".into()],
                exit_code: 0,
            }],
            details,
            suggested_next_actions: Vec::new(),
        };
        fs::write(
            staging.join("provenance.pb"),
            proto::encode_provenance(&provenance).unwrap(),
        )
        .unwrap();
        store.promote_staging_action_dir(&id, &staging).unwrap();
        provenance
    }
    fn temp() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "bvc-cache-test-{}-{}",
            std::process::id(),
            NONCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        path
    }
    fn store(path: PathBuf) -> ArtifactStore {
        let store = ArtifactStore::new(path);
        store.ensure_layout().unwrap();
        store
    }

    #[test]
    fn reuse_executes_all_three_stages_without_docker_and_retains_current_identity() {
        let root = temp();
        let source = store(root.join("source"));
        let mut target = store(root.join("target"));
        let cache = root.join("cache");
        target.downstream_cache_dir = Some(cache.clone());
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
        for kind in 0..3 {
            let verilog = kind == 0;
            let input_type = if verilog {
                ArtifactType::VerilogFile
            } else {
                ArtifactType::AigFile
            };
            let leaf = if verilog { "result.v" } else { "result.aig" };
            let bytes = if verilog {
                b"module top(); endmodule\n".as_slice()
            } else {
                b"aag 0 0 0 0 0\n".as_slice()
            };
            let old_input = stage(
                &source,
                input_action("0.74.0", verilog),
                input_type.clone(),
                leaf,
                bytes,
                vec![],
                json!({"driver_runtime": driver("0.74.0"), "xlsynth_version":"v0.59.0"}),
            );
            let new_input = stage(
                &target,
                input_action("0.75.0", verilog),
                input_type,
                leaf,
                bytes,
                vec![],
                json!({"driver_runtime": driver("0.75.0"), "xlsynth_version":"v0.59.0"}),
            );
            let action = |id: String| match kind {
                0 => ActionSpec::ComboVerilogToYosysAbcAig {
                    verilog_action_id: id,
                    verilog_top_module_name: Some("top".into()),
                    frontend: YosysVerilogFrontend::Builtin,
                    yosys_script_ref: script(),
                    runtime: yosys(),
                },
                1 => ActionSpec::AigToYosysAbcAig {
                    aig_action_id: id,
                    yosys_script_ref: script(),
                    runtime: yosys(),
                },
                _ => ActionSpec::DriverAigToStats {
                    aig_action_id: id,
                    version: "v0.59.0".into(),
                    runtime: driver("0.74.0"),
                },
            };
            let output_type = if kind == 2 {
                ArtifactType::AigStatsFile
            } else {
                ArtifactType::AigFile
            };
            let output_leaf = if kind == 2 {
                "stats.json"
            } else {
                "result.aig"
            };
            let output_bytes = if kind == 2 {
                b"{\"nodes\":0}\n".as_slice()
            } else {
                b"aag 0 0 0 0 0\n".as_slice()
            };
            let old = stage(
                &source,
                action(old_input.action_id),
                output_type,
                output_leaf,
                output_bytes,
                vec![old_input.output_artifact],
                json!({"yosys_runtime":yosys(), "yosys_script_ref":script(),
                    "driver_runtime":driver("0.74.0"), "xlsynth_version":"v0.59.0", "aig_stats_mode":"aiger_aig_stats"}),
            );
            assert_eq!(
                seed(&source, repo, &cache, &[old.action_id.clone()]).unwrap(),
                1
            );
            let next_action = action(new_input.action_id.clone());
            let expected_key = key(&next_action, &new_input).unwrap();
            let cached_path = entry_path(&cache, &expected_key);
            let good_bytes = fs::read(&cached_path).unwrap();
            if kind == 0 {
                let good = pb::Entry::decode(good_bytes.as_slice()).unwrap();
                std::thread::scope(|scope| {
                    for _ in 0..4 {
                        let entry = &good;
                        let cache = &cache;
                        scope.spawn(move || {
                            for _ in 0..10 {
                                write_entry(cache, entry).unwrap();
                            }
                        });
                    }
                    for _ in 0..20 {
                        let bytes = fs::read(&cached_path).unwrap();
                        validated(&pb::Entry::decode(bytes.as_slice()).unwrap(), &expected_key)
                            .unwrap();
                    }
                });
            }
            let probe = root.join(format!("probe{kind}"));
            fs::create_dir(&probe).unwrap();
            for mutation in 0..9 {
                let mut bad = pb::Entry::decode(good_bytes.as_slice()).unwrap();
                match mutation {
                    0 => bad.output.push(0),
                    1 => {
                        bad.key
                            .as_mut()
                            .unwrap()
                            .input_sha256
                            .as_mut()
                            .unwrap()
                            .value[0] ^= 1
                    }
                    2 => bad.source.as_mut().unwrap().commands[0].exit_code = 1,
                    3 => bad.source.as_mut().unwrap().dependencies.clear(),
                    4 => bad.source.as_mut().unwrap().output_files[0].bytes += 1,
                    5 => {
                        bad.input.as_mut().unwrap().output_files[0]
                            .sha256
                            .as_mut()
                            .unwrap()
                            .value[0] ^= 1
                    }
                    6 => bad.key.as_mut().unwrap().execution_revision += 1,
                    7 => bad.source = None,
                    _ => bad.input = None,
                }
                fs::write(&cached_path, bad.encode_to_vec()).unwrap();
                assert!(
                    !restore(&target, repo, &next_action, &probe).unwrap(),
                    "accepted mutation {mutation}"
                );
                assert_eq!(fs::read_dir(&probe).unwrap().count(), 0);
            }
            fs::write(&cached_path, b"truncated protobuf").unwrap();
            assert!(!restore(&target, repo, &next_action, &probe).unwrap());
            fs::write(&cached_path, &good_bytes).unwrap();
            assert_ne!(old.action_id, compute_action_id(&next_action).unwrap());
            let (new_id, artifact) = crate::executor::execute_action(&target, next_action).unwrap();
            let result = target.load_provenance(&new_id).unwrap();
            assert!(
                result.commands.is_empty(),
                "a hit must not run or fabricate a tool command"
            );
            assert_eq!(result.dependencies[0].action_id, new_input.action_id);
            assert_eq!(
                fs::read(target.resolve_artifact_ref_path(&artifact)).unwrap(),
                output_bytes
            );
            let receipt_bytes = fs::read(
                target
                    .materialize_action_dir(&new_id)
                    .unwrap()
                    .join("payload/downstream-reuse.pb"),
            )
            .unwrap();
            let receipt = pb::ReuseReceipt::decode(receipt_bytes.as_slice()).unwrap();
            assert_eq!(
                receipt.source_action_id,
                Some(proto::action_id_to_proto(&old.action_id, "source").unwrap())
            );
            assert_eq!(receipt.key, Some(key(&result.action, &new_input).unwrap()));
            assert_eq!(
                receipt.source,
                Some(
                    v1::Provenance::decode(proto::encode_provenance(&old).unwrap().as_slice())
                        .unwrap()
                )
            );
            assert_eq!(
                seed(&target, repo, &cache, &[new_id]).unwrap(),
                0,
                "do not replace original computation with a reuse chain"
            );
        }
        drop(target);
        drop(source);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn key_binds_bytes_tools_flow_top_frontend_and_language_not_parent_identity() {
        let root = temp();
        let store = store(root.join("store"));
        let input = stage(
            &store,
            input_action("0.74.0", true),
            ArtifactType::VerilogFile,
            "result.v",
            b"module top(); endmodule\n",
            vec![],
            json!({"driver_runtime":driver("0.74.0"),"xlsynth_version":"v0.59.0"}),
        );
        let base = ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id: input.action_id.clone(),
            verilog_top_module_name: Some("top".into()),
            frontend: YosysVerilogFrontend::Builtin,
            yosys_script_ref: script(),
            runtime: yosys(),
        };
        let original = key(&base, &input).unwrap();
        for change in 0..7 {
            let mut action = base.clone();
            let mut different = input.clone();
            let ActionSpec::ComboVerilogToYosysAbcAig {
                verilog_top_module_name,
                frontend,
                yosys_script_ref,
                runtime,
                ..
            } = &mut action
            else {
                unreachable!()
            };
            match change {
                0 => different.output_files[0].sha256 = "1".repeat(64),
                1 => runtime.docker_image_id = "2".repeat(64),
                2 => yosys_script_ref.sha256 = "3".repeat(64),
                3 => *verilog_top_module_name = Some("another".into()),
                4 => {
                    *frontend = YosysVerilogFrontend::Slang {
                        revision: "4".repeat(40),
                    };
                    runtime.slang_commit = Some("4".repeat(40));
                }
                5 => {
                    different.output_artifact.relpath = "payload/result.sv".into();
                    different.output_files[0].path = "result.sv".into();
                }
                _ => *verilog_top_module_name = None,
            }
            assert_ne!(
                original,
                key(&action, &different).unwrap(),
                "mutation {change}"
            );
        }
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stats_key_binds_estimator_and_rejects_modified_live_inputs() {
        let root = temp();
        let mut store = store(root.join("store"));
        store.downstream_cache_dir = Some(root.join("cache"));
        let input = stage(
            &store,
            input_action("0.74.0", false),
            ArtifactType::AigFile,
            "result.aig",
            b"aag 0 0 0 0 0\n",
            vec![],
            json!({"driver_runtime":driver("0.74.0"),"xlsynth_version":"v0.59.0"}),
        );
        let action = ActionSpec::DriverAigToStats {
            aig_action_id: input.action_id.clone(),
            version: "v0.59.0".into(),
            runtime: driver("0.74.0"),
        };
        let first = key(&action, &input).unwrap();
        for n in 0..3 {
            let mut changed = action.clone();
            let ActionSpec::DriverAigToStats {
                version, runtime, ..
            } = &mut changed
            else {
                unreachable!()
            };
            match n {
                0 => *runtime = driver("0.75.0"),
                1 => runtime.release_cache_input_sha256 = "1".repeat(64),
                _ => *version = "v0.58.0".into(),
            }
            assert_ne!(first, key(&changed, &input).unwrap());
        }
        let path = store.resolve_artifact_ref_path(&input.output_artifact);
        fs::write(path, b"aag 0 0 0 1 0\n0\n").unwrap();
        assert!(request(&store, Path::new(env!("CARGO_MANIFEST_DIR")), &action).is_err());
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }
}
