use super::*;

pub(crate) fn default_worker_id() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
    format!("{}:{}", host, std::process::id())
}

pub(crate) fn default_completed_by() -> String {
    "unknown".to_string()
}

pub(crate) fn insert_driver_version_labels(
    details: &mut serde_json::Map<String, serde_json::Value>,
    dso_version: &str,
    runtime: &DriverRuntimeSpec,
) {
    details.insert(
        "crate_version_label".to_string(),
        json!(version_label("crate", &runtime.driver_version)),
    );
    details.insert(
        "dso_version_label".to_string(),
        json!(version_label("dso", dso_version)),
    );
}

pub(crate) fn dslx_to_mangled_ir_fn_name(dslx_module_name: &str, dslx_fn_name: &str) -> String {
    let module = sanitize_mangle_component(dslx_module_name);
    let function = sanitize_mangle_component(dslx_fn_name);
    format!("__{}__{}", module, function)
}

pub(crate) fn sanitize_mangle_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    out
}

pub(crate) fn build_suggestion(
    reason: impl Into<String>,
    action: ActionSpec,
) -> Result<SuggestedAction> {
    let action_id = compute_action_id(&action)?;
    Ok(SuggestedAction {
        reason: reason.into(),
        action_id,
        action,
    })
}

pub(crate) fn same_driver_runtime(lhs: &DriverRuntimeSpec, rhs: &DriverRuntimeSpec) -> bool {
    lhs.driver_version == rhs.driver_version
        && lhs.release_platform == rhs.release_platform
        && lhs.docker_image == rhs.docker_image
        && lhs.dockerfile == rhs.dockerfile
}

pub(crate) fn details_input_ir_structural_hash(details: &serde_json::Value) -> Option<&str> {
    details
        .get(INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY)
        .and_then(|v| v.as_str())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KnownInputIrStructuralHash {
    pub(crate) hash: String,
    pub(crate) source: &'static str,
}

pub(crate) fn resolve_known_input_ir_structural_hash(
    store: &ArtifactStore,
    ir_action_id: &str,
    ir_top: Option<&str>,
) -> Result<Option<KnownInputIrStructuralHash>> {
    let provenance = match store.load_provenance(ir_action_id) {
        Ok(provenance) => provenance,
        Err(_) => return Ok(None),
    };

    if let Some(raw_hash) = provenance
        .details
        .get("output_ir_fn_structural_hash")
        .and_then(|v| v.as_str())
        && let Some(hash) = normalized_structural_hash(raw_hash)
    {
        let produced_top = provenance.details.get("ir_top").and_then(|v| v.as_str());
        if ir_top.is_none() || produced_top == ir_top {
            return Ok(Some(KnownInputIrStructuralHash {
                hash,
                source: "producer_output_ir_fn_structural_hash",
            }));
        }
    }

    let ActionSpec::IrFnToKBoolConeCorpus { k, .. } = &provenance.action else {
        return Ok(None);
    };
    let Some(ir_top) = ir_top else {
        return Ok(None);
    };
    let manifest_relpath = provenance
        .details
        .get("output_manifest_relpath")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("payload/k_bool_cones_k{k}_manifest.json"));
    let manifest_path = store
        .materialize_action_dir(&provenance.action_id)?
        .join(manifest_relpath);
    if !manifest_path.exists() {
        return Ok(None);
    }
    let manifest_text = fs::read_to_string(&manifest_path)
        .with_context(|| format!("reading k-bool-cone manifest: {}", manifest_path.display()))?;
    let manifest: KBoolConeCorpusManifest = serde_json::from_str(&manifest_text)
        .with_context(|| format!("parsing k-bool-cone manifest: {}", manifest_path.display()))?;
    let Some(entry) = manifest
        .entries
        .iter()
        .find(|entry| entry.fn_name == ir_top)
    else {
        return Ok(None);
    };
    let Some(hash) = normalized_structural_hash(&entry.structural_hash) else {
        bail!(
            "invalid structural hash in k-bool-cone manifest {} for {}",
            manifest_path.display(),
            ir_top
        );
    };
    Ok(Some(KnownInputIrStructuralHash {
        hash,
        source: "k_bool_cone_manifest_entry",
    }))
}

pub(crate) fn find_semantic_reuse_candidate<F>(
    store: &ArtifactStore,
    input_ir_structural_hash: &str,
    matcher: F,
) -> Result<Option<Provenance>>
where
    F: Fn(&Provenance) -> bool,
{
    let mut selected: Option<Provenance> = None;
    store.for_each_provenance(|provenance| {
        if !matcher(&provenance) {
            return Ok(std::ops::ControlFlow::Continue(()));
        }
        if details_input_ir_structural_hash(&provenance.details) != Some(input_ir_structural_hash) {
            return Ok(std::ops::ControlFlow::Continue(()));
        }
        let source_payload_dir = store
            .materialize_action_dir(&provenance.action_id)?
            .join("payload");
        if !source_payload_dir.is_dir() {
            return Ok(std::ops::ControlFlow::Continue(()));
        }
        selected = Some(provenance);
        Ok(std::ops::ControlFlow::Break(()))
    })?;
    Ok(selected)
}

pub(crate) fn copy_directory_contents(source_dir: &Path, destination_dir: &Path) -> Result<()> {
    if !source_dir.is_dir() {
        bail!(
            "source directory for reuse does not exist or is not a directory: {}",
            source_dir.display()
        );
    }
    for entry in WalkDir::new(source_dir).sort_by_file_name() {
        let entry = entry.context("walking source payload directory")?;
        let path = entry.path();
        let rel = path
            .strip_prefix(source_dir)
            .with_context(|| format!("stripping source prefix for {}", path.display()))?;
        if rel.as_os_str().is_empty() {
            continue;
        }
        let destination = destination_dir.join(rel);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&destination).with_context(|| {
                format!("creating destination directory {}", destination.display())
            })?;
            continue;
        }
        if !entry.file_type().is_file() {
            bail!(
                "unsupported file type in payload reuse copy: {}",
                path.display()
            );
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating parent directory {}", parent.display()))?;
        }
        fs::copy(path, &destination).with_context(|| {
            format!(
                "copying payload file for semantic reuse: {} -> {}",
                path.display(),
                destination.display()
            )
        })?;
    }
    Ok(())
}

pub(crate) fn reuse_payload_from_provenance(
    store: &ArtifactStore,
    source: &Provenance,
    payload_dir: &Path,
) -> Result<()> {
    let source_payload_dir = store
        .materialize_action_dir(&source.action_id)?
        .join("payload");
    copy_directory_contents(&source_payload_dir, payload_dir)
}

pub(crate) fn make_temp_work_dir(prefix: &str) -> Result<PathBuf> {
    let pid = std::process::id();
    for attempt in 0..1024_u32 {
        let ts = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "xlsynth-bvc-{prefix}-{pid}-{ts}-{}",
            i64::from(attempt)
        ));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("creating temporary work directory: {}", path.display())
                });
            }
        }
    }
    bail!("unable to create temporary work directory for prefix {prefix}")
}

pub(crate) fn compute_ir_fn_structural_hash(
    store: &ArtifactStore,
    repo_root: &Path,
    ir_input_path: &Path,
    ir_top: Option<&str>,
    version: &str,
    runtime: &DriverRuntimeSpec,
) -> Result<(String, CommandTrace)> {
    if !ir_input_path.exists() {
        bail!(
            "input IR path for structural hash does not exist: {}",
            ir_input_path.display()
        );
    }
    let runtime_for_hash = resolve_driver_runtime_for_aig_stats(repo_root, runtime)?;
    ensure_driver_release_cache(
        store,
        repo_root,
        version,
        &runtime_for_hash.release_platform,
    )?;
    let scratch_dir = make_temp_work_dir("ir-fn-structural-hash")?;
    let result = (|| -> Result<(String, CommandTrace)> {
        let script = if ir_top.is_some() {
            driver_script(
                r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml ir-fn-structural-hash /inputs/input.ir --top "${IR_TOP}" --json true > /scratch/structural_hash.json
test -s /scratch/structural_hash.json
"#,
            )
        } else {
            driver_script(
                r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml ir-fn-structural-hash /inputs/input.ir --json true > /scratch/structural_hash.json
test -s /scratch/structural_hash.json
"#,
            )
        };

        let mut env = BTreeMap::new();
        env.insert("XLSYNTH_VERSION".to_string(), version.to_string());
        env.insert(
            "XLSYNTH_PLATFORM".to_string(),
            runtime_for_hash.release_platform.to_string(),
        );
        if let Some(top) = ir_top {
            env.insert("IR_TOP".to_string(), top.to_string());
        }

        let mounts = vec![
            DockerMount::read_only(ir_input_path, "/inputs/input.ir")?,
            DockerMount::read_write(&scratch_dir, "/scratch")?,
            driver_cache_mount(store)?,
        ];
        let run_trace = run_docker_script(
            &runtime_for_hash.docker_image,
            &mounts,
            &env,
            &script,
            "ir-fn-structural-hash",
        )?;

        let hash_json_path = scratch_dir.join("structural_hash.json");
        let hash_text = fs::read_to_string(&hash_json_path).with_context(|| {
            format!(
                "reading structural hash output: {}",
                hash_json_path.display()
            )
        })?;
        let parsed: IrFnStructuralHashResponse =
            serde_json::from_str(&hash_text).with_context(|| {
                format!(
                    "parsing structural hash JSON from {}",
                    hash_json_path.display()
                )
            })?;
        let structural_hash = parsed.structural_hash.to_ascii_lowercase();
        if structural_hash.len() != 64 || !structural_hash.chars().all(|c| c.is_ascii_hexdigit()) {
            bail!("invalid ir-fn-structural-hash value: {}", structural_hash);
        }
        Ok((structural_hash, run_trace))
    })();
    fs::remove_dir_all(&scratch_dir).ok();
    result
}

#[derive(Debug, Clone)]
pub(crate) struct VerilogDriverContext {
    pub(crate) ir_action_id: String,
    pub(crate) top_fn_name: Option<String>,
    pub(crate) version: String,
    pub(crate) runtime: DriverRuntimeSpec,
}

#[derive(Debug, Clone)]
pub(crate) struct LegacyG8rStatsContext {
    pub(crate) ir_action_id: String,
    pub(crate) fraig: bool,
    pub(crate) version: String,
    pub(crate) runtime: DriverRuntimeSpec,
    pub(crate) stats_relpath: Option<String>,
}

pub(crate) fn infer_driver_context_from_verilog_action(
    store: &ArtifactStore,
    verilog_action_id: &str,
) -> Result<Option<(String, DriverRuntimeSpec)>> {
    Ok(infer_verilog_driver_context(store, verilog_action_id)?
        .map(|ctx| (ctx.version, ctx.runtime)))
}

pub(crate) fn infer_driver_context_from_aig_action(
    store: &ArtifactStore,
    aig_action_id: &str,
) -> Result<Option<(String, DriverRuntimeSpec)>> {
    let provenance = store.load_provenance(aig_action_id)?;
    match provenance.action {
        ActionSpec::DriverIrToG8rAig {
            version, runtime, ..
        } => Ok(Some((version, runtime))),
        ActionSpec::AigToYosysAbcAig { aig_action_id, .. } => {
            infer_driver_context_from_aig_action(store, &aig_action_id)
        }
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id, ..
        } => infer_driver_context_from_verilog_action(store, &verilog_action_id),
        _ => Ok(None),
    }
}

pub(crate) fn infer_legacy_g8r_stats_context(
    store: &ArtifactStore,
    aig_action_id: &str,
) -> Result<Option<LegacyG8rStatsContext>> {
    let provenance = store.load_provenance(aig_action_id)?;
    let ActionSpec::DriverIrToG8rAig {
        ir_action_id,
        fraig,
        version,
        runtime,
        ..
    } = &provenance.action
    else {
        return Ok(None);
    };
    let legacy_cli = provenance
        .details
        .get("driver_ir2g8r_cli_mode")
        .and_then(|v| v.as_str())
        .map(|s| s == "legacy_bin_out")
        .unwrap_or(false);
    let legacy_runtime =
        cmp_dotted_numeric_version(&runtime.driver_version, "0.24.0") == std::cmp::Ordering::Less;
    if !legacy_cli && !legacy_runtime {
        return Ok(None);
    }
    let stats_relpath = provenance
        .details
        .get("legacy_g8r_stats_relpath")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            if legacy_cli {
                Some(LEGACY_G8R_STATS_RELPATH.to_string())
            } else {
                None
            }
        });
    Ok(Some(LegacyG8rStatsContext {
        ir_action_id: ir_action_id.clone(),
        fraig: *fraig,
        version: version.clone(),
        runtime: runtime.clone(),
        stats_relpath,
    }))
}

pub(crate) fn infer_verilog_driver_context(
    store: &ArtifactStore,
    verilog_action_id: &str,
) -> Result<Option<VerilogDriverContext>> {
    let provenance = store.load_provenance(verilog_action_id)?;
    let ActionSpec::IrFnToCombinationalVerilog {
        ir_action_id,
        top_fn_name,
        version,
        runtime,
        ..
    } = provenance.action
    else {
        return Ok(None);
    };
    Ok(Some(VerilogDriverContext {
        ir_action_id,
        top_fn_name,
        version,
        runtime,
    }))
}

pub(crate) fn build_aig_stat_diff_suggestion_for_stats_action(
    store: &ArtifactStore,
    repo_root: &Path,
    stats_action_id: &str,
    producer_aig_action_id: &str,
) -> Result<Option<SuggestedAction>> {
    let producer = store.load_provenance(producer_aig_action_id)?;
    match producer.action {
        ActionSpec::DriverIrToG8rAig {
            ir_action_id,
            top_fn_name,
            fraig,
            lowering_mode,
            version,
            runtime,
        } => {
            if fraig || lowering_mode != G8rLoweringMode::Default {
                return Ok(None);
            }
            let yosys_script_ref = match make_script_ref(repo_root, DEFAULT_YOSYS_FLOW_SCRIPT) {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            let combo = ActionSpec::IrFnToCombinationalVerilog {
                ir_action_id: ir_action_id.clone(),
                top_fn_name: top_fn_name.clone(),
                use_system_verilog: false,
                version: version.clone(),
                runtime: runtime.clone(),
            };
            let combo_action_id = compute_action_id(&combo)?;
            let yosys_aig = ActionSpec::ComboVerilogToYosysAbcAig {
                verilog_action_id: combo_action_id,
                verilog_top_module_name: top_fn_name.clone(),
                yosys_script_ref,
                runtime: default_yosys_runtime(),
            };
            let yosys_aig_action_id = compute_action_id(&yosys_aig)?;
            let stats_runtime = resolve_driver_runtime_for_aig_stats(repo_root, &runtime)
                .unwrap_or(runtime.clone());
            let yosys_stats = ActionSpec::DriverAigToStats {
                aig_action_id: yosys_aig_action_id,
                version: version.clone(),
                runtime: stats_runtime,
            };
            let yosys_stats_action_id = compute_action_id(&yosys_stats)?;
            if !action_is_materialized_or_active(store, &yosys_stats_action_id) {
                return Ok(None);
            }
            let diff = ActionSpec::AigStatDiff {
                opt_ir_action_id: ir_action_id,
                g8r_aig_stats_action_id: stats_action_id.to_string(),
                yosys_abc_aig_stats_action_id: yosys_stats_action_id,
            };
            Ok(Some(build_suggestion(
                "Compare g8r vs yosys/abc AIG stats with AigStatDiff",
                diff,
            )?))
        }
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id,
            yosys_script_ref,
            ..
        } => {
            if !is_canonical_yosys_script_ref(&yosys_script_ref) {
                return Ok(None);
            }
            let Some(ctx) = infer_verilog_driver_context(store, &verilog_action_id)? else {
                return Ok(None);
            };
            let g8r = ActionSpec::DriverIrToG8rAig {
                ir_action_id: ctx.ir_action_id.clone(),
                top_fn_name: ctx.top_fn_name.clone(),
                fraig: false,
                lowering_mode: G8rLoweringMode::Default,
                version: ctx.version.clone(),
                runtime: ctx.runtime.clone(),
            };
            let g8r_aig_action_id = compute_action_id(&g8r)?;
            let stats_runtime = resolve_driver_runtime_for_aig_stats(repo_root, &ctx.runtime)
                .unwrap_or(ctx.runtime.clone());
            let g8r_stats = ActionSpec::DriverAigToStats {
                aig_action_id: g8r_aig_action_id,
                version: ctx.version.clone(),
                runtime: stats_runtime,
            };
            let g8r_stats_action_id = compute_action_id(&g8r_stats)?;
            if !action_is_materialized_or_active(store, &g8r_stats_action_id) {
                return Ok(None);
            }
            let diff = ActionSpec::AigStatDiff {
                opt_ir_action_id: ctx.ir_action_id,
                g8r_aig_stats_action_id: g8r_stats_action_id,
                yosys_abc_aig_stats_action_id: stats_action_id.to_string(),
            };
            Ok(Some(build_suggestion(
                "Compare g8r vs yosys/abc AIG stats with AigStatDiff",
                diff,
            )?))
        }
        _ => Ok(None),
    }
}

pub(crate) fn action_is_materialized_or_active(store: &ArtifactStore, action_id: &str) -> bool {
    if store.action_exists(action_id) {
        return true;
    }
    matches!(
        queue_state_for_action(store, action_id),
        QueueState::Pending | QueueState::Running { .. }
    )
}

pub(crate) fn collect_numeric_leaf_values(value: &serde_json::Value) -> BTreeMap<String, f64> {
    pub(crate) fn walk(prefix: &str, value: &serde_json::Value, out: &mut BTreeMap<String, f64>) {
        match value {
            serde_json::Value::Number(n) => {
                if let Some(v) = n.as_f64() {
                    out.insert(prefix.to_string(), v);
                }
            }
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    let next = if prefix.is_empty() {
                        k.to_string()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    walk(&next, v, out);
                }
            }
            serde_json::Value::Array(items) => {
                for (i, v) in items.iter().enumerate() {
                    let next = if prefix.is_empty() {
                        format!("[{i}]")
                    } else {
                        format!("{prefix}[{i}]")
                    };
                    walk(&next, v, out);
                }
            }
            _ => {}
        }
    }
    let mut out = BTreeMap::new();
    walk("", value, &mut out);
    out
}

pub(crate) fn infer_ir_top_function(ir_path: &Path) -> Result<String> {
    let content = fs::read_to_string(ir_path)
        .with_context(|| format!("reading IR file for top inference: {}", ir_path.display()))?;
    let re = Regex::new(r"(?m)^top\s+fn\s+([A-Za-z_][A-Za-z0-9_$.]*)\s*\(")
        .context("compiling top-fn regex")?;
    let caps = re
        .captures(&content)
        .ok_or_else(|| anyhow!("unable to infer top function from IR; pass --top-fn-name"))?;
    Ok(caps
        .get(1)
        .ok_or_else(|| anyhow!("regex capture for top function missing"))?
        .as_str()
        .to_string())
}

pub(crate) fn validate_relative_subpath(path: &str) -> Result<()> {
    let p = Path::new(path);
    if p.is_absolute() {
        bail!("path must be relative, got absolute path: {path}");
    }
    for comp in p.components() {
        match comp {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir => {
                bail!("path must not contain '..': {path}");
            }
            Component::Prefix(_) | Component::RootDir => {
                bail!("path must not contain path prefixes/root: {path}");
            }
        }
    }
    Ok(())
}

pub(crate) fn normalize_subtree_path(path: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        bail!("subtree path must not be empty");
    }
    let slash_normalized = trimmed.replace('\\', "/");
    if slash_normalized.starts_with('/') {
        bail!("subtree path must be relative: {}", path);
    }
    let slash_normalized = slash_normalized.trim_matches('/').to_string();
    let mut components: Vec<String> = Vec::new();
    for comp in Path::new(&slash_normalized).components() {
        match comp {
            Component::Normal(part) => {
                let part = part
                    .to_str()
                    .ok_or_else(|| anyhow!("subtree path component was not utf-8"))?;
                if part.is_empty() {
                    continue;
                }
                components.push(part.to_string());
            }
            Component::CurDir => {}
            Component::ParentDir => bail!("subtree path must not contain '..': {}", path),
            Component::Prefix(_) | Component::RootDir => {
                bail!("subtree path must be relative: {}", path)
            }
        }
    }
    if components.is_empty() {
        bail!("subtree path must contain at least one normal path component");
    }
    let normalized = components.join("/");
    validate_relative_subpath(&normalized)?;
    Ok(normalized)
}

pub(crate) fn make_script_ref(repo_root: &Path, script_path: &str) -> Result<ScriptRef> {
    validate_relative_subpath(script_path)?;
    let normalized = script_path.replace('\\', "/");
    let path = repo_root.join(&normalized);
    let metadata = fs::metadata(&path)
        .with_context(|| format!("reading script metadata: {}", path.display()))?;
    if !metadata.is_file() {
        bail!("yosys script path is not a file: {}", path.display());
    }
    let sha256 = sha256_file(&path)?;
    Ok(ScriptRef {
        path: normalized,
        sha256,
    })
}

pub(crate) fn is_canonical_yosys_script_ref(script_ref: &ScriptRef) -> bool {
    fn normalize_script_path(path: &str) -> String {
        path.replace('\\', "/")
            .trim()
            .trim_start_matches("./")
            .trim_matches('/')
            .to_string()
    }
    normalize_script_path(&script_ref.path) == normalize_script_path(DEFAULT_YOSYS_FLOW_SCRIPT)
}

pub(crate) fn resolve_script_ref(repo_root: &Path, script_ref: &ScriptRef) -> Result<PathBuf> {
    validate_relative_subpath(&script_ref.path)?;
    let path = repo_root.join(&script_ref.path);
    let metadata = fs::metadata(&path)
        .with_context(|| format!("reading script metadata: {}", path.display()))?;
    if !metadata.is_file() {
        bail!("yosys script path is not a file: {}", path.display());
    }
    let actual_sha = sha256_file(&path)?;
    if actual_sha != script_ref.sha256 {
        bail!(
            "yosys script sha256 mismatch for {}; expected {} got {}",
            script_ref.path,
            script_ref.sha256,
            actual_sha
        );
    }
    Ok(path)
}

pub(crate) fn dslx_module_name_from_path(path: &str) -> Result<String> {
    let stem = Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("could not derive DSLX module name from path: {}", path))?;
    Ok(stem.to_string())
}

pub(crate) fn load_dependency_of_type(
    store: &ArtifactStore,
    action_id: &str,
    want_type: ArtifactType,
) -> Result<ArtifactRef> {
    let prov = store.load_provenance(action_id)?;
    if prov.output_artifact.artifact_type != want_type {
        bail!(
            "dependency action {} has wrong artifact type; wanted {:?}, got {:?}",
            action_id,
            want_type,
            prov.output_artifact.artifact_type
        );
    }
    Ok(prov.output_artifact)
}

#[derive(Debug, Clone)]
pub(crate) struct DslxOriginActionContext {
    pub(crate) dslx_subtree_action_id: String,
    pub(crate) dslx_file: String,
    pub(crate) dslx_fn_name: String,
}

pub(crate) fn resolve_dslx_origin_action_context_from_ir_action(
    store: &ArtifactStore,
    ir_action_id: &str,
) -> Result<Option<DslxOriginActionContext>> {
    let mut current = ir_action_id.to_string();
    for _ in 0..16 {
        if !store.action_exists(&current) {
            return Ok(None);
        }
        let provenance = store.load_provenance(&current)?;
        match provenance.action {
            ActionSpec::DriverDslxFnToIr {
                dslx_subtree_action_id,
                dslx_file,
                dslx_fn_name,
                ..
            } => {
                return Ok(Some(DslxOriginActionContext {
                    dslx_subtree_action_id,
                    dslx_file,
                    dslx_fn_name,
                }));
            }
            ActionSpec::DriverIrToOpt { ir_action_id, .. }
            | ActionSpec::IrFnToKBoolConeCorpus { ir_action_id, .. } => {
                current = ir_action_id;
            }
            _ => return Ok(None),
        }
    }
    Ok(None)
}

pub(crate) fn parse_ir_top_node_count(ir_text: &str) -> Option<u64> {
    parse_ir_fn_node_count(ir_text, "top fn ").or_else(|| parse_ir_fn_node_count(ir_text, "fn "))
}

pub(crate) fn parse_ir_fn_node_count(ir_text: &str, fn_prefix: &str) -> Option<u64> {
    let mut in_fn = false;
    let mut saw_open_brace = false;
    let mut brace_depth: i64 = 0;
    let mut count: u64 = 0;
    for line in ir_text.lines() {
        let trimmed = line.trim_start();
        if !in_fn {
            if trimmed.starts_with(fn_prefix) {
                in_fn = true;
                let opens = line.bytes().filter(|b| *b == b'{').count() as i64;
                let closes = line.bytes().filter(|b| *b == b'}').count() as i64;
                if opens > 0 {
                    saw_open_brace = true;
                }
                brace_depth += opens - closes;
                if saw_open_brace && brace_depth <= 0 {
                    return Some(count);
                }
            }
            continue;
        }
        if trimmed.contains(" = ") {
            count = count.saturating_add(1);
        }
        let opens = line.bytes().filter(|b| *b == b'{').count() as i64;
        let closes = line.bytes().filter(|b| *b == b'}').count() as i64;
        if opens > 0 {
            saw_open_brace = true;
        }
        brace_depth += opens - closes;
        if saw_open_brace && brace_depth <= 0 {
            return Some(count);
        }
    }
    None
}

pub(crate) fn parse_ir_function_signature(ir_text: &str, ir_top: &str) -> Option<String> {
    for line in ir_text.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("fn ") else {
            continue;
        };
        let Some(open_paren) = rest.find('(') else {
            continue;
        };
        let candidate_name = rest[..open_paren].trim();
        if candidate_name != ir_top {
            continue;
        }
        let mut signature = trimmed.to_string();
        if signature.ends_with('{') {
            signature.pop();
        }
        let signature = signature.trim().to_string();
        if signature.is_empty() {
            return None;
        }
        return Some(signature);
    }
    None
}

pub(crate) fn parse_ir_function_signature_from_file(
    path: &Path,
    ir_top: &str,
) -> Result<Option<String>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading IR text for signature parse: {}", path.display()))?;
    Ok(parse_ir_function_signature(&text, ir_top))
}

pub(crate) fn parse_ir_fn_node_count_by_name(ir_text: &str, ir_fn_name: &str) -> Option<u64> {
    let mut in_fn = false;
    let mut saw_open_brace = false;
    let mut brace_depth: i64 = 0;
    let mut count: u64 = 0;
    for line in ir_text.lines() {
        let trimmed = line.trim_start();
        if !in_fn {
            let rest = if let Some(rest) = trimmed.strip_prefix("top fn ") {
                rest
            } else if let Some(rest) = trimmed.strip_prefix("fn ") {
                rest
            } else {
                continue;
            };
            let Some(open_paren) = rest.find('(') else {
                continue;
            };
            let candidate_name = rest[..open_paren].trim();
            if candidate_name != ir_fn_name {
                continue;
            }
            in_fn = true;
            let opens = line.bytes().filter(|b| *b == b'{').count() as i64;
            let closes = line.bytes().filter(|b| *b == b'}').count() as i64;
            if opens > 0 {
                saw_open_brace = true;
            }
            brace_depth += opens - closes;
            if saw_open_brace && brace_depth <= 0 {
                return Some(count);
            }
            continue;
        }
        if trimmed.contains(" = ") {
            count = count.saturating_add(1);
        }
        let opens = line.bytes().filter(|b| *b == b'{').count() as i64;
        let closes = line.bytes().filter(|b| *b == b'}').count() as i64;
        if opens > 0 {
            saw_open_brace = true;
        }
        brace_depth += opens - closes;
        if saw_open_brace && brace_depth <= 0 {
            return Some(count);
        }
    }
    None
}

pub(crate) fn parse_ir_fn_op_count_from_file(path: &Path, ir_top: &str) -> Result<Option<u64>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading IR text for op-count parse: {}", path.display()))?;
    Ok(parse_ir_fn_node_count_by_name(&text, ir_top).or_else(|| parse_ir_top_node_count(&text)))
}

pub(crate) fn load_failed_queue_records(store: &ArtifactStore) -> Result<Vec<QueueFailed>> {
    store.load_failed_action_records()
}

pub(crate) fn short_id(value: &str) -> &str {
    value.get(..12).unwrap_or(value)
}

pub(crate) fn summarize_error(error: &str) -> String {
    let line = error
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("");
    if line.is_empty() {
        return "(empty)".to_string();
    }
    if line.len() <= 180 {
        return line.to_string();
    }
    let mut s = line[..180].to_string();
    s.push_str("...");
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_test_store() -> (ArtifactStore, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-core-test-{}-{}",
            std::process::id(),
            nanos
        ));
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure store layout");
        (store, root)
    }

    fn sample_runtime() -> DriverRuntimeSpec {
        DriverRuntimeSpec {
            driver_version: "0.39.0".to_string(),
            release_platform: "ubuntu2004".to_string(),
            docker_image: "xlsynth-bvc-driver:0.39.0".to_string(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".to_string(),
        }
    }

    fn stage_provenance(
        store: &ArtifactStore,
        provenance: &Provenance,
        staged_files: Vec<(&str, &[u8])>,
    ) {
        let staging_dir = store
            .staging_dir()
            .join(format!("{}-staged", provenance.action_id));
        fs::create_dir_all(&staging_dir).expect("create staging dir");
        for (relpath, bytes) in staged_files {
            let path = staging_dir.join(relpath);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create parent");
            }
            fs::write(path, bytes).expect("write staged file");
        }
        fs::write(
            staging_dir.join("provenance.json"),
            serde_json::to_string_pretty(provenance).expect("serialize provenance"),
        )
        .expect("write provenance");
        store
            .promote_staging_action_dir(&provenance.action_id, &staging_dir)
            .expect("promote staging action");
    }

    #[test]
    fn resolve_known_input_ir_structural_hash_reads_producer_output_hash() {
        let (store, root) = make_test_store();
        let producer_id = "a".repeat(64);
        let hash = "b".repeat(64);
        let provenance = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: producer_id.clone(),
            created_utc: Utc::now(),
            action: ActionSpec::DriverIrToOpt {
                ir_action_id: "c".repeat(64),
                top_fn_name: Some("__top".to_string()),
                version: "v0.39.0".to_string(),
                runtime: sample_runtime(),
            },
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: producer_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: "payload/package.opt.ir".to_string(),
            },
            output_files: Vec::new(),
            commands: Vec::new(),
            details: json!({
                "ir_top": "__top",
                "output_ir_fn_structural_hash": hash,
            }),
            suggested_next_actions: Vec::new(),
        };
        stage_provenance(
            &store,
            &provenance,
            vec![("payload/package.opt.ir", b"package test\n")],
        );

        let resolved = resolve_known_input_ir_structural_hash(&store, &producer_id, Some("__top"))
            .expect("resolve known hash")
            .expect("known hash exists");
        assert_eq!(resolved.hash, hash);
        assert_eq!(resolved.source, "producer_output_ir_fn_structural_hash");

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn resolve_known_input_ir_structural_hash_reads_k_bool_manifest_entry() {
        let (store, root) = make_test_store();
        let action_id = "d".repeat(64);
        let hash = "e".repeat(64);
        let runtime = sample_runtime();
        let manifest = KBoolConeCorpusManifest {
            schema_version: 1,
            source_ir_action_id: "f".repeat(64),
            source_ir_top: "__orig".to_string(),
            k: 3,
            max_ir_ops: None,
            total_manifest_rows: 1,
            emitted_cone_files: 1,
            deduped_unique_cones: 1,
            filtered_out_ir_op_count: 0,
            output_ir_relpath: "payload/k_bool_cones_k3.ir".to_string(),
            entries: vec![KBoolConeCorpusEntry {
                structural_hash: hash.clone(),
                fn_name: "__k3_cone_deadbeef".to_string(),
                source_index: 0,
                sink_node_index: 0,
                frontier_leaf_indices: vec![1, 2],
                frontier_non_literal_count: 2,
                included_node_count: 3,
                ir_fn_signature: None,
                ir_op_count: Some(3),
            }],
        };
        let provenance = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.clone(),
            created_utc: Utc::now(),
            action: ActionSpec::IrFnToKBoolConeCorpus {
                ir_action_id: "f".repeat(64),
                top_fn_name: Some("__orig".to_string()),
                k: 3,
                max_ir_ops: Some(16),
                version: "v0.39.0".to_string(),
                runtime,
            },
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: "payload/k_bool_cones_k3.ir".to_string(),
            },
            output_files: Vec::new(),
            commands: Vec::new(),
            details: json!({
                "output_manifest_relpath": "payload/k_bool_cones_k3_manifest.json",
            }),
            suggested_next_actions: Vec::new(),
        };
        stage_provenance(
            &store,
            &provenance,
            vec![
                ("payload/k_bool_cones_k3.ir", b"package k_bool_cones_k3\n"),
                (
                    "payload/k_bool_cones_k3_manifest.json",
                    serde_json::to_string_pretty(&manifest)
                        .expect("serialize manifest")
                        .as_bytes(),
                ),
            ],
        );

        let resolved =
            resolve_known_input_ir_structural_hash(&store, &action_id, Some("__k3_cone_deadbeef"))
                .expect("resolve manifest hash")
                .expect("manifest hash exists");
        assert_eq!(resolved.hash, hash);
        assert_eq!(resolved.source, "k_bool_cone_manifest_entry");

        fs::remove_dir_all(root).expect("cleanup");
    }
}
