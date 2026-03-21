use super::*;

pub(crate) fn normalized_structural_hash(value: &str) -> Option<String> {
    let hash = value.trim().to_ascii_lowercase();
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(hash)
}

fn is_structural_index_source_action(action: &ActionSpec) -> bool {
    matches!(
        action,
        ActionSpec::DriverIrToOpt { .. } | ActionSpec::IrFnToKBoolConeCorpus { .. }
    )
}

pub(crate) fn structural_index_source_action_set_sha256_from_provenances(
    provenances: &[Provenance],
) -> String {
    let mut action_ids: Vec<&str> = provenances
        .iter()
        .filter(|provenance| is_structural_index_source_action(&provenance.action))
        .map(|provenance| provenance.action_id.as_str())
        .collect();
    action_ids.sort_unstable();

    let mut hasher = Sha256::new();
    for action_id in action_ids {
        hasher.update(action_id.as_bytes());
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

pub(crate) fn check_ir_fn_corpus_structural_freshness(
    store: &ArtifactStore,
) -> Result<CheckIrFnCorpusStructuralFreshnessSummary> {
    let provenances = load_all_provenances(store)?;
    let current_total_driver_ir_to_opt_actions = provenances
        .iter()
        .filter(|provenance| matches!(provenance.action, ActionSpec::DriverIrToOpt { .. }))
        .count();
    let current_total_ir_fn_to_k_bool_cone_corpus_actions = provenances
        .iter()
        .filter(|provenance| matches!(provenance.action, ActionSpec::IrFnToKBoolConeCorpus { .. }))
        .count();
    let latest_relevant_action_created_utc = provenances
        .iter()
        .filter(|provenance| is_structural_index_source_action(&provenance.action))
        .map(|provenance| provenance.created_utc)
        .max();
    let current_source_action_set_sha256 =
        structural_index_source_action_set_sha256_from_provenances(&provenances);

    let manifest_key = ir_fn_corpus_structural_manifest_index_key();
    let manifest_location = ir_fn_corpus_structural_index_location(manifest_key);
    let manifest = match store.load_web_index_bytes(manifest_key)? {
        Some(bytes) => Some(
            serde_json::from_slice::<IrFnCorpusStructuralManifest>(&bytes).with_context(|| {
                format!("parsing structural index manifest: {}", manifest_location)
            })?,
        ),
        None => None,
    };

    let mut stale_reasons = Vec::new();
    if let Some(manifest) = manifest.as_ref() {
        if manifest.total_driver_ir_to_opt_actions != current_total_driver_ir_to_opt_actions {
            stale_reasons.push(format!(
                "driver_ir_to_opt count mismatch manifest={} current={}",
                manifest.total_driver_ir_to_opt_actions, current_total_driver_ir_to_opt_actions
            ));
        }
        if manifest.total_ir_fn_to_k_bool_cone_corpus_actions
            != current_total_ir_fn_to_k_bool_cone_corpus_actions
        {
            stale_reasons.push(format!(
                "ir_fn_to_k_bool_cone_corpus count mismatch manifest={} current={}",
                manifest.total_ir_fn_to_k_bool_cone_corpus_actions,
                current_total_ir_fn_to_k_bool_cone_corpus_actions
            ));
        }
        match manifest.source_action_set_sha256.as_deref() {
            Some(manifest_hash) if manifest_hash == current_source_action_set_sha256 => {}
            Some(manifest_hash) => stale_reasons.push(format!(
                "source action fingerprint mismatch manifest={} current={}",
                manifest_hash, current_source_action_set_sha256
            )),
            None => stale_reasons.push(
                "manifest missing source_action_set_sha256 fingerprint (legacy index)".to_string(),
            ),
        }
        if let Some(latest_created_utc) = latest_relevant_action_created_utc
            && latest_created_utc > manifest.generated_utc
        {
            stale_reasons.push(format!(
                "newer structural-source action exists latest_created_utc={} manifest_generated_utc={}",
                latest_created_utc.to_rfc3339(),
                manifest.generated_utc.to_rfc3339(),
            ));
        }
    } else {
        stale_reasons.push(format!(
            "manifest missing at {} (run populate-ir-fn-corpus-structural)",
            manifest_location
        ));
    }

    Ok(CheckIrFnCorpusStructuralFreshnessSummary {
        up_to_date: manifest.is_some() && stale_reasons.is_empty(),
        stale_reasons,
        manifest_present: manifest.is_some(),
        manifest_generated_utc: manifest.as_ref().map(|m| m.generated_utc),
        manifest_source_action_set_sha256: manifest
            .as_ref()
            .and_then(|m| m.source_action_set_sha256.clone()),
        current_source_action_set_sha256,
        manifest_total_driver_ir_to_opt_actions: manifest
            .as_ref()
            .map(|m| m.total_driver_ir_to_opt_actions),
        current_total_driver_ir_to_opt_actions,
        manifest_total_ir_fn_to_k_bool_cone_corpus_actions: manifest
            .as_ref()
            .map(|m| m.total_ir_fn_to_k_bool_cone_corpus_actions),
        current_total_ir_fn_to_k_bool_cone_corpus_actions,
        latest_relevant_action_created_utc,
    })
}

pub(crate) fn load_all_provenances(store: &ArtifactStore) -> Result<Vec<Provenance>> {
    store.list_provenances()
}

pub(crate) fn collect_ir_structural_hash_hints(
    provenances: &[Provenance],
) -> BTreeMap<(String, Option<String>), IrStructuralHashHint> {
    let mut hints: BTreeMap<(String, Option<String>), IrStructuralHashHint> = BTreeMap::new();
    for provenance in provenances {
        let Some(raw_hash) = details_input_ir_structural_hash(&provenance.details) else {
            continue;
        };
        let Some(hash) = normalized_structural_hash(raw_hash) else {
            continue;
        };
        let ir_top = provenance
            .details
            .get("ir_top")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        for dep in provenance
            .dependencies
            .iter()
            .filter(|dep| dep.artifact_type == ArtifactType::IrPackageFile)
        {
            let entry = hints
                .entry((dep.action_id.clone(), ir_top.clone()))
                .or_default();
            entry.hashes.insert(hash.clone());
            entry.source_action_ids.insert(provenance.action_id.clone());
        }
    }
    hints
}

pub(crate) fn ir_fn_corpus_structural_group_index_key(structural_hash: &str) -> String {
    format!(
        "{}/{}",
        WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE,
        hash_group_relpath(structural_hash)
    )
}

pub(crate) fn ir_fn_corpus_structural_manifest_index_key() -> &'static str {
    WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY
}

pub(crate) fn ir_fn_corpus_structural_index_prefix() -> String {
    format!("{}/", WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE)
}

pub(crate) fn ir_fn_corpus_structural_index_location(index_key: &str) -> String {
    format!("sled://web_index_bytes/{}", index_key)
}

pub(crate) fn write_json_pretty_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating JSON parent directory: {}", parent.display()))?;
    }
    let serialized = serde_json::to_vec_pretty(value).context("serializing JSON value")?;
    fs::write(path, serialized).with_context(|| format!("writing JSON file: {}", path.display()))
}

pub(crate) fn output_file_metadata_for_provenance(
    provenance: &Provenance,
    output_path: &Path,
) -> Result<(u64, String)> {
    let payload_relpath = provenance
        .output_artifact
        .relpath
        .strip_prefix("payload/")
        .unwrap_or(provenance.output_artifact.relpath.as_str());
    if let Some(file) = provenance
        .output_files
        .iter()
        .find(|file| file.path == payload_relpath)
    {
        return Ok((file.bytes, file.sha256.clone()));
    }
    let metadata = fs::metadata(output_path)
        .with_context(|| format!("reading output metadata: {}", output_path.display()))?;
    Ok((metadata.len(), sha256_file(output_path)?))
}

pub(crate) fn hash_group_relpath(structural_hash: &str) -> String {
    format!(
        "by-hash/{}/{}/{}.json",
        &structural_hash[0..2],
        &structural_hash[2..4],
        structural_hash
    )
}

pub(crate) fn should_emit_structural_scan_progress(
    scanned_actions: usize,
    last_emit: &Instant,
) -> bool {
    scanned_actions == 1
        || scanned_actions.is_multiple_of(STRUCTURAL_SCAN_PROGRESS_EVERY_ACTIONS)
        || last_emit.elapsed() >= Duration::from_secs(STRUCTURAL_SCAN_PROGRESS_INTERVAL_SECS)
}

pub(crate) fn recompute_structural_hashes(
    store: &ArtifactStore,
    repo_root: &Path,
    jobs: Vec<StructuralHashRecomputeJob>,
    threads: usize,
) -> Result<Vec<StructuralHashRecomputeResult>> {
    if jobs.is_empty() {
        return Ok(Vec::new());
    }
    if threads == 0 {
        bail!("--threads must be > 0, got 0");
    }
    let worker_threads = threads.min(jobs.len());
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_threads)
        .build()
        .context("building structural hash recompute thread pool")?;
    let repo_root = repo_root.to_path_buf();
    let store_root = store.root.clone();
    let artifacts_sled_db_path = store.artifacts_sled_db_path();
    Ok(thread_pool.install(|| {
        jobs.into_par_iter()
            .map(|job| {
                let worker_store = ArtifactStore::new_with_sled(
                    store_root.clone(),
                    artifacts_sled_db_path.clone(),
                );
                match compute_ir_fn_structural_hash(
                    &worker_store,
                    &repo_root,
                    &job.output_path,
                    Some(job.member.ir_top.as_str()),
                    &job.dso_version,
                    &job.runtime,
                ) {
                    Ok((structural_hash, _trace)) => StructuralHashRecomputeResult {
                        member: job.member,
                        structural_hash: Some(structural_hash),
                        error: None,
                    },
                    Err(err) => StructuralHashRecomputeResult {
                        member: job.member,
                        structural_hash: None,
                        error: Some(format!("{:#}", err)),
                    },
                }
            })
            .collect()
    }))
}

pub(crate) fn populate_ir_fn_corpus_structural_index(
    store: &ArtifactStore,
    repo_root: &Path,
    _output_dir: &Path,
    recompute_missing_hashes: bool,
    threads: usize,
) -> Result<PopulateIrFnCorpusStructuralSummary> {
    if threads == 0 {
        bail!("--threads must be > 0, got 0");
    }
    let generated_utc = Utc::now();
    let provenances = load_all_provenances(store)?;
    let source_action_set_sha256 =
        structural_index_source_action_set_sha256_from_provenances(&provenances);
    let total_actions_scanned = provenances.len();
    let total_driver_ir_to_opt_actions = provenances
        .iter()
        .filter(|provenance| matches!(provenance.action, ActionSpec::DriverIrToOpt { .. }))
        .count();
    let total_ir_fn_to_k_bool_cone_corpus_actions = provenances
        .iter()
        .filter(|provenance| matches!(provenance.action, ActionSpec::IrFnToKBoolConeCorpus { .. }))
        .count();
    let hash_hints = collect_ir_structural_hash_hints(&provenances);
    let progress_start = Instant::now();
    let mut last_progress_emit = Instant::now();

    eprintln!(
        "populate-ir-fn-corpus-structural: scanning {} provenance records (driver_ir_to_opt={} recompute_missing_hashes={} threads={})",
        total_actions_scanned, total_driver_ir_to_opt_actions, recompute_missing_hashes, threads
    );

    let mut groups: BTreeMap<String, Vec<IrFnCorpusStructuralMember>> = BTreeMap::new();

    let mut scanned_driver_ir_to_opt_actions = 0_usize;
    let mut recompute_jobs: Vec<StructuralHashRecomputeJob> = Vec::new();
    let mut indexed_actions = 0_usize;
    let mut indexed_k_bool_cone_members = 0_usize;
    let mut hash_from_dependency_hint_count = 0_usize;
    let mut hash_recomputed_count = 0_usize;
    let mut hash_hint_conflict_count = 0_usize;
    let mut skipped_missing_output_count = 0_usize;
    let mut skipped_missing_ir_top_count = 0_usize;
    let mut skipped_missing_hash_hint_count = 0_usize;
    let mut skipped_hash_error_count = 0_usize;
    let mut skipped_k_bool_cone_manifest_errors = 0_usize;
    let mut skipped_k_bool_cone_empty_count = 0_usize;

    for provenance in &provenances {
        let ActionSpec::DriverIrToOpt {
            ir_action_id,
            top_fn_name,
            version,
            runtime,
        } = &provenance.action
        else {
            continue;
        };
        scanned_driver_ir_to_opt_actions += 1;
        if should_emit_structural_scan_progress(
            scanned_driver_ir_to_opt_actions,
            &last_progress_emit,
        ) {
            eprintln!(
                "populate-ir-fn-corpus-structural: progress scanned={}/{} indexed={} distinct={} hinted={} recomputed={} skipped_missing_hash_hint={} skipped_hash_error={} queued_recompute={} elapsed={:.1}s",
                scanned_driver_ir_to_opt_actions,
                total_driver_ir_to_opt_actions,
                indexed_actions,
                groups.len(),
                hash_from_dependency_hint_count,
                hash_recomputed_count,
                skipped_missing_hash_hint_count,
                skipped_hash_error_count,
                recompute_jobs.len(),
                progress_start.elapsed().as_secs_f64()
            );
            last_progress_emit = Instant::now();
        }

        let ir_top = provenance
            .details
            .get("ir_top")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| top_fn_name.clone());
        let Some(ir_top) = ir_top else {
            skipped_missing_ir_top_count += 1;
            continue;
        };

        let output_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
        if !output_path.exists() || !output_path.is_file() {
            skipped_missing_output_count += 1;
            continue;
        }

        let hint_key_exact = (provenance.action_id.clone(), Some(ir_top.clone()));
        let hint_key_fallback = (provenance.action_id.clone(), None);
        let mut hinted_hash = None;
        let mut hint_source_action_ids = Vec::new();

        if let Some(hint) = hash_hints.get(&hint_key_exact) {
            hint_source_action_ids = hint.source_action_ids.iter().cloned().collect();
            if hint.hashes.len() == 1 {
                hinted_hash = hint.hashes.iter().next().cloned();
            } else if hint.hashes.len() > 1 {
                hash_hint_conflict_count += 1;
            }
        } else if let Some(hint) = hash_hints.get(&hint_key_fallback) {
            hint_source_action_ids = hint.source_action_ids.iter().cloned().collect();
            if hint.hashes.len() == 1 {
                hinted_hash = hint.hashes.iter().next().cloned();
            } else if hint.hashes.len() > 1 {
                hash_hint_conflict_count += 1;
            }
        }

        if hinted_hash.is_none() && !recompute_missing_hashes {
            skipped_missing_hash_hint_count += 1;
            continue;
        }

        let (output_file_bytes, output_file_sha256) =
            output_file_metadata_for_provenance(provenance, &output_path)?;
        let ir_fn_signature = parse_ir_function_signature_from_file(&output_path, &ir_top)
            .ok()
            .flatten();
        let ir_op_count = parse_ir_fn_op_count_from_file(&output_path, &ir_top)
            .ok()
            .flatten();
        let dslx_origin =
            resolve_dslx_origin_action_context_from_ir_action(store, &provenance.action_id)?.map(
                |origin| IrFnCorpusStructuralDslxOrigin {
                    dslx_subtree_action_id: origin.dslx_subtree_action_id,
                    dslx_file: origin.dslx_file,
                    dslx_fn_name: origin.dslx_fn_name,
                },
            );

        let member = IrFnCorpusStructuralMember {
            opt_ir_action_id: provenance.action_id.clone(),
            source_ir_action_id: ir_action_id.clone(),
            ir_top,
            ir_fn_signature,
            ir_op_count,
            crate_version: normalize_tag_version(&runtime.driver_version).to_string(),
            dso_version: normalize_tag_version(version).to_string(),
            created_utc: provenance.created_utc,
            output_artifact: provenance.output_artifact.clone(),
            output_file_sha256,
            output_file_bytes,
            hash_source: String::new(),
            hash_hint_source_action_ids: Vec::new(),
            dslx_origin,
            producer_action_kind: Some("driver_ir_to_opt".to_string()),
        };
        if let Some(structural_hash) = hinted_hash {
            hash_from_dependency_hint_count += 1;
            let mut member = member;
            member.hash_source = "dependency_hint".to_string();
            member.hash_hint_source_action_ids = hint_source_action_ids;
            groups.entry(structural_hash).or_default().push(member);
            indexed_actions += 1;
            continue;
        }

        recompute_jobs.push(StructuralHashRecomputeJob {
            member,
            output_path,
            dso_version: version.clone(),
            runtime: runtime.clone(),
        });
    }

    if recompute_missing_hashes && !recompute_jobs.is_empty() {
        let recompute_job_count = recompute_jobs.len();
        let recompute_threads = threads.min(recompute_job_count);
        let mut last_recompute_emit = Instant::now();
        let mut recompute_done = 0_usize;
        let mut sampled_recompute_errors = Vec::new();

        eprintln!(
            "populate-ir-fn-corpus-structural: recomputing {} structural hashes with {} worker threads",
            recompute_job_count, recompute_threads
        );

        let recompute_results =
            recompute_structural_hashes(store, repo_root, recompute_jobs, recompute_threads)?;
        for mut result in recompute_results {
            recompute_done += 1;
            if should_emit_structural_scan_progress(recompute_done, &last_recompute_emit) {
                eprintln!(
                    "populate-ir-fn-corpus-structural: recompute completed={}/{} indexed={} distinct={} recomputed={} skipped_hash_error={} elapsed={:.1}s",
                    recompute_done,
                    recompute_job_count,
                    indexed_actions,
                    groups.len(),
                    hash_recomputed_count,
                    skipped_hash_error_count,
                    progress_start.elapsed().as_secs_f64()
                );
                last_recompute_emit = Instant::now();
            }

            if let Some(structural_hash) = result.structural_hash {
                result.member.hash_source = "recomputed".to_string();
                groups
                    .entry(structural_hash)
                    .or_default()
                    .push(result.member);
                indexed_actions += 1;
                hash_recomputed_count += 1;
                continue;
            }
            skipped_hash_error_count += 1;
            if sampled_recompute_errors.len() < 5 {
                let error_text = result
                    .error
                    .unwrap_or_else(|| "(unknown error)".to_string());
                sampled_recompute_errors.push(format!(
                    "action={} top={} error={}",
                    short_id(&result.member.opt_ir_action_id),
                    result.member.ir_top,
                    summarize_error(&error_text)
                ));
            }
        }

        if !sampled_recompute_errors.is_empty() {
            eprintln!(
                "populate-ir-fn-corpus-structural: sampled recompute hash errors (showing {} of {})",
                sampled_recompute_errors.len(),
                skipped_hash_error_count
            );
            for sample in sampled_recompute_errors {
                eprintln!("  {}", sample);
            }
        }
    }

    for provenance in &provenances {
        let ActionSpec::IrFnToKBoolConeCorpus {
            ir_action_id,
            k,
            version,
            runtime,
            ..
        } = &provenance.action
        else {
            continue;
        };

        let output_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
        if !output_path.exists() || !output_path.is_file() {
            skipped_missing_output_count += 1;
            continue;
        }

        let default_manifest_relpath = format!("payload/k_bool_cones_k{}_manifest.json", k);
        let manifest_relpath = provenance
            .details
            .get("output_manifest_relpath")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(default_manifest_relpath);
        let manifest_ref = ArtifactRef {
            action_id: provenance.action_id.clone(),
            artifact_type: ArtifactType::IrPackageFile,
            relpath: manifest_relpath.clone(),
        };
        let manifest_path = store.resolve_artifact_ref_path(&manifest_ref);
        if !manifest_path.exists() || !manifest_path.is_file() {
            skipped_k_bool_cone_manifest_errors += 1;
            continue;
        }

        let manifest_text = match fs::read_to_string(&manifest_path) {
            Ok(text) => text,
            Err(_) => {
                skipped_k_bool_cone_manifest_errors += 1;
                continue;
            }
        };
        let manifest: KBoolConeCorpusManifest = match serde_json::from_str(&manifest_text) {
            Ok(parsed) => parsed,
            Err(_) => {
                skipped_k_bool_cone_manifest_errors += 1;
                continue;
            }
        };
        if manifest.entries.is_empty() {
            skipped_k_bool_cone_empty_count += 1;
            continue;
        }

        let (output_file_bytes, output_file_sha256) =
            output_file_metadata_for_provenance(provenance, &output_path)?;
        let dslx_origin = resolve_dslx_origin_action_context_from_ir_action(store, ir_action_id)?
            .map(|origin| IrFnCorpusStructuralDslxOrigin {
                dslx_subtree_action_id: origin.dslx_subtree_action_id,
                dslx_file: origin.dslx_file,
                dslx_fn_name: origin.dslx_fn_name,
            });

        for cone_entry in &manifest.entries {
            let Some(structural_hash) = normalized_structural_hash(&cone_entry.structural_hash)
            else {
                skipped_k_bool_cone_manifest_errors += 1;
                continue;
            };
            let ir_top = cone_entry.fn_name.trim().to_string();
            if ir_top.is_empty() {
                skipped_k_bool_cone_manifest_errors += 1;
                continue;
            }
            let ir_fn_signature = cone_entry.ir_fn_signature.clone().or_else(|| {
                parse_ir_function_signature_from_file(&output_path, &ir_top)
                    .ok()
                    .flatten()
            });
            let ir_op_count = cone_entry.ir_op_count.or_else(|| {
                parse_ir_fn_op_count_from_file(&output_path, &ir_top)
                    .ok()
                    .flatten()
            });

            let member = IrFnCorpusStructuralMember {
                opt_ir_action_id: provenance.action_id.clone(),
                source_ir_action_id: ir_action_id.clone(),
                ir_top,
                ir_fn_signature,
                ir_op_count,
                crate_version: normalize_tag_version(&runtime.driver_version).to_string(),
                dso_version: normalize_tag_version(version).to_string(),
                created_utc: provenance.created_utc,
                output_artifact: provenance.output_artifact.clone(),
                output_file_sha256: output_file_sha256.clone(),
                output_file_bytes,
                hash_source: "k_bool_cone_manifest".to_string(),
                hash_hint_source_action_ids: vec![provenance.action_id.clone()],
                dslx_origin: dslx_origin.clone(),
                producer_action_kind: Some("ir_fn_to_k_bool_cone_corpus".to_string()),
            };
            groups.entry(structural_hash).or_default().push(member);
            indexed_actions += 1;
            indexed_k_bool_cone_members += 1;
        }
    }

    let index_prefix = ir_fn_corpus_structural_index_prefix();
    let cleared_count = store
        .delete_web_index_keys_with_prefix(&index_prefix)
        .with_context(|| {
            format!(
                "clearing existing structural index keys with prefix {}",
                index_prefix
            )
        })?;
    if cleared_count > 0 {
        eprintln!(
            "populate-ir-fn-corpus-structural: cleared {} prior structural index keys",
            cleared_count
        );
    }

    let mut manifest_groups = Vec::new();
    for (structural_hash, members) in &mut groups {
        members.sort_by(|a, b| {
            a.opt_ir_action_id
                .cmp(&b.opt_ir_action_id)
                .then(a.ir_top.cmp(&b.ir_top))
        });
        let mut ir_op_counts = BTreeSet::new();
        for member in members.iter() {
            if let Some(count) = member.ir_op_count {
                ir_op_counts.insert(count);
            }
        }
        let ir_node_count = if ir_op_counts.len() == 1 {
            ir_op_counts.iter().next().copied()
        } else {
            None
        };
        let relpath = hash_group_relpath(structural_hash);
        let group = IrFnCorpusStructuralGroupFile {
            schema_version: IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
            structural_hash: structural_hash.clone(),
            members: members.clone(),
        };
        let group_key = ir_fn_corpus_structural_group_index_key(structural_hash);
        let group_bytes =
            serde_json::to_vec_pretty(&group).context("serializing structural group")?;
        store
            .write_web_index_bytes(&group_key, &group_bytes)
            .with_context(|| format!("writing structural group index key {}", group_key))?;
        manifest_groups.push(IrFnCorpusStructuralManifestGroup {
            structural_hash: structural_hash.clone(),
            member_count: members.len(),
            relpath,
            ir_node_count,
        });
    }

    debug_assert_eq!(
        indexed_actions,
        groups.values().map(|members| members.len()).sum::<usize>()
    );
    let manifest = IrFnCorpusStructuralManifest {
        schema_version: IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
        generated_utc,
        store_root: store.root.display().to_string(),
        output_dir: ir_fn_corpus_structural_index_location(&index_prefix),
        recompute_missing_hashes,
        total_actions_scanned,
        total_driver_ir_to_opt_actions,
        total_ir_fn_to_k_bool_cone_corpus_actions,
        indexed_actions,
        indexed_k_bool_cone_members,
        distinct_structural_hashes: groups.len(),
        hash_from_dependency_hint_count,
        hash_recomputed_count,
        hash_hint_conflict_count,
        skipped_missing_output_count,
        skipped_missing_ir_top_count,
        skipped_missing_hash_hint_count,
        skipped_hash_error_count,
        skipped_k_bool_cone_manifest_errors,
        skipped_k_bool_cone_empty_count,
        source_action_set_sha256: Some(source_action_set_sha256),
        groups: manifest_groups,
    };
    let manifest_key = ir_fn_corpus_structural_manifest_index_key();
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).context("serializing structural index manifest")?;
    store
        .write_web_index_bytes(manifest_key, &manifest_bytes)
        .with_context(|| format!("writing structural manifest index key {}", manifest_key))?;
    store.flush_durable()?;

    eprintln!(
        "populate-ir-fn-corpus-structural: completed indexed_actions={} distinct_structural_hashes={} skipped_missing_hash_hint={} skipped_hash_error={} elapsed={:.1}s",
        indexed_actions,
        groups.len(),
        skipped_missing_hash_hint_count,
        skipped_hash_error_count,
        progress_start.elapsed().as_secs_f64()
    );

    Ok(PopulateIrFnCorpusStructuralSummary {
        output_dir: ir_fn_corpus_structural_index_location(&index_prefix),
        manifest_path: ir_fn_corpus_structural_index_location(manifest_key),
        generated_utc,
        recompute_missing_hashes,
        threads,
        total_actions_scanned,
        total_driver_ir_to_opt_actions,
        total_ir_fn_to_k_bool_cone_corpus_actions,
        indexed_actions,
        indexed_k_bool_cone_members,
        distinct_structural_hashes: groups.len(),
        hash_from_dependency_hint_count,
        hash_recomputed_count,
        hash_hint_conflict_count,
        skipped_missing_output_count,
        skipped_missing_ir_top_count,
        skipped_missing_hash_hint_count,
        skipped_hash_error_count,
        skipped_k_bool_cone_manifest_errors,
        skipped_k_bool_cone_empty_count,
    })
}

pub(crate) fn should_prefer_structural_opt_ir_candidate(
    candidate: &StructuralOptIrCandidate,
    current: &StructuralOptIrCandidate,
) -> bool {
    match cmp_dotted_numeric_version(&candidate.crate_version, &current.crate_version) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => {
            if candidate.created_utc > current.created_utc {
                true
            } else if candidate.created_utc < current.created_utc {
                false
            } else {
                candidate.opt_ir_action_id < current.opt_ir_action_id
            }
        }
    }
}

pub(crate) fn structural_hash_coverage_priority(state: StructuralHashCoverageState) -> u8 {
    match state {
        StructuralHashCoverageState::Done => 5,
        StructuralHashCoverageState::Running => 4,
        StructuralHashCoverageState::Pending => 3,
        StructuralHashCoverageState::Failed => 2,
        StructuralHashCoverageState::Canceled => 1,
    }
}

pub(crate) fn record_structural_hash_coverage(
    by_hash: &mut BTreeMap<String, StructuralHashCoverageRecord>,
    structural_hash: String,
    state: StructuralHashCoverageState,
) {
    if let Some(existing) = by_hash.get(&structural_hash)
        && structural_hash_coverage_priority(existing.state)
            >= structural_hash_coverage_priority(state)
    {
        return;
    }
    by_hash.insert(structural_hash, StructuralHashCoverageRecord { state });
}

pub(crate) fn structural_hash_for_matching_target_g8r_action(
    action: &ActionSpec,
    details: Option<&serde_json::Value>,
    target_crate_version: &str,
    target_dso_version: &str,
    target_fraig: bool,
    opt_structural_hash_by_action_id: &BTreeMap<String, String>,
) -> Option<String> {
    let ActionSpec::DriverIrToG8rAig {
        ir_action_id,
        fraig,
        lowering_mode,
        version,
        runtime,
        ..
    } = action
    else {
        return None;
    };
    if *lowering_mode != G8rLoweringMode::Default {
        return None;
    }
    if *fraig != target_fraig {
        return None;
    }
    if normalize_tag_version(version) != target_dso_version {
        return None;
    }
    if normalize_tag_version(&runtime.driver_version) != target_crate_version {
        return None;
    }
    if let Some(details) = details
        && let Some(hash) =
            details_input_ir_structural_hash(details).and_then(normalized_structural_hash)
    {
        return Some(hash);
    }
    opt_structural_hash_by_action_id.get(ir_action_id).cloned()
}

pub(crate) fn structural_hash_for_matching_target_k_bool_action(
    action: &ActionSpec,
    details: Option<&serde_json::Value>,
    target_crate_version: &str,
    target_dso_version: &str,
    target_k: u32,
    target_max_ir_ops: Option<u64>,
    opt_structural_hash_by_action_id: &BTreeMap<String, String>,
) -> Option<String> {
    let ActionSpec::IrFnToKBoolConeCorpus {
        ir_action_id,
        k,
        max_ir_ops,
        version,
        runtime,
        ..
    } = action
    else {
        return None;
    };
    if *k != target_k {
        return None;
    }
    if *max_ir_ops != target_max_ir_ops {
        return None;
    }
    if normalize_tag_version(version) != target_dso_version {
        return None;
    }
    if normalize_tag_version(&runtime.driver_version) != target_crate_version {
        return None;
    }
    if let Some(details) = details
        && let Some(hash) =
            details_input_ir_structural_hash(details).and_then(normalized_structural_hash)
    {
        return Some(hash);
    }
    opt_structural_hash_by_action_id.get(ir_action_id).cloned()
}

pub(crate) fn enqueue_structural_opt_ir_g8r_actions(
    store: &ArtifactStore,
    repo_root: &Path,
    requested_crate_version: &str,
    fraig: bool,
    recompute_missing_hashes: bool,
    dry_run: bool,
) -> Result<EnqueueStructuralOptIrG8rSummary> {
    let compat = load_version_compat_map(repo_root)?;
    let crate_version = normalize_tag_version(requested_crate_version).to_string();
    let entry = compat.get(&crate_version).ok_or_else(|| {
        anyhow!(
            "crate version `v{}` not found in {}",
            crate_version,
            VERSION_COMPAT_PATH
        )
    })?;
    let dso_version = if entry.xlsynth_release_version.starts_with('v') {
        entry.xlsynth_release_version.clone()
    } else {
        format!("v{}", entry.xlsynth_release_version)
    };
    let target_dso_version = normalize_tag_version(&dso_version).to_string();
    let target_runtime =
        explicit_driver_runtime_for_crate_version(repo_root, &crate_version, &dso_version)?;

    let provenances = load_all_provenances(store)?;
    let total_actions_scanned = provenances.len();
    let hash_hints = collect_ir_structural_hash_hints(&provenances);

    let mut representatives: BTreeMap<String, StructuralOptIrCandidate> = BTreeMap::new();
    let mut opt_structural_hash_by_action_id: BTreeMap<String, String> = BTreeMap::new();

    let mut total_driver_ir_to_opt_actions = 0_usize;
    let mut hash_from_dependency_hint_count = 0_usize;
    let mut hash_recomputed_count = 0_usize;
    let mut hash_hint_conflict_count = 0_usize;
    let mut skipped_missing_output_count = 0_usize;
    let mut skipped_missing_ir_top_count = 0_usize;
    let mut skipped_missing_hash_hint_count = 0_usize;
    let mut skipped_hash_error_count = 0_usize;

    for provenance in &provenances {
        let ActionSpec::DriverIrToOpt {
            top_fn_name,
            version,
            runtime,
            ..
        } = &provenance.action
        else {
            continue;
        };
        total_driver_ir_to_opt_actions += 1;

        let ir_top = provenance
            .details
            .get("ir_top")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| top_fn_name.clone());
        let Some(ir_top) = ir_top else {
            skipped_missing_ir_top_count += 1;
            continue;
        };

        let output_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
        if !output_path.exists() || !output_path.is_file() {
            skipped_missing_output_count += 1;
            continue;
        }

        let hint_key_exact = (provenance.action_id.clone(), Some(ir_top.clone()));
        let hint_key_fallback = (provenance.action_id.clone(), None);
        let mut hinted_hash = None;

        if let Some(hint) = hash_hints.get(&hint_key_exact) {
            if hint.hashes.len() == 1 {
                hinted_hash = hint.hashes.iter().next().cloned();
            } else if hint.hashes.len() > 1 {
                hash_hint_conflict_count += 1;
            }
        } else if let Some(hint) = hash_hints.get(&hint_key_fallback) {
            if hint.hashes.len() == 1 {
                hinted_hash = hint.hashes.iter().next().cloned();
            } else if hint.hashes.len() > 1 {
                hash_hint_conflict_count += 1;
            }
        }

        let structural_hash = if let Some(hash) = hinted_hash {
            hash_from_dependency_hint_count += 1;
            hash
        } else if !recompute_missing_hashes {
            skipped_missing_hash_hint_count += 1;
            continue;
        } else {
            match compute_ir_fn_structural_hash(
                store,
                repo_root,
                &output_path,
                Some(&ir_top),
                version,
                runtime,
            ) {
                Ok((hash, _trace)) => {
                    hash_recomputed_count += 1;
                    hash
                }
                Err(_) => {
                    skipped_hash_error_count += 1;
                    continue;
                }
            }
        };

        opt_structural_hash_by_action_id
            .insert(provenance.action_id.clone(), structural_hash.clone());

        let candidate = StructuralOptIrCandidate {
            structural_hash: structural_hash.clone(),
            opt_ir_action_id: provenance.action_id.clone(),
            ir_top: ir_top.clone(),
            crate_version: normalize_tag_version(&runtime.driver_version).to_string(),
            created_utc: provenance.created_utc,
        };
        match representatives.get(&structural_hash) {
            Some(current) => {
                if should_prefer_structural_opt_ir_candidate(&candidate, current) {
                    representatives.insert(structural_hash, candidate);
                }
            }
            None => {
                representatives.insert(structural_hash, candidate);
            }
        }
    }

    let mut existing_by_hash: BTreeMap<String, StructuralHashCoverageRecord> = BTreeMap::new();
    for provenance in &provenances {
        if let Some(structural_hash) = structural_hash_for_matching_target_g8r_action(
            &provenance.action,
            Some(&provenance.details),
            &crate_version,
            &target_dso_version,
            fraig,
            &opt_structural_hash_by_action_id,
        ) {
            record_structural_hash_coverage(
                &mut existing_by_hash,
                structural_hash,
                StructuralHashCoverageState::Done,
            );
        }
    }

    let mut pending_paths = list_queue_files(&store.queue_pending_dir())?;
    pending_paths.sort();
    for path in pending_paths {
        let text = fs::read_to_string(&path)
            .with_context(|| format!("reading pending queue record: {}", path.display()))?;
        let (_action_id, _, _, action) = parse_queue_work_item(&text, &path)?;
        if let Some(structural_hash) = structural_hash_for_matching_target_g8r_action(
            &action,
            None,
            &crate_version,
            &target_dso_version,
            fraig,
            &opt_structural_hash_by_action_id,
        ) {
            record_structural_hash_coverage(
                &mut existing_by_hash,
                structural_hash,
                StructuralHashCoverageState::Pending,
            );
        }
    }

    let mut running_paths = list_queue_files(&store.queue_running_dir())?;
    running_paths.sort();
    for path in running_paths {
        let text = fs::read_to_string(&path)
            .with_context(|| format!("reading running queue record: {}", path.display()))?;
        let (_action_id, _, _, action) = parse_queue_work_item(&text, &path)?;
        if let Some(structural_hash) = structural_hash_for_matching_target_g8r_action(
            &action,
            None,
            &crate_version,
            &target_dso_version,
            fraig,
            &opt_structural_hash_by_action_id,
        ) {
            record_structural_hash_coverage(
                &mut existing_by_hash,
                structural_hash,
                StructuralHashCoverageState::Running,
            );
        }
    }

    for failed in load_failed_queue_records(store)? {
        if let Some(structural_hash) = structural_hash_for_matching_target_g8r_action(
            &failed.action,
            None,
            &crate_version,
            &target_dso_version,
            fraig,
            &opt_structural_hash_by_action_id,
        ) {
            record_structural_hash_coverage(
                &mut existing_by_hash,
                structural_hash,
                StructuralHashCoverageState::Failed,
            );
        }
    }

    let mut canceled_paths = list_queue_files(&store.queue_canceled_dir())?;
    canceled_paths.sort();
    for path in canceled_paths {
        let text = fs::read_to_string(&path)
            .with_context(|| format!("reading canceled queue record: {}", path.display()))?;
        let canceled: QueueCanceled = serde_json::from_str(&text)
            .with_context(|| format!("parsing canceled queue record: {}", path.display()))?;
        if let Some(structural_hash) = structural_hash_for_matching_target_g8r_action(
            &canceled.action,
            None,
            &crate_version,
            &target_dso_version,
            fraig,
            &opt_structural_hash_by_action_id,
        ) {
            record_structural_hash_coverage(
                &mut existing_by_hash,
                structural_hash,
                StructuralHashCoverageState::Canceled,
            );
        }
    }

    let mut existing_done_count = 0_usize;
    let mut existing_pending_count = 0_usize;
    let mut existing_running_count = 0_usize;
    let mut existing_failed_count = 0_usize;
    let mut existing_canceled_count = 0_usize;
    for record in existing_by_hash.values() {
        match record.state {
            StructuralHashCoverageState::Done => existing_done_count += 1,
            StructuralHashCoverageState::Pending => existing_pending_count += 1,
            StructuralHashCoverageState::Running => existing_running_count += 1,
            StructuralHashCoverageState::Failed => existing_failed_count += 1,
            StructuralHashCoverageState::Canceled => existing_canceled_count += 1,
        }
    }

    let mut already_done_action_id_count = 0_usize;
    let mut already_pending_action_id_count = 0_usize;
    let mut already_running_action_id_count = 0_usize;
    let mut already_failed_action_id_count = 0_usize;
    let mut already_canceled_action_id_count = 0_usize;
    let mut enqueued_count = 0_usize;
    let mut enqueued_samples = Vec::new();
    let unique_structural_hashes = representatives.len();

    for (structural_hash, candidate) in representatives {
        if existing_by_hash.contains_key(&structural_hash) {
            continue;
        }
        let action = ActionSpec::DriverIrToG8rAig {
            ir_action_id: candidate.opt_ir_action_id.clone(),
            top_fn_name: None,
            fraig,
            lowering_mode: G8rLoweringMode::Default,
            version: dso_version.clone(),
            runtime: target_runtime.clone(),
        };
        let action_id = compute_action_id(&action)?;
        if store.action_exists(&action_id) {
            already_done_action_id_count += 1;
            continue;
        }
        match queue_state_for_action(store, &action_id) {
            QueueState::Pending => {
                already_pending_action_id_count += 1;
                continue;
            }
            QueueState::Running { .. } => {
                already_running_action_id_count += 1;
                continue;
            }
            QueueState::Done => {
                already_done_action_id_count += 1;
                continue;
            }
            QueueState::Failed => {
                already_failed_action_id_count += 1;
                continue;
            }
            QueueState::Canceled => {
                already_canceled_action_id_count += 1;
                continue;
            }
            QueueState::None => {}
        }

        if !dry_run {
            enqueue_action(store, action)?;
        }
        enqueued_count += 1;
        if enqueued_samples.len() < 32 {
            enqueued_samples.push(EnqueuedStructuralOptIrG8rSample {
                structural_hash: candidate.structural_hash,
                opt_ir_action_id: candidate.opt_ir_action_id,
                action_id,
            });
        }
    }

    Ok(EnqueueStructuralOptIrG8rSummary {
        crate_version: format!("v{}", crate_version),
        dso_version,
        fraig,
        dry_run,
        recompute_missing_hashes,
        total_actions_scanned,
        total_driver_ir_to_opt_actions,
        hashed_opt_ir_actions: opt_structural_hash_by_action_id.len(),
        unique_structural_hashes,
        hash_from_dependency_hint_count,
        hash_recomputed_count,
        hash_hint_conflict_count,
        skipped_missing_output_count,
        skipped_missing_ir_top_count,
        skipped_missing_hash_hint_count,
        skipped_hash_error_count,
        existing_done_count,
        existing_pending_count,
        existing_running_count,
        existing_failed_count,
        existing_canceled_count,
        already_done_action_id_count,
        already_pending_action_id_count,
        already_running_action_id_count,
        already_failed_action_id_count,
        already_canceled_action_id_count,
        enqueued_count,
        enqueued_samples,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_runtime() -> DriverRuntimeSpec {
        DriverRuntimeSpec {
            driver_version: "0.31.0".to_string(),
            release_platform: "ubuntu2004".to_string(),
            docker_image: "xlsynth-bvc-driver:0.31.0".to_string(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".to_string(),
        }
    }

    fn make_test_store(name: &str) -> (ArtifactStore, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-structural-index-{}-{}-{}",
            name,
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).expect("create temp root");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure layout");
        (store, root)
    }

    fn sample_provenance(
        action_id: &str,
        created_utc: DateTime<Utc>,
        action: ActionSpec,
    ) -> Provenance {
        Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.to_string(),
            created_utc,
            action,
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: action_id.to_string(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: "payload/result.ir".to_string(),
            },
            output_files: Vec::new(),
            commands: Vec::new(),
            details: serde_json::Value::Null,
            suggested_next_actions: Vec::new(),
        }
    }

    #[test]
    fn structural_index_source_action_set_hash_is_stable_and_filtered() {
        let created = Utc::now();
        let p1 = sample_provenance(
            "b".repeat(64).as_str(),
            created,
            ActionSpec::DriverIrToOpt {
                ir_action_id: "1".repeat(64),
                top_fn_name: Some("__foo".to_string()),
                version: "v0.35.0".to_string(),
                runtime: sample_runtime(),
            },
        );
        let p2 = sample_provenance(
            "a".repeat(64).as_str(),
            created,
            ActionSpec::IrFnToKBoolConeCorpus {
                ir_action_id: "2".repeat(64),
                top_fn_name: Some("__bar".to_string()),
                k: 3,
                max_ir_ops: Some(16),
                version: "v0.35.0".to_string(),
                runtime: sample_runtime(),
            },
        );
        let ignored = sample_provenance(
            "c".repeat(64).as_str(),
            created,
            ActionSpec::DriverIrToDelayInfo {
                ir_action_id: "3".repeat(64),
                top_fn_name: Some("__baz".to_string()),
                delay_model: "asap7".to_string(),
                output_format: crate::DELAY_INFO_OUTPUT_FORMAT_TEXTPROTO_V1.to_string(),
                version: "v0.35.0".to_string(),
                runtime: sample_runtime(),
            },
        );
        let h1 = structural_index_source_action_set_sha256_from_provenances(&[
            p1.clone(),
            p2.clone(),
            ignored.clone(),
        ]);
        let h2 = structural_index_source_action_set_sha256_from_provenances(&[ignored, p2, p1]);
        assert_eq!(h1, h2);
    }

    #[test]
    fn check_ir_fn_corpus_structural_freshness_detects_missing_manifest() {
        let (store, root) = make_test_store("missing-manifest");
        let summary =
            check_ir_fn_corpus_structural_freshness(&store).expect("freshness check runs");
        assert!(!summary.up_to_date);
        assert!(!summary.manifest_present);
        assert!(
            summary
                .stale_reasons
                .iter()
                .any(|reason| reason.contains("manifest missing")),
            "expected missing manifest reason, got: {:?}",
            summary.stale_reasons
        );
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn check_ir_fn_corpus_structural_freshness_accepts_matching_manifest() {
        let (store, root) = make_test_store("matching-manifest");
        let created = Utc::now();
        let action_id = "d".repeat(64);
        let provenance = sample_provenance(
            &action_id,
            created,
            ActionSpec::DriverIrToOpt {
                ir_action_id: "4".repeat(64),
                top_fn_name: Some("__foo".to_string()),
                version: "v0.35.0".to_string(),
                runtime: sample_runtime(),
            },
        );
        store
            .write_provenance(&provenance)
            .expect("write sample provenance");
        let provenances = load_all_provenances(&store).expect("load provenances");
        let source_hash = structural_index_source_action_set_sha256_from_provenances(&provenances);
        let manifest = IrFnCorpusStructuralManifest {
            schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
            generated_utc: created + chrono::Duration::seconds(1),
            store_root: store.root.display().to_string(),
            output_dir: ir_fn_corpus_structural_index_location(
                &ir_fn_corpus_structural_index_prefix(),
            ),
            recompute_missing_hashes: false,
            total_actions_scanned: provenances.len(),
            total_driver_ir_to_opt_actions: 1,
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
            source_action_set_sha256: Some(source_hash),
            groups: Vec::new(),
        };
        store
            .write_web_index_bytes(
                ir_fn_corpus_structural_manifest_index_key(),
                &serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
            )
            .expect("write manifest");
        let summary =
            check_ir_fn_corpus_structural_freshness(&store).expect("freshness check runs");
        assert!(
            summary.up_to_date,
            "unexpected stale reasons: {:?}",
            summary.stale_reasons
        );
        assert!(summary.stale_reasons.is_empty());
        fs::remove_dir_all(root).expect("cleanup");
    }
}

#[derive(Debug, Default)]
struct HistoricalKBoolLossSummary {
    scanned_aig_stat_diff_actions: usize,
    considered_k_bool_aig_stat_diffs: usize,
    loss_samples: usize,
}

fn parse_stats_metric(stats_json: &serde_json::Value, key: &str) -> Option<f64> {
    let value = stats_json.get(key)?;
    match value {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn load_stats_nodes_and_levels(
    store: &ArtifactStore,
    stats_provenance: &Provenance,
    cache: &mut BTreeMap<String, Option<(f64, f64)>>,
) -> Option<(f64, f64)> {
    if let Some(cached) = cache.get(&stats_provenance.action_id) {
        return *cached;
    }
    let stats_path = store.resolve_artifact_ref_path(&stats_provenance.output_artifact);
    let parsed = fs::read_to_string(&stats_path)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
        .and_then(|stats_json| {
            let nodes = parse_stats_metric(&stats_json, "and_nodes")
                .or_else(|| parse_stats_metric(&stats_json, "live_nodes"))?;
            let levels = parse_stats_metric(&stats_json, "depth")?;
            Some((nodes, levels))
        });
    cache.insert(stats_provenance.action_id.clone(), parsed);
    parsed
}

fn collect_previously_lossy_k_bool_source_structural_hashes(
    store: &ArtifactStore,
    provenances: &[Provenance],
    target_k: u32,
) -> (HashSet<String>, HistoricalKBoolLossSummary) {
    let mut by_action_id: BTreeMap<&str, &Provenance> = BTreeMap::new();
    for provenance in provenances {
        by_action_id.insert(provenance.action_id.as_str(), provenance);
    }

    let mut stats_cache: BTreeMap<String, Option<(f64, f64)>> = BTreeMap::new();
    let mut lossy_source_hashes = HashSet::new();
    let mut summary = HistoricalKBoolLossSummary::default();

    for provenance in provenances {
        let ActionSpec::AigStatDiff {
            opt_ir_action_id,
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
        } = &provenance.action
        else {
            continue;
        };
        summary.scanned_aig_stat_diff_actions += 1;

        let Some(k_bool_provenance) = by_action_id.get(opt_ir_action_id.as_str()).copied() else {
            continue;
        };
        let ActionSpec::IrFnToKBoolConeCorpus {
            ir_action_id, k, ..
        } = &k_bool_provenance.action
        else {
            continue;
        };
        if *k != target_k {
            continue;
        }
        summary.considered_k_bool_aig_stat_diffs += 1;

        let Some(g8r_stats_provenance) =
            by_action_id.get(g8r_aig_stats_action_id.as_str()).copied()
        else {
            continue;
        };
        let Some(yosys_stats_provenance) = by_action_id
            .get(yosys_abc_aig_stats_action_id.as_str())
            .copied()
        else {
            continue;
        };
        let Some((g8r_nodes, g8r_levels)) =
            load_stats_nodes_and_levels(store, g8r_stats_provenance, &mut stats_cache)
        else {
            continue;
        };
        let Some((yosys_nodes, yosys_levels)) =
            load_stats_nodes_and_levels(store, yosys_stats_provenance, &mut stats_cache)
        else {
            continue;
        };
        if !(g8r_nodes > yosys_nodes || g8r_levels > yosys_levels) {
            continue;
        }
        summary.loss_samples += 1;

        let Some(source_opt_provenance) = by_action_id.get(ir_action_id.as_str()).copied() else {
            continue;
        };
        let Some(structural_hash) = source_opt_provenance
            .details
            .get("output_ir_fn_structural_hash")
            .and_then(|v| v.as_str())
            .and_then(normalized_structural_hash)
        else {
            continue;
        };
        lossy_source_hashes.insert(structural_hash);
    }

    (lossy_source_hashes, summary)
}

pub(crate) fn load_previously_lossy_k_bool_source_structural_hashes(
    store: &ArtifactStore,
    target_k: u32,
) -> Result<HashSet<String>> {
    let provenances = load_all_provenances(store)?;
    Ok(collect_previously_lossy_k_bool_source_structural_hashes(store, &provenances, target_k).0)
}

pub(crate) fn lossy_k_bool_source_structural_hash_for_aig_stat_diff(
    store: &ArtifactStore,
    provenance: &Provenance,
) -> Option<(u32, String)> {
    let ActionSpec::AigStatDiff {
        opt_ir_action_id,
        g8r_aig_stats_action_id,
        yosys_abc_aig_stats_action_id,
    } = &provenance.action
    else {
        return None;
    };

    let k_bool_provenance = store.load_provenance(opt_ir_action_id).ok()?;
    let ActionSpec::IrFnToKBoolConeCorpus {
        ir_action_id, k, ..
    } = &k_bool_provenance.action
    else {
        return None;
    };

    let g8r_stats_provenance = store.load_provenance(g8r_aig_stats_action_id).ok()?;
    let yosys_stats_provenance = store.load_provenance(yosys_abc_aig_stats_action_id).ok()?;

    let mut stats_cache: BTreeMap<String, Option<(f64, f64)>> = BTreeMap::new();
    let (g8r_nodes, g8r_levels) =
        load_stats_nodes_and_levels(store, &g8r_stats_provenance, &mut stats_cache)?;
    let (yosys_nodes, yosys_levels) =
        load_stats_nodes_and_levels(store, &yosys_stats_provenance, &mut stats_cache)?;
    if !(g8r_nodes > yosys_nodes || g8r_levels > yosys_levels) {
        return None;
    }

    let source_opt_provenance = store.load_provenance(ir_action_id).ok()?;
    let structural_hash = source_opt_provenance
        .details
        .get("output_ir_fn_structural_hash")
        .and_then(|v| v.as_str())
        .and_then(normalized_structural_hash)?;
    Some((*k, structural_hash))
}

pub(crate) fn enqueue_structural_opt_ir_k_bool_cone_actions(
    store: &ArtifactStore,
    repo_root: &Path,
    requested_crate_version: &str,
    k: u32,
    only_previous_losses: bool,
    recompute_missing_hashes: bool,
    dry_run: bool,
) -> Result<EnqueueStructuralOptIrKBoolConeSummary> {
    if k == 0 {
        bail!("--k must be > 0");
    }
    let compat = load_version_compat_map(repo_root)?;
    let crate_version = normalize_tag_version(requested_crate_version).to_string();
    let entry = compat.get(&crate_version).ok_or_else(|| {
        anyhow!(
            "crate version `v{}` not found in {}",
            crate_version,
            VERSION_COMPAT_PATH
        )
    })?;
    let dso_version = if entry.xlsynth_release_version.starts_with('v') {
        entry.xlsynth_release_version.clone()
    } else {
        format!("v{}", entry.xlsynth_release_version)
    };
    let target_dso_version = normalize_tag_version(&dso_version).to_string();
    let target_max_ir_ops = crate::default_k_bool_cone_max_ir_ops_for_k(k);
    let target_runtime =
        explicit_driver_runtime_for_crate_version(repo_root, &crate_version, &dso_version)?;

    let provenances = load_all_provenances(store)?;
    let total_actions_scanned = provenances.len();
    let hash_hints = collect_ir_structural_hash_hints(&provenances);

    let mut representatives: BTreeMap<String, StructuralOptIrCandidate> = BTreeMap::new();
    let mut opt_structural_hash_by_action_id: BTreeMap<String, String> = BTreeMap::new();

    let mut total_driver_ir_to_opt_actions = 0_usize;
    let mut hash_from_dependency_hint_count = 0_usize;
    let mut hash_recomputed_count = 0_usize;
    let mut hash_hint_conflict_count = 0_usize;
    let mut skipped_missing_output_count = 0_usize;
    let mut skipped_missing_ir_top_count = 0_usize;
    let mut skipped_missing_hash_hint_count = 0_usize;
    let mut skipped_hash_error_count = 0_usize;

    for provenance in &provenances {
        let ActionSpec::DriverIrToOpt {
            top_fn_name,
            version,
            runtime,
            ..
        } = &provenance.action
        else {
            continue;
        };
        total_driver_ir_to_opt_actions += 1;

        let ir_top = provenance
            .details
            .get("ir_top")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| top_fn_name.clone());
        let Some(ir_top) = ir_top else {
            skipped_missing_ir_top_count += 1;
            continue;
        };

        let output_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
        if !output_path.exists() || !output_path.is_file() {
            skipped_missing_output_count += 1;
            continue;
        }

        let hint_key_exact = (provenance.action_id.clone(), Some(ir_top.clone()));
        let hint_key_fallback = (provenance.action_id.clone(), None);
        let mut hinted_hash = None;

        if let Some(hint) = hash_hints.get(&hint_key_exact) {
            if hint.hashes.len() == 1 {
                hinted_hash = hint.hashes.iter().next().cloned();
            } else if hint.hashes.len() > 1 {
                hash_hint_conflict_count += 1;
            }
        } else if let Some(hint) = hash_hints.get(&hint_key_fallback) {
            if hint.hashes.len() == 1 {
                hinted_hash = hint.hashes.iter().next().cloned();
            } else if hint.hashes.len() > 1 {
                hash_hint_conflict_count += 1;
            }
        }

        let structural_hash = if let Some(hash) = hinted_hash {
            hash_from_dependency_hint_count += 1;
            hash
        } else if !recompute_missing_hashes {
            skipped_missing_hash_hint_count += 1;
            continue;
        } else {
            match compute_ir_fn_structural_hash(
                store,
                repo_root,
                &output_path,
                Some(&ir_top),
                version,
                runtime,
            ) {
                Ok((hash, _trace)) => {
                    hash_recomputed_count += 1;
                    hash
                }
                Err(_) => {
                    skipped_hash_error_count += 1;
                    continue;
                }
            }
        };

        opt_structural_hash_by_action_id
            .insert(provenance.action_id.clone(), structural_hash.clone());

        let candidate = StructuralOptIrCandidate {
            structural_hash: structural_hash.clone(),
            opt_ir_action_id: provenance.action_id.clone(),
            ir_top: ir_top.clone(),
            crate_version: normalize_tag_version(&runtime.driver_version).to_string(),
            created_utc: provenance.created_utc,
        };
        match representatives.get(&structural_hash) {
            Some(current) => {
                if should_prefer_structural_opt_ir_candidate(&candidate, current) {
                    representatives.insert(structural_hash, candidate);
                }
            }
            None => {
                representatives.insert(structural_hash, candidate);
            }
        }
    }

    let mut historical_k_bool_aig_stat_diffs_scanned = 0_usize;
    let mut historical_k_bool_aig_stat_diffs_considered = 0_usize;
    let mut historical_k_bool_loss_samples = 0_usize;
    let mut historical_lossy_source_structural_hashes = 0_usize;
    let mut skipped_not_previously_lossy_structural_hashes = 0_usize;
    if only_previous_losses {
        let (lossy_hashes, summary) =
            collect_previously_lossy_k_bool_source_structural_hashes(store, &provenances, k);
        historical_k_bool_aig_stat_diffs_scanned = summary.scanned_aig_stat_diff_actions;
        historical_k_bool_aig_stat_diffs_considered = summary.considered_k_bool_aig_stat_diffs;
        historical_k_bool_loss_samples = summary.loss_samples;
        historical_lossy_source_structural_hashes = lossy_hashes.len();
        let before_filter = representatives.len();
        representatives.retain(|structural_hash, _| lossy_hashes.contains(structural_hash));
        skipped_not_previously_lossy_structural_hashes =
            before_filter.saturating_sub(representatives.len());
    }

    let mut existing_by_hash: BTreeMap<String, StructuralHashCoverageRecord> = BTreeMap::new();
    for provenance in &provenances {
        if let Some(structural_hash) = structural_hash_for_matching_target_k_bool_action(
            &provenance.action,
            Some(&provenance.details),
            &crate_version,
            &target_dso_version,
            k,
            target_max_ir_ops,
            &opt_structural_hash_by_action_id,
        ) {
            record_structural_hash_coverage(
                &mut existing_by_hash,
                structural_hash,
                StructuralHashCoverageState::Done,
            );
        }
    }

    let mut pending_paths = list_queue_files(&store.queue_pending_dir())?;
    pending_paths.sort();
    for path in pending_paths {
        let text = fs::read_to_string(&path)
            .with_context(|| format!("reading pending queue record: {}", path.display()))?;
        let (_action_id, _, _, action) = parse_queue_work_item(&text, &path)?;
        if let Some(structural_hash) = structural_hash_for_matching_target_k_bool_action(
            &action,
            None,
            &crate_version,
            &target_dso_version,
            k,
            target_max_ir_ops,
            &opt_structural_hash_by_action_id,
        ) {
            record_structural_hash_coverage(
                &mut existing_by_hash,
                structural_hash,
                StructuralHashCoverageState::Pending,
            );
        }
    }

    let mut running_paths = list_queue_files(&store.queue_running_dir())?;
    running_paths.sort();
    for path in running_paths {
        let text = fs::read_to_string(&path)
            .with_context(|| format!("reading running queue record: {}", path.display()))?;
        let (_action_id, _, _, action) = parse_queue_work_item(&text, &path)?;
        if let Some(structural_hash) = structural_hash_for_matching_target_k_bool_action(
            &action,
            None,
            &crate_version,
            &target_dso_version,
            k,
            target_max_ir_ops,
            &opt_structural_hash_by_action_id,
        ) {
            record_structural_hash_coverage(
                &mut existing_by_hash,
                structural_hash,
                StructuralHashCoverageState::Running,
            );
        }
    }

    for failed in load_failed_queue_records(store)? {
        if let Some(structural_hash) = structural_hash_for_matching_target_k_bool_action(
            &failed.action,
            None,
            &crate_version,
            &target_dso_version,
            k,
            target_max_ir_ops,
            &opt_structural_hash_by_action_id,
        ) {
            record_structural_hash_coverage(
                &mut existing_by_hash,
                structural_hash,
                StructuralHashCoverageState::Failed,
            );
        }
    }

    let mut canceled_paths = list_queue_files(&store.queue_canceled_dir())?;
    canceled_paths.sort();
    for path in canceled_paths {
        let text = fs::read_to_string(&path)
            .with_context(|| format!("reading canceled queue record: {}", path.display()))?;
        let canceled: QueueCanceled = serde_json::from_str(&text)
            .with_context(|| format!("parsing canceled queue record: {}", path.display()))?;
        if let Some(structural_hash) = structural_hash_for_matching_target_k_bool_action(
            &canceled.action,
            None,
            &crate_version,
            &target_dso_version,
            k,
            target_max_ir_ops,
            &opt_structural_hash_by_action_id,
        ) {
            record_structural_hash_coverage(
                &mut existing_by_hash,
                structural_hash,
                StructuralHashCoverageState::Canceled,
            );
        }
    }

    let mut existing_done_count = 0_usize;
    let mut existing_pending_count = 0_usize;
    let mut existing_running_count = 0_usize;
    let mut existing_failed_count = 0_usize;
    let mut existing_canceled_count = 0_usize;
    for record in existing_by_hash.values() {
        match record.state {
            StructuralHashCoverageState::Done => existing_done_count += 1,
            StructuralHashCoverageState::Pending => existing_pending_count += 1,
            StructuralHashCoverageState::Running => existing_running_count += 1,
            StructuralHashCoverageState::Failed => existing_failed_count += 1,
            StructuralHashCoverageState::Canceled => existing_canceled_count += 1,
        }
    }

    let mut already_done_action_id_count = 0_usize;
    let mut already_pending_action_id_count = 0_usize;
    let mut already_running_action_id_count = 0_usize;
    let mut already_failed_action_id_count = 0_usize;
    let mut already_canceled_action_id_count = 0_usize;
    let mut enqueued_count = 0_usize;
    let mut enqueued_samples = Vec::new();
    let unique_structural_hashes = representatives.len();

    for (structural_hash, candidate) in representatives {
        if existing_by_hash.contains_key(&structural_hash) {
            continue;
        }
        let action = ActionSpec::IrFnToKBoolConeCorpus {
            ir_action_id: candidate.opt_ir_action_id.clone(),
            top_fn_name: Some(candidate.ir_top.clone()),
            k,
            max_ir_ops: target_max_ir_ops,
            version: dso_version.clone(),
            runtime: target_runtime.clone(),
        };
        let action_id = compute_action_id(&action)?;
        if store.action_exists(&action_id) {
            already_done_action_id_count += 1;
            continue;
        }
        match queue_state_for_action(store, &action_id) {
            QueueState::Pending => {
                already_pending_action_id_count += 1;
                continue;
            }
            QueueState::Running { .. } => {
                already_running_action_id_count += 1;
                continue;
            }
            QueueState::Done => {
                already_done_action_id_count += 1;
                continue;
            }
            QueueState::Failed => {
                already_failed_action_id_count += 1;
                continue;
            }
            QueueState::Canceled => {
                already_canceled_action_id_count += 1;
                continue;
            }
            QueueState::None => {}
        }

        if !dry_run {
            enqueue_action(store, action)?;
        }
        enqueued_count += 1;
        if enqueued_samples.len() < 32 {
            enqueued_samples.push(EnqueuedStructuralOptIrKBoolConeSample {
                structural_hash: candidate.structural_hash,
                opt_ir_action_id: candidate.opt_ir_action_id,
                ir_top: candidate.ir_top,
                action_id,
            });
        }
    }

    Ok(EnqueueStructuralOptIrKBoolConeSummary {
        crate_version: format!("v{}", crate_version),
        dso_version,
        k,
        max_ir_ops: target_max_ir_ops,
        dry_run,
        only_previous_losses,
        recompute_missing_hashes,
        total_actions_scanned,
        total_driver_ir_to_opt_actions,
        hashed_opt_ir_actions: opt_structural_hash_by_action_id.len(),
        unique_structural_hashes,
        hash_from_dependency_hint_count,
        hash_recomputed_count,
        hash_hint_conflict_count,
        skipped_missing_output_count,
        skipped_missing_ir_top_count,
        skipped_missing_hash_hint_count,
        skipped_hash_error_count,
        historical_k_bool_aig_stat_diffs_scanned,
        historical_k_bool_aig_stat_diffs_considered,
        historical_k_bool_loss_samples,
        historical_lossy_source_structural_hashes,
        skipped_not_previously_lossy_structural_hashes,
        existing_done_count,
        existing_pending_count,
        existing_running_count,
        existing_failed_count,
        existing_canceled_count,
        already_done_action_id_count,
        already_pending_action_id_count,
        already_running_action_id_count,
        already_failed_action_id_count,
        already_canceled_action_id_count,
        enqueued_count,
        enqueued_samples,
    })
}
