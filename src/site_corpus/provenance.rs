// SPDX-License-Identifier: Apache-2.0

//! Join original function aliases by exact function bytes, then structural
//! identity. Never infer structural identity from a replay directory name.
use super::*;
use wire::InputKind;

#[derive(Default)]
pub(super) struct Index(BTreeMap<Vec<u8>, wire::InputProvenance>);

fn name_kind(name: &str) -> InputKind {
    if name.starts_with("__mffc_") {
        InputKind::Mffc
    } else if name.starts_with("__k3_cone_") {
        InputKind::K3
    } else {
        InputKind::Other
    }
}

fn canonicalize(origins: &mut Vec<wire::FunctionOrigin>) {
    origins.sort_by_cached_key(Message::encode_to_vec);
    origins.dedup();
}

fn function_digest(function: &str) -> Option<Vec<u8>> {
    // An exact body match is sufficient only for a self-contained function:
    // a called function could have different definitions in two packages.
    static CALL: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let call = CALL.get_or_init(|| {
        Regex::new(r"\b(?:invoke|map|counted_for|dynamic_counted_for)\s*\(")
            .expect("valid call pattern")
    });
    (!call.is_match(function)).then(|| Sha256::digest(function.as_bytes()).to_vec())
}

fn snapshot_bytes(
    snapshot: &Path,
    file: &crate::snapshot::StaticSnapshotDatasetFile,
) -> Result<Vec<u8>> {
    let bytes = fs::read(snapshot.join(&file.relpath))?;
    if bytes.len() as u64 != file.bytes || sha256_hex(&bytes) != file.sha256 {
        bail!("snapshot provenance input changed after verification");
    }
    Ok(bytes)
}

impl Index {
    pub(super) fn from_snapshot(snapshot: &Path) -> Result<Self> {
        if snapshot.as_os_str().is_empty() {
            return Ok(Self::default());
        }
        crate::snapshot::verify_static_snapshot(snapshot)?;
        let manifest = crate::snapshot::load_static_snapshot_manifest(snapshot)?;
        let snapshot_id = hex::decode(&manifest.snapshot_id)?;
        let mut groups = BTreeMap::new();
        let group_prefix = crate::service::ir_fn_corpus_structural_index_prefix();
        for file in &manifest.dataset_files {
            if !file.index_key.starts_with(&group_prefix)
                || file.index_key == crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY
            {
                continue;
            }
            let group: crate::model::IrFnCorpusStructuralGroupFile =
                serde_json::from_slice(&snapshot_bytes(snapshot, file)?)?;
            let mut origins = Vec::new();
            for member in group.members {
                let kind = match member.producer_action_kind.as_deref() {
                    Some("driver_ir_to_opt") if name_kind(&member.ir_top) == InputKind::Other => {
                        InputKind::Whole
                    }
                    Some("ir_fn_to_k_bool_cone_corpus")
                        if name_kind(&member.ir_top) == InputKind::K3 =>
                    {
                        InputKind::K3
                    }
                    _ => continue,
                };
                origins.push(wire::FunctionOrigin {
                    ir_top: member.ir_top,
                    kind: kind as i32,
                    ir_action_id: hex::decode(&member.opt_ir_action_id)?,
                    dslx_file: member.dslx_origin.as_ref().map(|o| o.dslx_file.clone()),
                    dslx_fn_name: member.dslx_origin.map(|o| o.dslx_fn_name),
                });
            }
            canonicalize(&mut origins);
            groups.insert(group.structural_hash, origins);
        }
        let mut index = Self::default();
        for file in &manifest.dataset_files {
            if !file
                .index_key
                .starts_with(&format!("{}/", crate::WEB_IR_FN_CORPUS_IR_SHARD_NAMESPACE))
            {
                continue;
            }
            let shard: crate::query::IrFnCorpusIrShardFile =
                serde_json::from_slice(&snapshot_bytes(snapshot, file)?)?;
            for entry in shard.entries {
                for side in [entry.g8r, entry.yosys_abc] {
                    let Some(function_sha256) = function_digest(&side.ir_text) else {
                        continue;
                    };
                    let mut origins = groups
                        .get(&entry.structural_hash)
                        .cloned()
                        .unwrap_or_default();
                    let kind = if side.mffc_structure.is_some() {
                        InputKind::Mffc
                    } else {
                        origins
                            .iter()
                            .find(|o| o.ir_top == side.ir_top && o.kind() != InputKind::Other)
                            .map(|o| o.kind())
                            .unwrap_or(InputKind::Other)
                    };
                    origins.push(wire::FunctionOrigin {
                        ir_top: side.ir_top,
                        kind: kind as i32,
                        ir_action_id: hex::decode(side.ir_action_id)?,
                        dslx_file: None,
                        dslx_fn_name: None,
                    });
                    index.insert(wire::InputProvenance {
                        source_snapshot_sha256: snapshot_id.clone(),
                        structural_hashes: vec![hex::decode(&entry.structural_hash)?],
                        function_sha256,
                        origins,
                    })?;
                }
            }
        }
        // Gather every known kind for each structural identity;
        // the whole-function representative must retain MFFC membership too.
        let mut aliases = BTreeMap::<Vec<u8>, Vec<wire::FunctionOrigin>>::new();
        for p in index.0.values() {
            for hash in &p.structural_hashes {
                aliases
                    .entry(hash.clone())
                    .or_default()
                    .extend(p.origins.clone());
            }
        }
        for origins in aliases.values_mut() {
            canonicalize(origins);
        }
        for p in index.0.values_mut() {
            for hash in &p.structural_hashes {
                p.origins.extend(aliases[hash].clone());
            }
            canonicalize(&mut p.origins);
        }
        Ok(index)
    }

    fn insert(&mut self, mut provenance: wire::InputProvenance) -> Result<()> {
        if let Some(previous) = self.0.get(&provenance.function_sha256) {
            if previous.source_snapshot_sha256 != provenance.source_snapshot_sha256 {
                bail!("conflicting snapshots in corpus provenance");
            }
            provenance
                .structural_hashes
                .extend(previous.structural_hashes.clone());
            provenance.origins.extend(previous.origins.clone());
        }
        provenance.structural_hashes.sort();
        provenance.structural_hashes.dedup();
        canonicalize(&mut provenance.origins);
        self.0
            .insert(provenance.function_sha256.clone(), provenance);
        Ok(())
    }

    pub(super) fn enrich(&self, sample: &mut wire::Sample) -> Result<()> {
        let input = required(&sample.input, "input")?;
        let function =
            crate::executor::extract_ir_fn_block_by_name(&sample.input_ir, &input.top_fn_name)?;
        sample.provenance =
            function_digest(&function).and_then(|digest| self.0.get(&digest).cloned());
        validate(sample)
    }
}

pub(super) fn validate(sample: &wire::Sample) -> Result<()> {
    let Some(p) = &sample.provenance else {
        return Ok(());
    };
    let input = required(&sample.input, "input")?;
    let function =
        crate::executor::extract_ir_fn_block_by_name(&sample.input_ir, &input.top_fn_name)?;
    if p.source_snapshot_sha256.len() != 32
        || p.structural_hashes.is_empty()
        || p.structural_hashes.iter().any(|h| h.len() != 32)
        || p.structural_hashes.windows(2).any(|w| w[0] >= w[1])
        || function_digest(&function).as_ref() != Some(&p.function_sha256)
        || p.origins.is_empty()
        || !p.origins.iter().any(|o| o.ir_top == input.top_fn_name)
        || p.origins
            .windows(2)
            .any(|w| w[0].encode_to_vec() >= w[1].encode_to_vec())
    {
        bail!("invalid corpus function provenance binding");
    }
    let identifier = |name: &str| {
        !name.is_empty()
            && name.len() <= 1024
            && name
                .bytes()
                .enumerate()
                .all(|(i, b)| b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit()))
    };
    for origin in &p.origins {
        if origin.ir_action_id.len() != 32
            || !identifier(&origin.ir_top)
            || InputKind::try_from(origin.kind).is_err()
            || origin.kind() == InputKind::Unspecified
            || origin.dslx_file.is_some() != origin.dslx_fn_name.is_some()
        {
            bail!("invalid corpus function origin");
        }
        if let Some(path) = &origin.dslx_file {
            normalized_relpath(path)?;
        }
        if origin.dslx_fn_name.as_ref().is_some_and(|n| !identifier(n)) {
            bail!("invalid corpus DSLX function name");
        }
    }
    Ok(())
}

pub(super) fn kinds(sample: &wire::Sample) -> Vec<String> {
    let values = match &sample.provenance {
        Some(p) => p.origins.iter().map(|o| o.kind()).collect::<Vec<_>>(),
        None => vec![name_kind(
            &sample.input.as_ref().expect("validated input").top_fn_name,
        )],
    };
    let known = values.iter().any(|k| *k != InputKind::Other);
    values
        .into_iter()
        .filter(|k| !known || *k != InputKind::Other)
        .map(|k| {
            match k {
                InputKind::Whole => "whole",
                InputKind::Mffc => "mffc",
                InputKind::K3 => "k3",
                _ => "other",
            }
            .to_string()
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn names(sample: &wire::Sample) -> Vec<String> {
    sample
        .provenance
        .iter()
        .flat_map(|p| &p.origins)
        .filter(|o| o.kind() == InputKind::Whole)
        .map(|o| o.ir_top.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // A small synthetic body exercises identity binding without making the
    // unit test depend on an external evaluator or a large arithmetic fixture.
    const TOP: &str = "__mffc_345679b6c3fb1f66";
    const ADDER_HASH: &str = "c6025eb9b691cf529ecf126e24136a4d6951dd10f6cabc66d636f3224de35f0b";

    fn sample() -> wire::Sample {
        let ir = format!(
            "package replay_input\n\nfn {TOP}(x: bits[1] id=1) -> bits[1] {{\n ret not.2: bits[1] = not(x, id=2)\n}}\n"
        );
        wire::Sample {
            input: Some(wire::InputIdentity {
                source_relpath: format!("{ADDER_HASH}/{TOP}.ir"),
                source_sha256: Sha256::digest(ir.as_bytes()).to_vec(),
                top_fn_name: TOP.into(),
            }),
            sample_id: "adder".into(),
            input_ir: ir,
            g8r_abc: Some(wire::Stats::default()),
            yosys_abc: Some(wire::Stats::default()),
            g8r_raw: Some(wire::Stats::default()),
            ..Default::default()
        }
    }

    fn evidence(sample: &wire::Sample) -> wire::InputProvenance {
        wire::InputProvenance {
            source_snapshot_sha256: vec![1; 32],
            structural_hashes: vec![hex::decode(ADDER_HASH).unwrap()],
            function_sha256: Sha256::digest(
                crate::executor::extract_ir_fn_block_by_name(&sample.input_ir, TOP)
                    .unwrap()
                    .as_bytes(),
            )
            .to_vec(),
            origins: vec![
                wire::FunctionOrigin {
                    ir_top: TOP.into(),
                    kind: InputKind::Mffc as i32,
                    ir_action_id: vec![2; 32],
                    ..Default::default()
                },
                wire::FunctionOrigin {
                    ir_top: "__float32__add".into(),
                    kind: InputKind::Whole as i32,
                    ir_action_id: vec![3; 32],
                    dslx_file: Some("xls/dslx/stdlib/float32.x".into()),
                    dslx_fn_name: Some("add".into()),
                },
            ],
        }
    }

    #[test]
    fn whole_alias_survives_extracted_representative_without_changing_identity() {
        let mut sample = sample();
        let original_input = sample.input.clone();
        let original_ir = sample.input_ir.clone();
        let mut index = Index::default();
        let p = evidence(&sample);
        index.insert(p.clone()).unwrap();
        index.insert(p).unwrap(); // Repeated releases do not duplicate aliases.
        index.enrich(&mut sample).unwrap();
        let encoded = sample.encode_to_vec();
        let decoded = wire::Sample::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, sample);
        validate(&decoded).unwrap();
        assert_eq!(sample.input, original_input);
        assert_eq!(sample.input_ir, original_ir);
        assert_eq!(sample.provenance.as_ref().unwrap().origins.len(), 2);
        let (metrics, ir) = project(&[sample.clone()]).unwrap();
        let rows: Vec<Metric> = serde_json::from_slice(&metrics).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].input_kinds, ["mffc", "whole"]);
        assert_eq!(rows[0].function_names, ["__float32__add"]);
        assert_eq!(rows[0].top_fn_name, TOP);
        assert_eq!(rows[0].provenance_source, "snapshot");
        assert_eq!(
            serde_json::from_slice::<BTreeMap<String, String>>(&ir).unwrap(),
            BTreeMap::from([("adder".into(), original_ir)])
        );
        let mut changed = sample.clone();
        changed.input_ir = changed.input_ir.replace("not(x,", "identity(x,");
        assert!(validate(&changed).is_err());
        index.enrich(&mut changed).unwrap();
        assert_eq!(changed.provenance, None); // Same hash-looking directory is not evidence.
        assert_eq!(kinds(&changed), ["mffc"]);
        assert_eq!(names(&changed), Vec::<String>::new());
    }

    #[test]
    fn unknown_names_are_not_whole_function_evidence() {
        let mut sample = sample();
        sample.input.as_mut().unwrap().top_fn_name = "ordinary_name".into();
        assert_eq!(kinds(&sample), ["other"]);
        sample.input.as_mut().unwrap().top_fn_name = "__k3_cone_abc".into();
        assert_eq!(kinds(&sample), ["k3"]);
    }

    #[test]
    fn called_function_context_is_not_proven_by_body_bytes() {
        for operation in ["invoke", "map", "counted_for", "dynamic_counted_for"] {
            let mut sample = sample();
            sample.input_ir = sample
                .input_ir
                .replace("not(x,", &format!("{operation}(x, to_apply=external,"));
            let mut index = Index::default();
            index.insert(evidence(&sample)).unwrap();
            index.enrich(&mut sample).unwrap();
            assert_eq!(sample.provenance, None);
        }
    }

    #[test]
    fn byte_identical_functions_retain_all_historical_structural_identities() {
        let p = evidence(&sample());
        let mut conflict = p.clone();
        conflict.structural_hashes[0][0] ^= 1;
        for candidates in [
            [p.clone(), conflict.clone(), p.clone()],
            [conflict, p.clone(), p.clone()],
        ] {
            let mut values = Index::default();
            for candidate in candidates {
                values.insert(candidate).unwrap();
            }
            let merged = &values.0[&p.function_sha256];
            let mut expected = vec![
                p.structural_hashes[0].clone(),
                p.structural_hashes[0].clone(),
            ];
            expected[1][0] ^= 1;
            expected.sort();
            assert_eq!(merged.structural_hashes, expected);
            assert_eq!(merged.origins.len(), 2);
        }
    }

    #[test]
    fn rejects_conflicting_bindings_and_malformed_provenance() {
        let mut sample = sample();
        let mut p = evidence(&sample);
        let mut index = Index::default();
        index.insert(p.clone()).unwrap();
        p.source_snapshot_sha256[0] ^= 1;
        assert!(index.insert(p).is_err());
        index.enrich(&mut sample).unwrap();
        for mutation in 0..7 {
            let mut changed = sample.clone();
            let p = changed.provenance.as_mut().unwrap();
            match mutation {
                0 => p.source_snapshot_sha256.clear(),
                1 => p.structural_hashes.clear(),
                2 => p.origins[0].kind = 99,
                3 => p.origins[0].ir_action_id.clear(),
                4 => p.origins.push(p.origins[0].clone()),
                5 => {
                    p.origins[0].dslx_file = Some("../private.x".into());
                    p.origins[0].dslx_fn_name = Some("f".into());
                }
                _ => p.origins.clear(),
            }
            assert!(validate(&changed).is_err(), "mutation {mutation}");
        }
    }

    #[test]
    #[ignore = "requires BVC_PROVENANCE_SNAPSHOT and BVC_PROVENANCE_SITE with frozen v0.74.0 evidence"]
    fn real_f32_adder_and_full_population() {
        let snapshot = PathBuf::from(std::env::var_os("BVC_PROVENANCE_SNAPSHOT").unwrap());
        let site_dir = PathBuf::from(std::env::var_os("BVC_PROVENANCE_SITE").unwrap());
        let index = Index::from_snapshot(&snapshot).unwrap();
        let site = wire::CorpusSite::decode(fs::read(site_dir.join(MANIFEST)).unwrap().as_slice())
            .unwrap();
        let generation = site
            .generations
            .iter()
            .find(|g| g.crate_version == "0.74.0")
            .unwrap();
        let mut counts = BTreeMap::<String, usize>::new();
        let mut total = 0;
        let mut adder = None;
        for shard in &generation.shards {
            let evidence = wire::EvidenceShard::decode(
                fs::read(site_dir.join(path(&shard.evidence).unwrap()))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            for mut sample in evidence.samples {
                let original = sample.input.clone();
                index.enrich(&mut sample).unwrap();
                assert_eq!(sample.input, original);
                total += 1;
                for kind in kinds(&sample) {
                    *counts.entry(kind).or_default() += 1;
                }
                if names(&sample).iter().any(|n| n == "__float32__add") {
                    assert_eq!(sample.input.as_ref().unwrap().top_fn_name, TOP);
                    assert_eq!(kinds(&sample), ["mffc", "whole"]);
                    assert_eq!(
                        &sample.provenance.as_ref().unwrap().structural_hashes,
                        &vec![hex::decode(ADDER_HASH).unwrap()]
                    );
                    let (metrics, _) = project(&[sample]).unwrap();
                    let rows: Vec<Metric> = serde_json::from_slice(&metrics).unwrap();
                    assert_eq!(rows[0].ir_node_count, 296);
                    assert!(adder.replace(rows).is_none());
                }
            }
        }
        assert_eq!(total, generation.sample_count);
        assert!(adder.is_some());
        eprintln!("Real corpus provenance: {total} inputs; {counts:?}; f32 adder verified");
    }
}
