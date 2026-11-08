// SPDX-License-Identifier: Apache-2.0

use super::*;

fn shard_generation<'a>(
    catalog: &'a BrowserCatalog,
    generation_id: &str,
) -> &'a BrowserProgressionGeneration {
    catalog
        .progression
        .cohorts
        .iter()
        .flat_map(|cohort| &cohort.generations)
        .find(|generation| generation.generation_id == generation_id)
        .expect("fixture generation")
}

fn rewrite_shard_and_publication_bindings(
    site_dir: &Path,
    generation_id: &str,
    shard_bytes: &[u8],
) {
    let catalog_path = site_dir.join("catalog.json");
    let mut catalog =
        decode_canonical_browser_catalog(&fs::read(&catalog_path).expect("read compact catalog"))
            .expect("decode compact catalog");
    let reference = catalog
        .progression
        .cohorts
        .iter_mut()
        .flat_map(|cohort| &mut cohort.generations)
        .find(|generation| generation.generation_id == generation_id)
        .expect("fixture generation")
        .run_samples_shard
        .as_mut()
        .expect("sample shard reference");
    reference.bytes = shard_bytes.len() as u64;
    reference.sha256 = sha256_hex(shard_bytes);
    let shard_url = reference.url.clone();
    fs::write(site_dir.join(&shard_url), shard_bytes).expect("rewrite sample shard");
    fs::write(
        catalog_path,
        serde_json::to_vec(&catalog).expect("encode canonical compact catalog"),
    )
    .expect("rewrite compact catalog");
    refresh_site_manifest_entry(site_dir, &shard_url);
    refresh_site_manifest_entry(site_dir, "catalog.json");
}

#[test]
fn progression_sample_shards_preserve_pb_derived_rows_and_canonical_catalog() {
    let root = temp_root();
    let (site_dir, generation_id) =
        build_historical_candidate_site_fixture_with_graph_le(&root, true);
    verify_static_site(&site_dir).expect("verify generated fixture");
    let catalog_bytes = fs::read(site_dir.join("catalog.json")).expect("read catalog");
    let metadata =
        decode_canonical_browser_catalog(&catalog_bytes).expect("decode compact catalog");
    let hydrated = read_browser_catalog(&site_dir).expect("load catalog and sample shards");
    assert_eq!(metadata.schema_version, 10);
    assert_eq!(catalog_bytes, serde_json::to_vec(&metadata).unwrap());
    assert!(catalog_bytes.len() < 25 * 1024 * 1024);
    assert!(
        decode_canonical_browser_catalog(&serde_json::to_vec_pretty(&metadata).unwrap()).is_err()
    );

    let metadata_json: serde_json::Value = serde_json::from_slice(&catalog_bytes).unwrap();
    let cohorts_json = metadata_json["progression"]["cohorts"].as_array().unwrap();
    let mut sample_shard_count = 0;
    for (cohort, cohort_json) in metadata.progression.cohorts.iter().zip(cohorts_json) {
        let generations_json = cohort_json["generations"].as_array().unwrap();
        assert_eq!(generations_json.len(), cohort.generations.len());
        for (generation, generation_json) in cohort.generations.iter().zip(generations_json) {
            assert!(generation.run_samples.is_empty());
            assert_eq!(
                generation_json.as_object().unwrap().get("run_samples"),
                None
            );
            let full_generation = shard_generation(&hydrated, &generation.generation_id);
            assert_eq!(full_generation.run_samples_shard, None);
            match &generation.run_samples_shard {
                Some(reference) => {
                    sample_shard_count += 1;
                    assert_eq!(
                        reference.url,
                        format!("data/progression-samples/{}.json", generation.generation_id)
                    );
                    let bytes = fs::read(site_dir.join(&reference.url)).unwrap();
                    assert_eq!(reference.bytes, bytes.len() as u64);
                    assert_eq!(reference.sha256, sha256_hex(&bytes));
                    let shard: BrowserProgressionSampleShard =
                        serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(bytes, serde_json::to_vec(&shard).unwrap());
                    assert_eq!(shard.schema_version, 1);
                    assert_eq!(shard.cohort_id, cohort.cohort_id);
                    assert_eq!(shard.generation_id, generation.generation_id);
                    assert_eq!(reference.sample_count, shard.samples.len() as u64);
                    assert_eq!(shard.samples, full_generation.run_samples);
                }
                None => assert!(full_generation.run_samples.is_empty()),
            }
        }
    }
    assert_eq!(sample_shard_count, 1);
    assert_eq!(hydrated.progression_evidence.len(), 1);
    for reference in &hydrated.progression_evidence {
        let evidence_bytes = fs::read(site_dir.join(&reference.url)).unwrap();
        let (cohort_id, expected_generation, _) =
            decode_progression_run_evidence(&evidence_bytes).expect("decode canonical evidence");
        assert_eq!(cohort_id, reference.cohort_id);
        assert_eq!(
            shard_generation(&hydrated, &reference.generation_id),
            &expected_generation
        );
    }
    let candidate = shard_generation(&hydrated, &generation_id);
    assert!(candidate.run_samples.iter().all(|sample| {
        sample.g8r_raw_stats.is_some()
            && sample.g8r_abc_graph_logical_effort.is_some()
            && sample.graph_le_estimator.is_some()
    }));
    for (relpath, expected_bytes) in browser_catalog_files(&hydrated).unwrap() {
        assert_eq!(fs::read(site_dir.join(relpath)).unwrap(), expected_bytes);
    }
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn progression_sample_shards_bound_twenty_six_full_release_histories() {
    let root = temp_root();
    let (site_dir, generation_id) =
        build_historical_candidate_site_fixture_with_graph_le(&root, true);
    let mut catalog = read_browser_catalog(&site_dir).expect("hydrate fixture catalog");
    let template = shard_generation(&catalog, &generation_id).clone();
    let sample_template = template.run_samples[0].clone();
    catalog.progression = empty_browser_progression_index().unwrap();
    catalog.progression_evidence.clear();
    let mut expected_sample_count = 0;
    for cohort in &mut catalog.progression.cohorts {
        let descriptor = progression_cohort(&cohort.cohort_id).unwrap();
        let artifacts = progression_ir_artifacts(descriptor).unwrap();
        let samples = artifacts
            .into_iter()
            .enumerate()
            .map(|(index, (structural_hash, source_sha256))| {
                let mut sample = sample_template.clone();
                sample.fn_key = format!("synthetic/{}/{index}", cohort.cohort_id);
                sample.structural_hash = structural_hash;
                sample.source_sha256 = source_sha256;
                sample
            })
            .collect::<Vec<_>>();
        assert_eq!(samples.len() as u64, cohort.cohort_ir_count);
        cohort.generations = (55..=80)
            .map(|release| {
                let version = format!("0.{release}.0");
                let mut generation = template.clone();
                generation.generation_id =
                    progression_generation_id(&cohort.cohort_id, &version, &template.dso_version);
                generation.origin = BrowserProgressionOrigin::CrateRelease;
                generation.display_label = format!("v{version}");
                generation.crate_version = Some(version);
                generation.baseline_generation_id = None;
                generation.observed_ir_count = cohort.cohort_ir_count;
                generation.cohort_ir_count = cohort.cohort_ir_count;
                generation.missing_cohort_ir_count = 0;
                generation.extra_ir_count = 0;
                generation.coverage = BrowserProgressionCoverage::CohortComplete;
                generation.run_samples = samples.clone();
                generation.run_samples_shard = None;
                generation
            })
            .collect();
        cohort.cohort_complete_generation_count = 26;
        expected_sample_count += 26 * samples.len();
    }
    assert_eq!(
        expected_sample_count,
        26 * (RELEASE_PROGRESSION_IR_COUNT + MFFC_PROGRESSION_IR_COUNT)
    );
    let files = browser_catalog_files(&catalog).expect("project full release history");
    assert_eq!(files.len(), 53);
    let metadata_bytes = files.get("catalog.json").expect("compact catalog");
    assert!(metadata_bytes.len() < 512 * 1024);
    assert!(metadata_bytes.len() < 25 * 1024 * 1024);
    let metadata = decode_canonical_browser_catalog(metadata_bytes).unwrap();
    assert_eq!(metadata.progression.cohorts.len(), 2);
    let mut actual_sample_count = 0;
    for cohort in &metadata.progression.cohorts {
        assert_eq!(cohort.generations.len(), 26);
        for generation in &cohort.generations {
            assert!(generation.run_samples.is_empty());
            let reference = generation.run_samples_shard.as_ref().unwrap();
            let bytes = files.get(&reference.url).expect("generation sample shard");
            assert!(bytes.len() < 2 * 1024 * 1024);
            assert_eq!(reference.bytes, bytes.len() as u64);
            assert_eq!(reference.sha256, sha256_hex(bytes));
            let shard: BrowserProgressionSampleShard = serde_json::from_slice(bytes).unwrap();
            assert_eq!(shard.cohort_id, cohort.cohort_id);
            assert_eq!(shard.generation_id, generation.generation_id);
            assert_eq!(shard.samples.len() as u64, cohort.cohort_ir_count);
            assert_eq!(reference.sample_count, cohort.cohort_ir_count);
            assert_eq!(
                shard.samples,
                shard_generation(&catalog, &generation.generation_id).run_samples
            );
            actual_sample_count += shard.samples.len();
        }
    }
    assert_eq!(actual_sample_count, expected_sample_count);
    let projection_dir = root.join("synthetic-projection");
    for (relpath, bytes) in &files {
        let path = projection_dir.join(relpath);
        fs::create_dir_all(path.parent().unwrap()).expect("create projection directory");
        fs::write(path, bytes).expect("write projected catalog or sample shard");
    }
    let reassembled = read_browser_catalog(&projection_dir).expect("hydrate both fixed cohorts");
    assert_eq!(reassembled.progression, catalog.progression);
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_rejects_self_consistent_progression_sample_shard_tamper() {
    let root = temp_root();
    let (site_dir, generation_id) =
        build_historical_candidate_site_fixture_with_graph_le(&root, true);
    verify_static_site(&site_dir).expect("verify original fixture");
    let metadata =
        decode_canonical_browser_catalog(&fs::read(site_dir.join("catalog.json")).unwrap())
            .unwrap();
    let reference = shard_generation(&metadata, &generation_id)
        .run_samples_shard
        .as_ref()
        .unwrap();
    let evidence_reference = &metadata.progression_evidence[0];
    let evidence_bytes = fs::read(site_dir.join(&evidence_reference.url)).unwrap();
    let original_shard = fs::read(site_dir.join(&reference.url)).unwrap();
    let mut shard: BrowserProgressionSampleShard = serde_json::from_slice(&original_shard).unwrap();
    let sample = &mut shard.samples[0];
    sample.g8r_nodes += 1.0;
    sample.g8r_product = sample.g8r_nodes * sample.g8r_levels;
    sample.g8r_product_loss = sample.g8r_product - sample.yosys_abc_product;
    let changed_sample = sample.clone();
    let tampered_bytes = serde_json::to_vec(&shard).unwrap();
    rewrite_shard_and_publication_bindings(&site_dir, &generation_id, &tampered_bytes);

    let hydrated = read_browser_catalog(&site_dir)
        .expect("byte counts, hashes, identities and row arithmetic are self-consistent");
    assert_eq!(
        shard_generation(&hydrated, &generation_id).run_samples[0],
        changed_sample
    );
    assert!(verify_static_site(&site_dir).is_err());
    assert_eq!(
        fs::read(site_dir.join(&evidence_reference.url)).unwrap(),
        evidence_bytes
    );
    rewrite_shard_and_publication_bindings(&site_dir, &generation_id, &original_shard);
    verify_static_site(&site_dir).expect("restored protobuf-derived projection verifies");
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn progression_sample_shard_reader_rejects_missing_mismatched_and_unknown_data() {
    let root = temp_root();
    let (site_dir, generation_id) =
        build_historical_candidate_site_fixture_with_graph_le(&root, true);
    let original_catalog = fs::read(site_dir.join("catalog.json")).unwrap();
    let metadata = decode_canonical_browser_catalog(&original_catalog).unwrap();
    let reference = shard_generation(&metadata, &generation_id)
        .run_samples_shard
        .as_ref()
        .unwrap();
    let shard_path = site_dir.join(&reference.url);
    let original_bytes = fs::read(&shard_path).unwrap();
    let original: BrowserProgressionSampleShard = serde_json::from_slice(&original_bytes).unwrap();
    assert_eq!(original_bytes, serde_json::to_vec(&original).unwrap());

    fs::remove_file(&shard_path).unwrap();
    assert!(read_browser_catalog(&site_dir).is_err());
    assert!(verify_static_site(&site_dir).is_err());
    fs::write(&shard_path, &original_bytes).unwrap();

    let mut wrong_size = original_bytes.clone();
    wrong_size.push(b' ');
    fs::write(&shard_path, wrong_size).unwrap();
    assert!(read_browser_catalog(&site_dir).is_err());
    fs::write(&shard_path, &original_bytes).unwrap();

    let mut wrong_count_metadata = metadata.clone();
    let generation = wrong_count_metadata
        .progression
        .cohorts
        .iter_mut()
        .flat_map(|cohort| &mut cohort.generations)
        .find(|generation| generation.generation_id == generation_id)
        .unwrap();
    generation.run_samples_shard.as_mut().unwrap().sample_count += 1;
    fs::write(
        site_dir.join("catalog.json"),
        serde_json::to_vec(&wrong_count_metadata).unwrap(),
    )
    .unwrap();
    refresh_site_manifest_entry(&site_dir, "catalog.json");
    assert!(read_browser_catalog(&site_dir).is_err());
    assert!(verify_static_site(&site_dir).is_err());
    fs::write(site_dir.join("catalog.json"), &original_catalog).unwrap();
    refresh_site_manifest_entry(&site_dir, "catalog.json");

    for mutation in 0..5 {
        let mut shard = original.clone();
        match mutation {
            0 => shard.cohort_id = MFFC_PROGRESSION_COHORT_ID.to_string(),
            1 => shard.generation_id = "f".repeat(64),
            2 => shard.schema_version += 1,
            3 => shard.samples.reverse(),
            _ => shard.samples[0].structural_hash = shard.samples[1].structural_hash.clone(),
        }
        let bytes = serde_json::to_vec(&shard).unwrap();
        rewrite_shard_and_publication_bindings(&site_dir, &generation_id, &bytes);
        if mutation < 3 {
            assert!(read_browser_catalog(&site_dir).is_err());
        }
        assert!(verify_static_site(&site_dir).is_err());
        rewrite_shard_and_publication_bindings(&site_dir, &generation_id, &original_bytes);
        verify_static_site(&site_dir).expect("restored valid sample shard");
    }
    for nested in [false, true] {
        let mut unknown: serde_json::Value = serde_json::from_slice(&original_bytes).unwrap();
        let object = if nested {
            unknown["samples"][0].as_object_mut().unwrap()
        } else {
            unknown.as_object_mut().unwrap()
        };
        object.insert("unrecognized_field".to_string(), json!(true));
        rewrite_shard_and_publication_bindings(
            &site_dir,
            &generation_id,
            &serde_json::to_vec(&unknown).unwrap(),
        );
        assert!(read_browser_catalog(&site_dir).is_err());
        assert!(verify_static_site(&site_dir).is_err());
        rewrite_shard_and_publication_bindings(&site_dir, &generation_id, &original_bytes);
    }
    verify_static_site(&site_dir).expect("all original shard bytes restored");
    fs::remove_dir_all(root).expect("cleanup");
}
