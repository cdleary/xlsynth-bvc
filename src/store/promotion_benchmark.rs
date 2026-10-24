// SPDX-License-Identifier: Apache-2.0

// Included in store::tests so the same fixture can benchmark either implementation.
#[test]
#[ignore = "manual durable promotion benchmark; creates a disposable artifact store"]
fn benchmark_sled_promote_and_consume() {
    let count = std::env::var("BVC_PROMOTION_BENCH_ACTIONS")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(1024);
    let threads = std::env::var("BVC_PROMOTION_BENCH_THREADS")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(16);
    assert!(count > 0 && threads > 0);
    let root = make_test_root("bvc-promotion-benchmark");
    let store = Arc::new(ArtifactStore::new_with_sled(
        root.clone(),
        root.join("store.sled"),
    ));
    store.ensure_layout().unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap();
    let started = std::time::Instant::now();
    pool.install(|| {
        use rayon::prelude::*;
        (0..count).into_par_iter().for_each(|index| {
            let payload: Vec<u8> = (0..16_384)
                .map(|i| ((i * 31 + index) % 251) as u8)
                .collect();
            let provenance = make_test_provenance(
                &format!("{index:064x}"),
                "payload/result.txt",
                payload.len() as u64,
            );
            let staging = stage_test_sled_action(&store, &provenance, "benchmark", &payload);
            fs::write(staging.join("payload/command.log"), b"successful command\n").unwrap();
            store
                .promote_staging_action_dir(&provenance.action_id, &staging)
                .unwrap();
            let output = store.materialize_action_dir(&provenance.action_id).unwrap();
            assert_eq!(
                fs::read(output.join("payload/result.txt")).unwrap(),
                payload
            );
            let cached =
                crate::proto::decode_provenance(&fs::read(output.join("provenance.pb")).unwrap())
                    .unwrap();
            assert_eq!(cached.action_id, provenance.action_id);
            assert_eq!(
                cached.output_files[0].sha256,
                format!("{:x}", Sha256::digest(&payload))
            );
        });
    });
    let seconds = started.elapsed().as_secs_f64();
    let mut persisted = 0;
    store
        .for_each_provenance(|_| {
            persisted += 1;
            Ok(ControlFlow::Continue(()))
        })
        .unwrap();
    assert_eq!(persisted, count);
    eprintln!(
        "promotion_benchmark actions={count} threads={threads} seconds={seconds:.6} actions_per_second={:.3}",
        count as f64 / seconds
    );
    drop(store);
    fs::remove_dir_all(root).unwrap();
}
