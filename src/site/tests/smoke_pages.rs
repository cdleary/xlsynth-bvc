// SPDX-License-Identifier: Apache-2.0

use super::*;

const LEGACY_ROUTES: [&str; 8] = [
    "",
    "runs.html",
    "progression.html",
    "releases.html",
    "mffc-discrepancies.html",
    "ir-fn-corpus-g8r-vs-yosys-abc/",
    "ir-fn-g8r-abc-vs-codegen-yosys-abc/",
    "dataset.html",
];

fn smoke_fixture(root: &Path, base_url: &str, output: &str) -> PathBuf {
    let store = ArtifactStore::new(root.join("fixture-store"));
    store.ensure_layout().expect("fixture store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write empty versions fixture");
    let snapshot_dir = root.join("fixture-snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build isolated snapshot");
    drop(store);
    let site_dir = root.join(output);
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: base_url.to_string(),
        overwrite: false,
    })
    .expect("build isolated legacy site");
    verify_static_site(&site_dir).expect("verify isolated fixture");
    site_dir
}

fn smoke_relpath(route: &str) -> String {
    if route.is_empty() || route.ends_with('/') {
        format!("{route}index.html")
    } else {
        route.to_string()
    }
}

fn smoke_html(content: &SmokePageContent) -> &str {
    match content {
        SmokePageContent::Html(html) => html,
        SmokePageContent::Text(_) => panic!("expected full canonical HTML verification"),
    }
}

fn rendered_marker() -> Vec<(String, usize)> {
    vec![("data-rendered=\"true\"".to_string(), 1)]
}

fn plot_marker() -> Vec<(String, usize)> {
    vec![("class=\"plotly-host js-plotly-plot\"".to_string(), 4)]
}

fn legacy_empty_markers() -> Vec<Vec<(String, usize)>> {
    vec![
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        plot_marker(),
        plot_marker(),
        vec![],
    ]
}

#[test]
fn legacy_smoke_pages_verify_canonical_html_at_root_and_subdirectory_bases() {
    for base_url in ["/", "/history/"] {
        let root = temp_root();
        let site_dir = smoke_fixture(&root, base_url, "standalone");
        let pages = smoke_pages(&site_dir, false, false).expect("plan standalone smoke pages");
        assert_eq!(
            pages.iter().map(|page| page.0.as_str()).collect::<Vec<_>>(),
            LEGACY_ROUTES
        );
        assert_eq!(
            pages.iter().map(|page| page.2.clone()).collect::<Vec<_>>(),
            legacy_empty_markers()
        );
        for (route, content, _) in &pages {
            let canonical = fs::read_to_string(site_dir.join(smoke_relpath(route))).unwrap();
            assert_eq!(smoke_html(content), canonical);
            let url = format!("https://smoke.invalid{base_url}{route}");
            content
                .verify(&canonical, &url)
                .expect("canonical response");
            let changed = format!("{canonical}\n<!-- unexpected response mutation -->");
            assert!(content.verify(&changed, &url).is_err());
        }

        let canonical_home = smoke_html(&pages[0].1).to_string();
        let changed_home = format!("{canonical_home}\n<!-- title and original HTML retained -->");
        fs::write(site_dir.join("index.html"), &changed_home).unwrap();
        let planned_after_mutation =
            legacy_smoke_pages(&site_dir).expect("derive expected HTML from canonical inputs");
        assert_eq!(smoke_html(&planned_after_mutation[0].1), canonical_home);
        assert!(
            planned_after_mutation[0]
                .1
                .verify(&changed_home, &format!("https://smoke.invalid{base_url}"))
                .is_err()
        );
        fs::remove_dir_all(root).expect("cleanup");
    }
}

fn install_planning_dashboard_context(dashboard: &Path, techmap: bool) {
    let placeholder = pb::StaticSiteManifest::default().encode_to_vec();
    let corpus_site_manifest = format!("corpus/{STATIC_SITE_MANIFEST_FILENAME}");
    write_file(dashboard, &corpus_site_manifest, &placeholder).unwrap();
    write_file(dashboard, &format!("corpus/{}", site_corpus::MANIFEST), &[]).unwrap();
    let techmap_manifest = if techmap {
        let relpath = format!("{}/{STATIC_SITE_MANIFEST_FILENAME}", site_techmap::ROOT);
        write_file(dashboard, &relpath, &placeholder).unwrap();
        write_file(
            dashboard,
            &format!("{}/{}", site_techmap::ROOT, site_techmap::MANIFEST),
            &[],
        )
        .unwrap();
        Some(publication_file(dashboard, &relpath).unwrap())
    } else {
        None
    };
    let context = site_corpus::wire::DashboardSite {
        record_version: 1,
        history_manifest: Some(
            publication_file(
                dashboard,
                &format!("history/{STATIC_SITE_MANIFEST_FILENAME}"),
            )
            .unwrap(),
        ),
        corpus_manifest: Some(publication_file(dashboard, &corpus_site_manifest).unwrap()),
        techmap_manifest,
    };
    write_file(
        dashboard,
        site_dashboard::MANIFEST,
        &context.encode_to_vec(),
    )
    .unwrap();
}

#[test]
fn dashboard_smoke_pages_cover_child_routes_exact_html_and_redirect_markers() {
    for (base_url, techmap) in [("/", false), ("/reports/", true)] {
        let root = temp_root();
        let history_base = format!("{base_url}history/");
        let history = smoke_fixture(&root, &history_base, "dashboard/history");
        let dashboard = root.join("dashboard");
        install_planning_dashboard_context(&dashboard, techmap);
        let navigation = site_dashboard::child_navigation(&history)
            .expect("decode typed parent context")
            .expect("history navigation context");
        let pages = smoke_pages(&dashboard, false, true).expect("plan dashboard smoke pages");
        let mut expected_routes = vec!["".to_string(), "corpus/".to_string()];
        expected_routes.extend(LEGACY_ROUTES.map(|route| format!("history/{route}")));
        if techmap {
            expected_routes.push("tech-mapped-asap7/".to_string());
        }
        assert_eq!(
            pages.iter().map(|page| page.0.clone()).collect::<Vec<_>>(),
            expected_routes
        );
        let mut expected_markers = vec![rendered_marker(), rendered_marker()];
        let mut history_markers = legacy_empty_markers();
        history_markers[0] = rendered_marker();
        expected_markers.extend(history_markers);
        if techmap {
            expected_markers.push(rendered_marker());
        }
        assert_eq!(
            pages.iter().map(|page| page.2.clone()).collect::<Vec<_>>(),
            expected_markers
        );
        match &pages[0].1 {
            SmokePageContent::Text(expected) => assert_eq!(*expected, "g8r cost by release"),
            SmokePageContent::Html(_) => panic!("dashboard title check changed unexpectedly"),
        }

        let corpus = dashboard.join("corpus");
        let corpus_html = site_dashboard::child_asset(
            site_dashboard::child_navigation(&corpus).unwrap(),
            "index.html",
            include_bytes!("../../site_assets/corpus.html"),
        )
        .unwrap();
        assert_eq!(smoke_html(&pages[1].1).as_bytes(), corpus_html);
        for ((route, content, _), history_route) in pages[2..10].iter().zip(LEGACY_ROUTES) {
            assert_eq!(route, &format!("history/{history_route}"));
            let relpath = smoke_relpath(history_route);
            let original = fs::read(history.join(&relpath)).unwrap();
            let canonical =
                site_dashboard::child_asset(Some(navigation), &relpath, &original).unwrap();
            assert_eq!(smoke_html(content).as_bytes(), canonical);
        }
        assert_eq!(
            smoke_html(&pages[2].1),
            r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="bvc-site-root" content="./"><meta http-equiv="refresh" content="0;url=./../index.html"><title>Results dashboard</title></head><body><p><a href="./../index.html">Open the results dashboard</a></p></body></html>"#
        );
        if techmap {
            assert_eq!(
                smoke_html(&pages[10].1),
                include_str!("../../site_assets/techmap.html")
            );
        }
        for (route, content, _) in pages.iter().skip(1) {
            let canonical = smoke_html(content);
            let url = format!("https://smoke.invalid{base_url}{route}");
            content.verify(canonical, &url).unwrap();
            let changed = format!("{canonical}\n<!-- original title retained -->");
            assert!(content.verify(&changed, &url).is_err());
        }
        assert_eq!(
            format!("https://smoke.invalid{base_url}{}", pages[2].0),
            format!("https://smoke.invalid{history_base}")
        );
        fs::remove_dir_all(root).expect("cleanup");
    }
}

fn marker_catalog() -> BrowserCatalog {
    BrowserCatalog {
        schema_version: BROWSER_CATALOG_SCHEMA_VERSION,
        snapshot_id: "a".repeat(64),
        base_url: "/".to_string(),
        datasets: vec![],
        runs: vec![],
        progression_evidence: vec![],
        progression: empty_browser_progression_index().unwrap(),
        releases: vec![],
        repository_head_observation: None,
    }
}

fn marker_generation(label: &str, git: bool) -> BrowserProgressionGeneration {
    BrowserProgressionGeneration {
        generation_id: sha256_hex(label.as_bytes()),
        origin: if git {
            BrowserProgressionOrigin::GitRevision {
                repository: crate::XLSYNTH_CRATE_GIT_REPOSITORY.to_string(),
                commit: "b".repeat(40),
                requested_ref: None,
                baseline_crate_version: "0.1.0".to_string(),
                baseline_release_commit: None,
            }
        } else {
            BrowserProgressionOrigin::CrateRelease
        },
        display_label: label.to_string(),
        event_time_utc: None,
        crate_version: (!git).then(|| label.to_string()),
        dso_version: "0.1.0".to_string(),
        baseline_generation_id: None,
        coverage: BrowserProgressionCoverage::CohortComplete,
        observed_ir_count: RELEASE_PROGRESSION_IR_COUNT as u64,
        cohort_ir_count: RELEASE_PROGRESSION_IR_COUNT as u64,
        missing_cohort_ir_count: 0,
        extra_ir_count: 0,
        run_samples: vec![],
        run_samples_shard: None,
    }
}

fn progression_markers(baseline: &str, current: &str) -> Vec<(String, usize)> {
    vec![
        ("data-progression-rendered=\"true\"".to_string(), 1),
        (
            format!("data-progression-baseline-version=\"{baseline}\""),
            1,
        ),
        (format!("data-progression-current-version=\"{current}\""), 1),
    ]
}

#[test]
fn legacy_smoke_progression_markers_preserve_complete_generation_and_git_selection() {
    let mut catalog = marker_catalog();
    assert_eq!(legacy_progression_markers(&catalog).unwrap(), vec![]);
    catalog.progression.cohorts[0].generations = vec![marker_generation("v0.1.0", false)];
    assert_eq!(legacy_progression_markers(&catalog).unwrap(), vec![]);
    catalog.progression.cohorts[0]
        .generations
        .push(marker_generation("v0.2.0", false));
    assert_eq!(
        legacy_progression_markers(&catalog).unwrap(),
        progression_markers("v0.1.0", "v0.2.0")
    );
    let mut candidate = marker_generation("git@bbbbbbbb", true);
    candidate.baseline_generation_id = Some(
        catalog.progression.cohorts[0].generations[0]
            .generation_id
            .clone(),
    );
    catalog.progression.cohorts[0].generations.push(candidate);
    catalog.progression.cohorts[0]
        .generations
        .push(marker_generation("v0.3.0", false));
    let mut incomplete = marker_generation("git@incomplete", true);
    incomplete.coverage = BrowserProgressionCoverage::Partial;
    incomplete.observed_ir_count -= 1;
    incomplete.missing_cohort_ir_count = 1;
    catalog.progression.cohorts[0].generations.push(incomplete);
    assert_eq!(
        legacy_progression_markers(&catalog).unwrap(),
        progression_markers("v0.1.0", "git@bbbbbbbb")
    );
    catalog.progression.cohorts[0].generations.truncate(3);
    catalog.progression.cohorts[0].generations[2].baseline_generation_id = None;
    assert_eq!(
        legacy_progression_markers(&catalog).unwrap(),
        progression_markers("v0.2.0", "git@bbbbbbbb")
    );
    catalog.progression.default_cohort_id = "missing-cohort".to_string();
    assert!(legacy_progression_markers(&catalog).is_err());
}

#[test]
fn standalone_corpus_smoke_plan_preserves_its_rendered_marker() {
    let pages = smoke_pages(Path::new("unused-planning-input"), true, false).unwrap();
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].0, "");
    assert_eq!(pages[0].2, rendered_marker());
    match &pages[0].1 {
        SmokePageContent::Text(expected) => assert_eq!(*expected, "IR corpus synthesis results"),
        SmokePageContent::Html(_) => panic!("standalone corpus smoke contract changed"),
    }
}
