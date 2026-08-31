// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::campaign::finalize_campaign_run;
use crate::executor::compute_action_id;
use crate::model::{ActionSpec, ArtifactRef, ArtifactType, Provenance, QueueFailed};
use crate::query::{canonical_root_actions_for_crate_version, rebuild_versions_cards_index};
use crate::snapshot::{
    BuildStaticSnapshotOptions, STATIC_SNAPSHOT_MANIFEST_FILENAME, build_static_snapshot,
};
use crate::store::ArtifactStore;
use crate::versioning::{load_version_compat_map, resolve_xlsynth_version_for_driver};
use chrono::Utc;
use serde_json::json;
use std::io::Write as _;

fn temp_root() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("xlsynth-bvc-site-{}-{nanos}", std::process::id()))
}

fn test_repo_root() -> PathBuf {
    std::env::current_dir().expect("current directory")
}

fn empty_versions_index_bytes() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "schema_version": crate::WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION,
        "generated_utc": "2026-08-29T12:00:00Z",
        "report": {"cards": [], "unattributed_actions": []}
    }))
    .expect("serialize empty versions index")
}

fn refresh_site_manifest_entry(site_dir: &Path, relpath: &str) {
    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let mut manifest = pb::StaticSiteManifest::decode(
        fs::read(&manifest_path)
            .expect("read site manifest")
            .as_slice(),
    )
    .expect("decode site manifest");
    let replacement = publication_file(site_dir, relpath).expect("describe changed site file");
    let slot = manifest
        .files
        .iter_mut()
        .find(|file| {
            file.relpath
                .as_ref()
                .is_some_and(|value| value.value == relpath)
        })
        .expect("declared site file");
    *slot = replacement;
    fs::write(manifest_path, manifest.encode_to_vec()).expect("rewrite site manifest");
}

#[test]
fn generated_site_links_are_relocatable() {
    assert_eq!(site_root_url("index.html").expect("root URL"), "./");
    assert_eq!(
        site_root_url("runs/abc/index.html").expect("nested root URL"),
        "../../"
    );
    assert_eq!(
        resolve_site_link("runs/abc/index.html", "../../assets/site.css").expect("nested asset"),
        "assets/site.css"
    );
    assert_eq!(
        resolve_site_link("runs/abc/index.html", "../../").expect("root page"),
        "index.html"
    );
    assert!(resolve_site_link("index.html", "/xlsynth-bvc/assets/site.css").is_err());
}

#[test]
fn site_build_and_verify_supports_subdirectory_base() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    let summary = build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/xlsynth-bvc/".into(),
        overwrite: false,
    })
    .expect("build site");
    assert_eq!(summary.dataset_count, 1);
    let verified = verify_static_site(&site_dir).expect("verify site");
    assert_eq!(verified.base_url, "/xlsynth-bvc/");
    let index_html = fs::read_to_string(site_dir.join("index.html")).expect("read homepage HTML");
    assert!(index_html.contains("Boolean synthesis comparison"));
    assert!(index_html.contains("id=\"home-overview\""));
    assert!(index_html.contains("paired IR samples"));
    assert!(
        index_html.contains(crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME)
    );
    assert_eq!(index_html.matches("class=\"analysis-link\"").count(), 4);
    assert!(index_html.contains("Raw datasets"));
    assert!(index_html.contains("Campaign runs"));
    assert!(index_html.contains(PLOTLY_ASSET_NAME));
    let (_, js_name) = static_site_asset_names();
    assert!(index_html.contains(&js_name));
    assert!(!index_html.contains("<h2>Datasets</h2>"));
    assert!(!index_html.contains("class=\"feature-card\""));
    assert!(!index_html.contains(crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME));
    assert!(
        !fs::read_to_string(site_dir.join("dataset.html"))
            .expect("read HTML")
            .contains("/api/")
    );
    let progression_html =
        fs::read_to_string(site_dir.join("progression.html")).expect("read progression HTML");
    assert!(progression_html.contains(crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME));
    assert!(progression_html.contains("Signed G8r − Yosys/ABC"));
    assert!(APP_JS.contains("Release progression data is not available in this snapshot."));
    assert!(APP_JS.contains("At least two populated complete crate releases are needed"));
    assert!(APP_JS.contains("negative means G8r is better"));
    assert!(APP_JS.contains("sample.stdlib_root_action_id"));
    assert!(APP_JS.contains("contains duplicate samples"));
    assert!(APP_JS.contains("query.get('all_versions')==='true'"));
    assert!(APP_JS.contains("Median paired per-function product-loss change"));
    assert!(APP_JS.contains("Current-only and baseline-only"));
    assert!(APP_JS.contains("completeGenerations=generations.filter"));
    assert!(APP_JS.contains("Degraded runs are never selected by default or plotted"));
    assert!(APP_JS.contains("Partial-data warning"));
    let comparison_html =
        fs::read_to_string(site_dir.join("ir-fn-corpus-g8r-vs-yosys-abc/index.html"))
            .expect("read comparison HTML");
    assert!(comparison_html.contains(crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME));
    assert!(comparison_html.contains("../assets/plotly-2.35.2.min.js"));
    assert!(!comparison_html.contains("cdn.plot.ly"));
    assert!(comparison_html.contains("script-src 'self';"));
    assert!(comparison_html.contains("style-src 'self' 'unsafe-inline';"));
    assert!(comparison_html.contains("../assets/"));
    assert_eq!(
        fs::read(site_dir.join(format!("assets/{PLOTLY_ASSET_NAME}")))
            .expect("read local Plotly asset"),
        PLOTLY_JS
    );
    assert!(
        site_dir
            .join(format!("assets/{PLOTLY_LICENSE_ASSET_NAME}"))
            .is_file()
    );
    assert!(
        site_dir
            .join(format!("assets/{PLOTLY_NOTICE_ASSET_NAME}"))
            .is_file()
    );

    let frontend_comparison_html =
        fs::read_to_string(site_dir.join("ir-fn-g8r-abc-vs-codegen-yosys-abc/index.html"))
            .expect("read frontend comparison HTML");
    assert!(
        frontend_comparison_html
            .contains(crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME)
    );
    assert!(frontend_comparison_html.contains("G8r+ABC vs codegen+Yosys/ABC"));
    assert!(APP_JS.contains("renderComparisonQuadrants"));
    assert!(APP_JS.contains("renderComparisonLoss"));
    assert!(!APP_JS.contains("scattergl"));
    assert!(!STYLE_CSS.contains(".table-wrap{max-height:60vh;overflow:auto}svg{"));
    assert!(STYLE_CSS.contains("#plot>svg,.progression-chart>svg{"));
    assert!(APP_JS.contains("losses_only"));
    assert!(APP_JS.contains("comparisonSelectionKey"));
    let mffc_html = fs::read_to_string(site_dir.join("mffc-discrepancies.html"))
        .expect("read MFFC discrepancies HTML");
    assert!(mffc_html.contains("MFFC discrepancies"));
    assert!(
        mffc_html.contains(crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME)
    );
    assert!(mffc_html.contains(crate::WEB_IR_FN_CORPUS_MFFC_IR_INDEX_FILENAME));
    assert!(mffc_html.contains("positive means G8r is worse"));
    assert!(APP_JS.contains("No paired MFFC samples are available in this snapshot."));
    assert!(APP_JS.contains("Largest G8r product losses"));
    assert!(APP_JS.contains("Both evidence paths reference this same action and top."));
    assert!(APP_JS.contains("entry.mffc_structural_hash===sample.structural_hash"));
    assert!(APP_JS.contains("mffcIrPanel('G8r'"));
    assert!(APP_JS.contains("mffcIrPanel('Yosys/ABC'"));
    assert!(APP_JS.contains("Representative subject"));
    assert!(APP_JS.contains("Yosys/ABC IR action"));
    let initial_render = APP_JS
        .split_once("async function mffcDiscrepancies")
        .expect("MFFC page initializer")
        .1
        .split_once("async function datasetExplorer")
        .expect("next page initializer")
        .0;
    assert!(!initial_render.contains("loadMffcIrIndex"));
    assert!(initial_render.contains("mffcDetailGeneration++"));
    let detail_render = APP_JS
        .split_once("async function showMffcDetail")
        .expect("MFFC detail renderer")
        .1
        .split_once("function mffcUnavailable")
        .expect("end of MFFC detail renderer")
        .0;
    assert!(detail_render.contains("loadMffcIrEntry"));
    assert_eq!(
        detail_render
            .matches("generation!==mffcDetailGeneration")
            .count(),
        2
    );
    assert!(APP_JS.contains("rankMffcSamples"));
    assert!(!APP_JS.contains("absolute difference"));
    assert!(!APP_JS.contains("after?.median??0"));
    assert!(
        fs::read_to_string(site_dir.join("index.html"))
            .expect("read index HTML")
            .contains("progression.html")
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn homepage_javascript_summarizes_all_published_corpus_samples() {
    const SCRIPT: &str = r#"
const fs = require('fs');
let capturedLayout = null;
let capturedTraces = null;
global.document = {
  querySelector: () => ({content: ''}),
  getElementById: () => null,
};
global.Plotly = {
  react: (_id, traces, layout) => {
capturedTraces = traces;
capturedLayout = layout;
return Promise.resolve();
  },
};
const app = fs.readFileSync(0, 'utf8');
const prefix = app.slice(0, app.indexOf('async function main()'));
const api = new Function(prefix + '\nreturn {homepageSummary,homepageExplorerHref,homepagePairPlot};')();
const sample = (crate_version, fn_key, g8r_nodes, g8r_levels, yosys_abc_nodes, yosys_abc_levels, g8r_product_loss, ir_top = fn_key) => ({
  crate_version,
  fn_key,
  ir_top,
  ir_node_count: 20,
  g8r_nodes,
  g8r_levels,
  yosys_abc_nodes,
  yosys_abc_levels,
  g8r_product_loss,
});
const samples = [
  sample('0.66.0', 'win', 10, 5, 12, 7, -34),
  sample('0.66.0', 'loss', 12, 8, 10, 6, 36),
  sample('0.66.0', 'tie', 1, 1, 1, 1, 0),
  sample('0.68.0', 'partial', 10, 5, 12, 7, -34),
  ...Array.from({length: 4}, (_, i) => sample('0.68.0', `mffc-${i}`, 1, 1, 1, 1, 0, `__mffc_${i}`)),
  ...Array.from({length: 4}, (_, i) => sample('0.68.0', `k3-${i}`, 1, 1, 1, 1, 0, `__k3_cone_${i}`)),
];
const summary = api.homepageSummary(samples);
if (JSON.stringify(summary.versions) !== JSON.stringify(['0.66.0', '0.68.0']) || summary.samples.length !== 12) {
  throw new Error(`unexpected all-corpus summary: ${JSON.stringify(summary)}`);
}
if (summary.selection !== 'all published paired IR samples' || summary.wholeFunctionCount !== 4 || summary.mffcCount !== 4 || summary.k3Count !== 4) {
  throw new Error(`unexpected all-corpus policy: ${JSON.stringify(summary)}`);
}
if (summary.pureWins !== 2 || summary.strictLosses !== 1 || summary.medianLoss !== 0) {
  throw new Error(`unexpected quadrant summary: ${JSON.stringify(summary)}`);
}
api.homepagePairPlot('test-plot', [sample('0.66.0', 'zero', 0, 0, 0, 0, 0)], 'lhs', 'rhs', value => value.g8r_nodes, value => value.yosys_abc_nodes, {lhs: 'lhs', rhs: 'rhs'});
if (!capturedLayout?.annotations?.[0]?.text.includes('1 zero-valued pair plotted at 1 for log scale')) {
  throw new Error(`missing zero-clamp disclosure: ${JSON.stringify(capturedLayout)}`);
}
const largeSamples = Array.from({length: 70000}, (_, i) => sample('0.68.0', 'large-' + i, i + 1, i + 1, i + 2, i + 2, 0));
api.homepagePairPlot('large-test-plot', largeSamples, 'lhs', 'rhs', value => value.g8r_nodes, value => value.yosys_abc_nodes, {lhs: 'lhs', rhs: 'rhs'});
const referenceLine = capturedTraces.at(-1);
if (JSON.stringify(referenceLine.x) !== JSON.stringify([1, 70001]) || JSON.stringify(referenceLine.y) !== JSON.stringify([1, 70001])) {
  throw new Error('unexpected large-corpus bounds: ' + JSON.stringify(referenceLine));
}

const href = api.homepageExplorerHref({
  crate_version: '0.68.0',
  g8r_stats_action_id: 'g8r-action',
  yosys_abc_stats_action_id: 'yosys-action',
}, true);
if (href !== 'ir-fn-g8r-abc-vs-codegen-yosys-abc/?crate_version=0.68.0&losses_only=true&sample=g8r-action%3Ayosys-action') {
  throw new Error(`unexpected homepage explorer link: ${href}`);
}
"#;
    let mut child = match Command::new("node")
        .arg("-e")
        .arg(SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => panic!("spawn node: {error}"),
    };
    child
        .stdin
        .as_mut()
        .expect("node stdin")
        .write_all(APP_JS.as_bytes())
        .expect("write app js");
    let output = child.wait_with_output().expect("wait for node");
    assert!(
        output.status.success(),
        "node failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn progression_javascript_uses_median_of_per_function_deltas() {
    const SCRIPT: &str = r#"
const fs = require('fs');
global.document = {
  querySelector: () => ({content: ''}),
  getElementById: () => null,
};
const app = fs.readFileSync(0, 'utf8');
const prefix = app.slice(0, app.indexOf('async function main()'));
const api = new Function(prefix + '\nreturn {compareSamples,medianPairedProductLossChange};')();
const sample = (fn_key, g8r_product_loss) => ({fn_key, ir_top: null, g8r_product_loss});
const before = [sample('a', 0), sample('b', 100), sample('c', 101)];
const after = [sample('a', 99), sample('b', 98), sample('c', 102)];
const {pairs} = api.compareSamples(before, after);
const actual = api.medianPairedProductLossChange(pairs);
if (actual !== 1) {
  throw new Error(`expected median paired delta 1, got ${actual}`);
}
"#;
    let mut child = match Command::new("node")
        .arg("-e")
        .arg(SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping embedded JavaScript behavior test: node is unavailable");
            return;
        }
        Err(error) => panic!("starting node: {error}"),
    };
    child
        .stdin
        .take()
        .expect("node stdin")
        .write_all(APP_JS.as_bytes())
        .expect("write embedded JavaScript");
    let output = child.wait_with_output().expect("wait for node");
    assert!(
        output.status.success(),
        "embedded JavaScript test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn comparison_javascript_matches_dynamic_quadrants_and_loss_filter() {
    const SCRIPT: &str = r#"
const fs = require('fs');
global.document = {
  querySelector: () => ({content: ''}),
  getElementById: () => null,
};
const app = fs.readFileSync(0, 'utf8');
const prefix = app.slice(0, app.indexOf('async function main()'));
const api = new Function(prefix + '\nreturn {comparisonDefaultVersion,comparisonIsLoss,comparisonQuadrant,comparisonSelectionKey};')();
const state = {lhs: 'G8r', rhs: 'Yosys/ABC'};
const sample = (g8r_nodes, g8r_levels, yosys_abc_nodes, yosys_abc_levels) => ({
  g8r_nodes,
  g8r_levels,
  yosys_abc_nodes,
  yosys_abc_levels,
});
const cases = [
  [sample(10, 10, 12, 12), 'Q1 pure win', false],
  [sample(12, 8, 10, 10), 'Q2 mixed', true],
  [sample(12, 12, 10, 10), 'Q3 strict loss', true],
  [sample(8, 12, 10, 10), 'Q4 mixed', true],
  [sample(10, 10, 10, 10), 'Tie exact', false],
];
for (const [value, prefix, loss] of cases) {
  const actual = api.comparisonQuadrant(value, state).label;
  if (!actual.startsWith(prefix)) {
throw new Error(`expected ${prefix}, got ${actual}`);
  }
  if (api.comparisonIsLoss(value, state) !== loss) {
throw new Error(`unexpected loss classification for ${actual}`);
  }
}
const key = api.comparisonSelectionKey({g8r_stats_action_id: 'g', yosys_abc_stats_action_id: 'y'});
if (key !== 'g:y') {
  throw new Error(`unexpected selection key: ${key}`);
}
const versions = ['0.9.0', '0.10.0'];
if (api.comparisonDefaultVersion(versions, null) !== '0.10.0') {
  throw new Error('missing version did not default to latest');
}
if (api.comparisonDefaultVersion(versions, '0.9.0') !== '0.9.0') {
  throw new Error('valid requested version was not preserved');
}
if (api.comparisonDefaultVersion(versions, 'invalid') !== '0.10.0') {
  throw new Error('invalid requested version did not default to latest');
}
if (api.comparisonDefaultVersion([], null) !== '') {
  throw new Error('empty version list did not stay empty');
}

"#;
    let mut child = match Command::new("node")
        .arg("-e")
        .arg(SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping embedded JavaScript behavior test: node is unavailable");
            return;
        }
        Err(error) => panic!("starting node: {error}"),
    };
    child
        .stdin
        .take()
        .expect("node stdin")
        .write_all(APP_JS.as_bytes())
        .expect("write embedded JavaScript");
    let output = child.wait_with_output().expect("wait for node");
    assert!(
        output.status.success(),
        "embedded JavaScript test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn comparison_javascript_ignores_stale_evidence_fetches() {
    const SCRIPT: &str = r#"
const fs = require('fs');
const elements = {
  'comparison-detail-empty': {hidden: false},
  'comparison-detail-json': {hidden: true, textContent: ''},
  'comparison-detail-evidence': {textContent: '', innerHTML: ''},
  'comparison-max-ir-nodes': {value: '100'},
  'comparison-crate-version': {value: '0.31.0'},
  'comparison-sample-mode': {value: 'all'},
};
global.document = {
  querySelector: () => ({content: ''}),
  getElementById: id => elements[id] || null,
};
global.location = {href: 'https://example.test/plots'};
global.history = {replaceState: () => {}};
const pending = new Map();
global.fetch = url => new Promise(resolve => pending.set(url, resolve));
const app = fs.readFileSync(0, 'utf8');
const prefix = app.slice(0, app.indexOf('async function main()'));
const api = new Function(prefix + '\nreturn {showComparisonDetail};')();
const firstHash = '1'.repeat(64);
const secondHash = '2'.repeat(64);
const datasetKey = hash => `ir-fn-corpus-structural.v2/by-hash/${hash.slice(0, 2)}/${hash.slice(2, 4)}/${hash}.json`;
const state = {
  catalog: {datasets: [
{logical_key: datasetKey(firstHash), url: 'first.json'},
{logical_key: datasetKey(secondHash), url: 'second.json'},
  ]},
  lhs: 'G8r',
  rhs: 'Yosys/ABC',
  selectedSampleKey: null,
};
const sample = (name, hash) => ({
  fn_key: name,
  crate_version: '0.31.0',
  dso_version: '0.54.7',
  ir_node_count: 10,
  ir_action_id: `action-${name}`,
  ir_top: `top-${name}`,
  structural_hash: hash,
  g8r_stats_action_id: `g8r-${name}`,
  yosys_abc_stats_action_id: `yabc-${name}`,
});
const response = name => ({
  ok: true,
  json: async () => ({members: [{
crate_version: '0.31.0',
opt_ir_action_id: `action-${name}`,
ir_top: `top-${name}`,
source_ir_action_id: `source-${name}`,
dslx_origin: {dslx_file: `${name}.x`, dslx_fn_name: name},
  }]}),
});
(async () => {
  const first = api.showComparisonDetail(sample('first', firstHash), 'plot-levels', state);
  const second = api.showComparisonDetail(sample('second', secondHash), 'plot-nodes', state);
  pending.get('second.json')(response('second'));
  await second;
  const secondHtml = elements['comparison-detail-evidence'].innerHTML;
  if (!secondHtml.includes(secondHash) || !secondHtml.includes('source-second')) {
throw new Error(`second selection did not render: ${secondHtml}`);
  }
  pending.get('first.json')(response('first'));
  await first;
  const finalHtml = elements['comparison-detail-evidence'].innerHTML;
  if (finalHtml !== secondHtml || finalHtml.includes(firstHash) || finalHtml.includes('source-first')) {
throw new Error(`stale first selection replaced second: ${finalHtml}`);
  }
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
"#;
    let mut child = match Command::new("node")
        .arg("-e")
        .arg(SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping embedded JavaScript race test: node is unavailable");
            return;
        }
        Err(error) => panic!("starting node: {error}"),
    };
    child
        .stdin
        .take()
        .expect("node stdin")
        .write_all(APP_JS.as_bytes())
        .expect("write embedded JavaScript");
    let output = child.wait_with_output().expect("wait for node");
    assert!(
        output.status.success(),
        "embedded JavaScript race test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn mffc_javascript_filters_and_ranks_paired_samples() {
    const SCRIPT: &str = r#"
const fs = require('fs');
global.document = {
  querySelector: () => ({content: ''}),
  getElementById: () => null,
};
const app = fs.readFileSync(0, 'utf8');
const prefix = app.slice(0, app.indexOf('async function main()'));
const api = new Function(prefix + '\nreturn {mffcComparisonKey,mffcLossPresentation,mffcSamples,mffcStructuralGroupKey,rankMffcSamples,sameMffcIrIdentity};')();
const sample = (ir_top, ir_node_count, g8r_product_loss) => ({
  ir_top,
  ir_node_count,
  g8r_nodes: 10,
  g8r_levels: 10,
  yosys_abc_nodes: 10,
  yosys_abc_levels: 10,
  g8r_product: 100 + g8r_product_loss,
  yosys_abc_product: 100,
  g8r_product_loss,
});
const filtered = api.mffcSamples([
  sample('__mffc_small', 4, 10),
  sample('__whole_function', 3, 1000),
  sample('__mffc_large', 20, 200),
]);
if (filtered.length !== 2) {
  throw new Error(`expected 2 MFFCs, got ${filtered.length}`);
}
const ranked = api.rankMffcSamples(filtered);
if (ranked.map(row => row.ir_top).join(',') !== '__mffc_large,__mffc_small') {
  throw new Error(`unexpected MFFC ranking: ${ranked.map(row => row.ir_top)}`);
}
const sourceHash = 'd'.repeat(64);
if (api.mffcStructuralGroupKey(sourceHash) !== `ir-fn-corpus-structural.v2/by-hash/dd/dd/${sourceHash}.json`) {
  throw new Error('unexpected source structural group key');
}
if (api.mffcComparisonKey('0.31.0', 'c'.repeat(64)) !== `0.31.0:${'c'.repeat(64)}`) {
  throw new Error('unexpected MFFC comparison key');
}
const pairedEntry = {
  g8r: {ir_action_id: 'a', ir_top: '__mffc_left', source_ir_top: '__source_left'},
  yosys_abc: {ir_action_id: 'b', ir_top: '__mffc_right', source_ir_top: '__source_right'},
};
if (api.sameMffcIrIdentity(pairedEntry.g8r, pairedEntry.yosys_abc)) {
  throw new Error('different backend IR identities were treated as shared');
}
const presentations = [api.mffcLossPresentation(12.5), api.mffcLossPresentation(-12.5), api.mffcLossPresentation(0)];
if (presentations.map(value => value.kind).join(',') !== 'regressed,improved,same') {
  throw new Error(`unexpected loss presentation kinds: ${presentations.map(value => value.kind)}`);
}
if (presentations.map(value => value.text).join(',') !== '12.5 units worse than,12.5 units better than,tied with') {
  throw new Error(`unexpected loss presentation text: ${presentations.map(value => value.text)}`);
}
"#;
    let mut child = match Command::new("node")
        .arg("-e")
        .arg(SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping embedded JavaScript behavior test: node is unavailable");
            return;
        }
        Err(error) => panic!("starting node: {error}"),
    };
    child
        .stdin
        .take()
        .expect("node stdin")
        .write_all(APP_JS.as_bytes())
        .expect("write embedded JavaScript");
    let output = child.wait_with_output().expect("wait for node");
    assert!(
        output.status.success(),
        "embedded JavaScript test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn public_site_never_contains_private_executor_error_text() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    let repo_root = std::env::current_dir().expect("current dir");
    let crate_version = load_version_compat_map(&repo_root)
        .expect("compat map")
        .into_keys()
        .next()
        .expect("known crate version");
    let dso_version = resolve_xlsynth_version_for_driver(&repo_root, &crate_version)
        .expect("resolve DSO version");
    let root_action =
        canonical_root_actions_for_crate_version(&repo_root, &crate_version, &dso_version)
            .expect("canonical roots")
            .into_iter()
            .find(|action| {
                matches!(
                    action,
                    ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. }
                )
            })
            .expect("stdlib root action");
    let discovery_runtime = match &root_action {
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            discovery_runtime: Some(runtime),
            ..
        } => runtime.clone(),
        _ => panic!("stdlib root has discovery runtime"),
    };
    let root_action_id = compute_action_id(&root_action).expect("root action id");
    let private_path = root.join("private-build/worker.log").display().to_string();
    let private_token = "BVC_TEST_CREDENTIAL=do-not-publish";
    let private_windows_path = "C:/bvc-private/worker.log";
    let private_file_uri = "file:///srv/bvc-private/worker.log";
    let failed_action = ActionSpec::DriverIrToOpt {
        ir_action_id: "a".repeat(64),
        top_fn_name: Some(format!(
            "top={private_path}; {private_windows_path}; {private_file_uri}; {private_token}"
        )),
        version: dso_version.clone(),
        runtime: discovery_runtime,
    };
    let failed_action_id = compute_action_id(&failed_action).expect("failed action id");
    let now = Utc::now();
    store
        .write_provenance(&Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: root_action_id.clone(),
            created_utc: now,
            action: root_action,
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: root_action_id,
                artifact_type: ArtifactType::DslxFileSubtree,
                relpath: "payload".to_string(),
            },
            output_files: Vec::new(),
            commands: Vec::new(),
            details: json!({
                "dslx_list_fns_discovery_error": format!(
                    "discovery failed at {private_path}; {private_token}"
                )
            }),
            suggested_next_actions: Vec::new(),
        })
        .expect("write private discovery failure provenance");
    store
        .write_failed_action_record(&QueueFailed {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: failed_action_id,
            enqueued_utc: now,
            failed_utc: now,
            failed_by: "test-worker".to_string(),
            action: failed_action,
            error: format!("executor failed at {private_path}; {private_token}"),
        })
        .expect("write private failure");
    rebuild_versions_cards_index(&store, &repo_root).expect("build public versions index");

    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &repo_root,
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let versions_json = fs::read_to_string(
        snapshot_dir
            .join("web_index")
            .join(crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME),
    )
    .expect("read public versions dataset");
    assert!(versions_json.contains("\"failure_class\":\"failed\""));
    assert!(versions_json.contains("\"reason\":\"discovery_failed\""));
    assert!(!versions_json.contains(&private_path));
    assert!(!versions_json.contains(private_token));
    assert!(!versions_json.contains(private_windows_path));
    assert!(!versions_json.contains(private_file_uri));

    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/xlsynth-bvc/".into(),
        overwrite: false,
    })
    .expect("build site");
    verify_static_site(&site_dir).expect("verify site");
    for entry in WalkDir::new(&site_dir) {
        let entry = entry.expect("walk public site");
        if !entry.file_type().is_file()
            || !matches!(
                entry.path().extension().and_then(|value| value.to_str()),
                Some("json" | "html")
            )
        {
            continue;
        }
        let text = fs::read_to_string(entry.path()).expect("read public text file");
        assert!(!text.contains(&private_path), "{}", entry.path().display());
        assert!(!text.contains(private_token), "{}", entry.path().display());
        assert!(
            !text.contains(private_windows_path),
            "{}",
            entry.path().display()
        );
        assert!(
            !text.contains(private_file_uri),
            "{}",
            entry.path().display()
        );
    }
    drop(store);
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_detects_tamper() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");
    fs::write(site_dir.join("index.html"), "tampered").expect("tamper");
    assert!(verify_static_site(&site_dir).is_err());
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_rejects_self_consistent_script_and_unknown_catalog_field() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");

    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let original_manifest = fs::read(&manifest_path).expect("read original manifest");
    let index_path = site_dir.join("index.html");
    let original_index = fs::read_to_string(&index_path).expect("read index");
    fs::write(
        &index_path,
        original_index.replace(
            "</body>",
            "<script src=\"https://attacker.example/payload.js\"></script></body>",
        ),
    )
    .expect("inject external script");
    refresh_site_manifest_entry(&site_dir, "index.html");
    let error = verify_static_site(&site_dir)
        .expect_err("self-consistent external script must fail verification");
    assert!(
        format!("{error:#}").contains("deterministic rendering"),
        "unexpected error: {error:#}"
    );

    fs::write(&index_path, original_index).expect("restore index");
    fs::write(&manifest_path, &original_manifest).expect("restore manifest");
    let catalog_path = site_dir.join("catalog.json");
    let mut catalog: serde_json::Value =
        serde_json::from_slice(&fs::read(&catalog_path).expect("read catalog"))
            .expect("decode catalog value");
    catalog.as_object_mut().expect("catalog object").insert(
        "private_path".to_string(),
        serde_json::Value::String("/srv/build/private".to_string()),
    );
    fs::write(
        &catalog_path,
        serde_json::to_vec_pretty(&catalog).expect("encode modified catalog"),
    )
    .expect("write modified catalog");
    refresh_site_manifest_entry(&site_dir, "catalog.json");
    let error = verify_static_site(&site_dir)
        .expect_err("self-consistent unknown catalog field must fail verification");
    assert!(
        format!("{error:#}").contains("unknown field `private_path`"),
        "unexpected error: {error:#}"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_output_rejects_protected_root_ancestors_and_descendants() {
    let root = temp_root();
    let snapshot_dir = root.join("snapshot");
    fs::create_dir_all(&snapshot_dir).expect("create snapshot placeholder");
    for label in [
        "resource checkout",
        "private store",
        "artifact backend storage",
    ] {
        let container = root.join(label.replace(' ', "-"));
        let protected = container.join("protected");
        fs::create_dir_all(&protected).expect("create protected root");

        for output in [container.clone(), protected.join("generated-site")] {
            let error =
                reject_site_output_overlap(&output, &snapshot_dir, &[(label, protected.as_path())])
                    .expect_err("bidirectional protected-root overlap must fail");
            assert!(
                format!("{error:#}").contains(&format!("must not overlap {label}")),
                "unexpected error for {label}: {error:#}"
            );
        }
    }
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_overwrite_rejects_snapshot_ancestor_before_deletion() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let marker = snapshot_dir.join(STATIC_SNAPSHOT_MANIFEST_FILENAME);

    let error = build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: root.clone(),
        base_url: "/".into(),
        overwrite: true,
    })
    .expect_err("snapshot ancestor output must fail");
    assert!(format!("{error:#}").contains("must not overlap source snapshot"));
    assert!(marker.is_file(), "source snapshot must survive rejection");
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_rejects_symlinks() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");
    std::os::unix::fs::symlink("/tmp", site_dir.join("private-link")).expect("create symlink");
    let error = verify_static_site(&site_dir).expect_err("symlink must fail");
    assert!(format!("{error:#}").contains("symlink or special"));
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_binds_dataset_bytes_to_source_snapshot() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");

    let dataset_relpath = format!("data/{}", crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME);
    let dataset_path = site_dir.join(&dataset_relpath);
    let original = fs::read_to_string(&dataset_path).expect("read dataset");
    let changed = original.replacen("1970-01-01T00:00:00Z", "1971-01-01T00:00:00Z", 1);
    assert_ne!(original, changed);
    fs::write(&dataset_path, changed.as_bytes()).expect("rewrite dataset");

    let catalog_path = site_dir.join("catalog.json");
    let mut catalog: BrowserCatalog =
        serde_json::from_slice(&fs::read(&catalog_path).expect("read catalog"))
            .expect("decode catalog");
    let dataset = catalog
        .datasets
        .iter_mut()
        .find(|dataset| dataset.url == dataset_relpath)
        .expect("catalog dataset");
    dataset.bytes = changed.len() as u64;
    dataset.sha256 = sha256_hex(changed.as_bytes());
    fs::write(
        &catalog_path,
        encode_browser_catalog(&catalog).expect("encode canonical catalog"),
    )
    .expect("rewrite catalog");
    let embedded_snapshot = load_static_snapshot_manifest(&site_dir)
        .expect("load embedded snapshot for fixed-page regeneration");
    for (relpath, bytes) in
        expected_fixed_site_files(&catalog, &embedded_snapshot).expect("render fixed files")
    {
        fs::write(site_dir.join(relpath), bytes).expect("rewrite fixed site file");
    }

    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let mut manifest = pb::StaticSiteManifest::decode(
        fs::read(&manifest_path)
            .expect("read site manifest")
            .as_slice(),
    )
    .expect("decode site manifest");
    for slot in &mut manifest.files {
        let relpath = slot
            .relpath
            .as_ref()
            .expect("declared relpath")
            .value
            .clone();
        *slot = publication_file(&site_dir, &relpath).expect("describe changed site file");
    }
    fs::write(&manifest_path, manifest.encode_to_vec()).expect("rewrite site manifest");

    let error =
        verify_static_site(&site_dir).expect_err("site data must remain bound to source snapshot");
    assert!(
        format!("{error:#}").contains("does not match embedded source snapshot"),
        "unexpected error: {error:#}"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_rejects_self_consistent_non_allowlisted_files() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");

    let private_files: [(&str, &[u8]); 3] = [
        (
            "data/internal-build-metadata.v1.json",
            br#"{"private_path":"/srv/build/secrets"}"#,
        ),
        ("data/private-provenance.pb", b"private protobuf bytes"),
        ("worker.log", b"private worker output"),
    ];
    for (relpath, bytes) in private_files {
        let path = site_dir.join(relpath);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create private file parent");
        }
        fs::write(path, bytes).expect("write non-allowlisted site file");
    }
    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let mut manifest = pb::StaticSiteManifest::decode(
        fs::read(&manifest_path)
            .expect("read site manifest")
            .as_slice(),
    )
    .expect("decode site manifest");
    for (relpath, _) in private_files {
        manifest
            .files
            .push(publication_file(&site_dir, relpath).expect("describe private file"));
    }
    fs::write(&manifest_path, manifest.encode_to_vec()).expect("rewrite site manifest");

    let error = verify_static_site(&site_dir)
        .expect_err("self-consistent non-allowlisted files must fail verification");
    assert!(
        format!("{error:#}").contains("allowlisted topology"),
        "unexpected error: {error:#}"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn empty_stdlib_evidence_is_degraded_and_rendered_as_verified_static_run_page() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    let repo_root = std::env::current_dir().expect("current dir");
    let crate_version = load_version_compat_map(&repo_root)
        .expect("compat map")
        .into_keys()
        .next()
        .expect("known crate version");
    let dso_version =
        resolve_xlsynth_version_for_driver(&repo_root, &crate_version).expect("dso version");
    for action in canonical_root_actions_for_crate_version(&repo_root, &crate_version, &dso_version)
        .expect("root actions")
    {
        let action_id = compute_action_id(&action).expect("action id");
        let details = match &action {
            crate::model::ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. } => {
                json!({"download": {}})
            }
            crate::model::ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
                subtree, ..
            } => json!({"download": {}, "subtree": subtree, "extracted_file_count": 1}),
            _ => unreachable!(),
        };
        store
            .write_provenance(&Provenance {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id: action_id.clone(),
                created_utc: Utc::now(),
                action,
                dependencies: Vec::new(),
                output_artifact: ArtifactRef {
                    action_id,
                    artifact_type: ArtifactType::DslxFileSubtree,
                    relpath: "payload".to_string(),
                },
                output_files: Vec::new(),
                commands: Vec::new(),
                details,
                suggested_next_actions: Vec::new(),
            })
            .expect("write root provenance");
    }
    let generated_utc = Utc::now();
    let versions_json = serde_json::to_vec(&json!({
        "schema_version": crate::WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION,
        "generated_utc": generated_utc,
        "report": {
            "cards": [{
                "crate_version": crate_version,
                "crate_release_datetime": null,
                "total_materialized": 1,
                "failed_total": 0,
                "dso_versions": [dso_version],
                "stdlib_enumeration": {
                    "state": "ok",
                    "reason": "discovery_counts",
                    "scanned_files": 1,
                    "failed_files": 0,
                    "concrete_functions": 1,
                    "suggested_actions": 1
                },
                "failed_by_kind": [],
                "failures": []
            }],
            "unattributed_actions": []
        }
    }))
    .expect("serialize versions dataset");
    store
        .write_web_index_bytes(crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME, &versions_json)
        .expect("versions dataset");
    let comparison_json = serde_json::to_vec(&json!({
        "schema_version": crate::WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
        "generated_utc": generated_utc,
        "dataset": {
            "fraig": false,
            "samples": [],
            "min_ir_nodes": 0,
            "max_ir_nodes": 0,
            "g8r_only_count": 0,
            "yosys_only_count": 0,
            "available_crate_versions": [crate_version]
        }
    }))
    .expect("serialize comparison dataset");
    store
        .write_web_index_bytes(
            crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME,
            &comparison_json,
        )
        .expect("comparison dataset");
    let finalized =
        finalize_campaign_run(&store, &repo_root, &crate_version).expect("finalize campaign");
    assert_eq!(finalized.status, "degraded");
    assert!(
        finalized
            .missing_outputs
            .iter()
            .any(|reason| reason.contains("stdlib enumeration is absent"))
    );
    assert!(
        finalized
            .missing_outputs
            .iter()
            .any(|reason| reason.contains("declared stdlib-root lineage"))
    );

    let snapshot_dir = root.join("snapshot-with-run");
    build_static_snapshot(
        &store,
        &repo_root,
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("snapshot");
    let site_dir = root.join("site-with-run");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/xlsynth-bvc/".to_string(),
        overwrite: false,
    })
    .expect("site");
    verify_static_site(&site_dir).expect("verify run site");
    assert!(site_dir.join("runs.html").exists());
    assert!(site_dir.join("progression.html").exists());
    assert!(site_dir.join("mffc-discrepancies.html").exists());
    assert!(
        site_dir
            .join("runs")
            .join(finalized.run_id)
            .join("index.html")
            .exists()
    );
    drop(store);
    fs::remove_dir_all(root).expect("cleanup");
}
