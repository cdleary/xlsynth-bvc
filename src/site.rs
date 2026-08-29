// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use prost::Message;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::analysis::decode_analysis_report;
use crate::proto::v1 as pb;
use crate::snapshot::{
    STATIC_SNAPSHOT_MANIFEST_FILENAME, load_static_snapshot_manifest, verify_static_snapshot,
};

pub(crate) const STATIC_SITE_RECORD_VERSION: u32 = 1;
pub(crate) const STATIC_SITE_MANIFEST_FILENAME: &str = "site_manifest.v1.pb";

const STYLE_CSS: &str = r#":root{color-scheme:light dark;--bg:#0d1117;--panel:#161b22;--text:#e6edf3;--muted:#8b949e;--accent:#58a6ff;--line:#30363d}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace}header,main{max-width:1180px;margin:auto;padding:24px}header{border-bottom:1px solid var(--line)}a{color:var(--accent)}h1,h2{font-family:ui-sans-serif,system-ui,sans-serif}.meta,.muted{color:var(--muted)}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px}.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px}.card code{overflow-wrap:anywhere}.toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:16px 0}select,input{font:inherit;padding:7px;background:var(--panel);color:var(--text);border:1px solid var(--line);border-radius:6px}pre{max-height:62vh;overflow:auto;background:#010409;padding:14px;border:1px solid var(--line);border-radius:8px}table{border-collapse:collapse;width:100%;font-size:12px}th,td{border:1px solid var(--line);padding:6px;text-align:left;vertical-align:top}th{position:sticky;top:0;background:var(--panel)}.table-wrap{max-height:60vh;overflow:auto}svg{width:100%;height:300px;background:var(--panel);border:1px solid var(--line)}"#;

const APP_JS: &str = r#"const base=document.querySelector('meta[name=bvc-site-root]').content;
const byId=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>\"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[c]));
function arrays(v,out=[],path='$'){if(Array.isArray(v)&&v.length&&typeof v[0]==='object')out.push([path,v]);else if(v&&typeof v==='object')for(const[k,x]of Object.entries(v))arrays(x,out,`${path}.${k}`);return out}
function renderTable(rows){if(!rows.length)return '<p class=muted>No row arrays found.</p>';const keys=[...new Set(rows.slice(0,200).flatMap(Object.keys))].slice(0,24);return `<div class=table-wrap><table><thead><tr>${keys.map(k=>`<th>${esc(k)}</th>`).join('')}</tr></thead><tbody>${rows.slice(0,500).map(r=>`<tr>${keys.map(k=>`<td>${esc(typeof r[k]==='object'?JSON.stringify(r[k]):r[k]??'')}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`}
function renderPlot(rows){const numeric=[...new Set(rows.slice(0,100).flatMap(r=>Object.keys(r).filter(k=>typeof r[k]==='number')))];if(numeric.length<2)return '';const[xk,yk]=numeric;const pts=rows.filter(r=>Number.isFinite(r[xk])&&Number.isFinite(r[yk])).slice(0,1000);if(!pts.length)return '';const xs=pts.map(r=>r[xk]),ys=pts.map(r=>r[yk]),xmin=Math.min(...xs),xmax=Math.max(...xs),ymin=Math.min(...ys),ymax=Math.max(...ys);const sx=x=>25+550*(x-xmin)/(xmax-xmin||1),sy=y=>275-250*(y-ymin)/(ymax-ymin||1);return `<h2>Numeric preview: ${esc(xk)} vs ${esc(yk)}</h2><svg viewBox='0 0 600 300' role=img aria-label='numeric dataset preview'>${pts.map(r=>`<circle cx='${sx(r[xk])}' cy='${sy(r[yk])}' r='2.5' fill='#58a6ff'/>`).join('')}<text x='300' y='296' fill='#8b949e' text-anchor='middle'>${esc(xk)}</text><text x='8' y='150' fill='#8b949e'>${esc(yk)}</text></svg>`}
async function main(){const catalog=await fetch(base+'catalog.json').then(r=>{if(!r.ok)throw Error(`catalog ${r.status}`);return r.json()});const select=byId('dataset');if(!select)return;for(const d of catalog.datasets){const o=document.createElement('option');o.value=d.logical_key;o.textContent=`${d.logical_key} (${d.bytes.toLocaleString()} B)`;select.appendChild(o)}const q=new URLSearchParams(location.search).get('key');if(q&&catalog.datasets.some(d=>d.logical_key===q))select.value=q;async function load(){const d=catalog.datasets.find(x=>x.logical_key===select.value);history.replaceState(null,'','?key='+encodeURIComponent(d.logical_key));byId('dataset-meta').textContent=`sha256 ${d.sha256} · ${d.bytes.toLocaleString()} bytes`;const data=await fetch(base+d.url).then(r=>{if(!r.ok)throw Error(`${d.url} ${r.status}`);return r.json()});const found=arrays(data);const rows=found[0]?.[1]||[];byId('plot').innerHTML=renderPlot(rows);byId('table').innerHTML=found.length?`<h2>Rows: ${esc(found[0][0])}</h2>${renderTable(rows)}`:'<p class=muted>No tabular row arrays found.</p>';byId('raw').textContent=JSON.stringify(data,null,2)}select.addEventListener('change',load);if(catalog.datasets.length)await load()}main().catch(e=>{const out=byId('error');if(out)out.textContent=e.stack||e});"#;

#[derive(Debug, Clone)]
pub(crate) struct BuildStaticSiteOptions {
    pub(crate) snapshot_dir: PathBuf,
    pub(crate) out_dir: PathBuf,
    pub(crate) base_url: String,
    pub(crate) overwrite: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BuildStaticSiteSummary {
    pub(crate) out_dir: String,
    pub(crate) snapshot_id: String,
    pub(crate) base_url: String,
    pub(crate) dataset_count: usize,
    pub(crate) file_count: usize,
    pub(crate) total_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VerifyStaticSiteSummary {
    pub(crate) site_dir: String,
    pub(crate) snapshot_id: String,
    pub(crate) base_url: String,
    pub(crate) file_count: usize,
    pub(crate) total_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SmokeStaticSiteSummary {
    pub(crate) site_dir: String,
    pub(crate) base_url: String,
    pub(crate) browser: String,
    pub(crate) pages_checked: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrowserCatalog {
    schema_version: u32,
    snapshot_id: String,
    base_url: String,
    datasets: Vec<BrowserDataset>,
    runs: Vec<BrowserRun>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrowserDataset {
    logical_key: String,
    url: String,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrowserRun {
    campaign_id: String,
    run_id: String,
    campaign_name: String,
    campaign_semantic_version: u32,
    crate_version: String,
    dso_version: String,
    status: String,
    updated_utc: String,
    root_action_ids: Vec<String>,
    completed_root_count: u64,
    failed_count: u64,
    canceled_count: u64,
    missing_output_count: u64,
    failed_sample_count: u64,
    protobuf_url: String,
    page_url: String,
    findings_protobuf_url: Option<String>,
    findings: Vec<BrowserFinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrowserFinding {
    finding_id: String,
    kind: String,
    subject_key: String,
    metric_name: String,
    baseline_value: Option<f64>,
    current_value: Option<f64>,
    unit: String,
    structural_hash: Option<String>,
    evidence_action_ids: Vec<String>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn normalize_base_url(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || !value.starts_with('/') {
        bail!("base URL must begin with '/': {value:?}");
    }
    if value.contains('?') || value.contains('#') || value.contains('\\') {
        bail!("base URL must be a path without query, fragment, or backslash");
    }
    if value.split('/').any(|part| part == "." || part == "..") {
        bail!("base URL must not contain '.' or '..' path components");
    }
    Ok(if value.ends_with('/') {
        value.to_string()
    } else {
        format!("{value}/")
    })
}

fn normalized_relpath(value: &str) -> Result<String> {
    if value.is_empty() || value.starts_with('/') {
        bail!("site relpath must be nonempty and relative: {value:?}");
    }
    let mut parts = Vec::new();
    for component in Path::new(value).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("site relpath is not normalized: {value:?}")
            }
        }
    }
    let normalized = parts.join("/");
    if normalized != value.replace('\\', "/") {
        bail!("site relpath is not normalized: {value:?}");
    }
    Ok(normalized)
}

fn ensure_empty_output_dir(path: &Path, overwrite: bool) -> Result<()> {
    if path.exists() {
        if !overwrite {
            bail!(
                "static site output directory already exists; rerun with --overwrite: {}",
                path.display()
            );
        }
        fs::remove_dir_all(path)
            .with_context(|| format!("removing existing static site: {}", path.display()))?;
    }
    fs::create_dir_all(path)
        .with_context(|| format!("creating static site directory: {}", path.display()))
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn html_shell(
    title: &str,
    site_root_url: &str,
    body: &str,
    css_name: &str,
    js_name: Option<&str>,
) -> String {
    let script = js_name
        .map(|name| format!(r#"<script defer src="{site_root_url}assets/{name}"></script>"#))
        .unwrap_or_default();
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta name=\"bvc-site-root\" content=\"{site_root_url}\"><title>{}</title><link rel=\"stylesheet\" href=\"{site_root_url}assets/{css_name}\">{script}</head><body>{body}</body></html>",
        escape_html(title)
    )
}

fn site_root_url(page_relpath: &str) -> Result<String> {
    let page_relpath = normalized_relpath(page_relpath)?;
    if !page_relpath.ends_with(".html") {
        bail!("site page relpath must end in .html: {page_relpath}");
    }
    let depth = Path::new(&page_relpath)
        .parent()
        .into_iter()
        .flat_map(Path::components)
        .filter(|component| matches!(component, Component::Normal(_)))
        .count();
    Ok(if depth == 0 {
        "./".to_string()
    } else {
        "../".repeat(depth)
    })
}

fn resolve_site_link(page_relpath: &str, url: &str) -> Result<String> {
    if url.starts_with('/') {
        bail!("site link must be relative so publication is relocatable: {url}");
    }
    let path = url.split(['?', '#']).next().unwrap_or("");
    let page_dir = Path::new(page_relpath).parent().unwrap_or(Path::new(""));
    let joined = page_dir.join(path);
    let mut parts = Vec::new();
    for component in joined.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::CurDir => {}
            Component::ParentDir => {
                if parts.pop().is_none() {
                    bail!("site link escapes the immutable site root: {page_relpath} -> {url}");
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("site link must be relative: {page_relpath} -> {url}")
            }
        }
    }
    if path.ends_with('/') || parts.is_empty() {
        parts.push("index.html".to_string());
    }
    Ok(parts.join("/"))
}

fn media_type(relpath: &str) -> &'static str {
    if relpath.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if relpath.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if relpath.ends_with(".js") {
        "text/javascript; charset=utf-8"
    } else if relpath.ends_with(".json") {
        "application/json"
    } else if relpath.ends_with(".pb") {
        "application/x-protobuf"
    } else {
        "application/octet-stream"
    }
}

fn publication_file(out_dir: &Path, relpath: &str) -> Result<pb::PublicationFile> {
    let relpath = normalized_relpath(relpath)?;
    let bytes = fs::read(out_dir.join(&relpath))
        .with_context(|| format!("reading generated site file: {relpath}"))?;
    Ok(pb::PublicationFile {
        logical_key: relpath.clone(),
        relpath: Some(pb::NormalizedRelpath {
            value: relpath.clone(),
        }),
        bytes: bytes.len() as u64,
        sha256: Some(pb::Sha256Digest {
            value: hex::decode(sha256_hex(&bytes)).expect("sha256 hex decodes"),
        }),
        media_type: media_type(&relpath).to_string(),
    })
}

fn write_file(out_dir: &Path, relpath: &str, bytes: &[u8]) -> Result<()> {
    let relpath = normalized_relpath(relpath)?;
    let path = out_dir.join(relpath);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating static site parent: {}", parent.display()))?;
    }
    fs::write(&path, bytes).with_context(|| format!("writing static site file: {}", path.display()))
}

pub(crate) fn build_static_site(
    options: &BuildStaticSiteOptions,
) -> Result<BuildStaticSiteSummary> {
    verify_static_snapshot(&options.snapshot_dir).context("verifying source snapshot")?;
    let snapshot = load_static_snapshot_manifest(&options.snapshot_dir)?;
    let base_url = normalize_base_url(&options.base_url)?;
    let root_site_url = site_root_url("index.html")?;
    ensure_empty_output_dir(&options.out_dir, options.overwrite)?;

    let css_hash = &sha256_hex(STYLE_CSS.as_bytes())[..16];
    let js_hash = &sha256_hex(APP_JS.as_bytes())[..16];
    let css_name = format!("site-{css_hash}.css");
    let js_name = format!("explorer-{js_hash}.js");
    write_file(
        &options.out_dir,
        &format!("assets/{css_name}"),
        STYLE_CSS.as_bytes(),
    )?;
    write_file(
        &options.out_dir,
        &format!("assets/{js_name}"),
        APP_JS.as_bytes(),
    )?;

    let mut datasets = Vec::new();
    for entry in &snapshot.dataset_files {
        if !entry.relpath.ends_with(".json") {
            continue;
        }
        let source = options.snapshot_dir.join(&entry.relpath);
        let suffix = entry
            .relpath
            .strip_prefix("web_index/")
            .unwrap_or(&entry.relpath);
        let target_relpath = format!("data/{suffix}");
        let bytes = fs::read(&source)
            .with_context(|| format!("reading snapshot dataset: {}", source.display()))?;
        if sha256_hex(&bytes) != entry.sha256 {
            bail!(
                "snapshot dataset changed after verification: {}",
                entry.relpath
            );
        }
        write_file(&options.out_dir, &target_relpath, &bytes)?;
        datasets.push(BrowserDataset {
            logical_key: entry.index_key.clone(),
            url: target_relpath,
            bytes: entry.bytes,
            sha256: entry.sha256.clone(),
        });
    }
    datasets.sort_by(|a, b| a.logical_key.cmp(&b.logical_key));
    let mut runs = Vec::new();
    for entry in &snapshot.dataset_files {
        if !entry.index_key.starts_with("runs/") || !entry.relpath.ends_with("/run.pb") {
            continue;
        }
        let source = options.snapshot_dir.join(&entry.relpath);
        let bytes = fs::read(&source)
            .with_context(|| format!("reading public run protobuf: {}", source.display()))?;
        if sha256_hex(&bytes) != entry.sha256 {
            bail!(
                "public run changed after snapshot verification: {}",
                entry.relpath
            );
        }
        let target_relpath = format!("data/{}", entry.relpath);
        write_file(&options.out_dir, &target_relpath, &bytes)?;
        let run = pb::PublicCampaignRun::decode(bytes.as_slice())
            .with_context(|| format!("decoding public run protobuf: {}", entry.relpath))?;
        runs.push(public_run_to_browser(&run, target_relpath)?);
    }
    runs.sort_by(|a, b| {
        a.crate_version
            .cmp(&b.crate_version)
            .then(a.run_id.cmp(&b.run_id))
    });
    for entry in &snapshot.dataset_files {
        if !entry.index_key.starts_with("runs/") || !entry.relpath.ends_with("/findings.pb") {
            continue;
        }
        let source = options.snapshot_dir.join(&entry.relpath);
        let bytes = fs::read(&source)
            .with_context(|| format!("reading findings protobuf: {}", source.display()))?;
        if sha256_hex(&bytes) != entry.sha256 {
            bail!(
                "findings changed after snapshot verification: {}",
                entry.relpath
            );
        }
        let target_relpath = format!("data/{}", entry.relpath);
        write_file(&options.out_dir, &target_relpath, &bytes)?;
        let report = decode_analysis_report(&bytes)?;
        let (run_id, findings) = analysis_to_browser(&report)?;
        let run = runs
            .iter_mut()
            .find(|run| run.run_id == run_id)
            .with_context(|| format!("findings reference unpublished run {run_id}"))?;
        run.findings_protobuf_url = Some(target_relpath);
        run.findings = findings;
    }
    let catalog = BrowserCatalog {
        schema_version: 1,
        snapshot_id: snapshot.snapshot_id.clone(),
        base_url: base_url.clone(),
        datasets,
        runs,
    };
    write_file(
        &options.out_dir,
        "catalog.json",
        &serde_json::to_vec_pretty(&catalog).context("serializing browser catalog")?,
    )?;
    fs::copy(
        options.snapshot_dir.join(STATIC_SNAPSHOT_MANIFEST_FILENAME),
        options.out_dir.join("snapshot_manifest.v1.pb"),
    )
    .context("copying source snapshot manifest into site")?;

    for run in &catalog.runs {
        let page_relpath = format!("runs/{}/index.html", run.run_id);
        let run_site_root_url = site_root_url(&page_relpath)?;
        let root_actions = run
            .root_action_ids
            .iter()
            .map(|id| format!("<li><code>{}</code></li>", escape_html(id)))
            .collect::<String>();
        let finding_rows = run
            .findings
            .iter()
            .map(|finding| {
                let baseline = finding
                    .baseline_value
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_else(|| "—".to_string());
                let current = finding
                    .current_value
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_else(|| "—".to_string());
                let structural = finding
                    .structural_hash
                    .as_deref()
                    .map(escape_html)
                    .unwrap_or_else(|| "—".to_string());
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{baseline}</td><td>{current}</td><td><code>{structural}</code></td><td>{}</td></tr>",
                    escape_html(&finding.kind),
                    escape_html(&finding.subject_key),
                    finding.evidence_action_ids.len(),
                )
            })
            .collect::<String>();
        let findings_download = run
            .findings_protobuf_url
            .as_deref()
            .map(|url| {
                format!(
                    "<p><a href=\"{run_site_root_url}{url}\">Download canonical findings protobuf</a></p>"
                )
            })
            .unwrap_or_default();
        let body = format!(
            "<header><p><a href=\"{run_site_root_url}runs.html\">← Runs</a></p><h1>{} crate v{}</h1><p class=\"meta\">Campaign {} v{} · DSO v{} · status <strong>{}</strong></p></header><main><div class=\"grid\"><article class=\"card\"><h2>Completion</h2><p>{} roots complete · {} failed · {} canceled</p><p>{} missing outputs · {} failed samples</p></article><article class=\"card\"><h2>Identity</h2><p>Run <code>{}</code></p><p>Campaign <code>{}</code></p><p><a href=\"{run_site_root_url}{}\">Download public run protobuf</a></p>{findings_download}</article></div><h2>Findings</h2><div class=\"table-wrap\"><table><thead><tr><th>Kind</th><th>Subject</th><th>Baseline loss</th><th>Current loss</th><th>Structural hash</th><th>Evidence actions</th></tr></thead><tbody>{finding_rows}</tbody></table></div><h2>Root actions</h2><ul>{root_actions}</ul><h2>Results</h2><p><a href=\"{run_site_root_url}dataset.html?key={}\">Open g8r versus Yosys/ABC dataset</a></p></main>",
            escape_html(&run.campaign_name),
            escape_html(&run.crate_version),
            escape_html(&run.campaign_name),
            run.campaign_semantic_version,
            escape_html(&run.dso_version),
            escape_html(&run.status),
            run.completed_root_count,
            run.failed_count,
            run.canceled_count,
            run.missing_output_count,
            run.failed_sample_count,
            run.run_id,
            run.campaign_id,
            run.protobuf_url,
            url_encode(crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME),
        );
        write_file(
            &options.out_dir,
            &page_relpath,
            html_shell(
                &format!("{} v{}", run.campaign_name, run.crate_version),
                &run_site_root_url,
                &body,
                &css_name,
                None,
            )
            .as_bytes(),
        )?;
    }

    let run_cards = catalog
        .runs
        .iter()
        .map(|run| {
            format!(
                "<article class=\"card\"><h2><a href=\"{root_site_url}{}\">{} v{}</a></h2><p>Status <strong>{}</strong> · DSO v{}</p><code>{}</code></article>",
                run.page_url,
                escape_html(&run.campaign_name),
                escape_html(&run.crate_version),
                escape_html(&run.status),
                escape_html(&run.dso_version),
                run.run_id,
            )
        })
        .collect::<String>();
    let runs_body = format!(
        "<header><p><a href=\"{root_site_url}\">← Results</a></p><h1>Campaign runs</h1><p class=\"meta\">{} verified public runs</p></header><main><div class=\"grid\">{run_cards}</div></main>",
        catalog.runs.len()
    );
    write_file(
        &options.out_dir,
        "runs.html",
        html_shell(
            "xlsynth-bvc campaign runs",
            &root_site_url,
            &runs_body,
            &css_name,
            None,
        )
        .as_bytes(),
    )?;

    let cards = catalog
        .datasets
        .iter()
        .map(|dataset| {
            format!(
                "<article class=\"card\"><h2><a href=\"{root_site_url}dataset.html?key={}\">{}</a></h2><p>{} bytes</p><code>{}</code></article>",
                url_encode(&dataset.logical_key),
                escape_html(&dataset.logical_key),
                dataset.bytes,
                dataset.sha256
            )
        })
        .collect::<String>();
    let index_body = format!(
        "<header><h1>xlsynth-bvc results</h1><p class=\"meta\">Snapshot <code>{}</code> · {} runs · {} datasets · generated {}</p></header><main><p>This is a self-contained static publication. The build machine and sled database are not involved at request time.</p><p><a href=\"{root_site_url}runs.html\">Browse campaign runs and versions →</a></p><h2>Datasets</h2><div class=\"grid\">{cards}</div></main>",
        snapshot.snapshot_id,
        catalog.runs.len(),
        catalog.datasets.len(),
        snapshot.generated_utc.to_rfc3339()
    );
    write_file(
        &options.out_dir,
        "index.html",
        html_shell(
            "xlsynth-bvc results",
            &root_site_url,
            &index_body,
            &css_name,
            None,
        )
        .as_bytes(),
    )?;
    let explorer_body = format!(
        "<header><p><a href=\"{root_site_url}\">← Results</a></p><h1>Dataset explorer</h1><div class=\"toolbar\"><label>Dataset <select id=\"dataset\"></select></label><span id=\"dataset-meta\" class=\"meta\"></span></div><p id=\"error\"></p></header><main><section id=\"plot\"></section><section id=\"table\"></section><h2>Raw JSON</h2><pre id=\"raw\">Loading…</pre></main>"
    );
    write_file(
        &options.out_dir,
        "dataset.html",
        html_shell(
            "xlsynth-bvc dataset explorer",
            &root_site_url,
            &explorer_body,
            &css_name,
            Some(&js_name),
        )
        .as_bytes(),
    )?;

    let mut relpaths = Vec::new();
    for entry in WalkDir::new(&options.out_dir).sort_by_file_name() {
        let entry = entry.context("walking generated site")?;
        if entry.file_type().is_file() {
            let relpath = entry
                .path()
                .strip_prefix(&options.out_dir)
                .context("stripping site root")?
                .to_string_lossy()
                .replace('\\', "/");
            if relpath != STATIC_SITE_MANIFEST_FILENAME {
                relpaths.push(relpath);
            }
        }
    }
    relpaths.sort();
    let files = relpaths
        .iter()
        .map(|path| publication_file(&options.out_dir, path))
        .collect::<Result<Vec<_>>>()?;
    let total_bytes = files.iter().map(|file| file.bytes).sum();
    let manifest = pb::StaticSiteManifest {
        record_version: STATIC_SITE_RECORD_VERSION,
        source_snapshot_id: Some(pb::Sha256Digest {
            value: hex::decode(&snapshot.snapshot_id).context("decoding snapshot id")?,
        }),
        base_url: base_url.clone(),
        files,
    };
    write_file(
        &options.out_dir,
        STATIC_SITE_MANIFEST_FILENAME,
        &manifest.encode_to_vec(),
    )?;

    Ok(BuildStaticSiteSummary {
        out_dir: options.out_dir.display().to_string(),
        snapshot_id: snapshot.snapshot_id,
        base_url,
        dataset_count: catalog.datasets.len(),
        file_count: manifest.files.len(),
        total_bytes,
    })
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn digest_hex(value: &Option<pb::Sha256Digest>, field: &str) -> Result<String> {
    let value = value.as_ref().with_context(|| format!("missing {field}"))?;
    if value.value.len() != 32 {
        bail!("{field} must contain exactly 32 bytes");
    }
    Ok(hex::encode(&value.value))
}

fn public_run_to_browser(run: &pb::PublicCampaignRun, protobuf_url: String) -> Result<BrowserRun> {
    if run.record_version != 1 {
        bail!(
            "unsupported public run record version {}",
            run.record_version
        );
    }
    let run_id = digest_hex(&run.run_id, "public_run.run_id")?;
    let campaign_id = digest_hex(&run.campaign_id, "public_run.campaign_id")?;
    let status =
        pb::CampaignRunStatus::try_from(run.status).context("public run status is unknown")?;
    if !matches!(
        status,
        pb::CampaignRunStatus::Complete | pb::CampaignRunStatus::Degraded
    ) {
        bail!("static site only accepts complete or degraded public runs");
    }
    let updated = run
        .updated_at
        .as_ref()
        .context("public run missing updated_at")?;
    let updated = chrono::DateTime::from_timestamp(updated.seconds, updated.nanos as u32)
        .context("public run updated_at is invalid")?;
    let root_action_ids = run
        .root_action_ids
        .iter()
        .map(|id| {
            if id.value.len() != 32 {
                bail!("public run root action id must contain 32 bytes");
            }
            Ok(hex::encode(&id.value))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BrowserRun {
        campaign_id,
        run_id: run_id.clone(),
        campaign_name: run.campaign_name.clone(),
        campaign_semantic_version: run.campaign_semantic_version,
        crate_version: run
            .crate_version
            .as_ref()
            .context("public run missing crate_version")?
            .value
            .clone(),
        dso_version: run
            .dso_version
            .as_ref()
            .context("public run missing dso_version")?
            .value
            .clone(),
        status: status
            .as_str_name()
            .trim_start_matches("CAMPAIGN_RUN_STATUS_")
            .to_ascii_lowercase(),
        updated_utc: updated.to_rfc3339(),
        root_action_ids,
        completed_root_count: run.completed_root_count,
        failed_count: run.failed_count,
        canceled_count: run.canceled_count,
        missing_output_count: run.missing_output_count,
        failed_sample_count: run.failed_sample_count,
        protobuf_url,
        page_url: format!("runs/{run_id}/"),
        findings_protobuf_url: None,
        findings: Vec::new(),
    })
}

fn analysis_to_browser(report: &pb::AnalysisReport) -> Result<(String, Vec<BrowserFinding>)> {
    let run_id = digest_hex(&report.run_id, "analysis.run_id")?;
    let mut findings = Vec::with_capacity(report.findings.len());
    for finding in &report.findings {
        let identity = finding
            .identity
            .as_ref()
            .context("analysis finding missing identity")?;
        let metric = identity
            .metric
            .as_ref()
            .context("analysis finding missing metric")?;
        let kind =
            pb::FindingKind::try_from(identity.kind).context("analysis finding kind is unknown")?;
        let structural_hash = finding
            .structural_hash
            .as_ref()
            .map(|digest| {
                if digest.value.len() != 32 {
                    bail!("analysis structural hash must contain 32 bytes");
                }
                Ok(hex::encode(&digest.value))
            })
            .transpose()?;
        let evidence_action_ids = finding
            .evidence
            .iter()
            .map(|evidence| {
                let id = evidence
                    .action_id
                    .as_ref()
                    .context("analysis evidence missing action_id")?;
                if id.value.len() != 32 {
                    bail!("analysis evidence action id must contain 32 bytes");
                }
                Ok(hex::encode(&id.value))
            })
            .collect::<Result<Vec<_>>>()?;
        findings.push(BrowserFinding {
            finding_id: digest_hex(&finding.finding_id, "finding.finding_id")?,
            kind: kind
                .as_str_name()
                .trim_start_matches("FINDING_KIND_")
                .to_ascii_lowercase(),
            subject_key: identity.subject_key.clone(),
            metric_name: metric.name.clone(),
            baseline_value: metric
                .baseline_microunits
                .map(|value| value as f64 / 1_000_000.0),
            current_value: metric
                .current_microunits
                .map(|value| value as f64 / 1_000_000.0),
            unit: metric.unit.clone(),
            structural_hash,
            evidence_action_ids,
        });
    }
    Ok((run_id, findings))
}

pub(crate) fn verify_static_site(site_dir: &Path) -> Result<VerifyStaticSiteSummary> {
    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let manifest = pb::StaticSiteManifest::decode(
        fs::read(&manifest_path)
            .with_context(|| format!("reading site manifest: {}", manifest_path.display()))?
            .as_slice(),
    )
    .context("decoding protobuf site manifest")?;
    if manifest.record_version != STATIC_SITE_RECORD_VERSION {
        bail!(
            "unsupported site manifest version {}; expected {}",
            manifest.record_version,
            STATIC_SITE_RECORD_VERSION
        );
    }
    let base_url = normalize_base_url(&manifest.base_url)?;
    if base_url != manifest.base_url {
        bail!("site manifest base_url is not normalized");
    }
    let snapshot_id = digest_hex(&manifest.source_snapshot_id, "source_snapshot_id")?;
    let mut declared = BTreeMap::new();
    let mut total_bytes = 0_u64;
    for file in &manifest.files {
        let relpath = file
            .relpath
            .as_ref()
            .context("site file missing relpath")?
            .value
            .clone();
        normalized_relpath(&relpath)?;
        if file.logical_key != relpath {
            bail!("site file logical_key must equal relpath: {relpath}");
        }
        if file.media_type != media_type(&relpath) {
            bail!("site file media type mismatch: {relpath}");
        }
        if declared.insert(relpath.clone(), file).is_some() {
            bail!("duplicate site file relpath: {relpath}");
        }
        let bytes = fs::read(site_dir.join(&relpath))
            .with_context(|| format!("reading declared site file: {relpath}"))?;
        if bytes.len() as u64 != file.bytes {
            bail!("site file size mismatch: {relpath}");
        }
        if sha256_hex(&bytes) != digest_hex(&file.sha256, "site_file.sha256")? {
            bail!("site file sha256 mismatch: {relpath}");
        }
        if matches!(
            media_type(&relpath),
            "text/html; charset=utf-8" | "text/javascript; charset=utf-8"
        ) {
            let text = std::str::from_utf8(&bytes)
                .with_context(|| format!("site text file is not UTF-8: {relpath}"))?;
            if text.contains("/api/") {
                bail!("static site file contains forbidden /api/ request path: {relpath}");
            }
        }
        total_bytes += file.bytes;
    }

    let mut found = BTreeSet::new();
    for entry in WalkDir::new(site_dir).sort_by_file_name() {
        let entry = entry.context("walking site during verification")?;
        if !entry.file_type().is_file() {
            continue;
        }
        let relpath = entry
            .path()
            .strip_prefix(site_dir)
            .context("stripping site root")?
            .to_string_lossy()
            .replace('\\', "/");
        if relpath == STATIC_SITE_MANIFEST_FILENAME {
            continue;
        }
        if !declared.contains_key(&relpath) {
            bail!("site contains undeclared file: {relpath}");
        }
        found.insert(relpath);
    }
    if found.len() != declared.len() {
        let missing: Vec<_> = declared
            .keys()
            .filter(|key| !found.contains(*key))
            .collect();
        bail!("site manifest declares missing files: {missing:?}");
    }

    let attr_re = Regex::new(r#"(?:href|src)=\"([^\"]+)\""#).expect("valid regex");
    for relpath in declared.keys().filter(|path| path.ends_with(".html")) {
        let html = fs::read_to_string(site_dir.join(relpath))?;
        if !html.to_ascii_lowercase().contains("<!doctype html>") {
            bail!("HTML file lacks doctype: {relpath}");
        }
        let expected_site_root = site_root_url(relpath)?;
        let expected_meta = format!("name=\"bvc-site-root\" content=\"{expected_site_root}\"");
        if !html.contains(&expected_meta) {
            bail!("HTML file has wrong site-root metadata in {relpath}");
        }
        for captures in attr_re.captures_iter(&html) {
            let url = &captures[1];
            if url.starts_with("http:") || url.starts_with("https:") || url.starts_with('#') {
                continue;
            }
            let local = resolve_site_link(relpath, url)?;
            if !declared.contains_key(&local) {
                bail!("broken static link in {relpath}: {url} -> {local}");
            }
        }
    }
    let catalog: BrowserCatalog = serde_json::from_slice(&fs::read(site_dir.join("catalog.json"))?)
        .context("decoding browser catalog")?;
    if catalog.snapshot_id != snapshot_id || catalog.base_url != base_url {
        bail!("browser catalog does not match protobuf site manifest");
    }
    for dataset in &catalog.datasets {
        if !declared.contains_key(&dataset.url) {
            bail!(
                "browser catalog references undeclared dataset: {}",
                dataset.url
            );
        }
    }
    if !catalog.runs.windows(2).all(|pair| {
        (&pair[0].crate_version, &pair[0].run_id) < (&pair[1].crate_version, &pair[1].run_id)
    }) {
        bail!("browser catalog runs are not strictly sorted by version and run id");
    }
    for run in &catalog.runs {
        if run.page_url != format!("runs/{}/", run.run_id)
            || !declared.contains_key(&format!("runs/{}/index.html", run.run_id))
            || !declared.contains_key(&run.protobuf_url)
        {
            bail!(
                "browser catalog run paths are missing or inconsistent: {}",
                run.run_id
            );
        }
        let public_bytes = fs::read(site_dir.join(&run.protobuf_url))?;
        let public = pb::PublicCampaignRun::decode(public_bytes.as_slice())
            .context("decoding catalog public run protobuf")?;
        let mut expected = public_run_to_browser(&public, run.protobuf_url.clone())?;
        if let Some(findings_url) = &run.findings_protobuf_url {
            if !declared.contains_key(findings_url) {
                bail!("run findings protobuf is undeclared: {findings_url}");
            }
            let report = decode_analysis_report(&fs::read(site_dir.join(findings_url))?)?;
            let (finding_run_id, findings) = analysis_to_browser(&report)?;
            if finding_run_id != run.run_id {
                bail!("run findings identity mismatch for {}", run.run_id);
            }
            expected.findings_protobuf_url = Some(findings_url.clone());
            expected.findings = findings;
        }
        if serde_json::to_value(&expected)? != serde_json::to_value(run)? {
            bail!(
                "browser run projection disagrees with protobuf for {}",
                run.run_id
            );
        }
    }

    Ok(VerifyStaticSiteSummary {
        site_dir: site_dir.display().to_string(),
        snapshot_id,
        base_url,
        file_count: manifest.files.len(),
        total_bytes,
    })
}

fn browser_path(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        if !path.is_file() {
            bail!("headless browser does not exist: {}", path.display());
        }
        return Ok(path.to_path_buf());
    }
    for candidate in [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ] {
        let path = PathBuf::from(candidate);
        if path.is_file() {
            return Ok(path);
        }
    }
    bail!("no Chrome/Chromium binary found; pass --browser PATH")
}

fn start_plain_static_server(directory: &Path) -> Result<(Child, std::net::SocketAddr)> {
    let port_probe =
        TcpListener::bind(("127.0.0.1", 0)).context("selecting local static smoke HTTP port")?;
    let address = port_probe
        .local_addr()
        .context("reading local static smoke HTTP port")?;
    drop(port_probe);
    let mut child = Command::new("python3")
        .args([
            "-m",
            "http.server",
            &address.port().to_string(),
            "--bind",
            "127.0.0.1",
            "--directory",
        ])
        .arg(directory)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("launching plain Python static HTTP server")?;
    let started = Instant::now();
    loop {
        if TcpStream::connect(address).is_ok() {
            return Ok((child, address));
        }
        if let Some(status) = child
            .try_wait()
            .context("checking plain static HTTP server")?
        {
            bail!("plain static HTTP server exited before startup: {status}");
        }
        if started.elapsed() >= Duration::from_secs(5) {
            let _ = child.kill();
            let _ = child.wait();
            bail!("plain static HTTP server did not start within 5s");
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn run_browser_page(
    browser: &Path,
    profile_dir: &Path,
    url: &str,
    expected_text: &str,
    timeout: Duration,
) -> Result<()> {
    let response = reqwest::blocking::get(url)
        .with_context(|| format!("fetching static smoke page: {url}"))?
        .error_for_status()
        .with_context(|| format!("checking static smoke page status: {url}"))?
        .text()
        .with_context(|| format!("reading static smoke page: {url}"))?;
    if !response.contains(expected_text) {
        bail!("static HTTP response for {url} did not contain {expected_text:?}");
    }
    let screenshot_name = format!("{}.png", &sha256_hex(url.as_bytes())[..16]);
    let screenshot_path = profile_dir.join(screenshot_name);
    let mut child = Command::new(browser)
        .args([
            "--headless=new",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--no-first-run",
            "--disable-features=NetworkServiceSandbox",
            "--disable-background-networking",
            "--disable-component-update",
            "--disable-default-apps",
            "--disable-sync",
            "--window-size=1280,900",
            &format!("--screenshot={}", screenshot_path.display()),
            &format!("--user-data-dir={}", profile_dir.display()),
            url,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("launching headless browser: {}", browser.display()))?;
    let mut stdout = child
        .stdout
        .take()
        .context("capturing headless browser stdout")?;
    let mut stderr = child
        .stderr
        .take()
        .context("capturing headless browser stderr")?;
    let stdout_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stdout.read_to_end(&mut bytes);
        (result, bytes)
    });
    let stderr_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stderr.read_to_end(&mut bytes);
        (result, bytes)
    });
    let started = Instant::now();
    let status = loop {
        if screenshot_path
            .metadata()
            .is_ok_and(|metadata| metadata.len() > 0)
        {
            let _ = child.kill();
            break child
                .wait()
                .context("waiting for rendered headless browser")?;
        }
        if let Some(status) = child.try_wait().context("waiting for headless browser")? {
            break status;
        }
        if started.elapsed() >= timeout {
            child.kill().context("killing timed-out headless browser")?;
            let _ = child.wait();
            bail!(
                "headless browser timed out after {}s for {url}",
                timeout.as_secs()
            );
        }
        thread::sleep(Duration::from_millis(50));
    };
    let (stdout_result, stdout) = stdout_reader
        .join()
        .map_err(|_| anyhow::anyhow!("headless browser stdout reader panicked"))?;
    stdout_result.context("reading headless browser stdout")?;
    let (stderr_result, stderr) = stderr_reader
        .join()
        .map_err(|_| anyhow::anyhow!("headless browser stderr reader panicked"))?;
    stderr_result.context("reading headless browser stderr")?;
    if !screenshot_path
        .metadata()
        .is_ok_and(|metadata| metadata.len() > 0)
    {
        bail!(
            "headless browser did not render a screenshot for {url} (status {status}): {}",
            String::from_utf8_lossy(&stderr)
        );
    }
    let _ = stdout;
    Ok(())
}

pub(crate) fn smoke_static_site(
    site_dir: &Path,
    explicit_browser: Option<&Path>,
    timeout_seconds: u64,
) -> Result<SmokeStaticSiteSummary> {
    if timeout_seconds == 0 {
        bail!("browser smoke timeout must be nonzero");
    }
    let verified = verify_static_site(site_dir)?;
    let browser = browser_path(explicit_browser)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let site_dir = fs::canonicalize(site_dir).context("canonicalizing static site directory")?;
    let mut temporary_server_root = None;
    let server_root = if verified.base_url == "/" {
        site_dir.clone()
    } else {
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-static-server-{}-{nonce}",
            std::process::id()
        ));
        let mount = root.join(verified.base_url.trim_matches('/'));
        fs::create_dir_all(
            mount
                .parent()
                .context("static smoke base URL mount has no parent")?,
        )
        .context("creating static smoke base URL mount")?;
        std::os::unix::fs::symlink(&site_dir, &mount)
            .context("mounting site below static smoke base URL")?;
        temporary_server_root = Some(root.clone());
        root
    };
    let (mut server, address) = start_plain_static_server(&server_root)?;
    let profile_dir =
        std::env::temp_dir().join(format!("xlsynth-bvc-chrome-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&profile_dir).context("creating temporary Chrome profile")?;
    let origin = format!("http://{address}");
    let timeout = Duration::from_secs(timeout_seconds);
    let pages = [
        ("", "xlsynth-bvc results"),
        ("runs.html", "Campaign runs"),
        ("dataset.html", "Dataset explorer"),
    ];
    let result = pages.iter().try_for_each(|(path, expected)| {
        run_browser_page(
            &browser,
            &profile_dir,
            &format!("{origin}{}{path}", verified.base_url),
            expected,
            timeout,
        )
    });
    let _ = server.kill();
    let _ = server.wait();
    fs::remove_dir_all(&profile_dir).ok();
    if let Some(root) = temporary_server_root {
        fs::remove_dir_all(root).ok();
    }
    result?;
    Ok(SmokeStaticSiteSummary {
        site_dir: site_dir.display().to_string(),
        base_url: verified.base_url,
        browser: browser.display().to_string(),
        pages_checked: pages.iter().map(|(path, _)| (*path).to_string()).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::campaign::finalize_campaign_run;
    use crate::executor::compute_action_id;
    use crate::model::{ArtifactRef, ArtifactType, Provenance};
    use crate::query::canonical_root_actions_for_crate_version;
    use crate::snapshot::{BuildStaticSnapshotOptions, build_static_snapshot};
    use crate::store::ArtifactStore;
    use crate::versioning::{load_version_compat_map, resolve_xlsynth_version_for_driver};
    use chrono::Utc;
    use serde_json::json;

    fn temp_root() -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("xlsynth-bvc-site-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn generated_site_links_are_relocatable() {
        assert_eq!(site_root_url("index.html").expect("root URL"), "./");
        assert_eq!(
            site_root_url("runs/abc/index.html").expect("nested root URL"),
            "../../"
        );
        assert_eq!(
            resolve_site_link("runs/abc/index.html", "../../assets/site.css")
                .expect("nested asset"),
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
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("store layout");
        store
            .write_web_index_bytes("versions-summary.v1.json", br#"{"cards":[]}"#)
            .expect("write dataset");
        let snapshot_dir = root.join("snapshot");
        build_static_snapshot(
            &store,
            &root,
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
        assert!(
            !fs::read_to_string(site_dir.join("dataset.html"))
                .expect("read HTML")
                .contains("/api/")
        );
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn site_verifier_detects_tamper() {
        let root = temp_root();
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("store layout");
        store
            .write_web_index_bytes("versions-summary.v1.json", br#"{"cards":[]}"#)
            .expect("write dataset");
        let snapshot_dir = root.join("snapshot");
        build_static_snapshot(
            &store,
            &root,
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
    fn finalized_campaign_is_rendered_as_verified_static_run_page() {
        let root = temp_root();
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("store layout");
        let repo_root = std::env::current_dir().expect("current dir");
        let crate_version = load_version_compat_map(&repo_root)
            .expect("compat map")
            .into_keys()
            .next()
            .expect("known crate version");
        let dso_version =
            resolve_xlsynth_version_for_driver(&repo_root, &crate_version).expect("dso version");
        for action in
            canonical_root_actions_for_crate_version(&repo_root, &crate_version, &dso_version)
                .expect("root actions")
        {
            let action_id = compute_action_id(&action).expect("action id");
            let details = match &action {
                crate::model::ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                    ..
                } => json!({"download": {}}),
                crate::model::ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
                    subtree,
                    ..
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
                        "badge_class": "ok",
                        "badge_label": "ok",
                        "summary": "complete"
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
        assert_eq!(finalized.status, "complete");

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
}
