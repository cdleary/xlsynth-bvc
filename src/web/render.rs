// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::Path;

use crate::model::*;
use crate::query::*;
use crate::queue::{
    QueueState, action_dependency_role_action_ids, queue_state_display_label,
    queue_state_for_action, queue_state_key,
};
use crate::service::{short_id, summarize_error};
use crate::sled_space::{SledSpaceCategory, SledSpaceReport, SledSpaceRow, SledTreeSpaceReport};
use crate::store::ArtifactStore;
use crate::versioning::normalize_tag_version;
use crate::view::*;

pub(crate) fn inject_server_timing_badge(html: &str, elapsed_ms: u128) -> String {
    let badge = format!(
        "<div id=\"bvc-server-ms-badge\" style=\"position:fixed;top:8px;right:8px;z-index:2147483647;pointer-events:none;font-family:'JetBrains Mono','IBM Plex Mono','Consolas',monospace;font-size:11px;line-height:1.2;padding:4px 7px;border-radius:6px;border:1px solid rgba(80,160,220,0.45);background:rgba(6,14,24,0.88);color:#9fddff;backdrop-filter:blur(2px);box-shadow:0 2px 8px rgba(0,0,0,0.35);\">server {elapsed_ms} ms</div>"
    );
    if let Some(body_close) = html.rfind("</body>") {
        let mut out = String::with_capacity(html.len() + badge.len());
        out.push_str(&html[..body_close]);
        out.push_str(&badge);
        out.push_str(&html[body_close..]);
        out
    } else {
        let mut out = String::with_capacity(html.len() + badge.len());
        out.push_str(&badge);
        out.push_str(html);
        out
    }
}

fn format_bytes_iec(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit_index = 0_usize;
    while value >= 1024.0 && unit_index + 1 < UNITS.len() {
        value /= 1024.0;
        unit_index += 1;
    }
    if unit_index == 0 {
        format!("{bytes} {}", UNITS[unit_index])
    } else {
        format!("{value:.2} {}", UNITS[unit_index])
    }
}

fn format_ratio(part: u64, total: u64) -> String {
    if total == 0 {
        "-".to_string()
    } else {
        format!("{:.1}%", (part as f64 * 100.0) / total as f64)
    }
}

fn render_db_size_categories(html: &mut String, title: &str, categories: &[SledSpaceCategory]) {
    use std::fmt::Write;

    let _ = write!(
        html,
        "<details><summary>{} ({})</summary>",
        html_escape(title),
        categories.len()
    );
    if categories.is_empty() {
        html.push_str("<p class=\"meta\">none</p></details>");
        return;
    }
    html.push_str(
        "<table><thead><tr><th>Category</th><th>Entries</th><th>Key Bytes</th><th>Value Bytes</th><th>Logical Bytes</th></tr></thead><tbody>",
    );
    for row in categories {
        let _ = write!(
            html,
            "<tr>\
<td><code>{}</code></td>\
<td>{}</td>\
<td>{}</td>\
<td>{}</td>\
<td>{}</td>\
</tr>",
            html_escape(&row.category),
            row.entries,
            html_escape(&format_bytes_iec(row.key_bytes)),
            html_escape(&format_bytes_iec(row.value_bytes)),
            html_escape(&format_bytes_iec(row.logical_bytes))
        );
    }
    html.push_str("</tbody></table></details>");
}

fn render_db_size_rows(html: &mut String, title: &str, rows: &[SledSpaceRow]) {
    use std::fmt::Write;

    let _ = write!(
        html,
        "<details><summary>{} ({})</summary>",
        html_escape(title),
        rows.len()
    );
    if rows.is_empty() {
        html.push_str("<p class=\"meta\">none</p></details>");
        return;
    }
    html.push_str(
        "<table><thead><tr><th>Tree</th><th>Key</th><th>Key Bytes</th><th>Value Bytes</th><th>Logical Bytes</th></tr></thead><tbody>",
    );
    for row in rows {
        let _ = write!(
            html,
            "<tr>\
<td><code>{}</code></td>\
<td><code>{}</code></td>\
<td>{}</td>\
<td>{}</td>\
<td>{}</td>\
</tr>",
            html_escape(&row.tree),
            html_escape(&row.key),
            html_escape(&format_bytes_iec(row.key_bytes)),
            html_escape(&format_bytes_iec(row.value_bytes)),
            html_escape(&format_bytes_iec(row.logical_bytes))
        );
    }
    html.push_str("</tbody></table></details>");
}

fn render_db_size_tree_details(html: &mut String, tree: &SledTreeSpaceReport, total_logical: u64) {
    use std::fmt::Write;

    let _ = write!(
        html,
        "<section class=\"panel\">\
<h3>{}</h3>\
<p class=\"meta\">entries={} | logical={} ({}) | key={} | value={}</p>",
        html_escape(&tree.tree),
        tree.entries,
        html_escape(&format_bytes_iec(tree.logical_bytes)),
        html_escape(&format_ratio(tree.logical_bytes, total_logical)),
        html_escape(&format_bytes_iec(tree.key_bytes)),
        html_escape(&format_bytes_iec(tree.value_bytes)),
    );

    html.push_str("<details><summary>Value Size Histogram</summary>");
    html.push_str(
        "<table><thead><tr><th>Bucket</th><th>Entries</th><th>Value Bytes</th></tr></thead><tbody>",
    );
    for bucket in &tree.value_size_histogram {
        let _ = write!(
            html,
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
            html_escape(&bucket.label),
            bucket.entries,
            html_escape(&format_bytes_iec(bucket.value_bytes))
        );
    }
    html.push_str("</tbody></table></details>");

    render_db_size_categories(html, "Top categories", &tree.categories);
    render_db_size_categories(html, "By extension", &tree.extension_categories);
    render_db_size_categories(html, "By filename", &tree.filename_categories);
    render_db_size_categories(
        html,
        "By relpath prefix depth 2",
        &tree.relpath_prefix_depth2_categories,
    );
    render_db_size_categories(
        html,
        "By relpath prefix depth 3",
        &tree.relpath_prefix_depth3_categories,
    );
    render_db_size_rows(html, "Largest rows", &tree.largest_rows);
    render_db_size_rows(html, "Sampled rows", &tree.sampled_rows);
    html.push_str("</section>");
}

pub(super) fn render_db_size_html(
    report: &SledSpaceReport,
    top: usize,
    sample: usize,
    generated_utc: DateTime<Utc>,
) -> String {
    use std::fmt::Write;

    let mut html = String::new();
    html.push_str(
        "<!doctype html>\
<html lang=\"en\">\
<head>\
  <meta charset=\"utf-8\">\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
  <title>xlsynth-bvc DB Size</title>\
  <style>\
    :root {\
      --bg: #070b12;\
      --bg2: #0b1220;\
      --panel: #0d1524;\
      --panel-hi: #111c2f;\
      --ink: #d8ffe9;\
      --muted: #8da9a0;\
      --accent: #35f2b3;\
      --accent2: #3fc2ff;\
      --border: #1f3350;\
      --chip: #132036;\
    }\
    * { box-sizing: border-box; }\
    body {\
      margin: 0;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      color: var(--ink);\
      background:\
        radial-gradient(circle at 15% -10%, rgba(53, 242, 179, 0.16) 0, transparent 38%),\
        radial-gradient(circle at 100% 0%, rgba(63, 194, 255, 0.12) 0, transparent 42%),\
        repeating-linear-gradient(0deg, rgba(141, 169, 160, 0.06) 0, rgba(141, 169, 160, 0.06) 1px, transparent 1px, transparent 22px),\
        repeating-linear-gradient(90deg, rgba(141, 169, 160, 0.045) 0, rgba(141, 169, 160, 0.045) 1px, transparent 1px, transparent 22px),\
        linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);\
      min-height: 100vh;\
    }\
    .wrap { width: 100%; max-width: none; margin: 0; padding: 24px 20px 34px; }\
    .title { margin: 0 0 8px; font-size: 1.9rem; letter-spacing: 0.04em; text-transform: uppercase; color: var(--accent); text-shadow: 0 0 14px rgba(53, 242, 179, 0.32); }\
    .subtitle { margin: 8px 0 16px; color: var(--muted); border-left: 3px solid #2f4b73; padding-left: 10px; }\
    .nav { margin-top: 10px; display: flex; gap: 9px; flex-wrap: wrap; }\
    .nav a { color: #b9e8ff; text-decoration: none; border: 1px solid var(--border); background: rgba(16, 34, 56, 0.8); border-radius: 6px; padding: 5px 9px; font-size: 0.82rem; }\
    .nav a.active { color: var(--accent); border-color: rgba(50, 241, 178, 0.55); box-shadow: 0 0 0 1px rgba(50, 241, 178, 0.15) inset; }\
    .controls { margin-top: 10px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }\
    .controls input { width: 120px; padding: 7px 8px; border-radius: 6px; border: 1px solid var(--border); background: rgba(12, 22, 37, 0.9); color: var(--ink); font-family: inherit; font-size: 0.82rem; }\
    .controls button { padding: 7px 12px; border-radius: 6px; border: 1px solid var(--border); background: rgba(20, 38, 60, 0.92); color: #c9edff; font-family: inherit; font-size: 0.8rem; cursor: pointer; }\
    .chips { margin-top: 10px; display: flex; flex-wrap: wrap; gap: 8px; }\
    .chip { border: 1px solid var(--border); border-radius: 999px; padding: 4px 10px; font-size: 0.78rem; background: var(--chip); color: #cde7f7; }\
    .panel { margin-top: 12px; background: linear-gradient(180deg, var(--panel-hi) 0%, var(--panel) 100%); border: 1px solid var(--border); border-radius: 8px; padding: 12px; }\
    .panel h3 { margin: 0 0 6px; color: var(--accent2); }\
    .meta { margin: 4px 0; color: var(--muted); }\
    table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.82rem; }\
    th, td { text-align: left; border-top: 1px solid var(--border); padding: 7px 6px; vertical-align: top; }\
    th { color: var(--muted); text-transform: uppercase; letter-spacing: 0.045em; font-size: 0.72rem; }\
    details { margin-top: 10px; }\
    summary { cursor: pointer; color: var(--accent2); }\
    code { font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace; font-size: 0.78rem; color: #bbffda; background: rgba(17, 35, 58, 0.72); border: 1px solid var(--border); border-radius: 4px; padding: 1px 4px; }\
  </style>\
</head>\
<body>\
  <main class=\"wrap\">",
    );

    let _ = write!(
        html,
        "<h1 class=\"title\">Database Size</h1>\
<p class=\"subtitle\">generated={} | analyzed_utc={}</p>",
        html_escape(&generated_utc.to_rfc3339()),
        html_escape(&report.analyzed_utc.to_rfc3339()),
    );

    html.push_str(
        "<nav class=\"nav\">\
<a href=\"/versions/\">/versions/</a>\
<a href=\"/dslx-fns/\">/dslx-fns/</a>\
<a href=\"/dslx-fns-g8r/\">/dslx-fns-g8r/</a>\
<a href=\"/dslx-fns-yosys-abc/\">/dslx-fns-yosys-abc/</a>\
<a href=\"/dslx-fns-g8r-vs-yosys-abc/\">/dslx-fns-g8r-vs-yosys-abc/</a>\
<a href=\"/ir-fn-corpus-g8r-vs-yosys-abc/\">/ir-fn-corpus-g8r-vs-yosys-abc/</a>\
<a href=\"/ir-fn-g8r-abc-vs-codegen-yosys-abc/\">/ir-fn-g8r-abc-vs-codegen-yosys-abc/</a>\
<a href=\"/dslx-file-action-graph/\">/dslx-file-action-graph/</a>\
<a href=\"/ir-fn-corpus-structural/\">/ir-fn-corpus-structural/</a>\
<a class=\"active\" href=\"/db-size/\">/db-size/</a>\
</nav>",
    );

    let _ = write!(
        html,
        "<form class=\"controls\" method=\"get\" action=\"/db-size/\">\
<label>top <input type=\"number\" min=\"1\" max=\"250\" name=\"top\" value=\"{}\"/></label>\
<label>sample <input type=\"number\" min=\"1\" max=\"250\" name=\"sample\" value=\"{}\"/></label>\
<button type=\"submit\">apply</button>\
</form>",
        top, sample
    );

    let _ = write!(
        html,
        "<div class=\"chips\">\
<span class=\"chip\">db_path <code>{}</code></span>\
<span class=\"chip\">on_disk {}</span>\
<span class=\"chip\">logical {}</span>\
<span class=\"chip\">entries {}</span>\
<span class=\"chip\">trees {}</span>\
</div>",
        html_escape(&report.db_path),
        html_escape(&format_bytes_iec(report.db_files_on_disk_bytes)),
        html_escape(&format_bytes_iec(report.total_logical_bytes)),
        report.total_entries,
        report.tree_count,
    );

    html.push_str("<section class=\"panel\"><h3>Tree Summary</h3>");
    html.push_str("<table><thead><tr><th>Tree</th><th>Entries</th><th>Logical Bytes</th><th>Share</th><th>Key Bytes</th><th>Value Bytes</th></tr></thead><tbody>");
    for tree in &report.trees {
        let _ = write!(
            html,
            "<tr>\
<td><code>{}</code></td>\
<td>{}</td>\
<td>{}</td>\
<td>{}</td>\
<td>{}</td>\
<td>{}</td>\
</tr>",
            html_escape(&tree.tree),
            tree.entries,
            html_escape(&format_bytes_iec(tree.logical_bytes)),
            html_escape(&format_ratio(
                tree.logical_bytes,
                report.total_logical_bytes
            )),
            html_escape(&format_bytes_iec(tree.key_bytes)),
            html_escape(&format_bytes_iec(tree.value_bytes))
        );
    }
    html.push_str("</tbody></table></section>");

    render_db_size_rows(
        &mut html,
        "Largest Rows Overall",
        &report.largest_rows_overall,
    );

    for tree in &report.trees {
        render_db_size_tree_details(&mut html, tree, report.total_logical_bytes);
    }

    html.push_str("</main></body></html>");
    html
}

pub(super) fn render_ir_fn_corpus_structural_html(
    manifest: Option<&IrFnCorpusStructuralManifest>,
    group_rows: &[IrFnCorpusStructuralGroupRowView],
    lookup_fn_name: Option<&str>,
    lookup_rows: &[IrFnCorpusFnLookupView],
    generated_utc: DateTime<Utc>,
) -> String {
    use std::fmt::Write;

    let mut html = String::new();
    html.push_str(
        "<!doctype html>\
<html lang=\"en\">\
<head>\
  <meta charset=\"utf-8\">\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
  <title>xlsynth-bvc IR Function Corpus (Structural)</title>\
  <style>\
    :root {\
      --bg: #070b12;\
      --bg2: #0b1220;\
      --panel: #0d1524;\
      --panel-hi: #111c2f;\
      --ink: #d8ffe9;\
      --muted: #8da9a0;\
      --accent: #35f2b3;\
      --accent2: #3fc2ff;\
      --border: #1f3350;\
      --chip: #132036;\
    }\
    * { box-sizing: border-box; }\
    body {\
      margin: 0;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      color: var(--ink);\
      background: linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);\
      min-height: 100vh;\
    }\
    .wrap { width: 100%; max-width: none; margin: 0; padding: 24px 18px 34px; }\
    .title { margin: 0 0 8px; font-size: 1.6rem; text-transform: uppercase; color: var(--accent); }\
    .subtitle { margin: 6px 0 0; color: var(--muted); }\
    .nav { margin-top: 10px; display: flex; gap: 9px; flex-wrap: wrap; }\
    .nav a {\
      color: #b9e8ff;\
      text-decoration: none;\
      border: 1px solid var(--border);\
      background: rgba(16, 34, 56, 0.8);\
      border-radius: 6px;\
      padding: 5px 9px;\
      font-size: 0.82rem;\
    }\
    .nav a.active {\
      color: var(--accent);\
      border-color: rgba(50, 241, 178, 0.55);\
      box-shadow: 0 0 0 1px rgba(50, 241, 178, 0.15) inset;\
    }\
    .panel {\
      margin-top: 12px;\
      background: linear-gradient(180deg, var(--panel-hi) 0%, var(--panel) 100%);\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      padding: 12px;\
    }\
    .chips { margin-top: 10px; display: flex; flex-wrap: wrap; gap: 8px; }\
    .chip {\
      border: 1px solid var(--border);\
      border-radius: 999px;\
      padding: 4px 10px;\
      font-size: 0.78rem;\
      background: var(--chip);\
      color: #cde7f7;\
    }\
    .chip.loss {\
      background: rgba(255, 100, 137, 0.14);\
      border-color: rgba(255, 100, 137, 0.48);\
      color: #ffc3d3;\
    }\
    .chip.ok {\
      background: rgba(105, 240, 174, 0.12);\
      border-color: rgba(105, 240, 174, 0.42);\
      color: #bdf7d8;\
    }\
    .lookup-form { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }\
    .lookup-form input {\
      min-width: 320px;\
      max-width: 100%;\
      padding: 7px 8px;\
      border-radius: 6px;\
      border: 1px solid var(--border);\
      background: rgba(12, 22, 37, 0.9);\
      color: var(--ink);\
      font-family: inherit;\
      font-size: 0.82rem;\
    }\
    .lookup-form button {\
      padding: 7px 12px;\
      border-radius: 6px;\
      border: 1px solid var(--border);\
      background: rgba(20, 38, 60, 0.92);\
      color: #c9edff;\
      font-family: inherit;\
      font-size: 0.8rem;\
      cursor: pointer;\
    }\
    details { margin-top: 4px; }\
    summary { cursor: pointer; color: var(--accent2); }\
    pre {\
      margin: 8px 0 0;\
      max-height: 320px;\
      overflow: auto;\
      background: rgba(8, 17, 30, 0.95);\
      border: 1px solid var(--border);\
      border-radius: 6px;\
      padding: 8px;\
    }\
    table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.82rem; }\
    th, td {\
      text-align: left;\
      border-top: 1px solid var(--border);\
      padding: 7px 6px;\
      vertical-align: top;\
    }\
    th { color: var(--muted); text-transform: uppercase; letter-spacing: 0.045em; font-size: 0.72rem; }\
    tbody tr:nth-child(even) td { background: rgba(255, 255, 255, 0.012); }\
    a { color: var(--accent2); text-decoration: none; }\
    code {\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
      color: #bbffda;\
      background: rgba(17, 35, 58, 0.72);\
      border: 1px solid var(--border);\
      border-radius: 4px;\
      padding: 1px 4px;\
    }\
  </style>\
</head>\
<body>\
  <main class=\"wrap\">",
    );
    let _ = write!(
        html,
        "<h1 class=\"title\">IR Fn Corpus Structural Index</h1>\
<p class=\"subtitle\">generated={} | route=/ir-fn-corpus-structural/</p>",
        html_escape(&generated_utc.to_rfc3339())
    );
    html.push_str(
        "<p class=\"subtitle\"><a href=\"/ir-fn-corpus-structural/download.tar.zst\">download full corpus (.tar.zst)</a></p>",
    );
    html.push_str(
        "<nav class=\"nav\">\
<a href=\"/versions/\">/versions/</a>\
<a href=\"/dslx-fns/\">/dslx-fns/</a>\
<a href=\"/dslx-fns-g8r/\">/dslx-fns-g8r/</a>\
<a href=\"/dslx-fns-yosys-abc/\">/dslx-fns-yosys-abc/</a>\
<a href=\"/dslx-fns-g8r-vs-yosys-abc/\">/dslx-fns-g8r-vs-yosys-abc/</a>\
<a href=\"/ir-fn-corpus-g8r-vs-yosys-abc/\">/ir-fn-corpus-g8r-vs-yosys-abc/</a>\
<a href=\"/ir-fn-g8r-abc-vs-codegen-yosys-abc/\">/ir-fn-g8r-abc-vs-codegen-yosys-abc/</a>\
<a href=\"/dslx-file-action-graph/\">/dslx-file-action-graph/</a>\
<a class=\"active\" href=\"/ir-fn-corpus-structural/\">/ir-fn-corpus-structural/</a>\
</nav>",
    );
    html.push_str("<section class=\"panel\">");
    let lookup_input = lookup_fn_name.unwrap_or("");
    let _ = write!(
        html,
        "<p class=\"subtitle\">Lookup IR fn by exact name (e.g. <code>__k3_cone_f139d8559988ff4e</code>):</p>\
<form class=\"lookup-form\" method=\"get\" action=\"/ir-fn-corpus-structural/\">\
<input type=\"text\" name=\"fn_name\" value=\"{}\" placeholder=\"__k3_cone_f139d8559988ff4e\" spellcheck=\"false\" />\
<button type=\"submit\">lookup</button>\
</form>",
        html_escape(lookup_input)
    );
    if let Some(fn_name) = lookup_fn_name {
        let _ = write!(
            html,
            "<div class=\"chips\">\
<span class=\"chip\">query {}</span>\
<span class=\"chip\">matches {}</span>\
</div>",
            html_escape(fn_name),
            lookup_rows.len()
        );
        if lookup_rows.is_empty() {
            html.push_str(
                "<p class=\"subtitle\">No corpus entries matched that exact function name.</p>",
            );
        } else {
            html.push_str(
                "<table><thead><tr>\
<th>IR Action</th><th>Structural Hash</th><th>crate</th><th>dso</th><th>source IR</th><th>ir ops</th><th>fn signature</th><th>IR fn body</th>\
</tr></thead><tbody>",
            );
            for row in lookup_rows {
                let member = &row.member.member;
                let ir_op_count = row
                    .member
                    .ir_op_count
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let fn_signature = row
                    .member
                    .fn_signature
                    .as_deref()
                    .filter(|v| !v.trim().is_empty())
                    .unwrap_or("-");
                let ir_body_cell = if let Some(ir_fn_text) = row.ir_fn_text.as_deref() {
                    format!(
                        "<details><summary>show</summary><pre><code>{}</code></pre></details>",
                        html_escape(ir_fn_text)
                    )
                } else {
                    "<span class=\"subtitle\">not available</span>".to_string()
                };
                let _ = write!(
                    html,
                    "<tr>\
<td><a href=\"/action/{}/\"><code>{}</code></a><br><code>{}</code></td>\
<td><a href=\"/ir-fn-corpus-structural/group/{}/\"><code>{}</code></a></td>\
<td>v{}</td>\
<td>v{}</td>\
<td><a href=\"/action/{}/\"><code>{}</code></a></td>\
<td>{}</td>\
<td><code>{}</code></td>\
<td>{}</td>\
</tr>",
                    html_escape(&member.opt_ir_action_id),
                    html_escape(&member.opt_ir_action_id),
                    html_escape(&member.ir_top),
                    html_escape(&row.structural_hash),
                    html_escape(&row.structural_hash),
                    html_escape(&member.crate_version),
                    html_escape(&member.dso_version),
                    html_escape(&member.source_ir_action_id),
                    html_escape(&member.source_ir_action_id),
                    html_escape(&ir_op_count),
                    html_escape(fn_signature),
                    ir_body_cell
                );
            }
            html.push_str("</tbody></table>");
        }
    }
    html.push_str("</section><section class=\"panel\">");
    match manifest {
        None => {
            html.push_str(
                "<p class=\"subtitle\">No manifest found. Materialize it with:</p>\
<pre><code>cargo run -- populate-ir-fn-corpus-structural</code></pre>",
            );
        }
        Some(manifest) => {
            let loss_hashes = group_rows
                .iter()
                .filter(|row| row.g8r_loss_sample_count > 0)
                .count();
            let _ = write!(
                html,
                "<p class=\"subtitle\">schema={} | manifest_generated={} | recompute_missing_hashes={}</p>",
                manifest.schema_version,
                html_escape(&manifest.generated_utc.to_rfc3339()),
                if manifest.recompute_missing_hashes {
                    "true"
                } else {
                    "false"
                }
            );
            let _ = write!(
                html,
                "<div class=\"chips\">\
<span class=\"chip\">distinct_hashes {}</span>\
<span class=\"chip\">indexed_actions {}</span>\
<span class=\"chip\">driver_ir_to_opt {}</span>\
<span class=\"chip\">hint_hashes {}</span>\
<span class=\"chip\">recomputed_hashes {}</span>\
<span class=\"chip\">missing_hint_skips {}</span>\
<span class=\"chip loss\">loss_hashes {}</span>\
</div>",
                manifest.distinct_structural_hashes,
                manifest.indexed_actions,
                manifest.total_driver_ir_to_opt_actions,
                manifest.hash_from_dependency_hint_count,
                manifest.hash_recomputed_count,
                manifest.skipped_missing_hash_hint_count,
                loss_hashes
            );

            let mut groups = group_rows.to_vec();
            groups.sort_by(|a, b| {
                b.member_count
                    .cmp(&a.member_count)
                    .then(a.structural_hash.cmp(&b.structural_hash))
            });
            html.push_str(
                "<table><thead><tr>\
<th>Structural Hash</th><th>Members</th><th>IR Nodes</th><th>g8r vs yabc</th><th>Group</th>\
</tr></thead><tbody>",
            );
            for group in groups {
                let ir_node_count = group
                    .ir_node_count
                    .map(|count| count.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let loss_chip = if group.g8r_loss_sample_count > 0 {
                    format!(
                        "<span class=\"chip loss\">losses {}</span>",
                        group.g8r_loss_sample_count
                    )
                } else {
                    "<span class=\"chip ok\">no-loss</span>".to_string()
                };
                let _ = write!(
                    html,
                    "<tr>\
<td><code>{}</code></td>\
<td>{}</td>\
<td>{}</td>\
<td>{}</td>\
<td><a href=\"/ir-fn-corpus-structural/group/{}/\">inspect</a></td>\
</tr>",
                    html_escape(&group.structural_hash),
                    group.member_count,
                    html_escape(&ir_node_count),
                    loss_chip,
                    html_escape(&group.structural_hash),
                );
            }
            html.push_str("</tbody></table>");
        }
    }
    html.push_str("</section></main></body></html>");
    html
}

pub(super) fn render_ir_fn_corpus_structural_group_html(
    structural_hash: &str,
    members: &[IrFnCorpusStructuralMemberView],
    generated_utc: DateTime<Utc>,
) -> String {
    use std::fmt::Write;

    let mut html = String::new();
    let mut op_counts = BTreeSet::new();
    for row in members {
        if let Some(count) = row.ir_op_count {
            op_counts.insert(count);
        }
    }
    let ir_ops_summary = if op_counts.is_empty() {
        "unknown".to_string()
    } else if op_counts.len() == 1 {
        op_counts
            .iter()
            .next()
            .map(u64::to_string)
            .unwrap_or_else(|| "unknown".to_string())
    } else {
        let values = op_counts
            .iter()
            .take(8)
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        if op_counts.len() > 8 {
            format!("mixed({values},...)")
        } else {
            format!("mixed({values})")
        }
    };
    html.push_str(
        "<!doctype html>\
<html lang=\"en\">\
<head>\
  <meta charset=\"utf-8\">\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
  <title>xlsynth-bvc IR Function Corpus Group</title>\
  <style>\
    :root {\
      --bg: #070b12;\
      --bg2: #0b1220;\
      --panel: #0d1524;\
      --panel-hi: #111c2f;\
      --ink: #d8ffe9;\
      --muted: #8da9a0;\
      --accent: #35f2b3;\
      --accent2: #3fc2ff;\
      --border: #1f3350;\
    }\
    * { box-sizing: border-box; }\
    body {\
      margin: 0;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      color: var(--ink);\
      background: linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);\
      min-height: 100vh;\
    }\
    .wrap { width: 100%; max-width: none; margin: 0; padding: 24px 18px 34px; }\
    .title { margin: 0 0 8px; font-size: 1.5rem; color: var(--accent); }\
    .subtitle { margin: 6px 0 0; color: var(--muted); }\
    .panel {\
      margin-top: 12px;\
      background: linear-gradient(180deg, var(--panel-hi) 0%, var(--panel) 100%);\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      padding: 12px;\
    }\
    table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.8rem; }\
    th, td {\
      text-align: left;\
      border-top: 1px solid var(--border);\
      padding: 7px 6px;\
      vertical-align: top;\
    }\
    th { color: var(--muted); text-transform: uppercase; letter-spacing: 0.045em; font-size: 0.7rem; }\
    tbody tr:nth-child(even) td { background: rgba(255, 255, 255, 0.012); }\
    a { color: var(--accent2); text-decoration: none; }\
    code {\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.77rem;\
      color: #bbffda;\
      background: rgba(17, 35, 58, 0.72);\
      border: 1px solid var(--border);\
      border-radius: 4px;\
      padding: 1px 4px;\
    }\
  </style>\
</head>\
<body>\
  <main class=\"wrap\">",
    );
    let _ = write!(
        html,
        "<h1 class=\"title\">IR Structural Group {}</h1>\
<p class=\"subtitle\">generated={} | members={} | ir_ops={}</p>\
<p class=\"subtitle\"><a href=\"/ir-fn-corpus-structural/\">/ir-fn-corpus-structural/</a></p>",
        html_escape(structural_hash),
        html_escape(&generated_utc.to_rfc3339()),
        members.len(),
        html_escape(&ir_ops_summary)
    );
    html.push_str("<section class=\"panel\">");
    html.push_str(
        "<table><thead><tr>\
<th>IR Action</th><th>Producer</th><th>Source IR</th><th>crate</th><th>dso</th><th>ir_top</th><th>ir ops</th><th>fn signature</th><th>hash_source</th><th>dslx origin</th>\
</tr></thead><tbody>",
    );
    for row in members {
        let member = &row.member;
        let dslx_origin = member.dslx_origin.as_ref().map_or_else(
            || "-".to_string(),
            |origin| format!("{}/{}", origin.dslx_file, origin.dslx_fn_name),
        );
        let fn_signature = row
            .fn_signature
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or("-");
        let ir_op_count = row
            .ir_op_count
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let producer_kind = member.producer_action_kind.as_deref().unwrap_or("-");
        let _ = write!(
            html,
            "<tr>\
<td><a href=\"/action/{}/\"><code>{}</code></a></td>\
<td><code>{}</code></td>\
<td><a href=\"/action/{}/\"><code>{}</code></a></td>\
<td>v{}</td>\
<td>v{}</td>\
<td><code>{}</code></td>\
<td>{}</td>\
<td><code>{}</code></td>\
<td>{}</td>\
<td><code>{}</code></td>\
</tr>",
            html_escape(&member.opt_ir_action_id),
            html_escape(&member.opt_ir_action_id),
            html_escape(producer_kind),
            html_escape(&member.source_ir_action_id),
            html_escape(&member.source_ir_action_id),
            html_escape(&member.crate_version),
            html_escape(&member.dso_version),
            html_escape(&member.ir_top),
            html_escape(&ir_op_count),
            html_escape(fn_signature),
            html_escape(&member.hash_source),
            html_escape(&dslx_origin)
        );
    }
    html.push_str("</tbody></table></section></main></body></html>");
    html
}

pub(super) fn render_stdlib_fns_trend_html(
    dataset: &StdlibFnsTrendDataset,
    metric: StdlibMetric,
    generated_utc: DateTime<Utc>,
) -> String {
    use std::fmt::Write;

    let mut html = String::new();
    html.push_str(
        "<!doctype html>\
<html lang=\"en\">\
<head>\
  <meta charset=\"utf-8\">\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
  <title>xlsynth-bvc DSLX Function Trends</title>\
  <style>\
    :root {\
      --bg: #050911;\
      --bg2: #0a1323;\
      --panel: #0f1a2d;\
      --panel-hi: #14233b;\
      --ink: #dcffee;\
      --muted: #8fa9be;\
      --accent: #32f1b2;\
      --accent2: #5ec9ff;\
      --warn: #ff6d8f;\
      --line: #2c4566;\
      --border: #213754;\
      --chip: #11233a;\
    }\
    * { box-sizing: border-box; }\
    body {\
      margin: 0;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      color: var(--ink);\
      background:\
        radial-gradient(circle at 12% -15%, rgba(50, 241, 178, 0.18) 0, transparent 36%),\
        radial-gradient(circle at 100% 0%, rgba(94, 201, 255, 0.16) 0, transparent 40%),\
        repeating-linear-gradient(0deg, rgba(143, 169, 190, 0.055) 0, rgba(143, 169, 190, 0.055) 1px, transparent 1px, transparent 24px),\
        repeating-linear-gradient(90deg, rgba(143, 169, 190, 0.04) 0, rgba(143, 169, 190, 0.04) 1px, transparent 1px, transparent 24px),\
        linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);\
      min-height: 100vh;\
    }\
    .wrap { width: 100%; max-width: none; margin: 0; padding: 24px 18px 34px; }\
    .head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 10px 16px; }\
    .title {\
      margin: 0;\
      font-size: 1.8rem;\
      text-transform: uppercase;\
      letter-spacing: 0.04em;\
      color: var(--accent);\
      text-shadow: 0 0 12px rgba(50, 241, 178, 0.34);\
    }\
    .meta {\
      margin: 6px 0 0;\
      color: var(--muted);\
      font-size: 0.86rem;\
      border-left: 3px solid var(--line);\
      padding-left: 10px;\
    }\
    .nav { margin-top: 10px; display: flex; gap: 9px; flex-wrap: wrap; }\
    .nav a {\
      color: #b9e8ff;\
      text-decoration: none;\
      border: 1px solid var(--border);\
      background: rgba(16, 34, 56, 0.8);\
      border-radius: 6px;\
      padding: 5px 9px;\
      font-size: 0.82rem;\
    }\
    .nav a.active {\
      color: var(--accent);\
      border-color: rgba(50, 241, 178, 0.55);\
      box-shadow: 0 0 0 1px rgba(50, 241, 178, 0.15) inset;\
    }\
    .controls {\
      margin-top: 14px;\
      display: flex;\
      flex-wrap: wrap;\
      gap: 10px 14px;\
      align-items: center;\
    }\
    .control-group { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }\
    .label { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; }\
    .chip {\
      color: #cde7f7;\
      text-decoration: none;\
      border: 1px solid var(--border);\
      background: var(--chip);\
      border-radius: 999px;\
      padding: 4px 10px;\
      font-size: 0.78rem;\
    }\
    .chip.active {\
      color: var(--accent);\
      border-color: rgba(50, 241, 178, 0.55);\
      box-shadow: 0 0 14px rgba(50, 241, 178, 0.18);\
    }\
    .file-filter-form { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }\
    .file-select {\
      min-width: 360px;\
      max-width: min(72vw, 820px);\
      color: #d7f6ff;\
      background: #102238;\
      border: 1px solid var(--border);\
      border-radius: 6px;\
      padding: 5px 9px;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
    }\
    .file-select:focus { outline: 1px solid rgba(50, 241, 178, 0.55); }\
    .chip.file-apply { cursor: pointer; font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace; }\
    .panel {\
      margin-top: 14px;\
      background: linear-gradient(180deg, var(--panel-hi) 0%, var(--panel) 100%);\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      padding: 11px 12px 12px;\
      box-shadow:\
        inset 0 1px 0 rgba(255, 255, 255, 0.03),\
        0 0 0 1px rgba(50, 241, 178, 0.04),\
        0 12px 28px rgba(0, 0, 0, 0.35);\
    }\
    .plot-wrap { overflow-x: auto; padding-bottom: 2px; }\
    .plotly-host { width: 100%; min-width: 960px; height: 620px; }\
    .plotly-host-sm { width: 100%; min-width: 960px; height: 360px; }\
    .plot-note { margin-top: 8px; color: var(--muted); font-size: 0.8rem; }\
    .dist-stats { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 8px; }\
    .dist-chip {\
      border: 1px solid var(--border);\
      border-radius: 999px;\
      padding: 4px 10px;\
      font-size: 0.78rem;\
      background: rgba(17, 35, 58, 0.66);\
      color: #cde7f7;\
    }\
    .dist-chip.good { color: #7dff9f; border-color: rgba(125, 255, 159, 0.44); }\
    .dist-chip.bad { color: #ff8ea9; border-color: rgba(255, 142, 169, 0.44); }\
    .dist-chip.flat { color: var(--muted); }\
    table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.82rem; }\
    th, td {\
      text-align: left;\
      border-top: 1px solid var(--border);\
      padding: 7px 6px;\
      vertical-align: top;\
    }\
    th { color: var(--muted); text-transform: uppercase; letter-spacing: 0.045em; font-size: 0.72rem; }\
    tbody tr:nth-child(even) td { background: rgba(255, 255, 255, 0.012); }\
    code {\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
      color: #bbffda;\
      background: rgba(17, 35, 58, 0.72);\
      border: 1px solid var(--border);\
      border-radius: 4px;\
      padding: 1px 4px;\
    }\
    .up { color: #7dff9f; }\
    .down { color: #ff8ea9; }\
    .flat { color: var(--muted); }\
    @media (max-width: 860px) {\
      table, thead, tbody, th, td, tr { display: block; }\
      thead { display: none; }\
      tr { border-top: 1px solid var(--border); padding-top: 8px; margin-top: 8px; }\
      td { border: none; padding: 2px 0; }\
    }\
  </style>\
</head>\
<body>\
  <main class=\"wrap\">",
    );

    let fraig_query = if dataset.fraig { "true" } else { "false" };
    let selected_file = dataset.selected_file.as_deref();
    let selected_file_label = selected_file.unwrap_or("all");
    let g8r_nav_url =
        stdlib_fns_trend_view_url(StdlibTrendKind::G8r, metric, dataset.fraig, selected_file);
    let yosys_nav_url =
        stdlib_fns_trend_view_url(StdlibTrendKind::YosysAbc, metric, false, selected_file);
    let compare_nav_url = stdlib_g8r_vs_yosys_view_url(
        if dataset.kind.supports_fraig() {
            dataset.fraig
        } else {
            false
        },
        None,
        None,
    );
    let file_graph_nav_url = stdlib_file_action_graph_view_url(
        dataset.crate_versions.last().map(String::as_str),
        selected_file,
        None,
        None,
        false,
    );
    let corpus_frontend_compare_url = ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_view_url(
        None,
        dataset.crate_versions.last().map(String::as_str),
    );
    let metric_label = html_escape(metric.as_query_value());
    let generated = html_escape(&generated_utc.to_rfc3339());
    let title = html_escape(dataset.kind.title());
    let _ = write!(
        html,
        "<section class=\"head\"><h1 class=\"title\">{}</h1></section>",
        title
    );
    if dataset.kind.supports_fraig() {
        let _ = write!(
            html,
            "<p class=\"meta\">generated={} | metric={} | fraig={} | file={} | functions={} | crate_versions={} | points={}</p>",
            generated,
            metric_label,
            fraig_query,
            html_escape(selected_file_label),
            dataset.series.len(),
            dataset.crate_versions.len(),
            dataset.total_points
        );
    } else {
        let _ = write!(
            html,
            "<p class=\"meta\">generated={} | metric={} | file={} | functions={} | crate_versions={} | points={}</p>",
            generated,
            metric_label,
            html_escape(selected_file_label),
            dataset.series.len(),
            dataset.crate_versions.len(),
            dataset.total_points
        );
    }
    let _ = write!(
        html,
        "<nav class=\"nav\">\
<a href=\"/versions/\">/versions/</a>\
<a href=\"/dslx-fns/\">/dslx-fns/</a>\
<a class=\"{}\" href=\"{}\">/dslx-fns-g8r/</a>\
<a class=\"{}\" href=\"{}\">/dslx-fns-yosys-abc/</a>\
<a href=\"{}\">/dslx-fns-g8r-vs-yosys-abc/</a>\
<a href=\"{}\">/ir-fn-corpus-g8r-vs-yosys-abc/</a>\
<a href=\"{}\">/ir-fn-g8r-abc-vs-codegen-yosys-abc/</a>\
<a href=\"{}\">/dslx-file-action-graph/</a>\
<a href=\"/ir-fn-corpus-structural/\">/ir-fn-corpus-structural/</a>\
</nav>",
        if dataset.kind == StdlibTrendKind::G8r {
            "active"
        } else {
            ""
        },
        html_escape(&g8r_nav_url),
        if dataset.kind == StdlibTrendKind::YosysAbc {
            "active"
        } else {
            ""
        },
        html_escape(&yosys_nav_url),
        html_escape(&compare_nav_url),
        html_escape(&ir_fn_corpus_g8r_vs_yosys_view_url(
            None,
            dataset.crate_versions.last().map(String::as_str)
        )),
        html_escape(&corpus_frontend_compare_url),
        html_escape(&file_graph_nav_url)
    );

    let and_nodes_url = stdlib_fns_trend_view_url(
        dataset.kind,
        StdlibMetric::AndNodes,
        dataset.fraig,
        selected_file,
    );
    let depth_url = stdlib_fns_trend_view_url(
        dataset.kind,
        StdlibMetric::Depth,
        dataset.fraig,
        selected_file,
    );
    let fraig_false_url = stdlib_fns_trend_view_url(dataset.kind, metric, false, selected_file);
    let fraig_true_url = stdlib_fns_trend_view_url(dataset.kind, metric, true, selected_file);
    let _ = write!(
        html,
        "<section class=\"controls\">\
<div class=\"control-group\"><span class=\"label\">Metric</span>\
<a class=\"chip {}\" href=\"{}\">and_nodes</a>\
<a class=\"chip {}\" href=\"{}\">depth</a></div>",
        if metric == StdlibMetric::AndNodes {
            "active"
        } else {
            ""
        },
        html_escape(&and_nodes_url),
        if metric == StdlibMetric::Depth {
            "active"
        } else {
            ""
        },
        html_escape(&depth_url)
    );
    if dataset.kind.supports_fraig() {
        let _ = write!(
            html,
            "<div class=\"control-group\"><span class=\"label\">Fraig</span>\
<a class=\"chip {}\" href=\"{}\">false</a>\
<a class=\"chip {}\" href=\"{}\">true</a></div>",
            if !dataset.fraig { "active" } else { "" },
            html_escape(&fraig_false_url),
            if dataset.fraig { "active" } else { "" },
            html_escape(&fraig_true_url)
        );
    }
    html.push_str("</section>");
    let fraig_hidden = if dataset.kind.supports_fraig() {
        format!(
            "<input type=\"hidden\" name=\"fraig\" value=\"{}\">",
            html_escape(fraig_query)
        )
    } else {
        String::new()
    };
    let _ = write!(
        html,
        "<section class=\"controls\">\
<div class=\"control-group\">\
<span class=\"label\">DSLX File</span>\
<form class=\"file-filter-form\" method=\"get\" action=\"{}\">\
<input type=\"hidden\" name=\"metric\" value=\"{}\">\
{}\
<select class=\"file-select\" name=\"file\">\
<option value=\"\" {}>all DSLX files</option>",
        html_escape(dataset.kind.view_path()),
        html_escape(metric.as_query_value()),
        fraig_hidden,
        if selected_file.is_none() {
            "selected"
        } else {
            ""
        }
    );
    for file in &dataset.available_files {
        let selected_attr = if Some(file.as_str()) == selected_file {
            "selected"
        } else {
            ""
        };
        let _ = write!(
            html,
            "<option value=\"{}\" {}>{}</option>",
            html_escape(file),
            selected_attr,
            html_escape(file)
        );
    }
    html.push_str(
        "</select><button class=\"chip file-apply\" type=\"submit\">apply</button></form>",
    );
    if selected_file.is_some() {
        let clear_url = stdlib_fns_trend_view_url(dataset.kind, metric, dataset.fraig, None);
        let _ = write!(
            html,
            "<a class=\"chip\" href=\"{}\">all files</a>",
            html_escape(&clear_url)
        );
    }
    html.push_str("</div></section>");

    let mut plotly_script_included = false;

    html.push_str("<section class=\"panel\"><div class=\"plot-wrap\">");
    if dataset.series.is_empty() || dataset.crate_versions.is_empty() {
        let _ = write!(
            html,
            "<p class=\"meta\">{}</p>",
            html_escape(dataset.kind.no_data_message())
        );
    } else {
        let crate_categories: Vec<String> = dataset
            .crate_versions
            .iter()
            .map(|v| format!("v{v}"))
            .collect();
        let mut plot_traces = Vec::new();
        let mut values = Vec::new();
        for series in &dataset.series {
            let mut x = Vec::new();
            let mut y = Vec::new();
            let mut customdata = Vec::new();
            for point in &series.points {
                x.push(format!("v{}", point.crate_version));
                let value = metric.value(point);
                y.push(value);
                values.push(value);
                customdata.push(vec![
                    format!("v{}", point.dso_version),
                    point.stats_action_id.clone(),
                ]);
            }
            if x.is_empty() {
                continue;
            }
            plot_traces.push(json!({
                "type": "scattergl",
                "mode": "lines+markers",
                "name": series.fn_key,
                "x": x,
                "y": y,
                "customdata": customdata,
                "hovertemplate": "%{fullData.name}<br>crate:%{x}<br>value:%{y}<br>dso:%{customdata[0]}<br>stats_action:%{customdata[1]}<extra></extra>",
                "line": { "width": 1.6, "color": stdlib_series_color(&series.fn_key) },
                "marker": { "size": 4 }
            }));
        }

        if plot_traces.is_empty() {
            html.push_str("<p class=\"meta\">No numeric data points for selected metric.</p>");
        } else {
            let mut y_min = values.iter().copied().fold(f64::INFINITY, f64::min);
            let mut y_max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            if (y_max - y_min).abs() < f64::EPSILON {
                y_min -= 1.0;
                y_max += 1.0;
            }
            let plot_data_json =
                serde_json::to_string(&plot_traces).expect("serializing stdlib trend traces");
            let plot_layout_json = serde_json::to_string(&json!({
                "paper_bgcolor": "rgba(0,0,0,0)",
                "plot_bgcolor": "#0b172a",
                "hovermode": "closest",
                "showlegend": true,
                "legend": {
                    "orientation": "v",
                    "font": { "color": "#cde7f7", "size": 11 },
                    "bgcolor": "rgba(17,35,58,0.78)",
                    "bordercolor": "#213754",
                    "borderwidth": 1
                },
                "margin": { "l": 72, "r": 18, "t": 16, "b": 72 },
                "xaxis": {
                    "title": "crate version",
                    "type": "category",
                    "categoryorder": "array",
                    "categoryarray": crate_categories,
                    "showgrid": true,
                    "gridcolor": "#243c5d",
                    "linecolor": "#5e7da2",
                    "tickfont": { "color": "#8fa9be", "size": 11 },
                    "titlefont": { "color": "#8fa9be", "size": 12 }
                },
                "yaxis": {
                    "title": metric.label(),
                    "range": [y_min, y_max],
                    "showgrid": true,
                    "gridcolor": "#243c5d",
                    "linecolor": "#5e7da2",
                    "tickfont": { "color": "#8fa9be", "size": 11 },
                    "titlefont": { "color": "#8fa9be", "size": 12 }
                }
            }))
            .expect("serializing stdlib trend plot layout");
            let plot_config_json = serde_json::to_string(&json!({
                "displaylogo": false,
                "responsive": true,
                "scrollZoom": true,
                "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d"]
            }))
            .expect("serializing stdlib trend plot config");

            if !plotly_script_included {
                html.push_str("<script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>");
                plotly_script_included = true;
            }
            html.push_str("<div id=\"stdlib-trend-plot\" class=\"plotly-host\"></div>");
            let _ = write!(
                html,
                "<script>\
(() => {{\
  const plotEl = document.getElementById('stdlib-trend-plot');\
  if (!plotEl || typeof Plotly === 'undefined') {{ return; }}\
  const data = {};\
  const layout = {};\
  const config = {};\
  Plotly.newPlot(plotEl, data, layout, config).then(() => {{\
    plotEl.removeAllListeners('plotly_click');\
    plotEl.on('plotly_click', (evt) => {{\
      const point = evt && evt.points && evt.points[0];\
      if (!point) {{ return; }}\
      const custom = point.customdata;\
      const actionId = Array.isArray(custom) ? custom[1] : custom;\
      if (!actionId) {{ return; }}\
      const actionUrl = '/action/' + encodeURIComponent(String(actionId)) + '/';\
      window.open(actionUrl, '_blank', 'noopener,noreferrer');\
    }});\
  }});\
}})();\
</script>",
                plot_data_json, plot_layout_json, plot_config_json
            );
            let _ = write!(
                html,
                "<p class=\"plot-note\">Plotly: scroll to zoom, drag to pan, click a point to open its action in a new tab, click legend entries to isolate function traces. y-range={}..{}.</p>",
                html_escape(&format_metric_value(y_min)),
                html_escape(&format_metric_value(y_max))
            );
        }
    }
    html.push_str("</div></section>");

    html.push_str("<section class=\"panel\">");
    let _ = write!(
        html,
        "<h2 style=\"margin:0 0 8px;color:#5ec9ff;font-size:1rem;\">Distribution of Improvements ({})</h2>",
        dataset.series.len()
    );
    if dataset.series.is_empty() {
        html.push_str("<p class=\"meta\">No DSLX functions available for this filter.</p>");
    } else {
        const IMPROVEMENT_EPSILON: f64 = 1e-9;
        let mut improved_values: Vec<f64> = Vec::new();
        let mut regressed_values: Vec<f64> = Vec::new();
        let mut flat_count = 0usize;
        for series in &dataset.series {
            let Some(first) = series.points.first() else {
                continue;
            };
            let Some(last) = series.points.last() else {
                continue;
            };
            let first_value = metric.value(first);
            let last_value = metric.value(last);
            let raw_delta = last_value - first_value;
            let improvement = if metric.lower_is_better() {
                -raw_delta
            } else {
                raw_delta
            };
            if improvement > IMPROVEMENT_EPSILON {
                improved_values.push(improvement);
            } else if improvement < -IMPROVEMENT_EPSILON {
                regressed_values.push(improvement);
            } else {
                flat_count += 1;
            }
        }

        let _ = write!(
            html,
            "<p class=\"meta\">Improvement uses <code>{}</code> (positive = better) for {}</p>\
<div class=\"dist-stats\">\
<span class=\"dist-chip good\">improved: {}</span>\
<span class=\"dist-chip bad\">regressed: {}</span>\
<span class=\"dist-chip flat\">flat: {}</span>\
</div>",
            if metric.lower_is_better() {
                "first - latest"
            } else {
                "latest - first"
            },
            html_escape(metric.label()),
            improved_values.len(),
            regressed_values.len(),
            flat_count
        );

        if improved_values.is_empty() && regressed_values.is_empty() {
            html.push_str(
                "<p class=\"plot-note\">No non-zero changes across first/latest crate versions for this metric.</p>",
            );
        } else {
            let mut x_abs_max = improved_values
                .iter()
                .chain(regressed_values.iter())
                .copied()
                .map(f64::abs)
                .fold(0.0, f64::max);
            if x_abs_max < IMPROVEMENT_EPSILON {
                x_abs_max = 1.0;
            }
            let dist_data_json = serde_json::to_string(&vec![
                json!({
                    "type": "histogram",
                    "name": "improved",
                    "x": improved_values,
                    "marker": { "color": "#7dff9f" },
                    "opacity": 0.8,
                    "hovertemplate": "improvement=%{x}<br>functions=%{y}<extra>improved</extra>"
                }),
                json!({
                    "type": "histogram",
                    "name": "regressed",
                    "x": regressed_values,
                    "marker": { "color": "#ff8ea9" },
                    "opacity": 0.8,
                    "hovertemplate": "improvement=%{x}<br>functions=%{y}<extra>regressed</extra>"
                }),
            ])
            .expect("serializing stdlib improvement histogram data");
            let dist_layout_json = serde_json::to_string(&json!({
                "paper_bgcolor": "rgba(0,0,0,0)",
                "plot_bgcolor": "#0b172a",
                "hovermode": "closest",
                "showlegend": true,
                "barmode": "overlay",
                "legend": {
                    "orientation": "h",
                    "font": { "color": "#cde7f7", "size": 11 },
                    "bgcolor": "rgba(17,35,58,0.78)",
                    "bordercolor": "#213754",
                    "borderwidth": 1
                },
                "margin": { "l": 72, "r": 18, "t": 14, "b": 66 },
                "xaxis": {
                    "title": format!("{} improvement (positive = better)", metric.label()),
                    "range": [-(x_abs_max * 1.05), x_abs_max * 1.05],
                    "zeroline": true,
                    "zerolinecolor": "#5ec9ff",
                    "zerolinewidth": 1.4,
                    "showgrid": true,
                    "gridcolor": "#243c5d",
                    "linecolor": "#5e7da2",
                    "tickfont": { "color": "#8fa9be", "size": 11 },
                    "titlefont": { "color": "#8fa9be", "size": 12 }
                },
                "yaxis": {
                    "title": "functions",
                    "showgrid": true,
                    "gridcolor": "#243c5d",
                    "linecolor": "#5e7da2",
                    "tickfont": { "color": "#8fa9be", "size": 11 },
                    "titlefont": { "color": "#8fa9be", "size": 12 }
                }
            }))
            .expect("serializing stdlib improvement histogram layout");
            let dist_config_json = serde_json::to_string(&json!({
                "displaylogo": false,
                "responsive": true,
                "scrollZoom": false,
                "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d"]
            }))
            .expect("serializing stdlib improvement histogram config");

            if !plotly_script_included {
                html.push_str("<script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>");
            }
            html.push_str("<div class=\"plot-wrap\"><div id=\"stdlib-improvement-dist-plot\" class=\"plotly-host-sm\"></div></div>");
            let _ = write!(
                html,
                "<script>\
(() => {{\
  const plotEl = document.getElementById('stdlib-improvement-dist-plot');\
  if (!plotEl || typeof Plotly === 'undefined') {{ return; }}\
  const data = {};\
  const layout = {};\
  const config = {};\
  Plotly.newPlot(plotEl, data, layout, config);\
}})();\
</script>",
                dist_data_json, dist_layout_json, dist_config_json
            );
        }
    }
    html.push_str("</section>");

    html.push_str("<section class=\"panel\">");
    let _ = write!(
        html,
        "<h2 style=\"margin:0 0 8px;color:#5ec9ff;font-size:1rem;\">Per-Function Summary ({})</h2>",
        dataset.series.len()
    );
    if dataset.series.is_empty() {
        html.push_str("<p class=\"meta\">No DSLX functions available for this filter.</p>");
    } else {
        let mut rows: Vec<&StdlibFnTrendSeries> = dataset.series.iter().collect();
        rows.sort_by(|a, b| {
            let a_delta = match (a.points.first(), a.points.last()) {
                (Some(first), Some(last)) => metric.value(last) - metric.value(first),
                _ => 0.0,
            };
            let b_delta = match (b.points.first(), b.points.last()) {
                (Some(first), Some(last)) => metric.value(last) - metric.value(first),
                _ => 0.0,
            };
            b_delta
                .abs()
                .partial_cmp(&a_delta.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.fn_key.cmp(&b.fn_key))
        });
        html.push_str(
            "<table><thead><tr>\
<th>Function</th><th>Points</th><th>First</th><th>Latest</th><th>Delta</th><th>Latest Stats Action</th>\
</tr></thead><tbody>",
        );
        for series in rows {
            let Some(first) = series.points.first() else {
                continue;
            };
            let Some(last) = series.points.last() else {
                continue;
            };
            let first_value = metric.value(first);
            let last_value = metric.value(last);
            let delta = last_value - first_value;
            let delta_class = metric.delta_class(delta);
            let function_cell_html =
                if let Some((dslx_file, dslx_fn_name)) = series.fn_key.rsplit_once("::") {
                    let function_url =
                        stdlib_fn_history_view_url(dslx_file, dslx_fn_name, dataset.fraig, "asap7");
                    format!(
                        "<a href=\"{}\"><code>{}</code></a>",
                        html_escape(&function_url),
                        html_escape(&series.fn_key)
                    )
                } else {
                    format!("<code>{}</code>", html_escape(&series.fn_key))
                };
            let _ = write!(
                html,
                "<tr>\
<td>{}</td>\
<td>{}</td>\
<td>crate:v{} ({})</td>\
<td>crate:v{} dso:v{} ({})</td>\
<td class=\"{}\">{}</td>\
<td><code>{}</code></td>\
</tr>",
                function_cell_html,
                series.points.len(),
                html_escape(&first.crate_version),
                html_escape(&format_metric_value(first_value)),
                html_escape(&last.crate_version),
                html_escape(&last.dso_version),
                html_escape(&format_metric_value(last_value)),
                delta_class,
                html_escape(&format_metric_delta(delta)),
                html_escape(&last.stats_action_id)
            );
        }
        html.push_str("</tbody></table>");
    }
    html.push_str("</section>");
    html.push_str("</main></body></html>");
    html
}

pub(super) fn render_stdlib_fn_version_timeline_html(
    dataset: &StdlibFnVersionTimelineDataset,
    generated_utc: DateTime<Utc>,
) -> String {
    use std::fmt::Write;

    let mut html = String::new();
    html.push_str(
        "<!doctype html>\
<html lang=\"en\">\
<head>\
  <meta charset=\"utf-8\">\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
  <title>xlsynth-bvc DSLX Function Timeline</title>\
  <style>\
    :root {\
      --bg: #050911;\
      --bg2: #0a1323;\
      --panel: #0f1a2d;\
      --panel-hi: #14233b;\
      --ink: #dcffee;\
      --muted: #8fa9be;\
      --accent: #32f1b2;\
      --accent2: #5ec9ff;\
      --warn: #ff6d8f;\
      --line: #2c4566;\
      --border: #213754;\
      --chip: #11233a;\
    }\
    * { box-sizing: border-box; }\
    body {\
      margin: 0;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      color: var(--ink);\
      background:\
        radial-gradient(circle at 12% -15%, rgba(50, 241, 178, 0.18) 0, transparent 36%),\
        radial-gradient(circle at 100% 0%, rgba(94, 201, 255, 0.16) 0, transparent 40%),\
        repeating-linear-gradient(0deg, rgba(143, 169, 190, 0.055) 0, rgba(143, 169, 190, 0.055) 1px, transparent 1px, transparent 24px),\
        repeating-linear-gradient(90deg, rgba(143, 169, 190, 0.04) 0, rgba(143, 169, 190, 0.04) 1px, transparent 1px, transparent 24px),\
        linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);\
      min-height: 100vh;\
    }\
    .wrap { width: 100%; max-width: none; margin: 0; padding: 24px 18px 34px; }\
    .head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 10px 16px; }\
    .title {\
      margin: 0;\
      font-size: 1.8rem;\
      text-transform: uppercase;\
      letter-spacing: 0.04em;\
      color: var(--accent);\
      text-shadow: 0 0 12px rgba(50, 241, 178, 0.34);\
    }\
    .meta {\
      margin: 6px 0 0;\
      color: var(--muted);\
      font-size: 0.86rem;\
      border-left: 3px solid var(--line);\
      padding-left: 10px;\
    }\
    .nav { margin-top: 10px; display: flex; gap: 9px; flex-wrap: wrap; }\
    .nav a {\
      color: #b9e8ff;\
      text-decoration: none;\
      border: 1px solid var(--border);\
      background: rgba(16, 34, 56, 0.8);\
      border-radius: 6px;\
      padding: 5px 9px;\
      font-size: 0.82rem;\
    }\
    .controls { margin-top: 14px; display: flex; flex-wrap: wrap; gap: 10px 14px; align-items: center; }\
    .control-group { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }\
    .label { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; }\
    .chip {\
      color: #cde7f7;\
      text-decoration: none;\
      border: 1px solid var(--border);\
      background: var(--chip);\
      border-radius: 999px;\
      padding: 4px 10px;\
      font-size: 0.78rem;\
    }\
    .chip.active {\
      color: var(--accent);\
      border-color: rgba(50, 241, 178, 0.55);\
      box-shadow: 0 0 14px rgba(50, 241, 178, 0.18);\
    }\
    .panel {\
      margin-top: 14px;\
      background: linear-gradient(180deg, var(--panel-hi) 0%, var(--panel) 100%);\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      padding: 11px 12px 12px;\
      box-shadow:\
        inset 0 1px 0 rgba(255, 255, 255, 0.03),\
        0 0 0 1px rgba(50, 241, 178, 0.04),\
        0 12px 28px rgba(0, 0, 0, 0.35);\
    }\
    .plot-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(520px, 1fr)); gap: 12px; }\
    .plot-panel { border: 1px solid var(--border); border-radius: 8px; padding: 8px; background: rgba(8, 18, 34, 0.6); }\
    .plot-title { margin: 0 0 6px; font-size: 0.88rem; color: #a6ddff; }\
    .plotly-host { width: 100%; min-height: 380px; }\
    table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.82rem; }\
    th, td { text-align: left; border-top: 1px solid var(--border); padding: 7px 6px; vertical-align: top; }\
    th { color: var(--muted); text-transform: uppercase; letter-spacing: 0.045em; font-size: 0.72rem; }\
    tbody tr:nth-child(even) td { background: rgba(255, 255, 255, 0.012); }\
    code {\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
      color: #bbffda;\
      background: rgba(17, 35, 58, 0.72);\
      border: 1px solid var(--border);\
      border-radius: 4px;\
      padding: 1px 4px;\
    }\
    .warn { color: #ffb7c7; }\
    @media (max-width: 860px) {\
      table, thead, tbody, th, td, tr { display: block; }\
      thead { display: none; }\
      tr { border-top: 1px solid var(--border); padding-top: 8px; margin-top: 8px; }\
      td { border: none; padding: 2px 0; }\
    }\
  </style>\
</head>\
<body>\
  <main class=\"wrap\">",
    );

    let display_file = dataset
        .dslx_file
        .as_deref()
        .unwrap_or(dataset.file_selector.as_str());
    let selector_label = format!("{}:{}", display_file, dataset.dslx_fn_name);
    let generated = generated_utc.to_rfc3339();
    let fraig_false_url = stdlib_fn_history_view_url(
        display_file,
        &dataset.dslx_fn_name,
        false,
        &dataset.delay_model,
    );
    let fraig_true_url = stdlib_fn_history_view_url(
        display_file,
        &dataset.dslx_fn_name,
        true,
        &dataset.delay_model,
    );
    let g8r_file_url = stdlib_fns_trend_view_url(
        StdlibTrendKind::G8r,
        StdlibMetric::AndNodes,
        dataset.fraig,
        dataset.dslx_file.as_deref(),
    );
    let yosys_file_url = stdlib_fns_trend_view_url(
        StdlibTrendKind::YosysAbc,
        StdlibMetric::AndNodes,
        false,
        dataset.dslx_file.as_deref(),
    );
    let compare_url = stdlib_g8r_vs_yosys_view_url(
        dataset.fraig,
        None,
        dataset.crate_versions.last().map(String::as_str),
    );
    let corpus_compare_url =
        ir_fn_corpus_g8r_vs_yosys_view_url(None, dataset.crate_versions.last().map(String::as_str));
    let corpus_frontend_compare_url = ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_view_url(
        None,
        dataset.crate_versions.last().map(String::as_str),
    );
    let file_graph_url = stdlib_file_action_graph_view_url(
        dataset.crate_versions.last().map(String::as_str),
        dataset.dslx_file.as_deref(),
        Some(&dataset.dslx_fn_name),
        None,
        false,
    );
    let _ = write!(
        html,
        "<section class=\"head\"><h1 class=\"title\">DSLX Function Timeline</h1></section>\
<p class=\"meta\">generated={} | selector=<code>{}</code> | matched_files={} | versions={} | fraig={} | delay_model={}</p>\
<nav class=\"nav\">\
<a href=\"/versions/\">/versions/</a>\
<a href=\"/dslx-fns/\">/dslx-fns/</a>\
<a href=\"{}\">/dslx-fns-g8r/</a>\
<a href=\"{}\">/dslx-fns-yosys-abc/</a>\
<a href=\"{}\">/dslx-fns-g8r-vs-yosys-abc/</a>\
<a href=\"{}\">/ir-fn-corpus-g8r-vs-yosys-abc/</a>\
<a href=\"{}\">/ir-fn-g8r-abc-vs-codegen-yosys-abc/</a>\
<a href=\"{}\">/dslx-file-action-graph/</a>\
<a href=\"/ir-fn-corpus-structural/\">/ir-fn-corpus-structural/</a>\
</nav>",
        html_escape(&generated),
        html_escape(&selector_label),
        dataset.matched_files.len(),
        dataset.crate_versions.len(),
        if dataset.fraig { "true" } else { "false" },
        html_escape(&dataset.delay_model),
        html_escape(&g8r_file_url),
        html_escape(&yosys_file_url),
        html_escape(&compare_url),
        html_escape(&corpus_compare_url),
        html_escape(&corpus_frontend_compare_url),
        html_escape(&file_graph_url),
    );

    let _ = write!(
        html,
        "<section class=\"controls\">\
<div class=\"control-group\"><span class=\"label\">Fraig</span>\
<a class=\"chip {}\" href=\"{}\">false</a>\
<a class=\"chip {}\" href=\"{}\">true</a></div>\
<div class=\"control-group\"><span class=\"label\">Delay Model</span><span class=\"chip active\">{}</span></div>\
</section>",
        if !dataset.fraig { "active" } else { "" },
        html_escape(&fraig_false_url),
        if dataset.fraig { "active" } else { "" },
        html_escape(&fraig_true_url),
        html_escape(&dataset.delay_model),
    );

    if dataset.matched_files.is_empty() {
        let _ = write!(
            html,
            "<section class=\"panel\"><p class=\"warn\">No DSLX file matched selector <code>{}</code>. Use either a basename like <code>float32.x</code> or full path like <code>xls/dslx/stdlib/float32.x</code>.</p>",
            html_escape(&dataset.file_selector)
        );
        if !dataset.available_files.is_empty() {
            html.push_str("<p class=\"meta\">Available DSLX files (sample):</p><p class=\"meta\">");
            for (i, file) in dataset.available_files.iter().take(24).enumerate() {
                if i > 0 {
                    html.push_str(" | ");
                }
                let _ = write!(html, "<code>{}</code>", html_escape(file));
            }
            html.push_str("</p>");
        }
        html.push_str("</section></main></body></html>");
        return html;
    }

    if !dataset.available_functions_for_file.is_empty() {
        html.push_str("<section class=\"panel\"><p class=\"meta\">Functions in selected file:</p><p class=\"meta\">");
        for (i, fn_name) in dataset.available_functions_for_file.iter().enumerate() {
            if i > 0 {
                html.push_str(" | ");
            }
            let fn_url = stdlib_fn_history_view_url(
                display_file,
                fn_name,
                dataset.fraig,
                &dataset.delay_model,
            );
            if fn_name == &dataset.dslx_fn_name {
                let _ = write!(html, "<code>{}</code>", html_escape(fn_name));
            } else {
                let _ = write!(
                    html,
                    "<a href=\"{}\"><code>{}</code></a>",
                    html_escape(&fn_url),
                    html_escape(fn_name)
                );
            }
        }
        html.push_str("</p></section>");
    }

    if dataset.points.is_empty() {
        let _ = write!(
            html,
            "<section class=\"panel\"><p class=\"warn\">No timeline points found for <code>{}</code> with fraig={} and delay_model=<code>{}</code>.</p></section>",
            html_escape(&selector_label),
            if dataset.fraig { "true" } else { "false" },
            html_escape(&dataset.delay_model),
        );
        html.push_str("</main></body></html>");
        return html;
    }

    let points_json = serde_json::to_string(&dataset.points).expect("serializing timeline points");
    let version_categories: Vec<String> = dataset
        .crate_versions
        .iter()
        .map(|v| format!("v{v}"))
        .collect();
    let version_categories_json =
        serde_json::to_string(&version_categories).expect("serializing version categories");
    html.push_str("<section class=\"panel\">\
<div class=\"plot-grid\">\
<div class=\"plot-panel\"><h2 class=\"plot-title\">IR delay (combinational_critical_path.total_delay_ps)</h2><div id=\"plot-ir-delay\" class=\"plotly-host\"></div></div>\
<div class=\"plot-panel\"><h2 class=\"plot-title\">AIG and nodes: g8r vs yosys/abc</h2><div id=\"plot-aig-nodes\" class=\"plotly-host\"></div></div>\
<div class=\"plot-panel\"><h2 class=\"plot-title\">AIG levels: g8r vs yosys/abc</h2><div id=\"plot-aig-levels\" class=\"plotly-host\"></div></div>\
</div>\
<p class=\"meta\">Plotly: click a point to open the corresponding action in a new tab.</p>\
</section>");
    html.push_str("<script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>");
    let _ = write!(
        html,
        "<script>\
(() => {{\
  if (typeof Plotly === 'undefined') {{ return; }}\
  const points = {};\
  const categories = {};\
  const x = points.map((p) => `v${{p.crate_version}}`);\
  const plotConfig = {{\
    displaylogo: false,\
    responsive: true,\
    scrollZoom: true,\
    modeBarButtonsToRemove: ['select2d', 'lasso2d', 'autoScale2d']\
  }};\
  const baseLayout = (yTitle) => ({{\
    paper_bgcolor: 'rgba(0,0,0,0)',\
    plot_bgcolor: '#0b172a',\
    hovermode: 'closest',\
    margin: {{ l: 72, r: 18, t: 14, b: 72 }},\
    legend: {{\
      orientation: 'h',\
      font: {{ color: '#cde7f7', size: 11 }},\
      bgcolor: 'rgba(17,35,58,0.78)',\
      bordercolor: '#213754',\
      borderwidth: 1\
    }},\
    xaxis: {{\
      title: 'crate version',\
      type: 'category',\
      categoryorder: 'array',\
      categoryarray: categories,\
      showgrid: true,\
      gridcolor: '#243c5d',\
      linecolor: '#5e7da2',\
      tickfont: {{ color: '#8fa9be', size: 11 }},\
      titlefont: {{ color: '#8fa9be', size: 12 }}\
    }},\
    yaxis: {{\
      title: yTitle,\
      showgrid: true,\
      gridcolor: '#243c5d',\
      linecolor: '#5e7da2',\
      tickfont: {{ color: '#8fa9be', size: 11 }},\
      titlefont: {{ color: '#8fa9be', size: 12 }}\
    }}\
  }});\
\
  const bindClick = (plotId) => {{\
    const plotEl = document.getElementById(plotId);\
    if (!plotEl) {{ return; }}\
    plotEl.removeAllListeners('plotly_click');\
    plotEl.on('plotly_click', (evt) => {{\
      const point = evt && evt.points && evt.points[0];\
      if (!point) {{ return; }}\
      const actionId = point.customdata;\
      if (!actionId) {{ return; }}\
      window.open('/action/' + encodeURIComponent(String(actionId)) + '/', '_blank', 'noopener,noreferrer');\
    }});\
  }};\
\
  const trace = (name, yField, actionField, color) => ({{\
    type: 'scatter',\
    mode: 'lines+markers',\
    name,\
    x,\
    y: points.map((p) => p[yField]),\
    customdata: points.map((p) => p[actionField] || ''),\
    connectgaps: false,\
    line: {{ width: 2, color }},\
    marker: {{ size: 6 }},\
    hovertemplate: `${{name}}<br>crate:%{{x}}<br>value:%{{y}}<extra></extra>`\
  }});\
\
  Plotly.newPlot(\
    'plot-ir-delay',\
    [trace('ir delay ps', 'ir_delay_ps', 'ir_delay_action_id', '#5ec9ff')],\
    baseLayout('delay (ps)'),\
    plotConfig,\
  ).then(() => bindClick('plot-ir-delay'));\
\
  Plotly.newPlot(\
    'plot-aig-nodes',\
    [\
      trace('g8r and_nodes', 'g8r_and_nodes', 'g8r_stats_action_id', '#32f1b2'),\
      trace('yosys/abc and_nodes', 'yosys_abc_and_nodes', 'yosys_abc_stats_action_id', '#ffb454'),\
    ],\
    baseLayout('and nodes'),\
    plotConfig,\
  ).then(() => bindClick('plot-aig-nodes'));\
\
  Plotly.newPlot(\
    'plot-aig-levels',\
    [\
      trace('g8r levels', 'g8r_levels', 'g8r_stats_action_id', '#00d9ff'),\
      trace('yosys/abc levels', 'yosys_abc_levels', 'yosys_abc_stats_action_id', '#ff6d8f'),\
    ],\
    baseLayout('levels'),\
    plotConfig,\
  ).then(() => bindClick('plot-aig-levels'));\
}})();\
</script>",
        points_json, version_categories_json
    );

    html.push_str("<section class=\"panel\"><h2 class=\"plot-title\">Per-Version Data</h2><table><thead><tr>\
<th>crate</th><th>IR delay ps</th><th>g8r and_nodes</th><th>g8r levels</th><th>yosys and_nodes</th><th>yosys levels</th>\
</tr></thead><tbody>");
    for point in &dataset.points {
        let ir_delay = point
            .ir_delay_ps
            .map(format_metric_value)
            .unwrap_or_else(|| "-".to_string());
        let g8r_nodes = point
            .g8r_and_nodes
            .map(format_metric_value)
            .unwrap_or_else(|| "-".to_string());
        let g8r_levels = point
            .g8r_levels
            .map(format_metric_value)
            .unwrap_or_else(|| "-".to_string());
        let yosys_nodes = point
            .yosys_abc_and_nodes
            .map(format_metric_value)
            .unwrap_or_else(|| "-".to_string());
        let yosys_levels = point
            .yosys_abc_levels
            .map(format_metric_value)
            .unwrap_or_else(|| "-".to_string());
        let ir_action_html = point
            .ir_delay_action_id
            .as_ref()
            .map(|id| {
                format!(
                    "<a href=\"/action/{}/\" target=\"_blank\" rel=\"noopener noreferrer\"><code>{}</code></a>",
                    html_escape(id),
                    html_escape(short_id(id))
                )
            })
            .unwrap_or_else(|| "-".to_string());
        let g8r_action_html = point
            .g8r_stats_action_id
            .as_ref()
            .map(|id| {
                format!(
                    "<a href=\"/action/{}/\" target=\"_blank\" rel=\"noopener noreferrer\"><code>{}</code></a>",
                    html_escape(id),
                    html_escape(short_id(id))
                )
            })
            .unwrap_or_else(|| "-".to_string());
        let yosys_action_html = point
            .yosys_abc_stats_action_id
            .as_ref()
            .map(|id| {
                format!(
                    "<a href=\"/action/{}/\" target=\"_blank\" rel=\"noopener noreferrer\"><code>{}</code></a>",
                    html_escape(id),
                    html_escape(short_id(id))
                )
            })
            .unwrap_or_else(|| "-".to_string());
        let _ = write!(
            html,
            "<tr>\
<td><code>v{}</code></td>\
<td>{} <span class=\"meta\">{}</span></td>\
<td>{} <span class=\"meta\">{}</span></td>\
<td>{}</td>\
<td>{} <span class=\"meta\">{}</span></td>\
<td>{}</td>\
</tr>",
            html_escape(&point.crate_version),
            html_escape(&ir_delay),
            ir_action_html,
            html_escape(&g8r_nodes),
            g8r_action_html,
            html_escape(&g8r_levels),
            html_escape(&yosys_nodes),
            yosys_action_html,
            html_escape(&yosys_levels),
        );
    }
    html.push_str("</tbody></table></section></main></body></html>");
    html
}

pub(super) fn render_stdlib_g8r_vs_yosys_html(
    dataset: &StdlibG8rVsYosysDataset,
    max_ir_nodes: u64,
    selected_crate_version: Option<&str>,
    losses_only: bool,
    generated_utc: DateTime<Utc>,
    scope: G8rVsYosysViewScope,
) -> String {
    use std::fmt::Write;

    let mut html = String::new();
    html.push_str(
        "<!doctype html>\
<html lang=\"en\">\
<head>\
  <meta charset=\"utf-8\">\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
  <title>xlsynth-bvc G8r vs Yosys/ABC</title>\
  <style>\
    :root {\
      --bg: #050911;\
      --bg2: #0a1323;\
      --panel: #0f1a2d;\
      --panel-hi: #14233b;\
      --ink: #dcffee;\
      --muted: #8fa9be;\
      --accent: #32f1b2;\
      --accent2: #5ec9ff;\
      --warn: #ff6d8f;\
      --line: #2c4566;\
      --border: #213754;\
      --chip: #11233a;\
    }\
    * { box-sizing: border-box; }\
    body {\
      margin: 0;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      color: var(--ink);\
      background:\
        radial-gradient(circle at 12% -15%, rgba(50, 241, 178, 0.18) 0, transparent 36%),\
        radial-gradient(circle at 100% 0%, rgba(94, 201, 255, 0.16) 0, transparent 40%),\
        repeating-linear-gradient(0deg, rgba(143, 169, 190, 0.055) 0, rgba(143, 169, 190, 0.055) 1px, transparent 1px, transparent 24px),\
        repeating-linear-gradient(90deg, rgba(143, 169, 190, 0.04) 0, rgba(143, 169, 190, 0.04) 1px, transparent 1px, transparent 24px),\
        linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);\
      min-height: 100vh;\
    }\
    .wrap { width: 100%; max-width: none; margin: 0; padding: 24px 18px 34px; }\
    .head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 10px 16px; }\
    .title {\
      margin: 0;\
      font-size: 1.8rem;\
      text-transform: uppercase;\
      letter-spacing: 0.04em;\
      color: var(--accent);\
      text-shadow: 0 0 12px rgba(50, 241, 178, 0.34);\
    }\
    .meta {\
      margin: 6px 0 0;\
      color: var(--muted);\
      font-size: 0.86rem;\
      border-left: 3px solid var(--line);\
      padding-left: 10px;\
    }\
    .nav { margin-top: 10px; display: flex; gap: 9px; flex-wrap: wrap; }\
    .nav a {\
      color: #b9e8ff;\
      text-decoration: none;\
      border: 1px solid var(--border);\
      background: rgba(16, 34, 56, 0.8);\
      border-radius: 6px;\
      padding: 5px 9px;\
      font-size: 0.82rem;\
    }\
    .nav a.active {\
      color: var(--accent);\
      border-color: rgba(50, 241, 178, 0.55);\
      box-shadow: 0 0 0 1px rgba(50, 241, 178, 0.15) inset;\
    }\
    .controls {\
      margin-top: 14px;\
      display: flex;\
      flex-wrap: wrap;\
      gap: 10px 14px;\
      align-items: center;\
    }\
    .control-group { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }\
    .label { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; }\
    .chip {\
      color: #cde7f7;\
      text-decoration: none;\
      border: 1px solid var(--border);\
      background: var(--chip);\
      border-radius: 999px;\
      padding: 4px 10px;\
      font-size: 0.78rem;\
    }\
    .chip.active {\
      color: var(--accent);\
      border-color: rgba(50, 241, 178, 0.55);\
      box-shadow: 0 0 14px rgba(50, 241, 178, 0.18);\
    }\
    .select-input {\
      min-width: 240px;\
      color: #d7f6ff;\
      background: #102238;\
      border: 1px solid var(--border);\
      border-radius: 6px;\
      padding: 5px 9px;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
    }\
    .select-input:focus { outline: 1px solid rgba(50, 241, 178, 0.55); }\
    .range-input {\
      width: min(520px, 88vw);\
      accent-color: #32f1b2;\
    }\
    .range-value {\
      color: #7dff9f;\
      border: 1px solid rgba(125, 255, 159, 0.35);\
      border-radius: 6px;\
      padding: 3px 8px;\
      background: rgba(16, 40, 32, 0.35);\
    }\
    .panel {\
      margin-top: 14px;\
      background: linear-gradient(180deg, var(--panel-hi) 0%, var(--panel) 100%);\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      padding: 11px 12px 12px;\
      box-shadow:\
        inset 0 1px 0 rgba(255, 255, 255, 0.03),\
        0 0 0 1px rgba(50, 241, 178, 0.04),\
        0 12px 28px rgba(0, 0, 0, 0.35);\
    }\
    .plot-grid {\
      display: grid;\
      grid-template-columns: repeat(auto-fit, minmax(520px, 1fr));\
      gap: 12px;\
    }\
    .plot-panel { border: 1px solid var(--border); border-radius: 8px; padding: 8px; background: rgba(8, 18, 34, 0.6); }\
    .plot-title { margin: 0 0 6px; font-size: 0.88rem; color: #a6ddff; }\
    .plotly-host { width: 100%; min-height: 400px; }\
    .status-note { margin-top: 8px; color: var(--muted); font-size: 0.8rem; }\
    .detail-title { margin: 0 0 8px; color: #5ec9ff; font-size: 0.95rem; }\
    .detail-empty { color: var(--muted); font-size: 0.83rem; }\
    .detail-pre {\
      margin: 0;\
      white-space: pre-wrap;\
      word-break: break-word;\
      color: #bbffda;\
      background: rgba(17, 35, 58, 0.72);\
      border: 1px solid var(--border);\
      border-radius: 6px;\
      padding: 10px;\
      font-size: 0.78rem;\
    }\
    .ir-full-wrap { margin-top: 8px; }\
    .ir-full-wrap > summary {\
      cursor: pointer;\
      color: #8fa9be;\
      font-size: 0.8rem;\
      margin-bottom: 8px;\
    }\
    code {\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
      color: #bbffda;\
      background: rgba(17, 35, 58, 0.72);\
      border: 1px solid var(--border);\
      border-radius: 4px;\
      padding: 1px 4px;\
    }\
  </style>\
</head>\
<body>\
  <main class=\"wrap\">",
    );

    let generated = html_escape(&generated_utc.to_rfc3339());
    let effective_fraig = if scope.supports_fraig_toggle() {
        dataset.fraig
    } else {
        false
    };
    let lhs_label = scope.lhs_label();
    let rhs_label = scope.rhs_label();
    let g8r_trend_url = stdlib_fns_trend_view_url(
        StdlibTrendKind::G8r,
        StdlibMetric::AndNodes,
        effective_fraig,
        None,
    );
    let yosys_trend_url = stdlib_fns_trend_view_url(
        StdlibTrendKind::YosysAbc,
        StdlibMetric::AndNodes,
        false,
        None,
    );
    let file_graph_url =
        stdlib_file_action_graph_view_url(selected_crate_version, None, None, None, false);
    fn with_losses_only_query(url: String, losses_only: bool) -> String {
        if !losses_only {
            return url;
        }
        let mut out = url;
        out.push(if out.contains('?') { '&' } else { '?' });
        out.push_str("losses_only=true");
        out
    }
    let fraig_false_url = with_losses_only_query(
        stdlib_g8r_vs_yosys_view_url(false, Some(max_ir_nodes), selected_crate_version),
        losses_only,
    );
    let fraig_true_url = with_losses_only_query(
        stdlib_g8r_vs_yosys_view_url(true, Some(max_ir_nodes), selected_crate_version),
        losses_only,
    );
    let corpus_compare_url = with_losses_only_query(
        ir_fn_corpus_g8r_vs_yosys_view_url(Some(max_ir_nodes), selected_crate_version),
        losses_only,
    );
    let default_k3_max_ir_nodes = crate::default_k_bool_cone_max_ir_ops_for_k(3).unwrap_or(16);
    let frontend_compare_url = with_losses_only_query(
        ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_view_url(
            Some(max_ir_nodes),
            selected_crate_version,
        ),
        losses_only,
    );
    let k3_loss_version_url = ir_fn_corpus_k3_losses_vs_crate_version_view_url(
        Some(max_ir_nodes.min(default_k3_max_ir_nodes)),
        selected_crate_version,
        false,
    );
    let stdlib_compare_url = with_losses_only_query(
        stdlib_g8r_vs_yosys_view_url(effective_fraig, Some(max_ir_nodes), selected_crate_version),
        losses_only,
    );
    let lhs_label_json = serde_json::to_string(lhs_label).expect("serializing lhs label");
    let rhs_label_json = serde_json::to_string(rhs_label).expect("serializing rhs label");
    let levels_plot_title = format!("{lhs_label} levels vs {rhs_label} levels");
    let nodes_plot_title = format!("{lhs_label} nodes vs {rhs_label} nodes");
    let delta_plot_title =
        format!("quadrant deltas: levels vs nodes (delta = {rhs_label} - {lhs_label})");
    let loss_plot_title = format!("{lhs_label} product loss amount vs IR node count");
    let zero_display_note = format!(
        "For the log/log levels and nodes plots, zero values are displayed as <code>1</code> so those samples remain visible ({lhs_label} vs {rhs_label})."
    );
    let samples_json =
        serde_json::to_string(&dataset.samples).expect("serializing g8r-vs-yosys samples");
    let selected_crate_json =
        serde_json::to_string(&selected_crate_version).expect("serializing selected crate version");
    let scope_label = selected_crate_version
        .map(|v| format!("crate:v{v}"))
        .unwrap_or_else(|| {
            format!(
                "all crate versions ({})",
                dataset.available_crate_versions.len()
            )
        });

    let _ = write!(
        html,
        "<section class=\"head\"><h1 class=\"title\">{}</h1></section>\
<p class=\"meta\">generated={} | fraig={} | paired_samples_total={} | {}_only={} | {}_only={} | crate_versions={} | ir_nodes={}..{} | scope={}</p>",
        html_escape(scope.title()),
        generated,
        if effective_fraig { "true" } else { "false" },
        dataset.samples.len(),
        html_escape(lhs_label),
        dataset.g8r_only_count,
        html_escape(rhs_label),
        dataset.yosys_only_count,
        dataset.available_crate_versions.len(),
        dataset.min_ir_nodes,
        dataset.max_ir_nodes,
        html_escape(&scope_label)
    );
    let _ = write!(
        html,
        "<nav class=\"nav\">\
<a href=\"/versions/\">/versions/</a>\
<a href=\"/dslx-fns/\">/dslx-fns/</a>\
<a href=\"{}\">/dslx-fns-g8r/</a>\
<a href=\"{}\">/dslx-fns-yosys-abc/</a>\
<a class=\"{}\" href=\"{}\">/dslx-fns-g8r-vs-yosys-abc/</a>\
<a class=\"{}\" href=\"{}\">/ir-fn-corpus-g8r-vs-yosys-abc/</a>\
<a class=\"{}\" href=\"{}\">/ir-fn-g8r-abc-vs-codegen-yosys-abc/</a>\
<a href=\"{}\">/ir-fn-corpus-k3-losses-vs-crate-version/</a>\
<a href=\"{}\">/dslx-file-action-graph/</a>\
<a href=\"/ir-fn-corpus-structural/\">/ir-fn-corpus-structural/</a>\
</nav>",
        html_escape(&g8r_trend_url),
        html_escape(&yosys_trend_url),
        if scope == G8rVsYosysViewScope::Stdlib {
            "active"
        } else {
            ""
        },
        html_escape(&stdlib_compare_url),
        if scope == G8rVsYosysViewScope::IrFnCorpus {
            "active"
        } else {
            ""
        },
        html_escape(&corpus_compare_url),
        if scope == G8rVsYosysViewScope::IrFnCorpusG8rAbcVsCodegenYosysAbc {
            "active"
        } else {
            ""
        },
        html_escape(&frontend_compare_url),
        html_escape(&k3_loss_version_url),
        html_escape(&file_graph_url)
    );
    if scope.supports_fraig_toggle() {
        let _ = write!(
            html,
            "<section class=\"controls\">\
<div class=\"control-group\"><span class=\"label\">Fraig</span>\
<a class=\"chip {}\" href=\"{}\">false</a>\
<a class=\"chip {}\" href=\"{}\">true</a></div>\
</section>",
            if !effective_fraig { "active" } else { "" },
            html_escape(&fraig_false_url),
            if effective_fraig { "active" } else { "" },
            html_escape(&fraig_true_url)
        );
    } else {
        html.push_str(
            "<section class=\"controls\">\
<div class=\"control-group\"><span class=\"label\">Fraig</span>\
<span class=\"chip active\">false (corpus default)</span></div>\
</section>",
        );
    }

    let slider_disabled = if dataset.samples.is_empty() {
        "disabled"
    } else {
        ""
    };
    let _ = write!(
        html,
        "<section class=\"panel\">\
<div class=\"controls\">\
<div class=\"control-group\"><span class=\"label\">Crate Version Scope</span>\
<select id=\"crate-version-select\" class=\"select-input\">\
<option value=\"\" {}>all crate versions</option>",
        if selected_crate_version.is_none() {
            "selected"
        } else {
            ""
        },
    );
    for version in &dataset.available_crate_versions {
        let selected_attr = if Some(version.as_str()) == selected_crate_version {
            "selected"
        } else {
            ""
        };
        let _ = write!(
            html,
            "<option value=\"{}\" {}>crate:v{}</option>",
            html_escape(version),
            selected_attr,
            html_escape(version)
        );
    }
    let _ = write!(
        html,
        "</select></div>\
<div class=\"control-group\"><span class=\"label\">Sample Mode</span>\
<select id=\"losses-only-select\" class=\"select-input\">\
<option value=\"all\" {}>all samples</option>\
<option value=\"losses_only\" {}>show losses only</option>\
</select></div>\
<div class=\"control-group\"><span class=\"label\">IR Node Filter</span>\
<span class=\"status-note\">show samples where <code>ir_node_count <= threshold</code></span></div>\
<div class=\"control-group\">\
<input id=\"ir-nodes-max\" class=\"range-input\" type=\"range\" min=\"{}\" max=\"{}\" value=\"{}\" step=\"1\" {}>\
<span class=\"range-value\" id=\"ir-nodes-max-value\">{}</span>\
</div>\
</div>\
<p class=\"status-note\">current scope: <span id=\"scope-label\">{}</span></p>\
<p class=\"status-note\">filtered candidates (before log-domain filtering): <span id=\"filtered-count\">0</span> / <span id=\"total-count\">{}</span></p>\
<p class=\"status-note\">loss-plot positive-loss samples: <span id=\"loss-positive-count\">0</span></p>\
<p id=\"no-data-note\" class=\"status-note\" style=\"display:none;\">No paired samples match the current IR-node threshold.</p>\
</section>",
        if !losses_only { "selected" } else { "" },
        if losses_only { "selected" } else { "" },
        dataset.min_ir_nodes,
        dataset.max_ir_nodes,
        max_ir_nodes,
        slider_disabled,
        max_ir_nodes,
        html_escape(&scope_label),
        dataset.samples.len()
    );
    let _ = write!(
        html,
        "<section class=\"panel\">\
<div class=\"plot-grid\">\
<div class=\"plot-panel\"><h2 class=\"plot-title\">{}</h2><div id=\"plot-levels\" class=\"plotly-host\"></div></div>\
<div class=\"plot-panel\"><h2 class=\"plot-title\">{}</h2><div id=\"plot-nodes\" class=\"plotly-host\"></div></div>\
	<div class=\"plot-panel\"><h2 class=\"plot-title\">{}</h2><div id=\"plot-delta-quadrant\" class=\"plotly-host\"></div></div>\
		<div class=\"plot-panel\"><h2 class=\"plot-title\">{}</h2><div id=\"plot-loss-vs-ir\" class=\"plotly-host\"></div></div>\
		</div>\
<p class=\"status-note\">{}</p>\
		</section>",
        html_escape(&levels_plot_title),
        html_escape(&nodes_plot_title),
        html_escape(&delta_plot_title),
        html_escape(&loss_plot_title),
        zero_display_note
    );
    html.push_str(
        "<section class=\"panel\">\
<h2 class=\"detail-title\">Selected Sample</h2>\
<p id=\"sample-detail-empty\" class=\"detail-empty\">Click any point in any plot to inspect the sample details.</p>\
<pre id=\"sample-detail-json\" class=\"detail-pre\" style=\"display:none;\"></pre>\
<h3 class=\"detail-title\" style=\"margin-top:10px;\">Associated IR</h3>\
<p id=\"sample-ir-meta\" class=\"detail-empty\" style=\"display:none;\"></p>\
<p id=\"sample-ir-focus-meta\" class=\"detail-empty\" style=\"display:none;\"></p>\
<pre id=\"sample-ir-focus-text\" class=\"detail-pre\" style=\"display:none;\"></pre>\
<details id=\"sample-ir-full-wrap\" class=\"ir-full-wrap\" style=\"display:none;\">\
<summary id=\"sample-ir-full-summary\">Full IR package</summary>\
<pre id=\"sample-ir-text\" class=\"detail-pre\" style=\"display:none;\"></pre>\
</details>\
<h3 class=\"detail-title\" style=\"margin-top:10px;\">Origin DSLX (.x)</h3>\
<p id=\"sample-dslx-meta\" class=\"detail-empty\" style=\"display:none;\"></p>\
<pre id=\"sample-dslx-text\" class=\"detail-pre\" style=\"display:none;\"></pre>\
</section>",
    );

    html.push_str("<script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>");
    html.push_str("<script>\n");
    html.push_str("const allSamples = ");
    html.push_str(&samples_json);
    html.push_str(";\nconst fraig = ");
    html.push_str(if effective_fraig { "true" } else { "false" });
    html.push_str(";\nconst allowFraigToggle = ");
    html.push_str(if scope.supports_fraig_toggle() {
        "true"
    } else {
        "false"
    });
    html.push_str(";\nlet lossesOnly = ");
    html.push_str(if losses_only { "true" } else { "false" });
    html.push_str(";\nconst viewScopePath = ");
    html.push_str(
        &serde_json::to_string(scope.route_label()).expect("serializing compare scope route"),
    );
    html.push_str(";\nlet selectedCrateVersion = ");
    html.push_str(&selected_crate_json);
    html.push_str(";\n");
    html.push_str("const lhsLabel = ");
    html.push_str(&lhs_label_json);
    html.push_str(";\nconst rhsLabel = ");
    html.push_str(&rhs_label_json);
    html.push_str(";\n");
    html.push_str(
        r##"
const sliderEl = document.getElementById('ir-nodes-max');
const sliderValueEl = document.getElementById('ir-nodes-max-value');
const filteredCountEl = document.getElementById('filtered-count');
const totalCountEl = document.getElementById('total-count');
const lossPositiveCountEl = document.getElementById('loss-positive-count');
const crateSelectEl = document.getElementById('crate-version-select');
const lossesOnlySelectEl = document.getElementById('losses-only-select');
const scopeLabelEl = document.getElementById('scope-label');
const noDataEl = document.getElementById('no-data-note');
const detailEmptyEl = document.getElementById('sample-detail-empty');
const detailJsonEl = document.getElementById('sample-detail-json');
const detailIrMetaEl = document.getElementById('sample-ir-meta');
const detailIrFocusMetaEl = document.getElementById('sample-ir-focus-meta');
const detailIrFocusTextEl = document.getElementById('sample-ir-focus-text');
const detailIrFullWrapEl = document.getElementById('sample-ir-full-wrap');
const detailIrFullSummaryEl = document.getElementById('sample-ir-full-summary');
const detailIrTextEl = document.getElementById('sample-ir-text');
const detailDslxMetaEl = document.getElementById('sample-dslx-meta');
const detailDslxTextEl = document.getElementById('sample-dslx-text');
const sampleDetailsApiPath = '/api/dslx-sample-details';
const initialSampleKeyFromQuery = new URLSearchParams(window.location.search).get('sample');
let selectedSampleKey = initialSampleKeyFromQuery || null;
let sampleDetailsRequestToken = 0;

function escDetailHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

if (totalCountEl) {
  totalCountEl.textContent = String(allSamples.length);
}
if (crateSelectEl) {
  crateSelectEl.value = selectedCrateVersion || '';
}
if (lossesOnlySelectEl) {
  lossesOnlySelectEl.value = lossesOnly ? 'losses_only' : 'all';
}

function commonLayout(title, xTitle, yTitle, xLog, yLog) {
  const layout = {
    title: { text: title, font: { color: '#cde7f7', size: 14 } },
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: '#0b172a',
    margin: { l: 68, r: 18, t: 38, b: 62 },
    hovermode: 'closest',
    xaxis: {
      title: xTitle,
      showgrid: true,
      gridcolor: '#243c5d',
      linecolor: '#5e7da2',
      tickfont: { color: '#8fa9be', size: 11 },
      titlefont: { color: '#8fa9be', size: 12 },
      zeroline: false
    },
    yaxis: {
      title: yTitle,
      showgrid: true,
      gridcolor: '#243c5d',
      linecolor: '#5e7da2',
      tickfont: { color: '#8fa9be', size: 11 },
      titlefont: { color: '#8fa9be', size: 12 },
      zeroline: false
    },
    legend: {
      orientation: 'h',
      font: { color: '#cde7f7', size: 11 },
      bgcolor: 'rgba(17,35,58,0.78)',
      bordercolor: '#213754',
      borderwidth: 1
    }
  };
  if (xLog) {
    layout.xaxis.type = 'log';
  }
  if (yLog) {
    layout.yaxis.type = 'log';
  }
  return layout;
}

function commonConfig() {
  return {
    displaylogo: false,
    responsive: true,
    scrollZoom: true,
    modeBarButtonsToRemove: ['select2d', 'lasso2d', 'autoScale2d']
  };
}

function sampleHover(sample) {
  return (
    sample.fn_key +
    '<br>crate:v' + sample.crate_version +
    '<br>dso:v' + sample.dso_version +
    '<br>ir_nodes=' + sample.ir_node_count +
    '<br>' + lhsLabel + '(nodes,levels)=(' + sample.g8r_nodes + ',' + sample.g8r_levels + ')' +
    '<br>' + rhsLabel + '(nodes,levels)=(' + sample.yosys_abc_nodes + ',' + sample.yosys_abc_levels + ')' +
    '<br>ir_action=' + sample.ir_action_id
  );
}

function sampleSelectionKey(sample) {
  const g8rStatsActionId = sample?.g8r_stats_action_id || '';
  const yosysAbcStatsActionId = sample?.yosys_abc_stats_action_id || '';
  return `${g8rStatsActionId}:${yosysAbcStatsActionId}`;
}

function findSampleBySelectionKey(selectionKey) {
  if (!selectionKey) {
    return null;
  }
  for (const sample of allSamples) {
    if (sampleSelectionKey(sample) === selectionKey) {
      return sample;
    }
  }
  return null;
}

function syncUrlQueryState() {
  const url = new URL(window.location.href);
  if (allowFraigToggle) {
    url.searchParams.set('fraig', fraig ? 'true' : 'false');
  } else {
    url.searchParams.delete('fraig');
  }
  const maxNodes = sliderEl ? Number(sliderEl.value) : 0;
  url.searchParams.set('max_ir_nodes', String(maxNodes));
  if (selectedCrateVersion) {
    url.searchParams.set('crate_version', selectedCrateVersion);
  } else {
    url.searchParams.delete('crate_version');
  }
  if (lossesOnly) {
    url.searchParams.set('losses_only', 'true');
  } else {
    url.searchParams.delete('losses_only');
  }
  if (selectedSampleKey) {
    url.searchParams.set('sample', selectedSampleKey);
  } else {
    url.searchParams.delete('sample');
  }
  const query = url.searchParams.toString();
  history.replaceState(null, '', viewScopePath + (query ? '?' + query : ''));
}

async function showSampleDetails(sample, sourcePlotId) {
  if (!detailJsonEl || !detailEmptyEl) {
    return;
  }
  selectedSampleKey = sampleSelectionKey(sample);
  syncUrlQueryState();
  detailEmptyEl.style.display = 'none';
  detailJsonEl.style.display = 'block';
  const enriched = {
    selection_key: selectedSampleKey,
    selected_from_plot: sourcePlotId,
    selected_at_utc: new Date().toISOString(),
    ...sample
  };
  detailJsonEl.textContent = JSON.stringify(enriched, null, 2);
  if (detailIrMetaEl) {
    detailIrMetaEl.style.display = 'block';
    detailIrMetaEl.textContent = 'loading IR...';
  }
  if (detailIrFocusMetaEl) {
    detailIrFocusMetaEl.style.display = 'none';
    detailIrFocusMetaEl.textContent = '';
  }
  if (detailIrFocusTextEl) {
    detailIrFocusTextEl.style.display = 'none';
    detailIrFocusTextEl.textContent = '';
  }
  if (detailIrFullWrapEl) {
    detailIrFullWrapEl.style.display = 'none';
    detailIrFullWrapEl.open = false;
  }
  if (detailIrFullSummaryEl) {
    detailIrFullSummaryEl.textContent = 'Full IR package';
  }
  if (detailIrTextEl) {
    detailIrTextEl.style.display = 'none';
    detailIrTextEl.textContent = '';
  }
  if (detailDslxMetaEl) {
    detailDslxMetaEl.style.display = 'block';
    detailDslxMetaEl.textContent = 'loading origin DSLX...';
  }
  if (detailDslxTextEl) {
    detailDslxTextEl.style.display = 'none';
    detailDslxTextEl.textContent = '';
  }

  const requestToken = ++sampleDetailsRequestToken;
  try {
    let url = `${sampleDetailsApiPath}?ir_action_id=${encodeURIComponent(sample.ir_action_id)}`;
    if (sample.ir_top) {
      url += `&ir_top=${encodeURIComponent(sample.ir_top)}`;
    }
    const resp = await fetch(url, { cache: 'no-store' });
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}`);
    }
    const data = await resp.json();
    if (requestToken !== sampleDetailsRequestToken) {
      return;
    }
    if (detailIrMetaEl) {
      detailIrMetaEl.style.display = 'block';
      const topPart = data.ir_top_fn_name ? ` top=<code>${escDetailHtml(data.ir_top_fn_name)}</code>` : '';
      detailIrMetaEl.innerHTML = `ir_action=<a href="/action/${encodeURIComponent(data.ir_action_id)}/"><code>${escDetailHtml(data.ir_action_id)}</code></a> artifact=<code>${escDetailHtml(data.ir_artifact_relpath)}</code>${topPart}`;
    }
    const fullIrText = data.ir_text || '(empty IR text)';
    const topFnText = data.ir_top_fn_text || '';
    if (detailIrFocusMetaEl) {
      detailIrFocusMetaEl.style.display = 'block';
      if (data.ir_top_fn_name) {
        detailIrFocusMetaEl.textContent = `top function: ${data.ir_top_fn_name}`;
      } else {
        detailIrFocusMetaEl.textContent = 'top function not resolved; showing full IR package below';
      }
    }
    if (detailIrFocusTextEl) {
      if (topFnText) {
        detailIrFocusTextEl.style.display = 'block';
        detailIrFocusTextEl.textContent = topFnText;
      } else {
        detailIrFocusTextEl.style.display = 'none';
        detailIrFocusTextEl.textContent = '';
      }
    }
    if (detailIrFullSummaryEl) {
      detailIrFullSummaryEl.textContent = topFnText ? 'Full IR package (expand)' : 'Full IR package';
    }
    if (detailIrFullWrapEl) {
      const showFullIr = !topFnText || topFnText !== fullIrText;
      detailIrFullWrapEl.style.display = showFullIr ? 'block' : 'none';
      detailIrFullWrapEl.open = false;
    }
    if (detailIrTextEl) {
      const showFullIr = !topFnText || topFnText !== fullIrText;
      detailIrTextEl.style.display = showFullIr ? 'block' : 'none';
      detailIrTextEl.textContent = fullIrText;
    }
    if (detailDslxMetaEl) {
      detailDslxMetaEl.style.display = 'block';
      if (data.dslx_file) {
        const fnPart = data.dslx_fn_name ? ` fn=${data.dslx_fn_name}` : '';
        const subtreePart = data.dslx_subtree_action_id ? ` subtree_action=${data.dslx_subtree_action_id}` : '';
        detailDslxMetaEl.textContent = `file=${data.dslx_file}${fnPart}${subtreePart}`;
      } else {
        detailDslxMetaEl.textContent = 'origin DSLX not available for this IR action';
      }
    }
    if (detailDslxTextEl) {
      if (data.dslx_text) {
        detailDslxTextEl.style.display = 'block';
        detailDslxTextEl.textContent = data.dslx_text;
      } else {
        detailDslxTextEl.style.display = 'none';
        detailDslxTextEl.textContent = '';
      }
    }
  } catch (err) {
    if (requestToken !== sampleDetailsRequestToken) {
      return;
    }
    const message = `failed loading sample details: ${err}`;
    if (detailIrMetaEl) {
      detailIrMetaEl.style.display = 'block';
      detailIrMetaEl.textContent = message;
    }
    if (detailIrFocusMetaEl) {
      detailIrFocusMetaEl.style.display = 'block';
      detailIrFocusMetaEl.textContent = message;
    }
    if (detailIrFocusTextEl) {
      detailIrFocusTextEl.style.display = 'none';
      detailIrFocusTextEl.textContent = '';
    }
    if (detailIrTextEl) {
      detailIrTextEl.style.display = 'none';
      detailIrTextEl.textContent = '';
    }
    if (detailIrFullWrapEl) {
      detailIrFullWrapEl.style.display = 'none';
      detailIrFullWrapEl.open = false;
    }
    if (detailDslxMetaEl) {
      detailDslxMetaEl.style.display = 'block';
      detailDslxMetaEl.textContent = message;
    }
  }
}

function bindPlotClick(plotId) {
  const plotEl = document.getElementById(plotId);
  if (!plotEl) {
    return;
  }
  if (typeof plotEl.removeAllListeners === 'function') {
    plotEl.removeAllListeners('plotly_click');
  }
  plotEl.on('plotly_click', (evt) => {
    const sample = evt?.points?.[0]?.customdata;
    if (!sample) {
      return;
    }
    showSampleDetails(sample, plotId);
  });
}

function finitePositive(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) {
    return null;
  }
  return n;
}

function finiteForLogDisplay(value, mapZeroToOne) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return null;
  }
  if (n > 0) {
    return { value: n, clamped: false };
  }
  if (n === 0 && mapZeroToOne) {
    return { value: 1, clamped: true };
  }
  return null;
}

function renderPairPlot(plotId, title, xTitle, yTitle, samples, xSel, ySel, mapZeroToOne) {
  const valid = [];
  for (const sample of samples) {
    const xInfo = finiteForLogDisplay(xSel(sample), mapZeroToOne);
    const yInfo = finiteForLogDisplay(ySel(sample), mapZeroToOne);
    if (xInfo === null || yInfo === null) {
      continue;
    }
    valid.push({
      sample,
      x: xInfo.value,
      y: yInfo.value,
      xClamped: xInfo.clamped,
      yClamped: yInfo.clamped,
      quadrant: sampleDeltaQuadrant(sample)
    });
  }
  const dropped = samples.length - valid.length;
  const clampedCount = valid.filter((v) => v.xClamped || v.yClamped).length;
  const x = valid.map((v) => v.x);
  const y = valid.map((v) => v.y);
  const hover = valid.map((v) => sampleHover(v.sample) + '<br>' + v.quadrant.label);
  const custom = valid.map((v) => v.sample);
  const colors = valid.map((v) => v.quadrant.color);
  const traces = [];
  if (x.length > 0) {
    traces.push({
      type: 'scattergl',
      mode: 'markers',
      name: 'samples',
      x,
      y,
      customdata: custom,
      text: hover,
      hovertemplate: '%{text}<br>x=%{x}<br>y=%{y}<extra></extra>',
      marker: { size: 6, color: colors, opacity: 0.88 }
    });
    const both = x.concat(y);
    const minVal = Math.min(...both);
    const maxVal = Math.max(...both);
    traces.push({
      type: 'scatter',
      mode: 'lines',
      name: 'y=x',
      x: [minVal, maxVal],
      y: [minVal, maxVal],
      line: { color: '#32f1b2', width: 1.2, dash: 'dot' },
      hoverinfo: 'skip'
    });
  }
  const layout = commonLayout(
    `${title} [n=${x.length}, zero_as_one=${clampedCount}, dropped_negative_or_nonfinite=${dropped}]`,
    xTitle,
    yTitle,
    true,
    true
  );
  if (x.length === 0) {
    layout.annotations = [{
      text: 'No positive-domain samples for current filter',
      showarrow: false,
      x: 0.5,
      y: 0.5,
      xref: 'paper',
      yref: 'paper',
      font: { color: '#8fa9be', size: 13 }
    }];
  }
  Plotly.react(plotId, traces, layout, commonConfig()).then(() => bindPlotClick(plotId));
}

function classifyDeltaQuadrant(dx, dy) {
  const eps = 1e-9;
  const dxZero = Math.abs(dx) <= eps;
  const dyZero = Math.abs(dy) <= eps;
  if (dxZero && dyZero) {
    return 'Tie exact (nodes+levels equal)';
  }
  if (dx > eps && dy > eps) {
    return 'Q1 pure win (g8r fewer nodes+levels)';
  }
  if ((dx > eps && dyZero) || (dy > eps && dxZero)) {
    return 'Q1 partial win (one better, one equal)';
  }
  if (dx < -eps && dy > eps) {
    return 'Q2 mixed (levels win, nodes loss)';
  }
  if (dx < -eps && dy < -eps) {
    return 'Q3 strict loss (g8r more nodes+levels)';
  }
  if ((dx < -eps && dyZero) || (dy < -eps && dxZero)) {
    return 'Q3 partial loss (one worse, one equal)';
  }
  if (dx > eps && dy < -eps) {
    return 'Q4 mixed (nodes win, levels loss)';
  }
  return 'Tie exact (nodes+levels equal)';
}

function quadrantColor(label) {
  if (label.startsWith('Q1')) {
    return '#7dff9f';
  }
  if (label.startsWith('Q3')) {
    return '#ff8ea9';
  }
  if (label.startsWith('Q2')) {
    return '#ffd36f';
  }
  if (label.startsWith('Q4')) {
    return '#5ec9ff';
  }
  return '#8a8f98';
}

function sampleDeltaQuadrant(sample) {
  const dx = Number(sample.yosys_abc_nodes) - Number(sample.g8r_nodes);
  const dy = Number(sample.yosys_abc_levels) - Number(sample.g8r_levels);
  if (!Number.isFinite(dx) || !Number.isFinite(dy)) {
    const label = 'Tie exact (nodes+levels equal)';
    return { dx: 0, dy: 0, label, color: quadrantColor(label) };
  }
  const label = classifyDeltaQuadrant(dx, dy);
  return { dx, dy, label, color: quadrantColor(label) };
}

function renderDeltaQuadrantPlot(samples) {
  const valid = [];
  for (const sample of samples) {
    const quadrant = sampleDeltaQuadrant(sample);
    if (!Number.isFinite(quadrant.dx) || !Number.isFinite(quadrant.dy)) {
      continue;
    }
    valid.push({
      sample,
      dx: quadrant.dx,
      dy: quadrant.dy,
      quadrant: quadrant.label,
      color: quadrant.color,
    });
  }
  let q1 = 0;
  let q2 = 0;
  let q3 = 0;
  let q4 = 0;
  let tie = 0;
  for (const v of valid) {
    if (v.quadrant.startsWith('Q1')) {
      q1 += 1;
    } else if (v.quadrant.startsWith('Q2')) {
      q2 += 1;
    } else if (v.quadrant.startsWith('Q3')) {
      q3 += 1;
    } else if (v.quadrant.startsWith('Q4')) {
      q4 += 1;
    } else {
      tie += 1;
    }
  }
  const x = valid.map((v) => v.dx);
  const y = valid.map((v) => v.dy);
  const hover = valid.map((v) =>
    sampleHover(v.sample) +
    '<br>delta_nodes(' + rhsLabel + '-' + lhsLabel + ')=' + v.dx +
    '<br>delta_levels(' + rhsLabel + '-' + lhsLabel + ')=' + v.dy +
    '<br>' + v.quadrant
  );
  const custom = valid.map((v) => v.sample);
  const colors = valid.map((v) => v.color);
  const traces = x.length > 0 ? [{
    type: 'scattergl',
    mode: 'markers',
    name: 'delta samples',
    x,
    y,
    customdata: custom,
    text: hover,
    hovertemplate: '%{text}<extra></extra>',
    marker: { size: 6, color: colors, opacity: 0.88 }
  }] : [];
  const layout = commonLayout(
    `delta quadrants [Q1=${q1} Q2=${q2} Q3=${q3} Q4=${q4} tie=${tie}]`,
    'nodes delta = ' + rhsLabel + '_nodes - ' + lhsLabel + '_nodes (positive => ' + lhsLabel + ' better)',
    'levels delta = ' + rhsLabel + '_levels - ' + lhsLabel + '_levels (positive => ' + lhsLabel + ' better)',
    false,
    false
  );
  layout.shapes = [
    {
      type: 'line',
      x0: 0,
      x1: 0,
      xref: 'x',
      y0: 0,
      y1: 1,
      yref: 'paper',
      line: { color: '#5ec9ff', width: 1.2, dash: 'dot' }
    },
    {
      type: 'line',
      x0: 0,
      x1: 1,
      xref: 'paper',
      y0: 0,
      y1: 0,
      yref: 'y',
      line: { color: '#5ec9ff', width: 1.2, dash: 'dot' }
    }
  ];
  layout.annotations = [
    {
      text: 'Q2 mixed',
      showarrow: false,
      x: 0.13,
      y: 0.93,
      xref: 'paper',
      yref: 'paper',
      font: { color: '#ffd36f', size: 11 }
    },
    {
      text: 'Q1 pure win',
      showarrow: false,
      x: 0.87,
      y: 0.93,
      xref: 'paper',
      yref: 'paper',
      font: { color: '#7dff9f', size: 11 }
    },
    {
      text: 'Q3 strict loss',
      showarrow: false,
      x: 0.13,
      y: 0.08,
      xref: 'paper',
      yref: 'paper',
      font: { color: '#ff8ea9', size: 11 }
    },
    {
      text: 'Q4 mixed',
      showarrow: false,
      x: 0.87,
      y: 0.08,
      xref: 'paper',
      yref: 'paper',
      font: { color: '#5ec9ff', size: 11 }
    }
  ];
  if (x.length === 0) {
    layout.annotations.push({
      text: 'No samples for current filter',
      showarrow: false,
      x: 0.5,
      y: 0.5,
      xref: 'paper',
      yref: 'paper',
      font: { color: '#8fa9be', size: 13 }
    });
  }
  Plotly.react('plot-delta-quadrant', traces, layout, commonConfig()).then(() => bindPlotClick('plot-delta-quadrant'));
}

function renderLossVsIrPlot(samples) {
  const valid = [];
  for (const sample of samples) {
    const quadrant = sampleDeltaQuadrant(sample).label;
    if (!quadrant.startsWith('Q3 strict loss')) {
      continue;
    }
    const x = finitePositive(sample.ir_node_count);
    const y = finitePositive(sample.g8r_product_loss);
    if (x === null || y === null) {
      continue;
    }
    valid.push({ sample, x, y });
  }
  if (lossPositiveCountEl) {
    lossPositiveCountEl.textContent = String(valid.length);
  }
  const dropped = samples.length - valid.length;
  const x = valid.map((v) => v.x);
  const y = valid.map((v) => v.y);
  const hover = valid.map((v) => sampleHover(v.sample));
  const custom = valid.map((v) => v.sample);
  const traces = x.length > 0 ? [{
    type: 'scattergl',
    mode: 'markers',
    name: 'positive loss',
    x,
    y,
    customdata: custom,
    text: hover,
    hovertemplate: '%{text}<br>loss=%{y}<extra></extra>',
    marker: { size: 6, color: '#ff8ea9', opacity: 0.9 }
  }] : [];
  const layout = commonLayout(
    `${lhsLabel} product strict-loss amount vs ir nodes [n=${x.length}, dropped_non_q3_strict_or_nonpositive_or_nonloss=${dropped}]`,
    'IR node count',
    lhsLabel + '_product - ' + rhsLabel + '_product (positive = ' + lhsLabel + ' worse)',
    true,
    true
  );
  if (x.length === 0) {
    layout.annotations = [{
      text: 'No strict-loss samples for current filter',
      showarrow: false,
      x: 0.5,
      y: 0.5,
      xref: 'paper',
      yref: 'paper',
      font: { color: '#8fa9be', size: 13 }
    }];
  }
  Plotly.react('plot-loss-vs-ir', traces, layout, commonConfig()).then(() => bindPlotClick('plot-loss-vs-ir'));
}

function currentScopeLabel() {
  if (selectedCrateVersion) {
    return `crate:v${selectedCrateVersion}`;
  }
  return 'all crate versions';
}

function isLossSample(sample) {
  const dx = Number(sample.yosys_abc_nodes) - Number(sample.g8r_nodes);
  const dy = Number(sample.yosys_abc_levels) - Number(sample.g8r_levels);
  if (!Number.isFinite(dx) || !Number.isFinite(dy)) {
    return false;
  }
  const eps = 1e-9;
  return dx < -eps || dy < -eps;
}

function updateFilteredViews() {
  if (!sliderEl) {
    return;
  }
  const maxNodes = Number(sliderEl.value);
  if (crateSelectEl) {
    selectedCrateVersion = crateSelectEl.value || null;
  }
  if (lossesOnlySelectEl) {
    lossesOnly = lossesOnlySelectEl.value === 'losses_only';
  }
  if (sliderValueEl) {
    sliderValueEl.textContent = String(maxNodes);
  }
  const filtered = allSamples.filter((sample) => {
    if (selectedCrateVersion && sample.crate_version !== selectedCrateVersion) {
      return false;
    }
    if (Number(sample.ir_node_count) > maxNodes) {
      return false;
    }
    if (lossesOnly && !isLossSample(sample)) {
      return false;
    }
    return true;
  });
  if (filteredCountEl) {
    filteredCountEl.textContent = String(filtered.length);
  }
  if (scopeLabelEl) {
    scopeLabelEl.textContent = currentScopeLabel();
  }
  if (noDataEl) {
    noDataEl.style.display = filtered.length === 0 ? 'block' : 'none';
  }

  renderPairPlot(
    'plot-levels',
    lhsLabel + ' levels vs ' + rhsLabel + ' levels',
    lhsLabel + ' levels',
    rhsLabel + ' levels',
    filtered,
    (s) => s.g8r_levels,
    (s) => s.yosys_abc_levels,
    true
  );
  renderPairPlot(
    'plot-nodes',
    lhsLabel + ' nodes vs ' + rhsLabel + ' nodes',
    lhsLabel + ' nodes',
    rhsLabel + ' nodes',
    filtered,
    (s) => s.g8r_nodes,
    (s) => s.yosys_abc_nodes,
    true
  );
  renderDeltaQuadrantPlot(filtered);
  renderLossVsIrPlot(filtered);
  syncUrlQueryState();
}

function restoreSelectedSampleFromQuery() {
  if (!selectedSampleKey) {
    return;
  }
  const sample = findSampleBySelectionKey(selectedSampleKey);
  if (!sample) {
    selectedSampleKey = null;
    syncUrlQueryState();
    return;
  }
  showSampleDetails(sample, 'query');
}

if (sliderEl) {
  sliderEl.addEventListener('input', updateFilteredViews);
}
if (crateSelectEl) {
  crateSelectEl.addEventListener('change', updateFilteredViews);
}
if (lossesOnlySelectEl) {
  lossesOnlySelectEl.addEventListener('change', updateFilteredViews);
}
updateFilteredViews();
restoreSelectedSampleFromQuery();
"##,
    );
    html.push_str("</script>");
    html.push_str("</main></body></html>");
    html
}

pub(super) fn render_stdlib_file_action_graph_html(
    dataset: &StdlibFileActionGraphDataset,
    generated_utc: DateTime<Utc>,
) -> String {
    use std::fmt::Write;

    let mut html = String::new();
    html.push_str(
        "<!doctype html>\
<html lang=\"en\">\
<head>\
  <meta charset=\"utf-8\">\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
  <title>xlsynth-bvc DSLX File Action Graph</title>\
  <style>\
    :root {\
      --bg: #050911;\
      --bg2: #0a1323;\
      --panel: #0f1a2d;\
      --panel-hi: #14233b;\
      --ink: #dcffee;\
      --muted: #8fa9be;\
      --accent: #32f1b2;\
      --accent2: #5ec9ff;\
      --warn: #ff6d8f;\
      --line: #2c4566;\
      --border: #213754;\
      --chip: #11233a;\
    }\
    * { box-sizing: border-box; }\
    body {\
      margin: 0;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      color: var(--ink);\
      background:\
        radial-gradient(circle at 12% -15%, rgba(50, 241, 178, 0.18) 0, transparent 36%),\
        radial-gradient(circle at 100% 0%, rgba(94, 201, 255, 0.16) 0, transparent 40%),\
        repeating-linear-gradient(0deg, rgba(143, 169, 190, 0.055) 0, rgba(143, 169, 190, 0.055) 1px, transparent 1px, transparent 24px),\
        repeating-linear-gradient(90deg, rgba(143, 169, 190, 0.04) 0, rgba(143, 169, 190, 0.04) 1px, transparent 1px, transparent 24px),\
        linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);\
      min-height: 100vh;\
    }\
    .wrap { width: 100%; max-width: none; margin: 0; padding: 24px 18px 34px; }\
    .title {\
      margin: 0;\
      font-size: 1.8rem;\
      text-transform: uppercase;\
      letter-spacing: 0.04em;\
      color: var(--accent);\
      text-shadow: 0 0 12px rgba(50, 241, 178, 0.34);\
    }\
    .meta {\
      margin: 6px 0 0;\
      color: var(--muted);\
      font-size: 0.86rem;\
      border-left: 3px solid var(--line);\
      padding-left: 10px;\
    }\
    .nav { margin-top: 10px; display: flex; gap: 9px; flex-wrap: wrap; }\
    .nav a {\
      color: #b9e8ff;\
      text-decoration: none;\
      border: 1px solid var(--border);\
      background: rgba(16, 34, 56, 0.8);\
      border-radius: 6px;\
      padding: 5px 9px;\
      font-size: 0.82rem;\
    }\
    .nav a.active {\
      color: var(--accent);\
      border-color: rgba(50, 241, 178, 0.55);\
      box-shadow: 0 0 0 1px rgba(50, 241, 178, 0.15) inset;\
    }\
    .panel {\
      margin-top: 14px;\
      background: linear-gradient(180deg, var(--panel-hi) 0%, var(--panel) 100%);\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      padding: 11px 12px 12px;\
      box-shadow:\
        inset 0 1px 0 rgba(255, 255, 255, 0.03),\
        0 0 0 1px rgba(50, 241, 178, 0.04),\
        0 12px 28px rgba(0, 0, 0, 0.35);\
    }\
    .controls-form {\
      display: flex;\
      flex-wrap: wrap;\
      align-items: center;\
      gap: 8px;\
    }\
    .label { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; }\
    .select-input {\
      min-width: 260px;\
      max-width: min(72vw, 920px);\
      color: #d7f6ff;\
      background: #102238;\
      border: 1px solid var(--border);\
      border-radius: 6px;\
      padding: 5px 9px;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
    }\
    .select-input:focus { outline: 1px solid rgba(50, 241, 178, 0.55); }\
    .btn {\
      color: #cde7f7;\
      text-decoration: none;\
      border: 1px solid var(--border);\
      background: var(--chip);\
      border-radius: 999px;\
      padding: 5px 11px;\
      font-size: 0.78rem;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      cursor: pointer;\
    }\
    .btn:hover { border-color: rgba(50, 241, 178, 0.55); color: var(--accent); }\
    .summary-row { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 7px; }\
    .summary-chip {\
      border: 1px solid var(--border);\
      border-radius: 999px;\
      padding: 4px 10px;\
      font-size: 0.78rem;\
      background: rgba(17, 35, 58, 0.66);\
      color: #cde7f7;\
    }\
    .graph-shell {\
      display: grid;\
      grid-template-columns: minmax(0, 1fr) 360px;\
      gap: 12px;\
      min-height: 72vh;\
    }\
    .graph-host {\
      min-height: 72vh;\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      background: rgba(8, 18, 34, 0.75);\
      overflow: hidden;\
    }\
    .detail {\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      background: rgba(8, 18, 34, 0.75);\
      padding: 10px;\
    }\
    .detail h3 { margin: 0 0 8px; color: #9fdfff; font-size: 0.92rem; }\
    .detail p { margin: 0 0 6px; color: var(--muted); font-size: 0.79rem; }\
    .detail code {\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
      color: #bbffda;\
      background: rgba(17, 35, 58, 0.72);\
      border: 1px solid var(--border);\
      border-radius: 4px;\
      padding: 1px 4px;\
    }\
    .detail a { color: #5ec9ff; text-decoration: none; }\
    .layout-buttons { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 9px; }\
    .legend { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 6px; }\
    .legend-chip {\
      border: 1px solid var(--border);\
      border-radius: 999px;\
      padding: 3px 8px;\
      font-size: 0.74rem;\
      background: rgba(17, 35, 58, 0.72);\
    }\
    .legend-chip.done { color: #7dff9f; border-color: rgba(125, 255, 159, 0.44); }\
    .legend-chip.pending { color: #ffd78c; border-color: rgba(255, 215, 140, 0.44); }\
    .legend-chip.running { color: #7ddfff; border-color: rgba(125, 223, 255, 0.44); }\
    .legend-chip.failed { color: #ff8ea9; border-color: rgba(255, 142, 169, 0.44); }\
    .legend-chip.canceled { color: #b1bfd2; border-color: rgba(177, 191, 210, 0.44); }\
    .legend-chip.dependency { color: #8fe0ff; border-color: rgba(94, 201, 255, 0.44); }\
    .legend-chip.suggested { color: #ffda8f; border-color: rgba(255, 198, 109, 0.44); }\
    @media (max-width: 1180px) {\
      .graph-shell { grid-template-columns: minmax(0, 1fr); }\
      .graph-host { min-height: 62vh; }\
    }\
  </style>\
</head>\
<body>\
  <main class=\"wrap\">",
    );

    let selected_crate = dataset.selected_crate_version.as_deref();
    let selected_file = dataset.selected_file.as_deref();
    let selected_function = dataset.selected_function.as_deref();
    let selected_action_id = dataset.selected_action_id.as_deref();
    let graph_view_url = stdlib_file_action_graph_view_url(
        selected_crate,
        selected_file,
        selected_function,
        selected_action_id,
        dataset.include_k3_descendants,
    );
    let g8r_url =
        stdlib_fns_trend_view_url(StdlibTrendKind::G8r, StdlibMetric::AndNodes, false, None);
    let yosys_url = stdlib_fns_trend_view_url(
        StdlibTrendKind::YosysAbc,
        StdlibMetric::AndNodes,
        false,
        None,
    );
    let g8r_vs_yosys_url = stdlib_g8r_vs_yosys_view_url(false, None, selected_crate);
    let corpus_g8r_vs_yosys_url = ir_fn_corpus_g8r_vs_yosys_view_url(None, selected_crate);
    let corpus_g8r_abc_vs_codegen_yosys_url =
        ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_view_url(None, selected_crate);
    let _ = write!(
        html,
        "<h1 class=\"title\">DSLX File Action Graph</h1>\
<p class=\"meta\">generated={} | crate_scope={} | file_scope={} | fn_scope={} | action_scope={} | k3_descendants={} | crate_actions={} | roots={} | graph_nodes={} | graph_edges={}</p>",
        html_escape(&generated_utc.to_rfc3339()),
        html_escape(
            selected_crate
                .map(|v| format!("crate:v{}", v))
                .as_deref()
                .unwrap_or("none")
        ),
        html_escape(selected_file.unwrap_or("none")),
        html_escape(selected_function.unwrap_or("none")),
        html_escape(selected_action_id.unwrap_or("none")),
        if dataset.include_k3_descendants {
            "shown"
        } else {
            "hidden"
        },
        dataset.total_actions_for_crate,
        dataset.root_action_ids.len(),
        dataset.nodes.len(),
        dataset.edges.len()
    );
    let _ = write!(
        html,
        "<nav class=\"nav\">\
<a href=\"/versions/\">/versions/</a>\
<a href=\"/dslx-fns/\">/dslx-fns/</a>\
<a href=\"{}\">/dslx-fns-g8r/</a>\
<a href=\"{}\">/dslx-fns-yosys-abc/</a>\
<a href=\"{}\">/dslx-fns-g8r-vs-yosys-abc/</a>\
<a href=\"{}\">/ir-fn-corpus-g8r-vs-yosys-abc/</a>\
<a href=\"{}\">/ir-fn-g8r-abc-vs-codegen-yosys-abc/</a>\
<a class=\"active\" href=\"{}\">/dslx-file-action-graph/</a>\
<a href=\"/ir-fn-corpus-structural/\">/ir-fn-corpus-structural/</a>\
</nav>",
        html_escape(&g8r_url),
        html_escape(&yosys_url),
        html_escape(&g8r_vs_yosys_url),
        html_escape(&corpus_g8r_vs_yosys_url),
        html_escape(&corpus_g8r_abc_vs_codegen_yosys_url),
        html_escape(&graph_view_url),
    );

    html.push_str("<section class=\"panel\">");
    let _ = write!(
        html,
        "<form class=\"controls-form\" method=\"get\" action=\"/dslx-file-action-graph/\">\
<span class=\"label\">crate version</span>\
<select class=\"select-input\" name=\"crate_version\">"
    );
    for version in &dataset.available_crate_versions {
        let selected_attr = if Some(version.as_str()) == selected_crate {
            "selected"
        } else {
            ""
        };
        let _ = write!(
            html,
            "<option value=\"{}\" {}>crate:v{}</option>",
            html_escape(version),
            selected_attr,
            html_escape(version),
        );
    }
    html.push_str(
        "</select><span class=\"label\">DSLX file</span><select class=\"select-input\" name=\"file\">",
    );
    html.push_str("<option value=\"\">(select a file)</option>");
    for file in &dataset.available_files {
        let selected_attr = if Some(file.as_str()) == selected_file {
            "selected"
        } else {
            ""
        };
        let _ = write!(
            html,
            "<option value=\"{}\" {}>{}</option>",
            html_escape(file),
            selected_attr,
            html_escape(file),
        );
    }
    html.push_str(
        "</select><span class=\"label\">function</span><select class=\"select-input\" name=\"fn_name\">",
    );
    if dataset.available_functions.is_empty() {
        if selected_file.is_none() {
            html.push_str("<option value=\"\">(select file first)</option>");
        } else {
            html.push_str("<option value=\"\">(no functions discovered)</option>");
        }
    } else {
        for fn_name in &dataset.available_functions {
            let selected_attr = if Some(fn_name.as_str()) == selected_function {
                "selected"
            } else {
                ""
            };
            let _ = write!(
                html,
                "<option value=\"{}\" {}>{}</option>",
                html_escape(fn_name),
                selected_attr,
                html_escape(fn_name),
            );
        }
    }
    let _ = write!(
        html,
        "</select>\
<span class=\"label\">k3 descendants</span>\
<select class=\"select-input\" name=\"include_k3_descendants\">\
<option value=\"false\" {}>hide</option>\
<option value=\"true\" {}>show</option>\
</select>\
<span class=\"label\">action id</span>\
<input class=\"select-input\" type=\"text\" name=\"action_id\" placeholder=\"64-hex action id\" value=\"{}\" />\
<button class=\"btn\" type=\"submit\">apply</button></form>",
        if dataset.include_k3_descendants {
            ""
        } else {
            "selected"
        },
        if dataset.include_k3_descendants {
            "selected"
        } else {
            ""
        },
        html_escape(selected_action_id.unwrap_or("")),
    );
    if selected_file.is_some() {
        let clear_url = stdlib_file_action_graph_view_url(
            selected_crate,
            None,
            None,
            selected_action_id,
            dataset.include_k3_descendants,
        );
        let _ = write!(
            html,
            "<div style=\"margin-top:8px;\"><a class=\"btn\" href=\"{}\">clear file filter</a></div>",
            html_escape(&clear_url)
        );
    }
    if selected_action_id.is_some() {
        let clear_action_url = stdlib_file_action_graph_view_url(
            selected_crate,
            selected_file,
            selected_function,
            None,
            dataset.include_k3_descendants,
        );
        let _ = write!(
            html,
            "<div style=\"margin-top:8px;\"><a class=\"btn\" href=\"{}\">clear action filter</a></div>",
            html_escape(&clear_action_url)
        );
    }
    html.push_str("<div class=\"summary-row\">");
    let done_count = dataset
        .nodes
        .iter()
        .filter(|node| node.state_key == "done")
        .count();
    let pending_count = dataset
        .nodes
        .iter()
        .filter(|node| node.state_key == "pending")
        .count();
    let running_count = dataset
        .nodes
        .iter()
        .filter(|node| node.state_key == "running")
        .count();
    let failed_count = dataset
        .nodes
        .iter()
        .filter(|node| node.state_key == "failed")
        .count();
    let canceled_count = dataset
        .nodes
        .iter()
        .filter(|node| node.state_key == "canceled")
        .count();
    let _ = write!(
        html,
        "<span class=\"summary-chip\">done {}</span>\
<span class=\"summary-chip\">pending {}</span>\
<span class=\"summary-chip\">running {}</span>\
<span class=\"summary-chip\">failed {}</span>\
<span class=\"summary-chip\">canceled {}</span>",
        done_count, pending_count, running_count, failed_count, canceled_count
    );
    html.push_str("</div></section>");

    if dataset.selected_action_id.is_some() && !dataset.action_focus_found {
        html.push_str(
            "<section class=\"panel\"><p class=\"meta\">Requested action id was not found in provenance or queue records.</p></section>",
        );
    } else if dataset.available_crate_versions.is_empty() && dataset.selected_action_id.is_none() {
        html.push_str(
            "<section class=\"panel\"><p class=\"meta\">No crate-scoped DSLX->IR actions are available yet.</p></section>",
        );
    } else if dataset.selected_action_id.is_none() && selected_file.is_none() {
        html.push_str(
            "<section class=\"panel\"><p class=\"meta\">Select a DSLX file to visualize the spawned action DAG for the selected crate version.</p></section>",
        );
    } else if dataset.selected_action_id.is_none() && dataset.root_action_ids.is_empty() {
        html.push_str(
            "<section class=\"panel\"><p class=\"meta\">No `driver_dslx_fn_to_ir` root actions found for this file in the selected crate version.</p></section>",
        );
    } else if dataset.nodes.is_empty() {
        if dataset.selected_action_id.is_some() {
            html.push_str(
                "<section class=\"panel\"><p class=\"meta\">No related dependency/dependent actions were found for the selected action id.</p></section>",
            );
        } else {
            html.push_str(
                "<section class=\"panel\"><p class=\"meta\">No reachable downstream actions were found from the selected file roots.</p></section>",
            );
        }
    } else {
        let nodes_json =
            serde_json::to_string(&dataset.nodes).expect("serializing stdlib file action nodes");
        let edges_json =
            serde_json::to_string(&dataset.edges).expect("serializing stdlib file action edges");
        let roots_json = serde_json::to_string(&dataset.root_action_ids)
            .expect("serializing stdlib file action roots");
        html.push_str(
            "<section class=\"panel\">\
<div class=\"graph-shell\">\
<div class=\"graph-host\" id=\"file-action-graph\"></div>\
<aside class=\"detail\">\
<div class=\"layout-buttons\">\
<button id=\"layout-flow\" class=\"btn\" type=\"button\">flow (dagre)</button>\
<button id=\"layout-breadthfirst\" class=\"btn\" type=\"button\">breadthfirst</button>\
<button id=\"layout-cose\" class=\"btn\" type=\"button\">force (cose)</button>\
<button id=\"layout-fit\" class=\"btn\" type=\"button\">fit</button>\
</div>\
<h3>Selected Node</h3>\
<p id=\"graph-node-empty\">click a node to inspect action details</p>\
<p id=\"graph-node-action\" style=\"display:none;\"></p>\
<p id=\"graph-node-kind\" style=\"display:none;\"></p>\
<p id=\"graph-node-subject\" style=\"display:none;\"></p>\
<p id=\"graph-node-state\" style=\"display:none;\"></p>\
<p id=\"graph-node-provenance\" style=\"display:none;\"></p>\
<p id=\"graph-node-root\" style=\"display:none;\"></p>\
<p id=\"graph-edge-info\" style=\"display:none;\"></p>\
<div class=\"legend\">\
<span class=\"legend-chip dependency\">dependency edge</span>\
<span class=\"legend-chip suggested\">suggested edge</span>\
<span class=\"legend-chip done\">done</span>\
<span class=\"legend-chip pending\">pending</span>\
<span class=\"legend-chip running\">running</span>\
<span class=\"legend-chip failed\">failed</span>\
<span class=\"legend-chip canceled\">canceled</span>\
</div>\
</aside>\
</div>\
</section>",
        );

        html.push_str(
            "<script src=\"https://unpkg.com/cytoscape@3.30.0/dist/cytoscape.min.js\"></script>\
<script src=\"https://unpkg.com/dagre@0.8.5/dist/dagre.min.js\"></script>\
<script src=\"https://unpkg.com/cytoscape-dagre@2.5.0/cytoscape-dagre.js\"></script>",
        );
        let _ = write!(
            html,
            "<script>\
(() => {{\
  const nodes = {};\
  const edges = {};\
  const rootIds = {};\
  const host = document.getElementById('file-action-graph');\
  if (!host || typeof cytoscape === 'undefined') {{ return; }}\
  const elements = [];\
  for (const node of nodes) {{\
    elements.push({{ data: {{\
      id: node.action_id,\
      action_id: node.action_id,\
      label: node.label,\
      kind: node.kind,\
      subject: node.subject,\
      state_label: node.state_label,\
      state_key: node.state_key,\
      has_provenance: node.has_provenance,\
      is_root: node.is_root\
    }} }});\
  }}\
  for (let i = 0; i < edges.length; i++) {{\
    const edge = edges[i];\
    elements.push({{ data: {{\
      id: `edge-${{i}}`,\
      source: edge.source,\
      target: edge.target,\
      edge_kind: edge.edge_kind,\
      role: edge.role\
    }} }});\
  }}\
  if (typeof cytoscapeDagre !== 'undefined') {{\
    cytoscape.use(cytoscapeDagre);\
  }}\
  const cy = cytoscape({{\
    container: host,\
    elements,\
    style: [\
      {{ selector: 'node', style: {{\
        'background-color': '#2b4f75',\
        'label': 'data(label)',\
        'font-size': '9px',\
        'color': '#ddf8ff',\
        'text-wrap': 'wrap',\
        'text-max-width': '120px',\
        'border-width': 1,\
        'border-color': '#1f3350',\
        'text-outline-width': 0,\
        'cursor': 'pointer'\
      }} }},\
      {{ selector: 'node[state_key = \"done\"]', style: {{ 'background-color': '#1f6b45', 'border-color': '#38c87f' }} }},\
      {{ selector: 'node[state_key = \"pending\"]', style: {{ 'background-color': '#7d5c16', 'border-color': '#ffc86d' }} }},\
      {{ selector: 'node[state_key = \"running\"]', style: {{ 'background-color': '#184f73', 'border-color': '#5ec9ff' }} }},\
      {{ selector: 'node[state_key = \"failed\"]', style: {{ 'background-color': '#6f1f3a', 'border-color': '#ff8ea9' }} }},\
      {{ selector: 'node[state_key = \"canceled\"]', style: {{ 'background-color': '#4b5362', 'border-color': '#b1bfd2' }} }},\
      {{ selector: 'node[state_key = \"not_queued\"]', style: {{ 'background-color': '#4b5362', 'border-color': '#b1bfd2' }} }},\
      {{ selector: 'node[is_root = true]', style: {{ 'shape': 'diamond', 'border-width': 2, 'border-color': '#32f1b2' }} }},\
      {{ selector: 'edge', style: {{\
        'curve-style': 'bezier',\
        'target-arrow-shape': 'triangle',\
        'target-arrow-color': '#5ec9ff',\
        'line-color': '#5ec9ff',\
        'width': 1.7\
      }} }},\
      {{ selector: 'edge[edge_kind = \"suggested\"]', style: {{\
        'line-color': '#ffc66d',\
        'target-arrow-color': '#ffc66d',\
        'line-style': 'dashed',\
        'width': 1.4\
      }} }},\
      {{ selector: 'edge:selected', style: {{ 'width': 3.2 }} }},\
      {{ selector: 'node:selected', style: {{ 'border-width': 3 }} }}\
    ]\
  }});\
\
  function runBreadthfirst() {{\
    const roots = rootIds.map((id) => '#' + id);\
    cy.layout({{\
      name: 'breadthfirst',\
      directed: true,\
      roots: roots.length ? roots : undefined,\
      fit: true,\
      padding: 28,\
      spacingFactor: 1.15,\
      animate: false\
    }}).run();\
  }}\
\
  function runFlow() {{\
    try {{\
      cy.layout({{\
        name: 'dagre',\
        rankDir: 'TB',\
        align: 'UL',\
        fit: true,\
        padding: 28,\
        animate: false,\
        spacingFactor: 1.05,\
        nodeSep: 24,\
        rankSep: 64,\
        edgeSep: 12\
      }}).run();\
      return;\
    }} catch (_err) {{\
      runBreadthfirst();\
    }}\
  }}\
\
  function runCose() {{\
    cy.layout({{\
      name: 'cose',\
      fit: true,\
      padding: 28,\
      animate: false,\
      nodeRepulsion: 8000,\
      idealEdgeLength: 120\
    }}).run();\
  }}\
\
  runFlow();\
\
  const emptyEl = document.getElementById('graph-node-empty');\
  const actionEl = document.getElementById('graph-node-action');\
  const kindEl = document.getElementById('graph-node-kind');\
  const subjectEl = document.getElementById('graph-node-subject');\
  const stateEl = document.getElementById('graph-node-state');\
  const provEl = document.getElementById('graph-node-provenance');\
  const rootEl = document.getElementById('graph-node-root');\
  const edgeInfoEl = document.getElementById('graph-edge-info');\
\
  function escHtml(value) {{\
    return String(value)\
      .replace(/&/g, '&amp;')\
      .replace(/</g, '&lt;')\
      .replace(/>/g, '&gt;')\
      .replace(/\"/g, '&quot;')\
      .replace(/'/g, '&#39;');\
  }}\
\
  function setNodeDetails(data) {{\
    if (!emptyEl || !actionEl || !kindEl || !subjectEl || !stateEl || !provEl || !rootEl) {{ return; }}\
    emptyEl.style.display = 'none';\
    actionEl.style.display = 'block';\
    kindEl.style.display = 'block';\
    subjectEl.style.display = 'block';\
    stateEl.style.display = 'block';\
    provEl.style.display = 'block';\
    rootEl.style.display = 'block';\
    actionEl.innerHTML = `action: <a href=\"/action/${{encodeURIComponent(data.action_id)}}/\"><code>${{escHtml(data.action_id)}}</code></a>`;\
    kindEl.innerHTML = `kind: <code>${{escHtml(data.kind)}}</code>`;\
    subjectEl.innerHTML = `subject: <code>${{escHtml(data.subject)}}</code>`;\
    stateEl.innerHTML = `state: <code>${{escHtml(data.state_label)}}</code>`;\
    provEl.innerHTML = `provenance: <code>${{data.has_provenance ? 'yes' : 'no'}}</code>`;\
    rootEl.innerHTML = `root: <code>${{data.is_root ? 'yes' : 'no'}}</code>`;\
    if (edgeInfoEl) {{ edgeInfoEl.style.display = 'none'; edgeInfoEl.textContent = ''; }}\
  }}\
\
  function setEdgeDetails(data) {{\
    if (!emptyEl || !edgeInfoEl) {{ return; }}\
    emptyEl.style.display = 'none';\
    if (actionEl) actionEl.style.display = 'none';\
    if (kindEl) kindEl.style.display = 'none';\
    if (subjectEl) subjectEl.style.display = 'none';\
    if (stateEl) stateEl.style.display = 'none';\
    if (provEl) provEl.style.display = 'none';\
    if (rootEl) rootEl.style.display = 'none';\
    edgeInfoEl.style.display = 'block';\
    edgeInfoEl.innerHTML = `edge: <code>${{escHtml(data.edge_kind)}}</code><br>source: <code>${{escHtml(data.source)}}</code><br>target: <code>${{escHtml(data.target)}}</code><br>role/reason: <code>${{escHtml(data.role)}}</code>`;\
  }}\
\
  cy.on('tap', 'node', (evt) => {{\
    const data = evt.target.data();\
    setNodeDetails(data);\
    if (data && data.action_id) {{\
      const actionUrl = `/action/${{encodeURIComponent(data.action_id)}}/`;\
      window.open(actionUrl, '_blank', 'noopener,noreferrer');\
    }}\
  }});\
  cy.on('tap', 'edge', (evt) => setEdgeDetails(evt.target.data()));\
\
  const flowBtn = document.getElementById('layout-flow');\
  if (flowBtn) flowBtn.addEventListener('click', runFlow);\
  const breadthfirstBtn = document.getElementById('layout-breadthfirst');\
  if (breadthfirstBtn) breadthfirstBtn.addEventListener('click', runBreadthfirst);\
  const coseBtn = document.getElementById('layout-cose');\
  if (coseBtn) coseBtn.addEventListener('click', runCose);\
  const fitBtn = document.getElementById('layout-fit');\
  if (fitBtn) fitBtn.addEventListener('click', () => cy.fit(undefined, 30));\
}})();\
</script>",
            nodes_json, edges_json, roots_json
        );
    }

    html.push_str("</main></body></html>");
    html
}

pub(super) fn render_ir_fn_corpus_k3_losses_vs_crate_version_html(
    dataset: &K3LossByVersionDataset,
    generated_utc: DateTime<Utc>,
) -> String {
    use std::fmt::Write;

    let mut html = String::new();
    html.push_str(
        "<!doctype html>\
<html lang=\"en\">\
<head>\
  <meta charset=\"utf-8\">\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
  <title>xlsynth-bvc K3 Losses vs Crate Version</title>\
  <style>\
    :root {\
      --bg: #050911;\
      --bg2: #0a1323;\
      --panel: #0f1a2d;\
      --panel-hi: #14233b;\
      --ink: #dcffee;\
      --muted: #8fa9be;\
      --accent: #32f1b2;\
      --accent2: #5ec9ff;\
      --warn: #ff6d8f;\
      --line: #2c4566;\
      --border: #213754;\
      --chip: #11233a;\
    }\
    * { box-sizing: border-box; }\
    body {\
      margin: 0;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      color: var(--ink);\
      background:\
        radial-gradient(circle at 12% -15%, rgba(50, 241, 178, 0.18) 0, transparent 36%),\
        radial-gradient(circle at 100% 0%, rgba(94, 201, 255, 0.16) 0, transparent 40%),\
        repeating-linear-gradient(0deg, rgba(143, 169, 190, 0.055) 0, rgba(143, 169, 190, 0.055) 1px, transparent 1px, transparent 24px),\
        repeating-linear-gradient(90deg, rgba(143, 169, 190, 0.04) 0, rgba(143, 169, 190, 0.04) 1px, transparent 1px, transparent 24px),\
        linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);\
      min-height: 100vh;\
    }\
    .wrap { width: 100%; max-width: none; margin: 0; padding: 24px 18px 34px; }\
    .title {\
      margin: 0;\
      font-size: 1.8rem;\
      text-transform: uppercase;\
      letter-spacing: 0.04em;\
      color: var(--accent);\
      text-shadow: 0 0 12px rgba(50, 241, 178, 0.34);\
    }\
    .meta {\
      margin: 8px 0 0;\
      color: var(--muted);\
      font-size: 0.86rem;\
      border-left: 3px solid var(--line);\
      padding-left: 10px;\
    }\
    .nav { margin-top: 10px; display: flex; gap: 9px; flex-wrap: wrap; }\
    .nav a {\
      color: #b9e8ff;\
      text-decoration: none;\
      border: 1px solid var(--border);\
      background: rgba(16, 34, 56, 0.8);\
      border-radius: 6px;\
      padding: 5px 9px;\
      font-size: 0.82rem;\
    }\
    .nav a.active {\
      color: var(--accent);\
      border-color: rgba(50, 241, 178, 0.55);\
      box-shadow: 0 0 0 1px rgba(50, 241, 178, 0.15) inset;\
    }\
    .panel {\
      margin-top: 14px;\
      background: linear-gradient(180deg, var(--panel-hi) 0%, var(--panel) 100%);\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      padding: 12px 13px 13px;\
      box-shadow:\
        inset 0 1px 0 rgba(255, 255, 255, 0.03),\
        0 0 0 1px rgba(50, 241, 178, 0.04),\
        0 12px 28px rgba(0, 0, 0, 0.35);\
    }\
    .controls { display: flex; flex-wrap: wrap; gap: 10px 14px; align-items: end; }\
    .field { display: flex; flex-direction: column; gap: 6px; }\
    .label { color: var(--muted); font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.05em; }\
    .input, .select {\
      min-width: 220px;\
      color: #d7f6ff;\
      background: #102238;\
      border: 1px solid var(--border);\
      border-radius: 6px;\
      padding: 6px 9px;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.8rem;\
    }\
    .submit {\
      color: #dff8ee;\
      background: linear-gradient(180deg, #16416b 0%, #133459 100%);\
      border: 1px solid #2f4b73;\
      border-radius: 6px;\
      padding: 7px 11px;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.8rem;\
      cursor: pointer;\
    }\
    .chip-row { margin-top: 12px; display: flex; flex-wrap: wrap; gap: 8px; }\
    .chip {\
      display: inline-block;\
      border-radius: 999px;\
      padding: 4px 10px;\
      font-size: 0.78rem;\
      border: 1px solid var(--border);\
      background: var(--chip);\
      color: #cde7f7;\
    }\
    .status-badge {\
      display: inline-block;\
      border-radius: 999px;\
      padding: 4px 10px;\
      font-size: 0.74rem;\
      border: 1px solid var(--border);\
      text-transform: uppercase;\
      letter-spacing: 0.04em;\
    }\
    .status-regressed { color: #ffd6df; background: rgba(255, 109, 143, 0.18); border-color: rgba(255, 109, 143, 0.55); }\
    .status-newly-lossy { color: #ffe0c4; background: rgba(255, 167, 95, 0.16); border-color: rgba(255, 167, 95, 0.5); }\
    .status-same { color: #d9e6ef; background: rgba(143, 169, 190, 0.14); border-color: rgba(143, 169, 190, 0.4); }\
    .status-improved { color: #a7ffd2; background: rgba(50, 241, 178, 0.16); border-color: rgba(50, 241, 178, 0.5); }\
    .status-resolved { color: #96ffe8; background: rgba(94, 201, 255, 0.16); border-color: rgba(94, 201, 255, 0.5); }\
    .status-missing-current { color: #ffe8a8; background: rgba(255, 208, 120, 0.16); border-color: rgba(255, 208, 120, 0.5); }\
    .table-wrap { margin-top: 12px; overflow-x: auto; }\
    table { width: 100%; border-collapse: collapse; min-width: 1280px; }\
    th, td {\
      text-align: left;\
      border-top: 1px solid var(--border);\
      padding: 8px 7px;\
      vertical-align: top;\
      font-size: 0.8rem;\
    }\
    thead th {\
      position: sticky;\
      top: 0;\
      background: rgba(11, 23, 42, 0.96);\
      color: var(--muted);\
      text-transform: uppercase;\
      letter-spacing: 0.05em;\
      z-index: 1;\
    }\
    tbody tr:nth-child(even) td { background: rgba(255, 255, 255, 0.015); }\
    .current-col { background: rgba(50, 241, 178, 0.08); }\
    .loss-pos { color: #ffd6df; }\
    .loss-nonpos { color: #a7ffd2; }\
    .loss-missing { color: var(--muted); }\
    .row-source { min-width: 180px; }\
    .row-source .primary {\
      display: inline-block;\
      max-width: 220px;\
      overflow: hidden;\
      text-overflow: ellipsis;\
      white-space: nowrap;\
      vertical-align: top;\
    }\
    .row-source .secondary { margin-top: 4px; color: var(--muted); font-size: 0.74rem; }\
    .sample-meta { margin-top: 4px; color: var(--muted); font-size: 0.72rem; }\
    .sample-meta a { color: #b9e8ff; text-decoration: none; }\
    .muted { color: var(--muted); }\
    code {\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
      color: #bbffda;\
      background: rgba(17, 35, 58, 0.72);\
      border: 1px solid var(--border);\
      border-radius: 4px;\
      padding: 1px 4px;\
    }\
    a { color: #9edcff; text-decoration: none; }\
  </style>\
</head>\
<body>\
  <main class=\"wrap\">",
    );

    let selected_crate_label = dataset
        .selected_crate_version
        .as_deref()
        .map(|version| format!("crate:v{version}"))
        .unwrap_or_else(|| "none".to_string());
    let compared_versions_label = if dataset.compared_crate_versions.is_empty() {
        "none".to_string()
    } else {
        dataset
            .compared_crate_versions
            .iter()
            .map(|version| format!("v{version}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let corpus_compare_url = ir_fn_corpus_g8r_vs_yosys_view_url(
        Some(dataset.applied_max_ir_nodes),
        dataset.selected_crate_version.as_deref(),
    );
    let frontend_compare_url = ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_view_url(
        Some(dataset.applied_max_ir_nodes),
        dataset.selected_crate_version.as_deref(),
    );
    let k3_loss_url = ir_fn_corpus_k3_losses_vs_crate_version_view_url(
        Some(dataset.applied_max_ir_nodes),
        dataset.selected_crate_version.as_deref(),
        dataset.show_same,
    );
    let same_rows_label = if dataset.show_same {
        format!("same: {}", dataset.status_counts.same)
    } else {
        format!("same hidden: {}", dataset.status_counts.same)
    };

    let _ = write!(
        html,
        "<h1 class=\"title\">IR Fn Corpus K3 Losses vs Crate Version</h1>\
<p class=\"meta\">generated={} | selected_crate={} | compared_versions={} | visible_rows={} | ir_node_filter<= {}</p>\
<p class=\"meta\">Status compares the selected crate against the nearest earlier crate version that has data for the same structural hash. Rows stay visible when the current loss resolved to <= 0 or when the current crate is still missing the row. Unchanged rows are hidden by default.</p>\
<nav class=\"nav\">\
<a href=\"/versions/\">/versions/</a>\
<a href=\"{}\">/ir-fn-corpus-g8r-vs-yosys-abc/</a>\
<a href=\"{}\">/ir-fn-g8r-abc-vs-codegen-yosys-abc/</a>\
<a class=\"active\" href=\"{}\">/ir-fn-corpus-k3-losses-vs-crate-version/</a>\
<a href=\"/ir-fn-corpus-structural/\">/ir-fn-corpus-structural/</a>\
</nav>",
        html_escape(&generated_utc.to_rfc3339()),
        html_escape(&selected_crate_label),
        html_escape(&compared_versions_label),
        dataset.rows.len(),
        dataset.applied_max_ir_nodes,
        html_escape(&corpus_compare_url),
        html_escape(&frontend_compare_url),
        html_escape(&k3_loss_url),
    );

    html.push_str(
        "<section class=\"panel\">\
<form method=\"get\" action=\"/ir-fn-corpus-k3-losses-vs-crate-version/\" class=\"controls\">\
<label class=\"field\"><span class=\"label\">Crate Version</span>\
<select class=\"select\" name=\"crate_version\">",
    );
    for version in &dataset.available_crate_versions {
        let selected = if Some(version.as_str()) == dataset.selected_crate_version.as_deref() {
            "selected"
        } else {
            ""
        };
        let _ = write!(
            html,
            "<option value=\"{}\" {}>crate:v{}</option>",
            html_escape(version),
            selected,
            html_escape(version),
        );
    }
    let _ = write!(
        html,
        "</select></label>\
<label class=\"field\"><span class=\"label\">Max IR Nodes</span>\
<input class=\"input\" type=\"number\" min=\"{}\" max=\"{}\" name=\"max_ir_nodes\" value=\"{}\"></label>\
<label class=\"field\"><span class=\"label\">Row Filter</span>\
<select class=\"select\" name=\"show_same\">\
<option value=\"false\" {}>hide same</option>\
<option value=\"true\" {}>show same</option>\
</select></label>\
<button class=\"submit\" type=\"submit\">Apply</button>\
</form>\
<div class=\"chip-row\">\
<span class=\"chip\">regressed: {}</span>\
<span class=\"chip\">newly_lossy: {}</span>\
<span class=\"chip\">{}</span>\
<span class=\"chip\">improved: {}</span>\
<span class=\"chip\">resolved: {}</span>\
<span class=\"chip\">missing_current: {}</span>\
</div>\
</section>",
        dataset.min_ir_nodes,
        dataset.max_ir_nodes,
        dataset.applied_max_ir_nodes,
        if !dataset.show_same { "selected" } else { "" },
        if dataset.show_same { "selected" } else { "" },
        dataset.status_counts.regressed,
        dataset.status_counts.newly_lossy,
        html_escape(&same_rows_label),
        dataset.status_counts.improved,
        dataset.status_counts.resolved,
        dataset.status_counts.missing_current,
    );

    if dataset.rows.is_empty() {
        html.push_str(
            "<section class=\"panel\"><p class=\"muted\">No k3 loss rows matched the selected crate version and IR-node threshold.</p></section>",
        );
        html.push_str("</main></body></html>");
        return html;
    }

    let status_badge_class = |status: K3LossChangeStatus| match status {
        K3LossChangeStatus::Regressed => "status-regressed",
        K3LossChangeStatus::NewlyLossy => "status-newly-lossy",
        K3LossChangeStatus::Same => "status-same",
        K3LossChangeStatus::Improved => "status-improved",
        K3LossChangeStatus::Resolved => "status-resolved",
        K3LossChangeStatus::MissingCurrent => "status-missing-current",
    };
    let loss_value_class = |loss: Option<f64>| match loss {
        Some(value) if value > 0.0 => "loss-pos",
        Some(_) => "loss-nonpos",
        None => "loss-missing",
    };
    let sample_cell_html = |sample: Option<&K3LossByVersionSampleRef>,
                            empty_text: &str|
     -> String {
        match sample {
            Some(sample) => format!(
                "<div class=\"{}\">{}</div><div class=\"sample-meta\"><code>v{}</code> · <a href=\"/action/{}/\">g8r</a> · <a href=\"/action/{}/\">yabc</a> · ({}, {}) vs ({}, {})</div>",
                loss_value_class(Some(sample.g8r_product_loss)),
                html_escape(&format_metric_value(sample.g8r_product_loss)),
                html_escape(&sample.crate_version),
                html_escape(&sample.g8r_stats_action_id),
                html_escape(&sample.yosys_abc_stats_action_id),
                html_escape(&format_metric_value(sample.g8r_nodes)),
                html_escape(&format_metric_value(sample.g8r_levels)),
                html_escape(&format_metric_value(sample.yosys_abc_nodes)),
                html_escape(&format_metric_value(sample.yosys_abc_levels)),
            ),
            None => format!("<span class=\"muted\">{}</span>", html_escape(empty_text)),
        }
    };

    html.push_str(
        "<section class=\"panel\"><div class=\"table-wrap\"><table><thead><tr>\
<th>status</th><th>source</th><th>k3 top</th><th>ir nodes</th><th>previous</th><th>current</th><th>delta</th>",
    );
    for version in &dataset.compared_crate_versions {
        let current_class = if Some(version.as_str()) == dataset.selected_crate_version.as_deref() {
            " class=\"current-col\""
        } else {
            ""
        };
        let _ = write!(html, "<th{}>v{}</th>", current_class, html_escape(version),);
    }
    html.push_str("</tr></thead><tbody>");
    for row in &dataset.rows {
        let compact_fn_key = row.fn_key.strip_prefix("fn ").unwrap_or(&row.fn_key);
        let compact_fn_key = compact_fn_key
            .split('(')
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("-");
        let current_loss_class = if row
            .current_sample
            .as_ref()
            .is_some_and(|sample| sample.g8r_product_loss > 0.0)
        {
            "loss-pos"
        } else if row.current_sample.is_some() {
            "loss-nonpos"
        } else {
            "loss-missing"
        };
        let _ = write!(
            html,
            "<tr>\
<td><span class=\"status-badge {}\">{}</span></td>\
<td class=\"row-source\"><span class=\"primary\" title=\"{}\"><code>{}</code></span><div class=\"secondary\"><a href=\"/ir-fn-corpus-structural/group/{}/\"><code>{}</code></a></div></td>\
<td>{}</td>\
<td><code>{}</code></td>\
<td>{}</td>\
<td class=\"{}\">{}</td>\
<td>{}</td>",
            status_badge_class(row.status),
            html_escape(row.status.label()),
            html_escape(&row.fn_key),
            html_escape(compact_fn_key),
            html_escape(&row.structural_hash),
            html_escape(short_id(&row.structural_hash)),
            html_escape(row.ir_top.as_deref().unwrap_or("-")),
            row.ir_node_count,
            sample_cell_html(row.previous_sample.as_ref(), "no prior sample"),
            current_loss_class,
            sample_cell_html(row.current_sample.as_ref(), "missing in selected crate"),
            row.delta_vs_previous
                .map(format_metric_delta)
                .map(|value| html_escape(&value))
                .unwrap_or_else(|| "<span class=\"muted\">-</span>".to_string()),
        );
        for (index, version) in dataset.compared_crate_versions.iter().enumerate() {
            let current_class =
                if Some(version.as_str()) == dataset.selected_crate_version.as_deref() {
                    " current-col"
                } else {
                    ""
                };
            let cell_class = loss_value_class(row.version_losses.get(index).copied().flatten());
            let cell_value = row
                .version_losses
                .get(index)
                .copied()
                .flatten()
                .map(format_metric_value)
                .map(|value| html_escape(&value))
                .unwrap_or_else(|| "<span class=\"muted\">-</span>".to_string());
            let _ = write!(
                html,
                "<td class=\"{}{}\">{}</td>",
                cell_class, current_class, cell_value,
            );
        }
        html.push_str("</tr>");
    }
    html.push_str("</tbody></table></div></section></main></body></html>");
    html
}

fn format_metric_value(value: f64) -> String {
    if (value - value.round()).abs() < 1e-9 {
        format!("{:.0}", value)
    } else {
        format!("{:.2}", value)
    }
}

fn format_metric_delta(value: f64) -> String {
    if value.abs() < 1e-9 {
        return "0".to_string();
    }
    if (value - value.round()).abs() < 1e-9 {
        format!("{:+.0}", value)
    } else {
        format!("{:+.2}", value)
    }
}

fn stdlib_series_color(seed: &str) -> String {
    let digest = Sha256::digest(seed.as_bytes());
    let hue = (((u16::from(digest[0])) << 8) | u16::from(digest[1])) % 360;
    format!("hsl({hue} 84% 62%)")
}

pub(super) fn render_action_not_found_html(action_id: &str) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\"><title>Action Not Found</title><style>body{{margin:0;font-family:\"JetBrains Mono\",\"IBM Plex Mono\",\"Consolas\",monospace;background:#070b12;color:#d8ffe9;}}.wrap{{max-width:920px;margin:0 auto;padding:28px 20px;}}.card{{background:#111c2f;border:1px solid #1f3350;border-radius:8px;padding:14px;}}a{{color:#3fc2ff;text-decoration:none;}}</style></head><body><main class=\"wrap\"><p><a href=\"/versions/\">/versions/</a></p><section class=\"card\"><h1>Action Not Found</h1><p>No provenance or queue records found for <code>{}</code>.</p></section></main></body></html>",
        html_escape(action_id)
    )
}

fn extract_failed_command_from_error(error: &str) -> Option<String> {
    let mut lines = error.lines();
    while let Some(line) = lines.next() {
        if let Some(rest) = line.strip_prefix("command:") {
            let inline = rest.trim();
            if !inline.is_empty() {
                return Some(inline.to_string());
            }
            let next = lines.next()?.trim();
            if !next.is_empty() {
                return Some(next.to_string());
            }
        }
    }
    None
}

fn render_related_actions_table(html: &mut String, title: &str, rows: &[RelatedActionRowView]) {
    use std::fmt::Write;

    let _ = write!(
        html,
        "<details open><summary>{} ({})</summary>",
        html_escape(title),
        rows.len()
    );
    if rows.is_empty() {
        html.push_str("<p class=\"meta\">none</p></details>");
        return;
    }

    html.push_str(
        "<table>\
<thead><tr><th>Hops</th><th>Action ID</th><th>State</th><th>Kind</th><th>Subject</th><th>Output</th></tr></thead>\
<tbody>",
    );
    for row in rows {
        let _ = write!(
            html,
            "<tr>\
<td>{}</td>\
<td><a href=\"/action/{}/\"><code>{}</code></a></td>\
<td><span class=\"state-tag {}\">{}</span></td>\
<td><code>{}</code></td>\
<td>{}</td>\
<td><code>{}</code></td>\
</tr>",
            row.distance_hops,
            html_escape(&row.action_id),
            html_escape(&row.action_id),
            html_escape(&row.queue_state_key),
            html_escape(&row.queue_state_label),
            html_escape(&row.action_kind),
            html_escape(&row.subject),
            html_escape(row.output_artifact.as_deref().unwrap_or("-"))
        );
    }
    html.push_str("</tbody></table></details>");
}

pub(super) fn render_action_detail_html(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
) -> Result<Option<String>> {
    use std::fmt::Write;

    let records = load_action_detail_records(store, action_id)?;
    if !action_detail_has_any_records(&records) {
        return Ok(None);
    }

    let queue_state = queue_state_for_action(store, action_id);
    let queue_state_css_key = queue_state_key(&queue_state);
    let queue_state_label = queue_state_display_label(&queue_state);

    let action = action_spec_from_records(&records);

    let compat_by_dso = load_compat_by_dso(repo_root);
    let inferred_crate_version =
        action.and_then(|a| infer_crate_version_for_action(store, a, &compat_by_dso));
    let action_kind = action.map(action_kind_label).unwrap_or("unknown");
    let action_subject_value = action
        .map(action_subject)
        .unwrap_or_else(|| "(not available)".to_string());
    let action_dso = action
        .and_then(action_dso_version)
        .map(|v| normalize_tag_version(v).to_string());
    let action_driver = action
        .and_then(action_driver_version)
        .map(|v| normalize_tag_version(v).to_string());
    let action_json = action
        .map(serde_json::to_string_pretty)
        .transpose()
        .context("serializing action for action detail page")?;
    let related_actions = build_action_related_actions_view(store, action_id, action)?;

    let mut html = String::new();
    html.push_str(
        "<!doctype html>\
<html lang=\"en\">\
<head>\
  <meta charset=\"utf-8\">\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
  <title>xlsynth-bvc Action</title>\
  <style>\
    :root {\
      --bg: #070b12;\
      --bg2: #0b1220;\
      --panel: #0d1524;\
      --panel-hi: #111c2f;\
      --ink: #d8ffe9;\
      --muted: #8da9a0;\
      --accent: #35f2b3;\
      --accent2: #3fc2ff;\
      --warn: #ff6489;\
      --border: #1f3350;\
    }\
    * { box-sizing: border-box; }\
    body {\
      margin: 0;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      color: var(--ink);\
      background: linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);\
      min-height: 100vh;\
    }\
    .wrap { width: 100%; max-width: 1200px; margin: 0 auto; padding: 24px 20px 34px; }\
    .title { margin: 0 0 6px; font-size: 1.5rem; color: var(--accent); }\
    .subtitle { margin: 8px 0 16px; color: var(--muted); }\
    .subtitle a { color: var(--accent2); text-decoration: none; border-bottom: 1px dotted #2f4b73; }\
    .card {\
      background: linear-gradient(180deg, var(--panel-hi) 0%, var(--panel) 100%);\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      padding: 14px;\
      margin-bottom: 12px;\
    }\
    .card h2 { margin: 0 0 8px; font-size: 1.03rem; color: var(--accent2); }\
    .meta { margin: 4px 0; color: var(--muted); }\
    .meta strong { color: var(--ink); }\
    .chip-row { margin-top: 10px; display: flex; flex-wrap: wrap; gap: 8px; }\
    .chip {\
      display: inline-block;\
      border: 1px solid var(--border);\
      border-radius: 999px;\
      padding: 4px 9px;\
      font-size: 0.78rem;\
      color: #cbe6da;\
      background: rgba(19, 32, 54, 0.72);\
    }\
    .chip.timeout { color: #ffe8a8; border-color: rgba(255, 208, 120, 0.55); background: rgba(255, 208, 120, 0.14); }\
    .state-tag {\
      display: inline-block;\
      border: 1px solid var(--border);\
      border-radius: 999px;\
      padding: 2px 8px;\
      font-size: 0.74rem;\
      color: #cbe6da;\
      background: rgba(19, 32, 54, 0.68);\
    }\
    .state-tag.pending, .state-tag.running, .state-tag.timeout { color: #ffe8a8; border-color: rgba(255, 208, 120, 0.45); }\
    .state-tag.failed, .state-tag.canceled { color: #ffc3d3; border-color: rgba(255, 100, 137, 0.45); }\
    .state-tag.done { color: #9af7c8; border-color: rgba(105, 240, 174, 0.5); }\
    .state-tag.not_queued { color: #b8c9d7; border-color: rgba(142, 168, 190, 0.45); }\
    table { width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 0.84rem; }\
    th, td { text-align: left; border-top: 1px solid var(--border); padding: 7px 6px; vertical-align: top; }\
    th { color: var(--muted); text-transform: uppercase; font-size: 0.73rem; letter-spacing: 0.05em; }\
    a { color: var(--accent2); text-decoration: none; }\
    code {\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.8rem;\
      color: #b8ffd9;\
      background: rgba(20, 37, 58, 0.6);\
      border: 1px solid var(--border);\
      border-radius: 4px;\
      padding: 1px 4px;\
    }\
    pre {\
      margin: 8px 0 0;\
      padding: 10px;\
      border: 1px solid var(--border);\
      border-radius: 6px;\
      background: rgba(10, 20, 35, 0.88);\
      color: #d8ffe9;\
      overflow-x: auto;\
      max-height: 360px;\
      white-space: pre-wrap;\
      word-break: break-word;\
    }\
  </style>\
</head>\
<body>\
  <main class=\"wrap\">",
    );

    let _ = write!(
        html,
        "<p class=\"subtitle\"><a href=\"/versions/\">/versions/</a></p>\
<h1 class=\"title\">Action <code>{}</code></h1>",
        html_escape(action_id)
    );

    let _ = write!(
        html,
        "<section class=\"card\">\
<h2>Summary</h2>\
<p class=\"meta\">state: <span class=\"state-tag {}\">{}</span></p>\
<p class=\"meta\">kind: <strong>{}</strong></p>\
<p class=\"meta\">subject: <strong>{}</strong></p>\
<div class=\"chip-row\">\
<span class=\"chip\">crate:{}</span>\
<span class=\"chip\">dso:{}</span>\
<span class=\"chip\">driver:{}</span>\
</div>",
        html_escape(queue_state_css_key),
        html_escape(queue_state_label),
        html_escape(action_kind),
        html_escape(&action_subject_value),
        html_escape(
            inferred_crate_version
                .as_ref()
                .map(|v| format!("v{}", normalize_tag_version(v)))
                .as_deref()
                .unwrap_or("unknown")
        ),
        html_escape(
            action_dso
                .as_ref()
                .map(|v| format!("v{}", normalize_tag_version(v)))
                .as_deref()
                .unwrap_or("-")
        ),
        html_escape(
            action_driver
                .as_ref()
                .map(|v| format!("crate:v{}", normalize_tag_version(v)))
                .as_deref()
                .unwrap_or("-")
        )
    );

    if let Some(done) = &records.done {
        let _ = write!(
            html,
            "<p class=\"meta\">done_utc: <code>{}</code> by <code>{}</code></p>",
            html_escape(&done.completed_utc.to_rfc3339()),
            html_escape(&done.completed_by)
        );
    }
    if let Some(running) = &records.running {
        let _ = write!(
            html,
            "<p class=\"meta\">running: owner=<code>{}</code> acquired=<code>{}</code> expires=<code>{}</code></p>",
            html_escape(&running.lease_owner),
            html_escape(&running.lease_acquired_utc.to_rfc3339()),
            html_escape(&running.lease_expires_utc.to_rfc3339())
        );
    }
    if let Some(pending) = &records.pending {
        let _ = write!(
            html,
            "<p class=\"meta\">enqueued_utc: <code>{}</code></p>",
            html_escape(&pending.enqueued_utc.to_rfc3339())
        );
    }
    if let Some(canceled) = &records.canceled {
        let _ = write!(
            html,
            "<p class=\"meta\">canceled_utc: <code>{}</code> by <code>{}</code></p>\
<p class=\"meta\">blocked by: <a href=\"/action/{}/\"><code>{}</code></a> | root failed: <a href=\"/action/{}/\"><code>{}</code></a></p>",
            html_escape(&canceled.canceled_utc.to_rfc3339()),
            html_escape(&canceled.canceled_by),
            html_escape(&canceled.canceled_due_to_action_id),
            html_escape(&canceled.canceled_due_to_action_id),
            html_escape(&canceled.root_failed_action_id),
            html_escape(&canceled.root_failed_action_id)
        );
    }
    html.push_str("</section>");

    if let Some(action) = action {
        let dependency_entries = action_dependency_role_action_ids(action);
        if !dependency_entries.is_empty() {
            html.push_str(
                "<section class=\"card\">\
<h2>Upstream Dependencies</h2>\
<table>\
  <thead><tr><th>Role</th><th>Action ID</th><th>State</th><th>Kind</th><th>Subject</th><th>Output</th></tr></thead>\
  <tbody>",
            );
            for (role, dependency_action_id) in dependency_entries {
                let dependency_state = if store.action_exists(dependency_action_id) {
                    QueueState::Done
                } else {
                    queue_state_for_action(store, dependency_action_id)
                };
                let dependency_records =
                    match load_action_detail_records(store, dependency_action_id) {
                        Ok(records) if action_detail_has_any_records(&records) => Some(records),
                        _ => None,
                    };
                let dependency_action = dependency_records
                    .as_ref()
                    .and_then(action_spec_from_records);
                let dependency_kind = dependency_action
                    .map(action_kind_label)
                    .unwrap_or("unknown");
                let dependency_subject = dependency_action
                    .map(action_subject)
                    .unwrap_or_else(|| "(not available)".to_string());
                let dependency_output = dependency_records
                    .as_ref()
                    .and_then(|records| records.provenance.as_ref())
                    .map(|provenance| {
                        format!(
                            "{} {}",
                            artifact_type_label(&provenance.output_artifact.artifact_type),
                            provenance.output_artifact.relpath
                        )
                    })
                    .unwrap_or_else(|| "-".to_string());
                let _ = write!(
                    html,
                    "<tr>\
<td><code>{}</code></td>\
<td><a href=\"/action/{}/\"><code>{}</code></a></td>\
<td><span class=\"state-tag {}\">{}</span></td>\
<td><code>{}</code></td>\
<td>{}</td>\
<td><code>{}</code></td>\
</tr>",
                    html_escape(role),
                    html_escape(dependency_action_id),
                    html_escape(dependency_action_id),
                    html_escape(queue_state_key(&dependency_state)),
                    html_escape(queue_state_display_label(&dependency_state)),
                    html_escape(dependency_kind),
                    html_escape(&dependency_subject),
                    html_escape(&dependency_output)
                );
            }
            html.push_str("</tbody></table></section>");
        }
    }

    html.push_str(
        "<section class=\"card\">\
<h2>Related Actions</h2>\
<p class=\"meta\">transitive closure over dependency edges (upstream and downstream)</p>",
    );
    render_related_actions_table(
        &mut html,
        "Upstream (dependencies, transitive)",
        &related_actions.upstream,
    );
    render_related_actions_table(
        &mut html,
        "Downstream (dependents, transitive)",
        &related_actions.downstream,
    );
    html.push_str("</section>");

    if let Some(failed) = &records.failed {
        let failure_state_class = if is_timeout_error(&failed.error) {
            "state-tag timeout"
        } else {
            "state-tag failed"
        };
        let failure_state_label = if is_timeout_error(&failed.error) {
            "timeout"
        } else {
            "failed"
        };
        let _ = write!(
            html,
            "<section class=\"card\">\
<h2>Failure</h2>\
<p class=\"meta\"><span class=\"{}\">{}</span> at <code>{}</code> by <code>{}</code></p>\
<p class=\"meta\">summary: <strong>{}</strong></p>\
<pre>{}</pre>",
            failure_state_class,
            html_escape(failure_state_label),
            html_escape(&failed.failed_utc.to_rfc3339()),
            html_escape(&failed.failed_by),
            html_escape(&summarize_error(&failed.error)),
            html_escape(&failed.error)
        );
        if let Some(command) = extract_failed_command_from_error(&failed.error) {
            let _ = write!(
                html,
                "<details open><summary>Attempted Command</summary><pre>{}</pre></details>",
                html_escape(&command)
            );
        }
        html.push_str("</section>");
    }

    if let Some(canceled) = &records.canceled {
        let _ = write!(
            html,
            "<section class=\"card\">\
<h2>Cancellation Reason</h2>\
<pre>{}</pre>\
</section>",
            html_escape(&canceled.reason)
        );
    }

    if let Some(provenance) = &records.provenance {
        let output_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
        let output_path_display = repo_relative_display_path(repo_root, &output_path);
        let _ = write!(
            html,
            "<section class=\"card\">\
<h2>Provenance</h2>\
<p class=\"meta\">created_utc: <code>{}</code></p>\
<p class=\"meta\">output artifact: <code>{}</code> relpath=<code>{}</code></p>\
<p class=\"meta\">output path: <code>{}</code></p>",
            html_escape(&provenance.created_utc.to_rfc3339()),
            html_escape(artifact_type_label(
                &provenance.output_artifact.artifact_type
            )),
            html_escape(&provenance.output_artifact.relpath),
            html_escape(&output_path_display)
        );

        if !provenance.dependencies.is_empty() {
            html.push_str(
                "<details open><summary>Dependencies</summary><table><thead><tr><th>Action ID</th><th>Artifact Type</th><th>Relpath</th><th>Path</th></tr></thead><tbody>",
            );
            for dep in &provenance.dependencies {
                let dep_path = store.resolve_artifact_ref_path(dep);
                let dep_path_display = repo_relative_display_path(repo_root, &dep_path);
                let _ = write!(
                    html,
                    "<tr>\
<td><a href=\"/action/{}/\"><code>{}</code></a></td>\
<td><code>{}</code></td>\
<td><code>{}</code></td>\
<td><code>{}</code></td>\
</tr>",
                    html_escape(&dep.action_id),
                    html_escape(&dep.action_id),
                    html_escape(artifact_type_label(&dep.artifact_type)),
                    html_escape(&dep.relpath),
                    html_escape(&dep_path_display)
                );
            }
            html.push_str("</tbody></table></details>");
        }

        if !provenance.suggested_next_actions.is_empty() {
            html.push_str(
                "<details><summary>Suggested Next Actions</summary><table><thead><tr><th>Action ID</th><th>State</th><th>Kind</th><th>Subject</th><th>Reason</th></tr></thead><tbody>",
            );
            for suggested in &provenance.suggested_next_actions {
                let suggestion_state = if store.action_exists(&suggested.action_id) {
                    QueueState::Done
                } else {
                    queue_state_for_action(store, &suggested.action_id)
                };
                let _ = write!(
                    html,
                    "<tr>\
<td><a href=\"/action/{}/\"><code>{}</code></a></td>\
<td><span class=\"state-tag {}\">{}</span></td>\
<td><code>{}</code></td>\
<td>{}</td>\
<td>{}</td>\
</tr>",
                    html_escape(&suggested.action_id),
                    html_escape(&suggested.action_id),
                    html_escape(queue_state_key(&suggestion_state)),
                    html_escape(queue_state_display_label(&suggestion_state)),
                    html_escape(action_kind_label(&suggested.action)),
                    html_escape(&action_subject(&suggested.action)),
                    html_escape(&suggested.reason)
                );
            }
            html.push_str("</tbody></table></details>");
        }

        if !provenance.output_files.is_empty() {
            html.push_str(
                "<details><summary>Output Files</summary><table><thead><tr><th>Path</th><th>Bytes</th><th>SHA256</th></tr></thead><tbody>",
            );
            for file in &provenance.output_files {
                let output_file_url = format!(
                    "/action/{}/output-file?path={}",
                    query_component_escape(action_id),
                    query_component_escape(&file.path)
                );
                let _ = write!(
                    html,
                    "<tr>\
<td><a href=\"{}\" target=\"_blank\" rel=\"noopener noreferrer\"><code>{}</code></a></td>\
<td>{}</td>\
<td><a href=\"{}\" target=\"_blank\" rel=\"noopener noreferrer\"><code>{}</code></a></td>\
</tr>",
                    html_escape(&output_file_url),
                    html_escape(&file.path),
                    file.bytes,
                    html_escape(&output_file_url),
                    html_escape(&file.sha256)
                );
            }
            html.push_str("</tbody></table></details>");
        }

        if !provenance.commands.is_empty() {
            html.push_str(
                "<details open><summary>Executed Commands</summary><table><thead><tr><th>#</th><th>Exit</th><th>Command</th></tr></thead><tbody>",
            );
            for (i, command) in provenance.commands.iter().enumerate() {
                let _ = write!(
                    html,
                    "<tr><td>{}</td><td>{}</td><td><pre>{}</pre></td></tr>",
                    i + 1,
                    command.exit_code,
                    html_escape(&command.argv.join(" "))
                );
            }
            html.push_str("</tbody></table></details>");
        }

        if let Ok(details_json) = serde_json::to_string_pretty(&provenance.details) {
            let _ = write!(
                html,
                "<details><summary>Details JSON</summary><pre>{}</pre></details>",
                html_escape(&details_json)
            );
        }

        html.push_str("</section>");
    }

    if let Some(action_json) = action_json {
        let _ = write!(
            html,
            "<section class=\"card\"><h2>Action JSON</h2><pre>{}</pre></section>",
            html_escape(&action_json)
        );
    }

    html.push_str("</main></body></html>");
    Ok(Some(html))
}

pub(super) fn render_versions_html(
    cards: &[VersionCardView],
    unattributed_actions: &[UnattributedActionView],
    unprocessed: &[UnprocessedVersionRowView],
    live_status: &QueueLiveStatusView,
    show_live_queue: bool,
    show_db_size_link: bool,
    db_size_bytes: Option<u64>,
    generated_utc: DateTime<Utc>,
) -> String {
    use std::fmt::Write;

    let mut html = String::new();
    html.push_str(
        "<!doctype html>\
<html lang=\"en\">\
<head>\
  <meta charset=\"utf-8\">\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">\
  <title>xlsynth-bvc Versions</title>\
  <style>\
    :root {\
      --bg: #070b12;\
      --bg2: #0b1220;\
      --panel: #0d1524;\
      --panel-hi: #111c2f;\
      --ink: #d8ffe9;\
      --muted: #8da9a0;\
      --accent: #35f2b3;\
      --accent2: #3fc2ff;\
      --warn: #ff6489;\
      --chip: #132036;\
      --border: #1f3350;\
      --border-hi: #2f4b73;\
    }\
    * { box-sizing: border-box; }\
    body {\
      margin: 0;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      color: var(--ink);\
      background:\
        radial-gradient(circle at 15% -10%, rgba(53, 242, 179, 0.16) 0, transparent 38%),\
        radial-gradient(circle at 100% 0%, rgba(63, 194, 255, 0.12) 0, transparent 42%),\
        repeating-linear-gradient(0deg, rgba(141, 169, 160, 0.06) 0, rgba(141, 169, 160, 0.06) 1px, transparent 1px, transparent 22px),\
        repeating-linear-gradient(90deg, rgba(141, 169, 160, 0.045) 0, rgba(141, 169, 160, 0.045) 1px, transparent 1px, transparent 22px),\
        linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);\
      min-height: 100vh;\
    }\
    .wrap { width: 100%; max-width: none; margin: 0; padding: 24px 20px 36px; }\
    .title {\
      margin: 0;\
      font-size: 1.95rem;\
      letter-spacing: 0.04em;\
      text-transform: uppercase;\
      color: var(--accent);\
      text-shadow: 0 0 14px rgba(53, 242, 179, 0.32);\
    }\
    .subtitle {\
      margin: 8px 0 20px;\
      color: var(--muted);\
      font-size: 0.9rem;\
      border-left: 3px solid var(--border-hi);\
      padding-left: 10px;\
    }\
    .subtitle a {\
      color: var(--accent2);\
      text-decoration: none;\
      border-bottom: 1px dotted var(--border-hi);\
    }\
    .diag {\
      margin: 8px 0 18px;\
      padding: 10px 12px;\
      border-radius: 8px;\
      border: 1px solid rgba(255, 100, 137, 0.45);\
      background: rgba(255, 100, 137, 0.08);\
      color: #ffc3d3;\
      font-size: 0.84rem;\
    }\
    .diag code { color: #ffdce7; }\
    .cards { display: grid; gap: 14px; }\
    .card {\
      background: linear-gradient(180deg, var(--panel-hi) 0%, var(--panel) 100%);\
      border: 1px solid var(--border);\
      border-radius: 8px;\
      padding: 15px 15px 13px;\
      box-shadow:\
        inset 0 1px 0 rgba(255, 255, 255, 0.03),\
        0 0 0 1px rgba(53, 242, 179, 0.05),\
        0 10px 30px rgba(0, 0, 0, 0.35);\
    }\
    .card h2 {\
      margin: 0 0 8px;\
      font-size: 1.12rem;\
      color: var(--accent2);\
      letter-spacing: 0.02em;\
    }\
    .meta { margin: 2px 0; color: var(--muted); }\
    .meta strong { color: var(--ink); }\
    .chip-row { margin-top: 10px; display: flex; flex-wrap: wrap; gap: 8px; }\
    .chip {\
      display: inline-block;\
      background: var(--chip);\
      border: 1px solid var(--border);\
      border-radius: 999px;\
      padding: 4px 10px;\
      font-size: 0.78rem;\
      color: #cbe6da;\
    }\
    .chip.fail {\
      background: rgba(255, 100, 137, 0.14);\
      border-color: rgba(255, 100, 137, 0.45);\
      color: #ffc3d3;\
    }\
    .chip.timeout {\
      background: rgba(255, 208, 120, 0.14);\
      border-color: rgba(255, 208, 120, 0.55);\
      color: #ffe8a8;\
    }\
    .chip.running-owned {\
      background: rgba(105, 240, 174, 0.14);\
      border-color: rgba(105, 240, 174, 0.5);\
      color: #9af7c8;\
    }\
    .chip.running-foreign {\
      background: rgba(255, 208, 120, 0.14);\
      border-color: rgba(255, 208, 120, 0.55);\
      color: #ffe8a8;\
    }\
    details { margin-top: 12px; }\
    summary {\
      cursor: pointer;\
      color: var(--accent);\
      font-weight: 600;\
      letter-spacing: 0.01em;\
    }\
    table { width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 0.84rem; }\
    th, td {\
      text-align: left;\
      border-top: 1px solid var(--border);\
      padding: 7px 6px;\
      vertical-align: top;\
    }\
    tbody tr:nth-child(even) td { background: rgba(255, 255, 255, 0.015); }\
    th { color: var(--muted); font-weight: 600; text-transform: uppercase; font-size: 0.74rem; letter-spacing: 0.05em; }\
    code {\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.8rem;\
      color: #b8ffd9;\
      background: rgba(20, 37, 58, 0.6);\
      border: 1px solid var(--border);\
      border-radius: 4px;\
      padding: 1px 4px;\
    }\
    .ok { color: #69f0ae; font-weight: 700; }\
    .fail-flag { color: var(--warn); font-weight: 700; text-shadow: 0 0 10px rgba(255, 100, 137, 0.35); }\
    .status-grid {\
      margin-top: 10px;\
      display: grid;\
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));\
      gap: 8px;\
    }\
    .stat {\
      border: 1px solid var(--border);\
      background: rgba(19, 32, 54, 0.72);\
      border-radius: 7px;\
      padding: 8px 10px;\
    }\
    .stat .k { color: var(--muted); font-size: 0.74rem; text-transform: uppercase; }\
    .stat .v { color: var(--ink); font-size: 1.12rem; font-weight: 700; margin-top: 2px; }\
    .queue-running-title { margin: 10px 0 6px; color: var(--accent2); font-size: 0.9rem; }\
    .enqueue-btn {\
      color: #dff8ee;\
      background: linear-gradient(180deg, #16416b 0%, #133459 100%);\
      border: 1px solid #2f4b73;\
      border-radius: 6px;\
      padding: 6px 9px;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
      cursor: pointer;\
    }\
    .enqueue-btn:hover { border-color: #5ec9ff; }\
    .enqueue-btn:disabled {\
      cursor: default;\
      color: #8ea8be;\
      border-color: #2a3e59;\
      background: rgba(19, 32, 54, 0.55);\
    }\
    .runner-btn {\
      color: #dff8ee;\
      background: linear-gradient(180deg, #16416b 0%, #133459 100%);\
      border: 1px solid #2f4b73;\
      border-radius: 6px;\
      padding: 6px 9px;\
      font-family: \"JetBrains Mono\", \"IBM Plex Mono\", \"Consolas\", monospace;\
      font-size: 0.78rem;\
      cursor: pointer;\
      margin-left: 8px;\
    }\
    .runner-btn:hover { border-color: #5ec9ff; }\
    .runner-btn:disabled {\
      cursor: default;\
      color: #8ea8be;\
      border-color: #2a3e59;\
      background: rgba(19, 32, 54, 0.55);\
    }\
    .state-tag {\
      display: inline-block;\
      border: 1px solid var(--border);\
      border-radius: 999px;\
      padding: 2px 8px;\
      font-size: 0.74rem;\
      color: #cbe6da;\
      background: rgba(19, 32, 54, 0.68);\
    }\
    .state-tag.pending, .state-tag.running { color: #ffe8a8; border-color: rgba(255, 208, 120, 0.45); }\
    .state-tag.running-owned { color: #9af7c8; border-color: rgba(105, 240, 174, 0.5); }\
    .state-tag.running-foreign { color: #ffe8a8; border-color: rgba(255, 208, 120, 0.55); }\
    .state-tag.runner-active { color: #9af7c8; border-color: rgba(105, 240, 174, 0.5); }\
    .state-tag.runner-paused { color: #ffe8a8; border-color: rgba(255, 208, 120, 0.55); }\
    .state-tag.failed, .state-tag.canceled { color: #ffc3d3; border-color: rgba(255, 100, 137, 0.45); }\
    .state-tag.done { color: #9af7c8; border-color: rgba(105, 240, 174, 0.5); }\
    .state-tag.not_queued { color: #b8c9d7; border-color: rgba(142, 168, 190, 0.45); }\
    .state-tag.enum-ok { color: #9af7c8; border-color: rgba(105, 240, 174, 0.5); }\
    .state-tag.enum-partial { color: #ffe8a8; border-color: rgba(255, 208, 120, 0.45); }\
    .state-tag.enum-failed { color: #ffc3d3; border-color: rgba(255, 100, 137, 0.45); }\
    .state-tag.enum-missing, .state-tag.enum-unknown { color: #b8c9d7; border-color: rgba(142, 168, 190, 0.45); }\
    @media (max-width: 780px) {\
      table, thead, tbody, th, td, tr { display: block; }\
      thead { display: none; }\
      tr { border-top: 1px solid var(--border); padding-top: 8px; margin-top: 8px; }\
      td { border: none; padding: 3px 0; }\
    }\
  </style>\
</head>\
<body>\
  <main class=\"wrap\">",
    );

    let total_versions = cards.len();
    let total_failed: usize = cards.iter().map(|c| c.failed_total).sum();
    let db_size_gib = db_size_bytes
        .map(|bytes| format!("{:.1}", bytes as f64 / (1024.0 * 1024.0 * 1024.0)))
        .unwrap_or_else(|| "?".to_string());
    let db_size_fragment = if show_db_size_link {
        format!(" | db_size={}GiB", db_size_gib)
    } else {
        String::new()
    };
    let subtitle = format!(
        "generated={} | crate_versions={} | failed_actions={} | unprocessed_versions={}{}",
        generated_utc.to_rfc3339(),
        total_versions,
        total_failed,
        unprocessed.len(),
        db_size_fragment
    );
    let _ = write!(
        html,
        "<h1 class=\"title\">XLSynth Crate Versions</h1>\
<p class=\"subtitle\">{}</p>",
        html_escape(&subtitle)
    );
    let k3_losses_vs_crate_version_url = ir_fn_corpus_k3_losses_vs_crate_version_view_url(
        crate::default_k_bool_cone_max_ir_ops_for_k(3),
        None,
        false,
    );
    let _ = write!(
        html,
        "<p class=\"subtitle\">Explore trends: <a href=\"/dslx-fns/\">/dslx-fns/</a> | <a href=\"/dslx-fns-g8r/?metric=and_nodes&fraig=false\">/dslx-fns-g8r/</a> | <a href=\"/dslx-fns-yosys-abc/?metric=and_nodes\">/dslx-fns-yosys-abc/</a> | <a href=\"/dslx-fns-g8r-vs-yosys-abc/?fraig=false\">/dslx-fns-g8r-vs-yosys-abc/</a> | <a href=\"/ir-fn-corpus-g8r-vs-yosys-abc/\">/ir-fn-corpus-g8r-vs-yosys-abc/</a> | <a href=\"/ir-fn-g8r-abc-vs-codegen-yosys-abc/\">/ir-fn-g8r-abc-vs-codegen-yosys-abc/</a> | <a href=\"{}\">/ir-fn-corpus-k3-losses-vs-crate-version/</a> | <a href=\"/dslx-file-action-graph/\">/dslx-file-action-graph/</a> | <a href=\"/ir-fn-corpus-structural/\">/ir-fn-corpus-structural/</a>{}</p>",
        html_escape(&k3_losses_vs_crate_version_url),
        if show_db_size_link {
            " | <a href=\"/db-size/\">/db-size/</a>"
        } else {
            ""
        }
    );
    if !unattributed_actions.is_empty() {
        let invariant_count = unattributed_actions
            .iter()
            .filter(|row| row.reason.is_invariant_failure())
            .count();
        let _ = write!(
            html,
            "<section class=\"diag\">suppressed unattributed actions: <strong>{}</strong> (not rendered as crate cards); invariant_failures=<strong>{}</strong><details><summary>show diagnostics</summary><table><thead><tr><th>Action ID</th><th>Source</th><th>Kind</th><th>DSO</th><th>Reason</th></tr></thead><tbody>",
            unattributed_actions.len(),
            invariant_count
        );
        for row in unattributed_actions {
            let _ = write!(
                html,
                "<tr><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td></tr>",
                html_escape(&row.action_id),
                row.source.as_label(),
                html_escape(&row.action_kind),
                html_escape(
                    row.dso_version
                        .as_ref()
                        .map(|v| format!("v{}", v))
                        .as_deref()
                        .unwrap_or("-")
                ),
                row.reason.as_label()
            );
        }
        html.push_str("</tbody></table></details></section>");
    }

    if show_live_queue {
        let runner_state_class = if live_status.runner_paused {
            "runner-paused"
        } else {
            "runner-active"
        };
        let runner_state_label = if live_status.runner_paused {
            if live_status.runner_drained {
                "paused (drained)"
            } else {
                "pausing (draining)"
            }
        } else {
            "active"
        };
        let runner_button_label = if live_status.runner_paused {
            "Resume runner"
        } else {
            "Pause and drain"
        };
        let runner_sync_summary = if let Some(err) = live_status.runner_last_sync_error.as_deref() {
            format!("last sync error: {}", err)
        } else if let Some(ts) = live_status.runner_last_sync_utc {
            format!("last sync UTC: {}", ts.to_rfc3339())
        } else if live_status.runner_paused {
            "awaiting drained sync".to_string()
        } else {
            "sync on pause when drained".to_string()
        };
        let _ = write!(
            html,
            "<section class=\"card\">\
<h2>Live Queue Status</h2>\
<div class=\"status-grid\">\
<div class=\"stat\"><div class=\"k\">pending</div><div class=\"v\" id=\"queue-stat-pending\">{}</div></div>\
<div class=\"stat\"><div class=\"k\">pending expanders</div><div class=\"v\" id=\"queue-stat-pending-expanders\">{}</div></div>\
<div class=\"stat\"><div class=\"k\">pending non-expanders</div><div class=\"v\" id=\"queue-stat-pending-non-expanders\">{}</div></div>\
<div class=\"stat\"><div class=\"k\">running</div><div class=\"v\" id=\"queue-stat-running\">{}</div></div>\
<div class=\"stat\"><div class=\"k\">running expanders</div><div class=\"v\" id=\"queue-stat-running-expanders\">{}</div></div>\
<div class=\"stat\"><div class=\"k\">running non-expanders</div><div class=\"v\" id=\"queue-stat-running-non-expanders\">{}</div></div>\
<div class=\"stat\"><div class=\"k\">failed</div><div class=\"v\" id=\"queue-stat-failed\">{}</div></div>\
<div class=\"stat\"><div class=\"k\">canceled</div><div class=\"v\" id=\"queue-stat-canceled\">{}</div></div>\
<div class=\"stat\"><div class=\"k\">done</div><div class=\"v\" id=\"queue-stat-done\">{}</div></div>\
<div class=\"stat\"><div class=\"k\">throughput</div><div class=\"v\" id=\"queue-stat-throughput\">-</div></div>\
</div>\
<div class=\"chip-row\">\
<span class=\"chip running-owned\">running-owned: <strong id=\"queue-stat-running-owned\">{}</strong></span>\
<span class=\"chip running-foreign\">running-foreign: <strong id=\"queue-stat-running-foreign\">{}</strong></span>\
</div>\
<p class=\"meta\" id=\"queue-pending-lower-bound\">pending interpretation: {}</p>\
<p class=\"meta\">runner: <span class=\"state-tag {}\" id=\"queue-runner-state\">{}</span><button class=\"runner-btn\" id=\"queue-pause-toggle\" type=\"button\" data-paused=\"{}\">{}</button></p>\
<p class=\"meta\" id=\"queue-sync-state\">{}</p>\
<p class=\"meta\" id=\"queue-updated-utc\">updated: {}</p>\
<p class=\"queue-running-title\">Running Actions</p>\
<table>\
  <thead>\
    <tr><th>Action ID</th><th>Kind</th><th>Crate</th><th>DSO</th><th>Subject</th><th>Worker</th><th>Lease Expires (Local)</th></tr>\
  </thead>\
  <tbody id=\"running-actions-body\">",
            live_status.pending,
            live_status.pending_expanders,
            live_status.pending_non_expanders,
            live_status.running,
            live_status.running_expanders,
            live_status.running_non_expanders,
            live_status.failed,
            live_status.canceled,
            live_status.done,
            live_status.running_owned,
            live_status.running_foreign,
            if live_status.pending_is_lower_bound {
                "lower bound (expanders may enqueue more actions)"
            } else {
                "exact snapshot (no expanders pending/running)"
            },
            runner_state_class,
            html_escape(runner_state_label),
            if live_status.runner_paused { "1" } else { "0" },
            runner_button_label,
            html_escape(&runner_sync_summary),
            html_escape(&live_status.updated_utc.to_rfc3339())
        );
        if live_status.running_actions.is_empty() {
            html.push_str("<tr><td colspan=\"7\">No running actions.</td></tr>");
        } else {
            for row in &live_status.running_actions {
                let crate_label = if row.crate_version == "unknown" {
                    "crate:unknown".to_string()
                } else {
                    format!("crate:v{}", row.crate_version)
                };
                let owner_chip_class = if row.owned_by_this_runner {
                    "running-owned"
                } else {
                    "running-foreign"
                };
                let owner_chip_label = if row.owned_by_this_runner {
                    "owned"
                } else {
                    "foreign"
                };
                let _ = write!(
                    html,
                    "<tr>\
<td><code>{}</code></td>\
<td><code>{}</code></td>\
<td>{}</td>\
<td>{}</td>\
<td>{}</td>\
<td><span class=\"state-tag {}\">{}</span> {}</td>\
<td data-lease-expires-utc=\"{}\">{}</td>\
</tr>",
                    html_escape(&row.action_id),
                    html_escape(&row.action_kind),
                    html_escape(&crate_label),
                    html_escape(
                        row.dso_version
                            .as_ref()
                            .map(|v| format!("dso:v{}", v))
                            .as_deref()
                            .unwrap_or("-")
                    ),
                    html_escape(&row.subject),
                    owner_chip_class,
                    owner_chip_label,
                    html_escape(&row.lease_owner),
                    html_escape(&row.lease_expires_utc.to_rfc3339()),
                    html_escape(&row.lease_expires_utc.to_rfc3339())
                );
            }
        }
        html.push_str("</tbody></table></section>");
    }

    html.push_str("<section class=\"cards\">");
    for card in cards {
        let crate_label = format!("crate:v{}", card.crate_version);
        let crate_release_datetime = card.crate_release_datetime.as_deref().unwrap_or("unknown");
        let _ = write!(
            html,
            "<article class=\"card\">\
<h2>{}</h2>\
<p class=\"meta\">released: <strong>{}</strong></p>\
<p class=\"meta\">materialized actions: <strong>{}</strong></p>\
<p class=\"meta\">stdlib enumeration: <span class=\"state-tag {}\">{}</span> {}</p>\
<p class=\"meta\">failed actions: <strong class=\"{}\">{}</strong></p>",
            html_escape(&crate_label),
            html_escape(crate_release_datetime),
            card.total_materialized,
            html_escape(&card.stdlib_enumeration.badge_class),
            html_escape(&card.stdlib_enumeration.badge_label),
            html_escape(&card.stdlib_enumeration.summary),
            if card.failed_total > 0 {
                "fail-flag"
            } else {
                "ok"
            },
            card.failed_total
        );

        if !card.dso_versions.is_empty() {
            html.push_str("<div class=\"chip-row\">");
            for dso_version in &card.dso_versions {
                let _ = write!(
                    html,
                    "<span class=\"chip\">dso:{}</span>",
                    html_escape(dso_version)
                );
            }
            html.push_str("</div>");
        }

        if !card.failed_by_kind.is_empty() {
            html.push_str("<div class=\"chip-row\">");
            for failed_kind in &card.failed_by_kind {
                let (chip_class, chip_text) = if failed_kind.timeout_count == failed_kind.count {
                    (
                        "chip timeout",
                        format!("{}: {} timeout", failed_kind.kind, failed_kind.count),
                    )
                } else if failed_kind.timeout_count > 0 {
                    (
                        "chip fail",
                        format!(
                            "{}: {} ({} timeout)",
                            failed_kind.kind, failed_kind.count, failed_kind.timeout_count
                        ),
                    )
                } else {
                    (
                        "chip fail",
                        format!("{}: {}", failed_kind.kind, failed_kind.count),
                    )
                };
                let _ = write!(
                    html,
                    "<span class=\"{}\">{}</span>",
                    chip_class,
                    html_escape(&chip_text)
                );
            }
            html.push_str("</div>");
        }

        if card.failures.is_empty() {
            html.push_str("<p class=\"meta ok\">No failed actions for this version.</p>");
        } else {
            let _ = write!(
                html,
                "<details><summary>Failed actions ({})</summary>\
<table>\
  <thead>\
    <tr><th>Action ID</th><th>Kind</th><th>DSO</th><th>Subject</th><th>When (UTC)</th><th>Error</th></tr>\
  </thead>\
  <tbody>",
                card.failures.len()
            );
            for row in &card.failures {
                let _ = write!(
                    html,
                    "<tr>\
  <td><a href=\"/action/{}/\"><code>{}</code></a></td>\
  <td><code>{}</code></td>\
  <td><code>{}</code></td>\
  <td>{}</td>\
  <td>{}</td>\
  <td>{}</td>\
</tr>",
                    html_escape(&row.action_id),
                    html_escape(&row.action_id),
                    html_escape(&row.action_kind),
                    html_escape(row.dso_version.as_deref().unwrap_or("-")),
                    html_escape(&row.subject),
                    html_escape(&row.failed_utc.to_rfc3339()),
                    html_escape(&row.error_summary)
                );
            }
            html.push_str("</tbody></table></details>");
        }

        html.push_str("</article>");
    }
    html.push_str("</section>");

    html.push_str("<section class=\"card\">");
    let _ = write!(
        html,
        "<h2>Not Yet Materialized Crate Versions ({})</h2>",
        unprocessed.len()
    );
    if unprocessed.is_empty() {
        html.push_str("<p class=\"meta ok\">All crate versions in compatibility map have at least one materialized action.</p>");
    } else {
        if !show_live_queue {
            html.push_str(
                "<p class=\"meta\">enqueue is disabled on this instance (runner disabled: started with <code>--no-runner</code>).</p>",
            );
        }
        html.push_str(
            "<table>\
  <thead>\
    <tr><th>Crate</th><th>Published</th><th>DSO</th><th>Materialized</th><th>Active Queue</th><th>Stdlib Root</th><th>Action</th></tr>\
  </thead>\
  <tbody>",
        );
        for row in unprocessed {
            let root_state_class = html_escape(&row.root_queue_state_key);
            let root_state =
                QueueState::from_key(&row.root_queue_state_key).unwrap_or(QueueState::None);
            let enqueue_disabled =
                !show_live_queue || root_state.is_active() || row.active_queue_actions > 0;
            let button_text = if !show_live_queue {
                "Runner disabled"
            } else if enqueue_disabled {
                "Queued"
            } else if root_state.is_failure() {
                "Retry"
            } else {
                "Enqueue"
            };
            let _ = write!(
                html,
                "<tr>\
<td><code>crate:v{}</code></td>\
<td>{}</td>\
<td><code>dso:v{}</code></td>\
<td>{}</td>\
<td>{}</td>\
<td><span class=\"state-tag {}\">{}</span></td>\
<td><form method=\"post\" action=\"/versions/enqueue-crate\"><input type=\"hidden\" name=\"crate_version\" value=\"{}\"><input type=\"hidden\" name=\"priority\" value=\"0\"><button class=\"enqueue-btn\" type=\"submit\" {}>{}</button></form></td>\
</tr>",
                html_escape(&row.crate_version),
                html_escape(&row.crate_release_datetime),
                html_escape(&row.dso_version),
                row.materialized_actions,
                row.active_queue_actions,
                root_state_class,
                html_escape(&row.root_queue_state_label),
                html_escape(&row.crate_version),
                if enqueue_disabled { "disabled" } else { "" },
                button_text
            );
        }
        html.push_str("</tbody></table>");
    }
    html.push_str("</section>");
    if show_live_queue {
        html.push_str(
            "<script>\
(() => {\
  const setText = (id, value) => {\
    const el = document.getElementById(id);\
    if (el) el.textContent = String(value);\
  };\
  const rateState = {\
    samples: [],\
    maxAgeMs: 120000,\
  };\
  const normalizeDone = (value) => {\
    const n = Number(value);\
    return Number.isFinite(n) && n >= 0 ? n : null;\
  };\
  const formatRatePerMinute = (value) => {\
    if (!Number.isFinite(value)) return '-';\
    return `${value.toFixed(2)}/min`;\
  };\
  const updateThroughput = (doneValue) => {\
    const done = normalizeDone(doneValue);\
    if (done === null) {\
      setText('queue-stat-throughput', '-');\
      return;\
    }\
    const now = Date.now();\
    const samples = rateState.samples;\
    const last = samples.length ? samples[samples.length - 1] : null;\
    if (last && done < last.done) {\
      samples.length = 0;\
    }\
    if (!last || last.done !== done || now - last.ts >= 1000) {\
      samples.push({ ts: now, done });\
    }\
    while (samples.length > 1 && now - samples[0].ts > rateState.maxAgeMs) {\
      samples.shift();\
    }\
    if (samples.length < 2) {\
      setText('queue-stat-throughput', '0.00/min');\
      return;\
    }\
    const first = samples[0];\
    const lastSample = samples[samples.length - 1];\
    const deltaDone = lastSample.done - first.done;\
    const deltaMs = lastSample.ts - first.ts;\
    if (deltaMs <= 0 || deltaDone < 0) {\
      setText('queue-stat-throughput', '0.00/min');\
      return;\
    }\
    const ratePerMinute = (deltaDone * 60000) / deltaMs;\
    setText('queue-stat-throughput', formatRatePerMinute(ratePerMinute));\
  };\
  const esc = (value) => String(value)\
    .replaceAll('&', '&amp;')\
    .replaceAll('<', '&lt;')\
    .replaceAll('>', '&gt;')\
    .replaceAll('\"', '&quot;')\
    .replaceAll(\"'\", '&#39;');\
  const toLocalLeaseTime = (isoUtc) => {\
    if (!isoUtc) return 'unknown';\
    const d = new Date(isoUtc);\
    if (Number.isNaN(d.getTime())) return String(isoUtc);\
    return d.toLocaleString(undefined, {\
      year: 'numeric',\
      month: '2-digit',\
      day: '2-digit',\
      hour: '2-digit',\
      minute: '2-digit',\
      second: '2-digit',\
      hour12: false,\
      timeZoneName: 'short',\
    });\
  };\
  const localizeExistingLeaseCells = () => {\
    document.querySelectorAll('[data-lease-expires-utc]').forEach((cell) => {\
      const isoUtc = cell.getAttribute('data-lease-expires-utc') || '';\
      cell.textContent = toLocalLeaseTime(isoUtc);\
    });\
  };\
  const renderRunningRows = (rows) => {\
    const body = document.getElementById('running-actions-body');\
    if (!body) return;\
    if (!Array.isArray(rows) || rows.length === 0) {\
      body.innerHTML = '<tr><td colspan=\"7\">No running actions.</td></tr>';\
      return;\
    }\
    body.innerHTML = rows.map((row) => {\
      const crate = row.crate_version === 'unknown' ? 'crate:unknown' : `crate:v${row.crate_version}`;\
      const dso = row.dso_version ? `dso:v${row.dso_version}` : '-';\
      const leaseUtc = row.lease_expires_utc ?? '';\
      const leaseLocal = toLocalLeaseTime(leaseUtc);\
      const owned = !!row.owned_by_this_runner;\
      const ownerClass = owned ? 'running-owned' : 'running-foreign';\
      const ownerLabel = owned ? 'owned' : 'foreign';\
      return `<tr>\
<td><code>${esc(row.action_id)}</code></td>\
<td><code>${esc(row.action_kind)}</code></td>\
<td>${esc(crate)}</td>\
<td>${esc(dso)}</td>\
<td>${esc(row.subject)}</td>\
<td><span class=\"state-tag ${esc(ownerClass)}\">${esc(ownerLabel)}</span> ${esc(row.lease_owner)}</td>\
<td data-lease-expires-utc=\"${esc(leaseUtc)}\">${esc(leaseLocal)}</td>\
</tr>`;\
    }).join('');\
  };\
  const renderRunnerState = (data) => {\
    const paused = !!data.runner_paused;\
    const drained = !!data.runner_drained;\
    const syncPending = !!data.runner_sync_pending;\
    const runnerStateEl = document.getElementById('queue-runner-state');\
    if (runnerStateEl) {\
      runnerStateEl.classList.remove('runner-active', 'runner-paused');\
      runnerStateEl.classList.add(paused ? 'runner-paused' : 'runner-active');\
      runnerStateEl.textContent = paused ? (drained ? 'paused (drained)' : 'pausing (draining)') : 'active';\
    }\
    const pauseBtn = document.getElementById('queue-pause-toggle');\
    if (pauseBtn) {\
      pauseBtn.textContent = paused ? 'Resume runner' : 'Pause and drain';\
      pauseBtn.setAttribute('data-paused', paused ? '1' : '0');\
      pauseBtn.disabled = false;\
    }\
    const syncEl = document.getElementById('queue-sync-state');\
    if (syncEl) {\
      if (data.runner_last_sync_error) {\
        syncEl.textContent = `last sync error: ${data.runner_last_sync_error}`;\
      } else if (data.runner_last_sync_utc) {\
        syncEl.textContent = `last sync UTC: ${data.runner_last_sync_utc}${syncPending ? ' (pending)' : ''}`;\
      } else if (paused) {\
        syncEl.textContent = syncPending ? 'awaiting drained sync' : 'paused; sync not started';\
      } else {\
        syncEl.textContent = 'sync on pause when drained';\
      }\
    }\
  };\
  const applyQueueData = (data) => {\
    setText('queue-stat-pending', data.pending ?? '?');\
    setText('queue-stat-pending-expanders', data.pending_expanders ?? '?');\
    setText('queue-stat-pending-non-expanders', data.pending_non_expanders ?? '?');\
    setText('queue-stat-running', data.running ?? '?');\
    setText('queue-stat-running-expanders', data.running_expanders ?? '?');\
    setText('queue-stat-running-non-expanders', data.running_non_expanders ?? '?');\
    setText('queue-stat-running-owned', data.running_owned ?? '?');\
    setText('queue-stat-running-foreign', data.running_foreign ?? '?');\
    setText('queue-stat-failed', data.failed ?? '?');\
    setText('queue-stat-canceled', data.canceled ?? '?');\
    setText('queue-stat-done', data.done ?? '?');\
    setText('queue-pending-lower-bound', `pending interpretation: ${data.pending_is_lower_bound ? 'lower bound (expanders may enqueue more actions)' : 'exact snapshot (no expanders pending/running)'}`);\
    updateThroughput(data.done);\
    setText('queue-updated-utc', `updated: ${data.updated_utc ?? 'unknown'}`);\
    renderRunningRows(data.running_actions);\
    renderRunnerState(data);\
  };\
  const postPauseState = async (paused) => {\
    const endpoint = paused ? '/api/runner-pause' : '/api/runner-resume';\
    const resp = await fetch(endpoint, {\
      method: 'POST',\
      cache: 'no-store',\
    });\
    if (!resp.ok) {\
      throw new Error(`runner pause toggle failed (${endpoint}): ${resp.status}`);\
    }\
    return resp.json();\
  };\
  const refresh = async () => {\
    try {\
      const resp = await fetch('/api/queue-status', { cache: 'no-store' });\
      if (!resp.ok) return;\
      const data = await resp.json();\
      applyQueueData(data);\
    } catch (_) {\
      /* keep previous values on transient failures */\
    }\
  };\
  const wirePauseToggle = () => {\
    const pauseBtn = document.getElementById('queue-pause-toggle');\
    if (!pauseBtn) return;\
    pauseBtn.addEventListener('click', async () => {\
      pauseBtn.disabled = true;\
      try {\
        const currentlyPaused = pauseBtn.getAttribute('data-paused') === '1';\
        const data = await postPauseState(!currentlyPaused);\
        applyQueueData(data);\
      } catch (_) {\
        pauseBtn.disabled = false;\
      }\
    });\
  };\
  localizeExistingLeaseCells();\
  wirePauseToggle();\
  refresh();\
  setInterval(refresh, 2500);\
})();\
</script>",
        );
    }
    html.push_str("</main></body></html>");
    html
}
