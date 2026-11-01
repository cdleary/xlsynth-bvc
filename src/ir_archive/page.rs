// SPDX-License-Identifier: Apache-2.0

//! Deterministic, human-readable projection of the canonical download index.

use super::wire;
use prost::Message;
use std::collections::BTreeMap;

const HTML: &str = include_str!("../site_assets/ir_downloads.html");
const CSS: &[u8] = include_bytes!("../site_assets/ir_downloads.css");
const JS: &[u8] = include_bytes!("../site_assets/ir_downloads.js");

#[derive(Debug, PartialEq, Eq)]
struct Summary {
    roots: usize,
    blobs: usize,
    download_bytes: u64,
    ir_bytes: u64,
    corpora: BTreeMap<String, usize>,
}

fn summary(index: &wire::IrArchiveIndex) -> Summary {
    let mut corpora = BTreeMap::new();
    for root in &index.roots {
        *corpora.entry(root.corpus.clone()).or_default() += 1;
    }
    Summary {
        roots: index.roots.len(),
        blobs: index.buckets.iter().map(|b| b.blobs.len()).sum(),
        download_bytes: index.buckets.iter().map(|b| b.archive_bytes).sum(),
        ir_bytes: index
            .buckets
            .iter()
            .flat_map(|b| &b.blobs)
            .map(|b| b.bytes)
            .sum(),
        corpora,
    }
}

fn number(n: u64) -> String {
    let digits = n.to_string();
    let mut result = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(c);
    }
    result
}

fn size(n: u64) -> String {
    if n >= 1024 * 1024 {
        format!("{:.2} MiB", n as f64 / (1024.0 * 1024.0))
    } else if n >= 1024 {
        format!("{:.1} KiB", n as f64 / 1024.0)
    } else {
        format!("{n} B")
    }
}

fn bucket_row(bucket: &wire::IrArchiveBucket) -> String {
    let prefix = &bucket.prefix;
    let bytes = bucket.archive_bytes;
    format!(
        "<tr id=\"bucket-{prefix}\" data-prefix=\"{prefix}\" data-blobs=\"{}\" data-bytes=\"{bytes}\"><th scope=\"row\"><a href=\"{prefix}.tar.zst\" download>{prefix}.tar.zst</a></th><td>{}</td><td title=\"{} bytes\">{}</td><td><details><summary>SHA-256</summary><code>{}</code></details></td></tr>\n",
        bucket.blobs.len(),
        number(bucket.blobs.len() as u64),
        number(bytes),
        size(bytes),
        hex::encode(&bucket.archive_sha256)
    )
}

pub(super) fn files(index: &wire::IrArchiveIndex) -> [(&'static str, Vec<u8>); 3] {
    // Callers generate/validate the index first. Corpus IDs and prefixes are
    // restricted to lowercase ASCII identifiers; no free-form IR enters HTML.
    let summary = summary(index);
    let corpora = summary.corpora.iter().map(|(id, count)| format!(
        "<tr data-corpus=\"{id}\" data-roots=\"{count}\"><th scope=\"row\"><code>{id}</code></th><td>{}</td></tr>\n", number(*count as u64)
    )).collect::<String>();
    let rows = index.buckets.iter().map(bucket_row).collect::<String>();
    let jumps = (0..16)
        .map(|prefix| format!("<a href=\"#bucket-{prefix:x}0\">{prefix:x}0–{prefix:x}f</a>"))
        .collect::<String>();
    let html = HTML
        .replace("{{ROOTS}}", &number(summary.roots as u64))
        .replace("{{BLOBS}}", &number(summary.blobs as u64))
        .replace("{{DOWNLOAD_BYTES}}", &summary.download_bytes.to_string())
        .replace("{{DOWNLOAD_SIZE}}", &size(summary.download_bytes))
        .replace("{{IR_SIZE}}", &size(summary.ir_bytes))
        .replace("{{INDEX_SIZE}}", &size(index.encoded_len() as u64))
        .replace("{{CORPORA}}", &corpora)
        .replace("{{JUMPS}}", &jumps)
        .replace("{{BUCKETS}}", &rows);
    [
        ("ir/index.html", html.into_bytes()),
        ("ir/downloads.css", CSS.to_vec()),
        ("ir/downloads.js", JS.to_vec()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_counts_shared_bytes_once_but_retains_each_corpus_root() {
        let (index, _) = super::super::tests::fixture_builder().packs().unwrap();
        assert_eq!(
            summary(&index),
            Summary {
                roots: 2,
                blobs: 1,
                download_bytes: index.buckets.iter().map(|b| b.archive_bytes).sum(),
                ir_bytes: super::super::tests::ir().len() as u64,
                corpora: BTreeMap::from([
                    ("full-corpus".into(), 1),
                    ("whole-functions-v1".into(), 1)
                ]),
            }
        );
        assert_eq!(
            [0, 999, 1000, 1_234_567].map(number),
            ["0", "999", "1,000", "1,234,567"]
        );
        assert_eq!(
            [0, 1024, 1024 * 1024].map(size),
            ["0 B", "1.0 KiB", "1.00 MiB"]
        );
    }

    #[test]
    fn bucket_row_renders_the_exact_download_identity() {
        let bucket = wire::IrArchiveBucket {
            prefix: "ab".into(),
            archive_bytes: 1024,
            archive_sha256: vec![0x12; 32],
            blobs: vec![],
        };
        assert_eq!(
            bucket_row(&bucket),
            concat!(
                "<tr id=\"bucket-ab\" data-prefix=\"ab\" data-blobs=\"0\" data-bytes=\"1024\"><th scope=\"row\"><a href=\"ab.tar.zst\" download>ab.tar.zst</a></th><td>0</td><td title=\"1,024 bytes\">1.0 KiB</td><td><details><summary>SHA-256</summary><code>",
                "1212121212121212121212121212121212121212121212121212121212121212",
                "</code></details></td></tr>\n"
            )
        );
    }
}
