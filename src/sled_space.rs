use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;
use walkdir::WalkDir;

const TREE_PROVENANCE_BY_ACTION: &str = "provenance_by_action";
const TREE_ACTION_FILE_BYTES: &str = "action_file_bytes";
const TREE_WEB_INDEX_BYTES: &str = "web_index_bytes";
const ANALYZE_CACHE_CAPACITY_BYTES: u64 = 16 * 1024 * 1024;
const MAX_KEY_DISPLAY_LEN: usize = 160;
const VALUE_SIZE_BUCKETS: [(&str, u64, Option<u64>); 12] = [
    ("0..1KiB", 0, Some(1024)),
    ("1KiB..4KiB", 1024, Some(4 * 1024)),
    ("4KiB..16KiB", 4 * 1024, Some(16 * 1024)),
    ("16KiB..64KiB", 16 * 1024, Some(64 * 1024)),
    ("64KiB..256KiB", 64 * 1024, Some(256 * 1024)),
    ("256KiB..1MiB", 256 * 1024, Some(1024 * 1024)),
    ("1MiB..4MiB", 1024 * 1024, Some(4 * 1024 * 1024)),
    ("4MiB..16MiB", 4 * 1024 * 1024, Some(16 * 1024 * 1024)),
    ("16MiB..64MiB", 16 * 1024 * 1024, Some(64 * 1024 * 1024)),
    ("64MiB..256MiB", 64 * 1024 * 1024, Some(256 * 1024 * 1024)),
    ("256MiB..1GiB", 256 * 1024 * 1024, Some(1024 * 1024 * 1024)),
    (">=1GiB", 1024 * 1024 * 1024, None),
];

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SledSpaceCategory {
    pub(crate) category: String,
    pub(crate) entries: u64,
    pub(crate) key_bytes: u64,
    pub(crate) value_bytes: u64,
    pub(crate) logical_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SledSpaceRow {
    pub(crate) tree: String,
    pub(crate) key: String,
    pub(crate) key_bytes: u64,
    pub(crate) value_bytes: u64,
    pub(crate) logical_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SledValueSizeHistogramBucket {
    pub(crate) label: String,
    pub(crate) lower_inclusive: u64,
    pub(crate) upper_exclusive: Option<u64>,
    pub(crate) entries: u64,
    pub(crate) value_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SledTreeSpaceReport {
    pub(crate) tree: String,
    pub(crate) entries: u64,
    pub(crate) key_bytes: u64,
    pub(crate) value_bytes: u64,
    pub(crate) logical_bytes: u64,
    pub(crate) value_size_histogram: Vec<SledValueSizeHistogramBucket>,
    pub(crate) categories: Vec<SledSpaceCategory>,
    pub(crate) extension_categories: Vec<SledSpaceCategory>,
    pub(crate) filename_categories: Vec<SledSpaceCategory>,
    pub(crate) relpath_prefix_depth2_categories: Vec<SledSpaceCategory>,
    pub(crate) relpath_prefix_depth3_categories: Vec<SledSpaceCategory>,
    pub(crate) sampled_rows: Vec<SledSpaceRow>,
    pub(crate) largest_rows: Vec<SledSpaceRow>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SledSpaceReport {
    pub(crate) analyzed_utc: DateTime<Utc>,
    pub(crate) db_path: String,
    pub(crate) db_files_on_disk_bytes: u64,
    pub(crate) tree_count: usize,
    pub(crate) total_entries: u64,
    pub(crate) total_key_bytes: u64,
    pub(crate) total_value_bytes: u64,
    pub(crate) total_logical_bytes: u64,
    pub(crate) trees: Vec<SledTreeSpaceReport>,
    pub(crate) largest_rows_overall: Vec<SledSpaceRow>,
}

#[derive(Debug, Default, Clone)]
struct CategoryAccumulator {
    entries: u64,
    key_bytes: u64,
    value_bytes: u64,
}

#[derive(Debug, Default, Clone)]
struct ValueSizeHistogramAccumulator {
    entries: u64,
    value_bytes: u64,
}

fn accumulate_category(
    map: &mut BTreeMap<String, CategoryAccumulator>,
    category: String,
    key_bytes: u64,
    value_bytes: u64,
) {
    let entry = map.entry(category).or_default();
    entry.entries += 1;
    entry.key_bytes += key_bytes;
    entry.value_bytes += value_bytes;
}

fn finalize_categories(
    categories: BTreeMap<String, CategoryAccumulator>,
    top_n: usize,
) -> Vec<SledSpaceCategory> {
    let mut rows: Vec<SledSpaceCategory> = categories
        .into_iter()
        .map(|(category, acc)| SledSpaceCategory {
            category,
            entries: acc.entries,
            key_bytes: acc.key_bytes,
            value_bytes: acc.value_bytes,
            logical_bytes: acc.key_bytes + acc.value_bytes,
        })
        .collect();
    rows.sort_by(|a, b| {
        b.logical_bytes
            .cmp(&a.logical_bytes)
            .then_with(|| a.category.cmp(&b.category))
    });
    rows.truncate(top_n);
    rows
}

fn push_top_row(top: &mut Vec<SledSpaceRow>, candidate: SledSpaceRow, top_n: usize) {
    if top_n == 0 {
        return;
    }
    top.push(candidate);
    top.sort_by(|a, b| {
        a.logical_bytes
            .cmp(&b.logical_bytes)
            .then_with(|| a.tree.cmp(&b.tree))
            .then_with(|| a.key.cmp(&b.key))
    });
    if top.len() > top_n {
        top.remove(0);
    }
}

fn reverse_desc(mut rows: Vec<SledSpaceRow>) -> Vec<SledSpaceRow> {
    rows.sort_by(|a, b| {
        b.logical_bytes
            .cmp(&a.logical_bytes)
            .then_with(|| a.tree.cmp(&b.tree))
            .then_with(|| a.key.cmp(&b.key))
    });
    rows
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    hash
}

fn push_hashed_sample(
    samples: &mut Vec<(u64, SledSpaceRow)>,
    score: u64,
    candidate: SledSpaceRow,
    sample_n: usize,
) {
    if sample_n == 0 {
        return;
    }
    if samples.len() < sample_n {
        samples.push((score, candidate));
        return;
    }
    let mut max_index = 0_usize;
    let mut max_score = samples[0].0;
    for (index, (existing_score, _)) in samples.iter().enumerate().skip(1) {
        if *existing_score > max_score {
            max_score = *existing_score;
            max_index = index;
        }
    }
    if score < max_score {
        samples[max_index] = (score, candidate);
    }
}

fn value_size_bucket_index(value_bytes: u64) -> usize {
    for (index, (_, lower, upper)) in VALUE_SIZE_BUCKETS.iter().enumerate() {
        if value_bytes < *lower {
            continue;
        }
        if let Some(upper) = upper {
            if value_bytes < *upper {
                return index;
            }
        } else {
            return index;
        }
    }
    VALUE_SIZE_BUCKETS.len() - 1
}

fn finalize_value_size_histogram(
    histogram: &[ValueSizeHistogramAccumulator],
) -> Vec<SledValueSizeHistogramBucket> {
    VALUE_SIZE_BUCKETS
        .iter()
        .enumerate()
        .map(|(index, (label, lower, upper))| {
            let acc = histogram.get(index).cloned().unwrap_or_default();
            SledValueSizeHistogramBucket {
                label: (*label).to_string(),
                lower_inclusive: *lower,
                upper_exclusive: *upper,
                entries: acc.entries,
                value_bytes: acc.value_bytes,
            }
        })
        .collect()
}

fn path_total_bytes(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    if path.is_file() {
        return std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    }
    WalkDir::new(path)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn truncate_key_display(mut key: String) -> String {
    if key.len() <= MAX_KEY_DISPLAY_LEN {
        return key;
    }
    key.truncate(MAX_KEY_DISPLAY_LEN);
    key.push_str("...");
    key
}

fn decode_action_file_key(key: &[u8]) -> Option<(String, String)> {
    let split = key.iter().position(|b| *b == 0)?;
    let action_id = String::from_utf8_lossy(&key[..split]).to_string();
    let relpath = String::from_utf8_lossy(&key[split + 1..]).to_string();
    Some((action_id, relpath))
}

fn render_key_for_tree(tree: &str, key: &[u8]) -> String {
    let rendered = if tree == TREE_ACTION_FILE_BYTES {
        if let Some((action_id, relpath)) = decode_action_file_key(key) {
            format!("{action_id}:{relpath}")
        } else {
            String::from_utf8_lossy(key).to_string()
        }
    } else {
        String::from_utf8_lossy(key).to_string()
    };
    truncate_key_display(rendered)
}

fn provenance_action_category(value: &[u8]) -> String {
    let action_kind = serde_json::from_slice::<serde_json::Value>(value)
        .ok()
        .and_then(|json| json.get("action").cloned())
        .and_then(|action| action.as_object().cloned())
        .and_then(|action_obj| action_obj.into_iter().next().map(|(kind, _)| kind))
        .unwrap_or_else(|| "<unparseable>".to_string());
    format!("action:{action_kind}")
}

fn action_file_root_category(relpath: &str) -> String {
    let root = relpath.split('/').next().unwrap_or("<empty>");
    format!("root:{root}")
}

fn action_file_extension_category(relpath: &str) -> String {
    let ext = std::path::Path::new(relpath)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase());
    match ext {
        Some(ext) if !ext.is_empty() => format!("ext:.{ext}"),
        _ => "ext:<none>".to_string(),
    }
}

fn action_file_filename_category(relpath: &str) -> String {
    let filename = std::path::Path::new(relpath)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("<none>");
    format!("filename:{filename}")
}

fn action_file_relpath_prefix_category(relpath: &str, depth: usize) -> String {
    let components: Vec<&str> = relpath.split('/').filter(|part| !part.is_empty()).collect();
    if components.is_empty() {
        return format!("prefix{depth}:<empty>");
    }
    let capped_depth = depth.max(1).min(components.len());
    let prefix = components[..capped_depth].join("/");
    format!("prefix{depth}:{prefix}")
}

fn web_index_namespace_category(index_key: &str) -> String {
    let namespace = index_key.split('/').next().unwrap_or("<empty>");
    format!("namespace:{namespace}")
}

pub(crate) fn analyze_sled_space(
    db_path: &Path,
    top_n: usize,
    sample_rows_per_tree: usize,
) -> Result<SledSpaceReport> {
    let top_n = top_n.max(1);
    let sample_rows_per_tree = sample_rows_per_tree.max(1);
    eprintln!("analyze-sled-space: opening {}", db_path.display());
    let db = match sled::Config::new()
        .path(db_path)
        .cache_capacity(ANALYZE_CACHE_CAPACITY_BYTES)
        .open()
    {
        Ok(db) => db,
        Err(err) => {
            let err_text = err.to_string();
            if err_text.contains("could not acquire lock") {
                return Err(anyhow!(
                    "opening sled db for analysis: {} (database lock is held by another process; stop the service or analyze a copied snapshot directory)",
                    db_path.display()
                ));
            }
            return Err(err)
                .with_context(|| format!("opening sled db for analysis: {}", db_path.display()));
        }
    };

    let mut trees = Vec::<SledTreeSpaceReport>::new();
    let mut largest_rows_overall = Vec::<SledSpaceRow>::new();
    let mut total_entries = 0_u64;
    let mut total_key_bytes = 0_u64;
    let mut total_value_bytes = 0_u64;
    let mut total_logical_bytes = 0_u64;

    let tree_names = db.tree_names();
    eprintln!("analyze-sled-space: scanning {} trees", tree_names.len());
    for tree_name_bytes in tree_names {
        let tree_name = String::from_utf8_lossy(tree_name_bytes.as_ref()).to_string();
        eprintln!("analyze-sled-space: tree={} begin", tree_name);
        let tree = db
            .open_tree(&tree_name_bytes)
            .with_context(|| format!("opening sled tree for analysis: {}", tree_name))?;

        let mut entries = 0_u64;
        let mut key_bytes = 0_u64;
        let mut value_bytes = 0_u64;
        let mut logical_bytes = 0_u64;
        let mut value_size_histogram =
            vec![ValueSizeHistogramAccumulator::default(); VALUE_SIZE_BUCKETS.len()];
        let mut categories = BTreeMap::<String, CategoryAccumulator>::new();
        let mut extension_categories = BTreeMap::<String, CategoryAccumulator>::new();
        let mut filename_categories = BTreeMap::<String, CategoryAccumulator>::new();
        let mut relpath_prefix_depth2_categories = BTreeMap::<String, CategoryAccumulator>::new();
        let mut relpath_prefix_depth3_categories = BTreeMap::<String, CategoryAccumulator>::new();
        let mut sampled_rows = Vec::<(u64, SledSpaceRow)>::new();
        let mut largest_rows = Vec::<SledSpaceRow>::new();
        let tree_started = Instant::now();
        let mut last_progress_log = Instant::now();

        for row in tree.iter() {
            let (key, value) =
                row.with_context(|| format!("iterating row in sled tree `{}`", tree_name))?;
            let row_key_bytes = key.len() as u64;
            let row_value_bytes = value.len() as u64;
            let row_logical_bytes = row_key_bytes + row_value_bytes;
            entries += 1;
            key_bytes += row_key_bytes;
            value_bytes += row_value_bytes;
            logical_bytes += row_logical_bytes;
            let histogram_index = value_size_bucket_index(row_value_bytes);
            if let Some(bucket) = value_size_histogram.get_mut(histogram_index) {
                bucket.entries += 1;
                bucket.value_bytes += row_value_bytes;
            }

            let rendered_key = render_key_for_tree(&tree_name, key.as_ref());
            let row_summary = SledSpaceRow {
                tree: tree_name.clone(),
                key: rendered_key,
                key_bytes: row_key_bytes,
                value_bytes: row_value_bytes,
                logical_bytes: row_logical_bytes,
            };
            let sample_score = fnv1a64(key.as_ref());
            push_hashed_sample(
                &mut sampled_rows,
                sample_score,
                row_summary.clone(),
                sample_rows_per_tree,
            );
            push_top_row(&mut largest_rows, row_summary.clone(), top_n);
            push_top_row(&mut largest_rows_overall, row_summary, top_n);

            match tree_name.as_str() {
                TREE_PROVENANCE_BY_ACTION => {
                    let category = provenance_action_category(value.as_ref());
                    accumulate_category(&mut categories, category, row_key_bytes, row_value_bytes);
                }
                TREE_ACTION_FILE_BYTES => {
                    let (_, relpath) = decode_action_file_key(key.as_ref()).unwrap_or_else(|| {
                        ("<unknown-action>".to_string(), "<unparseable>".to_string())
                    });
                    accumulate_category(
                        &mut categories,
                        action_file_root_category(&relpath),
                        row_key_bytes,
                        row_value_bytes,
                    );
                    accumulate_category(
                        &mut extension_categories,
                        action_file_extension_category(&relpath),
                        row_key_bytes,
                        row_value_bytes,
                    );
                    accumulate_category(
                        &mut filename_categories,
                        action_file_filename_category(&relpath),
                        row_key_bytes,
                        row_value_bytes,
                    );
                    accumulate_category(
                        &mut relpath_prefix_depth2_categories,
                        action_file_relpath_prefix_category(&relpath, 2),
                        row_key_bytes,
                        row_value_bytes,
                    );
                    accumulate_category(
                        &mut relpath_prefix_depth3_categories,
                        action_file_relpath_prefix_category(&relpath, 3),
                        row_key_bytes,
                        row_value_bytes,
                    );
                }
                TREE_WEB_INDEX_BYTES => {
                    let index_key = String::from_utf8_lossy(key.as_ref()).to_string();
                    accumulate_category(
                        &mut categories,
                        web_index_namespace_category(&index_key),
                        row_key_bytes,
                        row_value_bytes,
                    );
                }
                _ => {
                    accumulate_category(
                        &mut categories,
                        "rows:all".to_string(),
                        row_key_bytes,
                        row_value_bytes,
                    );
                }
            }

            if last_progress_log.elapsed().as_secs() >= 5 {
                eprintln!(
                    "analyze-sled-space: tree={} entries={} logical_gib={:.3} elapsed_s={:.1}",
                    tree_name,
                    entries,
                    logical_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                    tree_started.elapsed().as_secs_f64()
                );
                last_progress_log = Instant::now();
            }
        }

        total_entries += entries;
        total_key_bytes += key_bytes;
        total_value_bytes += value_bytes;
        total_logical_bytes += logical_bytes;
        eprintln!(
            "analyze-sled-space: tree={} done entries={} logical_gib={:.3} elapsed_s={:.1}",
            tree_name,
            entries,
            logical_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            tree_started.elapsed().as_secs_f64()
        );

        trees.push(SledTreeSpaceReport {
            tree: tree_name,
            entries,
            key_bytes,
            value_bytes,
            logical_bytes,
            value_size_histogram: finalize_value_size_histogram(&value_size_histogram),
            categories: finalize_categories(categories, top_n),
            extension_categories: finalize_categories(extension_categories, top_n),
            filename_categories: finalize_categories(filename_categories, top_n),
            relpath_prefix_depth2_categories: finalize_categories(
                relpath_prefix_depth2_categories,
                top_n,
            ),
            relpath_prefix_depth3_categories: finalize_categories(
                relpath_prefix_depth3_categories,
                top_n,
            ),
            sampled_rows: reverse_desc(sampled_rows.into_iter().map(|(_, row)| row).collect()),
            largest_rows: reverse_desc(largest_rows),
        });
    }

    trees.sort_by(|a, b| {
        b.logical_bytes
            .cmp(&a.logical_bytes)
            .then_with(|| a.tree.cmp(&b.tree))
    });

    Ok(SledSpaceReport {
        analyzed_utc: Utc::now(),
        db_path: db_path.display().to_string(),
        db_files_on_disk_bytes: path_total_bytes(db_path),
        tree_count: trees.len(),
        total_entries,
        total_key_bytes,
        total_value_bytes,
        total_logical_bytes,
        trees,
        largest_rows_overall: reverse_desc(largest_rows_overall),
    })
}
