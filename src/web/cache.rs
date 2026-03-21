use anyhow::Result;
use log::info;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::view::{StdlibFileActionGraphDataset, StdlibG8rVsYosysDataset};

const PAGE_CACHE_MAX_ENTRIES: usize = 32;
const DEFAULT_PAGE_CACHE_MAX_BYTES: usize = 1_500_000;
const DEFAULT_FILE_GRAPH_CACHE_MAX_NODES: usize = 50_000;
const DEFAULT_FILE_GRAPH_CACHE_MAX_EDGES: usize = 200_000;

#[derive(Debug, Clone)]
struct CachedArc<T> {
    cached_at: Instant,
    value: Arc<T>,
}

#[derive(Debug, Clone)]
struct CachedPage {
    cached_at: Instant,
    html: String,
}

#[derive(Debug)]
pub(crate) struct WebUiCache {
    page_ttl: Duration,
    dataset_ttl: Duration,
    page_cache_max_bytes: usize,
    file_graph_cache_max_nodes: usize,
    file_graph_cache_max_edges: usize,
    pages: Mutex<HashMap<String, CachedPage>>,
    stdlib_file_action_graph_by_query:
        Mutex<HashMap<String, CachedArc<StdlibFileActionGraphDataset>>>,
    stdlib_g8r_vs_yosys_by_fraig: Mutex<HashMap<bool, CachedArc<StdlibG8rVsYosysDataset>>>,
    ir_fn_corpus_g8r_vs_yosys: Mutex<Option<CachedArc<StdlibG8rVsYosysDataset>>>,
    ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc: Mutex<Option<CachedArc<StdlibG8rVsYosysDataset>>>,
}

impl WebUiCache {
    fn env_duration_secs(name: &str, default_secs: u64) -> Duration {
        let secs = std::env::var(name)
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(default_secs);
        Duration::from_secs(secs)
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default)
    }

    fn prune_stale_pages(pages: &mut HashMap<String, CachedPage>, ttl: Duration) {
        pages.retain(|_, cached| Self::cache_fresh(cached.cached_at, ttl));
    }

    fn prune_stale_cached_arcs<T>(cache: &mut HashMap<String, CachedArc<T>>, ttl: Duration) {
        cache.retain(|_, cached| Self::cache_fresh(cached.cached_at, ttl));
    }

    pub(crate) fn new(runner_enabled: bool) -> Self {
        let page_ttl = if runner_enabled {
            Self::env_duration_secs("BVC_WEB_PAGE_CACHE_TTL_SECS", 3)
        } else {
            Self::env_duration_secs("BVC_WEB_PAGE_CACHE_TTL_SECS", 600)
        };
        let dataset_ttl = if runner_enabled {
            // Heavy corpus views are expensive to materialize from sled under write load.
            // Keep a longer in-memory dataset TTL by default while runner is enabled.
            Self::env_duration_secs("BVC_WEB_DATASET_CACHE_TTL_SECS", 60)
        } else {
            Self::env_duration_secs("BVC_WEB_DATASET_CACHE_TTL_SECS", 900)
        };
        let page_cache_max_bytes =
            Self::env_usize("BVC_WEB_PAGE_CACHE_MAX_BYTES", DEFAULT_PAGE_CACHE_MAX_BYTES);
        let file_graph_cache_max_nodes = Self::env_usize(
            "BVC_WEB_FILE_GRAPH_CACHE_MAX_NODES",
            DEFAULT_FILE_GRAPH_CACHE_MAX_NODES,
        );
        let file_graph_cache_max_edges = Self::env_usize(
            "BVC_WEB_FILE_GRAPH_CACHE_MAX_EDGES",
            DEFAULT_FILE_GRAPH_CACHE_MAX_EDGES,
        );
        Self {
            page_ttl,
            dataset_ttl,
            page_cache_max_bytes,
            file_graph_cache_max_nodes,
            file_graph_cache_max_edges,
            pages: Mutex::new(HashMap::new()),
            stdlib_file_action_graph_by_query: Mutex::new(HashMap::new()),
            stdlib_g8r_vs_yosys_by_fraig: Mutex::new(HashMap::new()),
            ir_fn_corpus_g8r_vs_yosys: Mutex::new(None),
            ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc: Mutex::new(None),
        }
    }

    fn cache_fresh(cached_at: Instant, ttl: Duration) -> bool {
        cached_at.elapsed() <= ttl
    }

    pub(crate) fn get_page(&self, key: &str) -> Option<String> {
        if self.page_ttl.is_zero() {
            return None;
        }
        let mut pages = self.pages.lock().ok()?;
        Self::prune_stale_pages(&mut pages, self.page_ttl);
        let Some(cached) = pages.get(key) else {
            return None;
        };
        if Self::cache_fresh(cached.cached_at, self.page_ttl) {
            info!(
                "web.cache page_hit key={} age_ms={}",
                key,
                cached.cached_at.elapsed().as_millis()
            );
            return Some(cached.html.clone());
        }
        pages.remove(key);
        None
    }

    pub(crate) fn put_page(&self, key: String, html: String) {
        if self.page_ttl.is_zero() {
            return;
        }
        if html.len() > self.page_cache_max_bytes {
            info!(
                "web.cache page_skip_oversize key={} bytes={} max_bytes={}",
                key,
                html.len(),
                self.page_cache_max_bytes
            );
            return;
        }
        let Ok(mut pages) = self.pages.lock() else {
            return;
        };
        Self::prune_stale_pages(&mut pages, self.page_ttl);
        pages.insert(
            key,
            CachedPage {
                cached_at: Instant::now(),
                html,
            },
        );
        while pages.len() > PAGE_CACHE_MAX_ENTRIES {
            let Some(oldest_key) = pages
                .iter()
                .min_by_key(|(_, cached)| cached.cached_at)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            pages.remove(&oldest_key);
        }
    }

    pub(crate) fn get_or_compute_stdlib_g8r_vs_yosys_dataset<F>(
        &self,
        fraig: bool,
        compute: F,
    ) -> Result<Arc<StdlibG8rVsYosysDataset>>
    where
        F: FnOnce() -> Result<StdlibG8rVsYosysDataset>,
    {
        if self.dataset_ttl.is_zero() {
            return Ok(Arc::new(compute()?));
        }
        if let Ok(mut cache) = self.stdlib_g8r_vs_yosys_by_fraig.lock() {
            if let Some(cached) = cache.get(&fraig) {
                if Self::cache_fresh(cached.cached_at, self.dataset_ttl) {
                    info!(
                        "web.cache stdlib_g8r_vs_yosys_hit fraig={} age_ms={}",
                        fraig,
                        cached.cached_at.elapsed().as_millis()
                    );
                    return Ok(cached.value.clone());
                }
                cache.remove(&fraig);
            }
        }
        info!("web.cache stdlib_g8r_vs_yosys_miss fraig={}", fraig);
        let value = Arc::new(compute()?);
        if let Ok(mut cache) = self.stdlib_g8r_vs_yosys_by_fraig.lock() {
            cache.insert(
                fraig,
                CachedArc {
                    cached_at: Instant::now(),
                    value: value.clone(),
                },
            );
        }
        Ok(value)
    }

    pub(crate) fn get_or_compute_stdlib_file_action_graph_dataset<F>(
        &self,
        key: &str,
        compute: F,
    ) -> Result<Arc<StdlibFileActionGraphDataset>>
    where
        F: FnOnce() -> Result<StdlibFileActionGraphDataset>,
    {
        if self.dataset_ttl.is_zero() {
            return Ok(Arc::new(compute()?));
        }
        if let Ok(mut cache) = self.stdlib_file_action_graph_by_query.lock() {
            Self::prune_stale_cached_arcs(&mut cache, self.dataset_ttl);
            if let Some(cached) = cache.get(key) {
                if Self::cache_fresh(cached.cached_at, self.dataset_ttl) {
                    info!(
                        "web.cache stdlib_file_action_graph_hit key={} age_ms={}",
                        key,
                        cached.cached_at.elapsed().as_millis()
                    );
                    return Ok(cached.value.clone());
                }
                cache.remove(key);
            }
        }
        info!("web.cache stdlib_file_action_graph_miss key={}", key);
        let value = Arc::new(compute()?);
        let node_count = value.nodes.len();
        let edge_count = value.edges.len();
        if node_count > self.file_graph_cache_max_nodes
            || edge_count > self.file_graph_cache_max_edges
        {
            info!(
                "web.cache stdlib_file_action_graph_skip_oversize key={} nodes={} edges={} max_nodes={} max_edges={}",
                key,
                node_count,
                edge_count,
                self.file_graph_cache_max_nodes,
                self.file_graph_cache_max_edges
            );
            return Ok(value);
        }
        if let Ok(mut cache) = self.stdlib_file_action_graph_by_query.lock() {
            Self::prune_stale_cached_arcs(&mut cache, self.dataset_ttl);
            cache.insert(
                key.to_string(),
                CachedArc {
                    cached_at: Instant::now(),
                    value: value.clone(),
                },
            );
            while cache.len() > PAGE_CACHE_MAX_ENTRIES {
                let Some(oldest_key) = cache
                    .iter()
                    .min_by_key(|(_, cached)| cached.cached_at)
                    .map(|(entry_key, _)| entry_key.clone())
                else {
                    break;
                };
                cache.remove(&oldest_key);
            }
        }
        Ok(value)
    }

    pub(crate) fn get_or_compute_ir_fn_corpus_g8r_vs_yosys_dataset<F>(
        &self,
        compute: F,
    ) -> Result<Arc<StdlibG8rVsYosysDataset>>
    where
        F: FnOnce() -> Result<StdlibG8rVsYosysDataset>,
    {
        if self.dataset_ttl.is_zero() {
            return Ok(Arc::new(compute()?));
        }
        if let Ok(mut cache) = self.ir_fn_corpus_g8r_vs_yosys.lock() {
            if let Some(cached) = cache.as_ref() {
                if Self::cache_fresh(cached.cached_at, self.dataset_ttl) {
                    info!(
                        "web.cache ir_fn_corpus_g8r_vs_yosys_hit age_ms={}",
                        cached.cached_at.elapsed().as_millis()
                    );
                    return Ok(cached.value.clone());
                }
                *cache = None;
            }
        }
        info!("web.cache ir_fn_corpus_g8r_vs_yosys_miss");
        let value = Arc::new(compute()?);
        if let Ok(mut cache) = self.ir_fn_corpus_g8r_vs_yosys.lock() {
            *cache = Some(CachedArc {
                cached_at: Instant::now(),
                value: value.clone(),
            });
        }
        Ok(value)
    }

    pub(crate) fn get_or_compute_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset<F>(
        &self,
        compute: F,
    ) -> Result<Arc<StdlibG8rVsYosysDataset>>
    where
        F: FnOnce() -> Result<StdlibG8rVsYosysDataset>,
    {
        if self.dataset_ttl.is_zero() {
            return Ok(Arc::new(compute()?));
        }
        if let Ok(mut cache) = self.ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc.lock() {
            if let Some(cached) = cache.as_ref() {
                if Self::cache_fresh(cached.cached_at, self.dataset_ttl) {
                    info!(
                        "web.cache ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_hit age_ms={}",
                        cached.cached_at.elapsed().as_millis()
                    );
                    return Ok(cached.value.clone());
                }
                *cache = None;
            }
        }
        info!("web.cache ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_miss");
        let value = Arc::new(compute()?);
        if let Ok(mut cache) = self.ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc.lock() {
            *cache = Some(CachedArc {
                cached_at: Instant::now(),
                value: value.clone(),
            });
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn sample_dataset() -> StdlibG8rVsYosysDataset {
        StdlibG8rVsYosysDataset {
            fraig: false,
            samples: Vec::new(),
            min_ir_nodes: 0,
            max_ir_nodes: 0,
            g8r_only_count: 0,
            yosys_only_count: 0,
            available_crate_versions: Vec::new(),
        }
    }

    fn sample_graph_dataset() -> StdlibFileActionGraphDataset {
        StdlibFileActionGraphDataset {
            available_crate_versions: vec!["0.31.0".to_string()],
            selected_crate_version: Some("0.31.0".to_string()),
            available_files: Vec::new(),
            selected_file: None,
            available_functions: Vec::new(),
            selected_function: None,
            include_k3_descendants: false,
            selected_action_id: None,
            action_focus_found: false,
            root_action_ids: Vec::new(),
            total_actions_for_crate: 0,
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    #[test]
    fn page_cache_roundtrip() {
        let cache = WebUiCache::new(false);
        cache.put_page("page:/versions/".to_string(), "<html/>".to_string());
        let first = cache
            .get_page("page:/versions/")
            .expect("expected cached html");
        assert_eq!(first, "<html/>");
        let second = cache
            .get_page("page:/versions/")
            .expect("expected cached html on subsequent read");
        assert_eq!(second, "<html/>");
    }

    #[test]
    fn stdlib_dataset_cache_hits_without_recompute() {
        let cache = WebUiCache::new(false);
        let calls = AtomicUsize::new(0);
        let first = cache
            .get_or_compute_stdlib_g8r_vs_yosys_dataset(false, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(sample_dataset())
            })
            .expect("first compute");
        let second = cache
            .get_or_compute_stdlib_g8r_vs_yosys_dataset(false, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(sample_dataset())
            })
            .expect("second compute");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn stdlib_file_action_graph_dataset_cache_hits_without_recompute() {
        let cache = WebUiCache::new(false);
        let calls = AtomicUsize::new(0);
        let first = cache
            .get_or_compute_stdlib_file_action_graph_dataset(
                "crate=0.31.0;file=float32.x;fn=add;action=",
                || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(sample_graph_dataset())
                },
            )
            .expect("first compute");
        let second = cache
            .get_or_compute_stdlib_file_action_graph_dataset(
                "crate=0.31.0;file=float32.x;fn=add;action=",
                || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(sample_graph_dataset())
                },
            )
            .expect("second compute");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }
}
