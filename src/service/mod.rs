use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use rayon::prelude::*;
use regex::Regex;
use reqwest::blocking::Client;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};
use walkdir::WalkDir;

use crate::executor::{compute_action_id, download_to_file};
use crate::model::*;
use crate::queue::*;
use crate::runtime::*;
use crate::store::ArtifactStore;
use crate::versioning::*;
use crate::{
    DEFAULT_ACTION_TIMEOUT_SECONDS, DEFAULT_YOSYS_FLOW_SCRIPT, DOCKER_RUN_NAME_COUNTER,
    DRIVER_RELEASE_CACHE_BINARIES, DRIVER_RELEASE_CACHE_LOCK_FILE, DRIVER_RELEASE_CACHE_READY_FILE,
    DRIVER_RELEASE_CACHE_SETUP_POLL_MILLIS, DRIVER_RELEASE_CACHE_SETUP_TIMEOUT_SECS,
    DRIVER_TOOLS_SETUP_FROM_CACHE_SNIPPET, INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY,
    IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION, LEGACY_G8R_STATS_RELPATH,
    STRUCTURAL_SCAN_PROGRESS_EVERY_ACTIONS, STRUCTURAL_SCAN_PROGRESS_INTERVAL_SECS,
    VENDORED_DOWNLOAD_RELEASE_SCRIPT, VERSION_COMPAT_PATH,
    WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY, WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE,
};

mod core;
mod runtime_docker;
mod structural_index;

pub(crate) use core::*;
pub(crate) use runtime_docker::*;
pub(crate) use structural_index::*;
