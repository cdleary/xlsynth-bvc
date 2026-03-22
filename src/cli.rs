use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "xlsynth-bvc")]
#[command(about = "Hermetic action execution with artifact provenance for xlsynth workflows")]
pub struct Cli {
    #[arg(long, default_value = crate::DEFAULT_STORE_DIR)]
    pub(crate) store_dir: PathBuf,

    #[arg(long, value_name = "PATH")]
    pub(crate) artifacts_via_sled: Option<PathBuf>,

    #[command(subcommand)]
    pub(crate) command: TopCommand,
}

#[derive(Debug, Subcommand)]
pub enum TopCommand {
    RunIrDirCorpus {
        #[arg(long, value_name = "DIR")]
        input_dir: PathBuf,
        #[arg(long, value_name = "DIR")]
        output_dir: PathBuf,
        #[arg(long, value_enum, default_value_t = CorpusRecipePreset::G8rVsYabcAigDiff)]
        recipe_preset: CorpusRecipePreset,
        #[arg(long, value_enum, default_value_t = CorpusExecutionMode::Enqueue)]
        execution_mode: CorpusExecutionMode,
        #[arg(long, value_enum, default_value_t = CorpusTopFnPolicy::InferSinglePackage)]
        top_fn_policy: CorpusTopFnPolicy,
        #[arg(long)]
        top_fn_name: Option<String>,
        #[arg(long, default_value_t = false)]
        fraig: bool,
        #[arg(long)]
        version: String,
        #[arg(long, default_value = crate::DEFAULT_YOSYS_FLOW_SCRIPT)]
        yosys_script: String,
        #[arg(long, default_value_t = crate::DEFAULT_QUEUE_PRIORITY)]
        priority: i32,
        #[command(flatten)]
        driver: DriverCli,
        #[command(flatten)]
        yosys: YosysCli,
    },
    Run {
        #[command(subcommand)]
        action: RunAction,
    },
    Enqueue {
        #[arg(long, default_value_t = crate::DEFAULT_QUEUE_PRIORITY)]
        priority: i32,
        #[command(subcommand)]
        action: RunAction,
    },
    EnqueueCrateVersion {
        #[arg(long)]
        crate_version: String,
        #[arg(long, default_value_t = crate::DEFAULT_QUEUE_PRIORITY)]
        priority: i32,
    },
    DrainQueue {
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        worker_id: Option<String>,
        #[arg(long, default_value_t = crate::DEFAULT_QUEUE_LEASE_SECONDS)]
        lease_seconds: i64,
        #[arg(long)]
        no_reclaim_expired: bool,
    },
    RunWorkers {
        #[arg(long, default_value_t = crate::runtime::default_web_runner_workers())]
        workers: usize,
        #[arg(long)]
        worker_id: Option<String>,
        #[arg(long, default_value_t = crate::DEFAULT_QUEUE_LEASE_SECONDS)]
        lease_seconds: i64,
        #[arg(long, default_value_t = crate::DEFAULT_WEB_RUNNER_POLL_MILLIS)]
        poll_millis: u64,
        #[arg(long, default_value_t = crate::DEFAULT_WEB_RUNNER_DRAIN_BATCH_SIZE)]
        batch_size: usize,
        #[arg(long)]
        exit_when_idle: bool,
        #[arg(long)]
        no_reclaim_expired: bool,
    },
    ShowCorpusProgress {
        #[arg(long, value_name = "DIR")]
        output_dir: PathBuf,
        #[arg(long, default_value_t = 1800)]
        throughput_window_seconds: i64,
        #[arg(long, default_value_t = 10)]
        failed_sample_examples: usize,
    },
    RefreshCorpusStatus {
        #[arg(long, value_name = "DIR")]
        output_dir: PathBuf,
        #[arg(long, default_value_t = 1800)]
        throughput_window_seconds: i64,
        #[arg(long, default_value_t = 10)]
        failed_sample_examples: usize,
    },
    ServeWeb {
        #[arg(long, default_value = crate::DEFAULT_WEB_BIND)]
        bind: String,
        #[arg(long)]
        no_runner: bool,
        #[arg(long, value_name = "DIR")]
        snapshot_dir: Option<PathBuf>,
        #[arg(long, default_value_t = crate::DEFAULT_QUEUE_LEASE_SECONDS)]
        runner_lease_seconds: i64,
        #[arg(long, default_value_t = crate::DEFAULT_WEB_RUNNER_POLL_MILLIS)]
        runner_poll_millis: u64,
        #[arg(long, default_value_t = crate::DEFAULT_WEB_RUNNER_DRAIN_BATCH_SIZE)]
        runner_batch_size: usize,
        #[arg(long, default_value_t = crate::runtime::default_web_runner_workers())]
        runner_workers: usize,
    },
    DiscoverReleases {
        #[arg(long)]
        after: String,
        #[arg(long, default_value_t = 10)]
        max_pages: u32,
        #[arg(long)]
        dry_run: bool,
    },
    ShowSuggested {
        action_id: String,
        #[arg(long)]
        recursive: bool,
        #[arg(long, default_value_t = 3)]
        max_depth: u32,
    },
    EnqueueSuggested {
        action_id: String,
        #[arg(long)]
        recursive: bool,
        #[arg(long, default_value_t = 3)]
        max_depth: u32,
        #[arg(long, default_value_t = crate::DEFAULT_QUEUE_PRIORITY)]
        priority: i32,
    },
    AuditSuggested {
        #[arg(long)]
        include_completed: bool,
    },
    FindAigStatDiffs {
        #[arg(long)]
        opt_ir_action_id: String,
    },
    PopulateIrFnCorpusStructural {
        #[arg(long, default_value = ".", hide = true)]
        output_dir: PathBuf,
        #[arg(long)]
        recompute_missing_hashes: bool,
        #[arg(long, default_value_t = crate::runtime::default_structural_index_threads())]
        threads: usize,
    },
    CheckIrFnCorpusStructuralFreshness,
    RebuildWebIndices,
    BuildStaticSnapshot {
        #[arg(long, value_name = "DIR")]
        out_dir: PathBuf,
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        skip_rebuild_web_indices: bool,
    },
    VerifyStaticSnapshot {
        #[arg(long, value_name = "DIR")]
        snapshot_dir: PathBuf,
    },
    AnalyzeSledSpace {
        #[arg(long, default_value_t = 25)]
        top: usize,
        #[arg(long, default_value_t = 40)]
        sample: usize,
    },
    CompactSledDb {
        #[arg(long, value_name = "PATH")]
        output_path: Option<PathBuf>,
        #[arg(long)]
        replace_source: bool,
    },
    BackfillSledActionFileCompression {
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        target_relpath: Option<String>,
    },
    PruneSledActionsByRelpathSize {
        #[arg(long, default_value = "payload/prep_for_gatify.ir")]
        relpath: String,
        #[arg(long, default_value_t = 1024 * 1024)]
        min_bytes: usize,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        no_downstream: bool,
    },
    EnqueueStructuralOptIrG8r {
        #[arg(long)]
        crate_version: String,
        #[arg(long, value_enum)]
        fraig: ExplicitBool,
        #[arg(long)]
        recompute_missing_hashes: bool,
        #[arg(long)]
        dry_run: bool,
    },
    EnqueueStructuralOptIrKBoolCones {
        #[arg(long)]
        crate_version: String,
        #[arg(long, default_value_t = 3)]
        k: u32,
        #[arg(long)]
        only_previous_losses: bool,
        #[arg(long)]
        recompute_missing_hashes: bool,
        #[arg(long)]
        dry_run: bool,
    },
    BackfillKBoolConeSuggestions {
        #[arg(long)]
        enqueue: bool,
        #[arg(long)]
        dry_run: bool,
    },
    BackfillStdlibOptIrAigEquivSuggestions {
        #[arg(long)]
        enqueue: bool,
        #[arg(long)]
        dry_run: bool,
    },
    BackfillOptIrFrontendCompareSuggestions {
        #[arg(long)]
        enqueue: bool,
        #[arg(long)]
        dry_run: bool,
        #[arg(long, default_value_t = crate::DEFAULT_QUEUE_PRIORITY)]
        priority: i32,
        #[arg(long)]
        crate_version: Option<String>,
    },
    RepairQueue {
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        reenqueue_missing_suggested: bool,
    },
    IngestLegacyFailedRecords {
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        keep_legacy_files: bool,
    },
    RefreshVersionCompat,
    DslxToMangledIrFnName {
        #[arg(long)]
        dslx_module_name: String,
        #[arg(long)]
        dslx_fn_name: String,
    },
    Rematerialize {
        action_id: String,
    },
    ShowProvenance {
        action_id: String,
    },
    Resolve {
        action_id: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ExplicitBool {
    True,
    False,
}

impl ExplicitBool {
    pub fn as_bool(self) -> bool {
        matches!(self, Self::True)
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CorpusRecipePreset {
    G8rVsYabcAigDiff,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CorpusExecutionMode {
    Enqueue,
    Run,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CorpusTopFnPolicy {
    InferSinglePackage,
    Explicit,
    FromFilename,
}

#[derive(Debug, Subcommand)]
pub enum RunAction {
    DownloadStdlib {
        #[arg(long)]
        version: String,
    },
    DownloadSourceSubtree {
        #[arg(long)]
        version: String,
        #[arg(long)]
        subtree: String,
    },
    DslxFnToIr {
        #[arg(long)]
        dslx_subtree_action_id: String,
        #[arg(long)]
        dslx_file: String,
        #[arg(long)]
        dslx_fn_name: String,
        #[arg(long)]
        version: String,
        #[command(flatten)]
        driver: DriverCli,
    },
    IrToOpt {
        #[arg(long)]
        ir_action_id: String,
        #[arg(long)]
        top_fn_name: Option<String>,
        #[arg(long)]
        version: String,
        #[command(flatten)]
        driver: DriverCli,
    },
    IrToDelayInfo {
        #[arg(long)]
        ir_action_id: String,
        #[arg(long)]
        top_fn_name: Option<String>,
        #[arg(long, default_value = "asap7")]
        delay_model: String,
        #[arg(long)]
        version: String,
        #[command(flatten)]
        driver: DriverCli,
    },
    IrEquiv {
        #[arg(long)]
        lhs_ir_action_id: String,
        #[arg(long)]
        rhs_ir_action_id: String,
        #[arg(long)]
        top_fn_name: Option<String>,
        #[arg(long)]
        version: String,
        #[command(flatten)]
        driver: DriverCli,
    },
    IrAigEquiv {
        #[arg(long)]
        ir_action_id: String,
        #[arg(long)]
        aig_action_id: String,
        #[arg(long)]
        top_fn_name: Option<String>,
        #[arg(long)]
        version: String,
        #[command(flatten)]
        driver: DriverCli,
    },
    IrToG8rAig {
        #[arg(long)]
        ir_action_id: String,
        #[arg(long)]
        top_fn_name: Option<String>,
        #[arg(long)]
        fraig: bool,
        #[arg(long)]
        version: String,
        #[command(flatten)]
        driver: DriverCli,
    },
    IrToComboVerilog {
        #[arg(long)]
        ir_action_id: String,
        #[arg(long)]
        top_fn_name: Option<String>,
        #[arg(long)]
        use_system_verilog: bool,
        #[arg(long)]
        version: String,
        #[command(flatten)]
        driver: DriverCli,
    },
    IrToKBoolConeCorpus {
        #[arg(long)]
        ir_action_id: String,
        #[arg(long)]
        top_fn_name: Option<String>,
        #[arg(long, default_value_t = 3)]
        k: u32,
        #[arg(long)]
        max_ir_ops: Option<u64>,
        #[arg(long)]
        version: String,
        #[command(flatten)]
        driver: DriverCli,
    },
    ComboVerilogToYosysAbcAig {
        #[arg(long)]
        verilog_action_id: String,
        #[arg(long)]
        verilog_top_module_name: Option<String>,
        #[arg(long)]
        yosys_script: String,
        #[command(flatten)]
        yosys: YosysCli,
    },
    AigToYosysAbcAig {
        #[arg(long)]
        aig_action_id: String,
        #[arg(long)]
        yosys_script: String,
        #[command(flatten)]
        yosys: YosysCli,
    },
    AigToStats {
        #[arg(long)]
        aig_action_id: String,
        #[arg(long)]
        version: String,
        #[command(flatten)]
        driver: DriverCli,
    },
    AigStatDiff {
        #[arg(long)]
        opt_ir_action_id: String,
        #[arg(long)]
        g8r_aig_stats_action_id: String,
        #[arg(long)]
        yosys_abc_aig_stats_action_id: String,
    },
}

#[derive(Debug, Clone, Args)]
pub struct DriverCli {
    #[arg(
        long,
        help = "Driver crate version (e.g. 0.29.0) or `latest`; defaults to latest compatible for the requested xlsynth version from generated_version_compat.json"
    )]
    pub(crate) driver_version: Option<String>,
    #[arg(long, default_value = crate::DEFAULT_RELEASE_PLATFORM)]
    pub(crate) release_platform: String,
    #[arg(long, default_value = crate::DEFAULT_DOCKERFILE)]
    pub(crate) dockerfile: PathBuf,
    #[arg(long)]
    pub(crate) docker_image: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct YosysCli {
    #[arg(long, default_value = crate::DEFAULT_YOSYS_DOCKERFILE)]
    pub(crate) yosys_dockerfile: PathBuf,
    #[arg(long, default_value = crate::DEFAULT_YOSYS_DOCKER_IMAGE)]
    pub(crate) yosys_docker_image: String,
}
