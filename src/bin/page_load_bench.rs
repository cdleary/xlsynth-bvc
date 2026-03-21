use anyhow::{Context, Result, bail};
use clap::Parser;
use reqwest::blocking::Client;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:3000";
const DEFAULT_TIMEOUT_SECS: u64 = 60;
const DEFAULT_WARMUP: usize = 1;
const DEFAULT_SAMPLES: usize = 5;
const DEFAULT_ROUTES: &[&str] = &[
    "/versions/",
    "/dslx-fns-g8r/?metric=and_nodes&fraig=false&file=",
    "/dslx-fns-yosys-abc/?metric=and_nodes&fraig=false&file=",
    "/dslx-fns-g8r-vs-yosys-abc/?losses_only=false",
    "/ir-fn-corpus-structural/",
    "/ir-fn-corpus-g8r-vs-yosys-abc/?losses_only=false",
];

#[derive(Debug, Parser)]
#[command(name = "page-load-bench")]
#[command(about = "Benchmark page load latency for a running xlsynth-bvc web server")]
struct Cli {
    #[arg(long, default_value = DEFAULT_BASE_URL)]
    base_url: String,

    #[arg(long = "route")]
    routes: Vec<String>,

    #[arg(long, default_value_t = DEFAULT_WARMUP)]
    warmup: usize,

    #[arg(long, default_value_t = DEFAULT_SAMPLES)]
    samples: usize,

    #[arg(long, default_value_t = DEFAULT_TIMEOUT_SECS)]
    timeout_secs: u64,

    #[arg(long)]
    fail_on_non_200: bool,
}

#[derive(Debug)]
struct RequestSample {
    elapsed_ms: f64,
    status_code: Option<u16>,
    body_bytes: usize,
    error: Option<String>,
}

impl RequestSample {
    fn ok(&self) -> bool {
        self.error.is_none() && self.status_code == Some(200)
    }
}

#[derive(Debug)]
struct RouteSummary {
    route: String,
    total_requests: usize,
    ok_count: usize,
    error_count: usize,
    status_counts: BTreeMap<u16, usize>,
    body_bytes_min: usize,
    body_bytes_max: usize,
    first_sample_ms: f64,
    min_ms: f64,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    max_ms: f64,
}

fn normalize_base_url(raw: &str) -> String {
    raw.trim_end_matches('/').to_string()
}

fn normalize_route(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{}", trimmed)
    }
}

fn percentile(sorted_samples: &[f64], p: f64) -> f64 {
    if sorted_samples.is_empty() {
        return 0.0;
    }
    let idx = (((sorted_samples.len() - 1) as f64) * p).round() as usize;
    sorted_samples[idx]
}

fn run_one_request(client: &Client, url: &str) -> RequestSample {
    let started = Instant::now();
    match client.get(url).send() {
        Ok(response) => {
            let status = response.status().as_u16();
            match response.bytes() {
                Ok(bytes) => RequestSample {
                    elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
                    status_code: Some(status),
                    body_bytes: bytes.len(),
                    error: None,
                },
                Err(err) => RequestSample {
                    elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
                    status_code: Some(status),
                    body_bytes: 0,
                    error: Some(format!("reading response body: {:#}", err)),
                },
            }
        }
        Err(err) => RequestSample {
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
            status_code: None,
            body_bytes: 0,
            error: Some(format!("{:#}", err)),
        },
    }
}

fn summarize_route(route: &str, measured: &[RequestSample]) -> Result<RouteSummary> {
    if measured.is_empty() {
        bail!("no measured samples for route {}", route);
    }
    let mut ok_count = 0_usize;
    let mut error_count = 0_usize;
    let mut status_counts: BTreeMap<u16, usize> = BTreeMap::new();
    let mut body_bytes_min = usize::MAX;
    let mut body_bytes_max = 0_usize;
    let mut times: Vec<f64> = Vec::with_capacity(measured.len());
    for sample in measured {
        times.push(sample.elapsed_ms);
        body_bytes_min = body_bytes_min.min(sample.body_bytes);
        body_bytes_max = body_bytes_max.max(sample.body_bytes);
        if let Some(status) = sample.status_code {
            *status_counts.entry(status).or_insert(0) += 1;
        }
        if sample.ok() {
            ok_count += 1;
        } else {
            error_count += 1;
        }
    }
    if body_bytes_min == usize::MAX {
        body_bytes_min = 0;
    }
    times.sort_by(|a, b| a.total_cmp(b));
    let min_ms = *times.first().unwrap_or(&0.0);
    let max_ms = *times.last().unwrap_or(&0.0);
    let avg_ms = times.iter().sum::<f64>() / (times.len() as f64);
    let p50_ms = percentile(&times, 0.50);
    let p95_ms = percentile(&times, 0.95);
    Ok(RouteSummary {
        route: route.to_string(),
        total_requests: measured.len(),
        ok_count,
        error_count,
        status_counts,
        body_bytes_min,
        body_bytes_max,
        first_sample_ms: measured[0].elapsed_ms,
        min_ms,
        avg_ms,
        p50_ms,
        p95_ms,
        max_ms,
    })
}

fn print_summary(
    base_url: &str,
    warmup: usize,
    summaries: &[RouteSummary],
    total_elapsed: Duration,
) {
    println!(
        "page-load-bench base_url={} warmup={} elapsed_total_ms={:.2}",
        base_url,
        warmup,
        total_elapsed.as_secs_f64() * 1000.0
    );
    println!(
        "{:<58} {:>6} {:>6} {:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>15} {:>15}",
        "route",
        "ok",
        "err",
        "n",
        "first_ms",
        "min_ms",
        "p50_ms",
        "p95_ms",
        "avg_ms",
        "max_ms",
        "bytes_min",
        "bytes_max",
    );
    for summary in summaries {
        println!(
            "{:<58} {:>6} {:>6} {:>6} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>15} {:>15}",
            summary.route,
            summary.ok_count,
            summary.error_count,
            summary.total_requests,
            summary.first_sample_ms,
            summary.min_ms,
            summary.p50_ms,
            summary.p95_ms,
            summary.avg_ms,
            summary.max_ms,
            summary.body_bytes_min,
            summary.body_bytes_max,
        );
        if !summary.status_counts.is_empty() {
            let status_line = summary
                .status_counts
                .iter()
                .map(|(status, count)| format!("{}:{}", status, count))
                .collect::<Vec<_>>()
                .join(", ");
            println!("  status_counts: {}", status_line);
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.samples == 0 {
        bail!("--samples must be > 0");
    }
    let base_url = normalize_base_url(&cli.base_url);
    let routes = if cli.routes.is_empty() {
        DEFAULT_ROUTES
            .iter()
            .map(|route| route.to_string())
            .collect::<Vec<_>>()
    } else {
        cli.routes.clone()
    };
    let routes = routes
        .iter()
        .map(|route| normalize_route(route))
        .collect::<Vec<_>>();

    let client = Client::builder()
        .timeout(Duration::from_secs(cli.timeout_secs))
        .build()
        .context("building HTTP client")?;

    let started_total = Instant::now();
    let mut summaries = Vec::new();
    let mut saw_non_200 = false;

    for route in &routes {
        let url = format!("{}{}", base_url, route);
        for _ in 0..cli.warmup {
            let _ = run_one_request(&client, &url);
        }
        let mut measured = Vec::with_capacity(cli.samples);
        for _ in 0..cli.samples {
            measured.push(run_one_request(&client, &url));
        }
        if measured.iter().any(|sample| !sample.ok()) {
            saw_non_200 = true;
        }
        summaries.push(summarize_route(route, &measured)?);
    }

    print_summary(&base_url, cli.warmup, &summaries, started_total.elapsed());
    if cli.fail_on_non_200 && saw_non_200 {
        bail!("at least one sample had non-200 status or request error");
    }
    Ok(())
}
