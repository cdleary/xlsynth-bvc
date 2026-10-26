// SPDX-License-Identifier: Apache-2.0

//! Bounded final-web projections, derived from verified per-input measurements.
use super::*;

const EXAMPLE_LIMIT: usize = 10;

#[derive(Clone, Debug, Serialize)]
pub(super) struct Metrics {
    pub nodes: f64,
    pub depth: f64,
    pub graph_le: Option<f64>,
}

impl Metrics {
    pub fn cost(&self) -> f64 {
        self.nodes * self.depth
    }
}

#[derive(Clone, Debug)]
pub(super) struct Input {
    pub label: String,
    pub ir_nodes: u64,
    pub metrics: Metrics,
}

#[derive(Debug, Serialize)]
pub(super) struct Transition {
    pub baseline_version: String,
    pub baseline_cost: f64,
    pub cost_decreases: f64,
    pub cost_increases: f64,
    pub lower: usize,
    pub equal: usize,
    pub higher: usize,
    pub url: String,
    pub details: String,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct Regression {
    sample_id: String,
    label: String,
    ir_nodes: u64,
    before: Metrics,
    after: Metrics,
    delta: f64,
    percent: Option<f64>,
    contribution: Option<f64>,
    url: String,
}

#[derive(Debug, Serialize)]
pub(super) struct Detail {
    schema_version: u32,
    baseline_version: String,
    version: String,
    higher: usize,
    limit: usize,
    largest: Vec<Regression>,
    smallest: Vec<Regression>,
}

fn relative(delta: f64, reference: f64) -> Option<f64> {
    (reference > 0.)
        .then(|| 100. * (delta / reference))
        .filter(|v| v.is_finite())
}

pub(super) fn input_url(url: &str, sample_id: &str) -> String {
    let encoded: String = sample_id
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    let (base, fragment) = url.split_once('#').unwrap_or((
        url,
        if url.starts_with("corpus/") {
            "input-detail"
        } else {
            ""
        },
    ));
    let key = if base.starts_with("corpus/") {
        "sample"
    } else {
        "vector_sample"
    };
    format!(
        "{base}&{key}={encoded}{}",
        if fragment.is_empty() {
            String::new()
        } else {
            format!("#{fragment}")
        }
    )
}

pub(super) fn compare(
    baseline_version: &str,
    version: &str,
    before: &BTreeMap<String, Input>,
    after: &BTreeMap<String, Input>,
    url: String,
    details: String,
) -> Result<(Transition, Detail)> {
    if before.is_empty() || before.keys().ne(after.keys()) {
        bail!("dashboard transition populations differ");
    }
    for input in before.values().chain(after.values()) {
        if [
            input.metrics.nodes,
            input.metrics.depth,
            input.metrics.cost(),
        ]
        .into_iter()
        .any(|v| !v.is_finite() || v < 0.)
        {
            bail!("dashboard transition metrics must be finite and nonnegative");
        }
    }
    let baseline_cost: f64 = before.values().map(|s| s.metrics.cost()).sum();
    let mut transition = Transition {
        baseline_version: baseline_version.into(),
        baseline_cost,
        cost_decreases: 0.,
        cost_increases: 0.,
        lower: 0,
        equal: 0,
        higher: 0,
        url,
        details,
    };
    let mut regressions = Vec::new();
    for (key, now) in after {
        let old = &before[key];
        if old.ir_nodes != now.ir_nodes {
            bail!("dashboard transition IR sizes differ");
        }
        let delta = now.metrics.cost() - old.metrics.cost();
        if delta < 0. {
            transition.lower += 1;
            transition.cost_decreases += delta;
        } else if delta > 0. {
            transition.higher += 1;
            transition.cost_increases += delta;
            regressions.push(Regression {
                sample_id: key.clone(),
                label: now.label.clone(),
                ir_nodes: now.ir_nodes,
                before: old.metrics.clone(),
                after: now.metrics.clone(),
                delta,
                percent: relative(delta, old.metrics.cost()),
                contribution: relative(delta, baseline_cost),
                url: input_url(&transition.url, key),
            });
        } else {
            transition.equal += 1;
        }
    }
    if [
        baseline_cost,
        transition.cost_decreases,
        transition.cost_increases,
    ]
    .into_iter()
    .any(|v| !v.is_finite())
    {
        bail!("dashboard transition totals must be finite");
    }
    regressions.sort_by(|a, b| {
        b.delta
            .total_cmp(&a.delta)
            .then_with(|| a.sample_id.cmp(&b.sample_id))
    });
    let largest = regressions.iter().take(EXAMPLE_LIMIT).cloned().collect();
    // This is an explicitly size-first ranking, not an invented QoR score.
    // Undefined relative changes (zero-cost reference) remain visible.
    regressions.sort_by(|a, b| {
        a.ir_nodes
            .cmp(&b.ir_nodes)
            .then_with(|| match (a.percent, b.percent) {
                (Some(a), Some(b)) => b.total_cmp(&a),
                (None, None) => b.delta.total_cmp(&a.delta),
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
            })
            .then_with(|| a.sample_id.cmp(&b.sample_id))
    });
    let smallest = regressions.into_iter().take(EXAMPLE_LIMIT).collect();
    let detail = Detail {
        schema_version: 1,
        baseline_version: baseline_version.into(),
        version: version.into(),
        higher: transition.higher,
        limit: EXAMPLE_LIMIT,
        largest,
        smallest,
    };
    Ok((transition, detail))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input(nodes: f64, depth: f64, ir_nodes: u64) -> Input {
        Input {
            label: "function".into(),
            ir_nodes,
            metrics: Metrics {
                nodes,
                depth,
                graph_le: None,
            },
        }
    }
    #[test]
    fn cancellation_keeps_gross_changes_counts_tradeoffs_and_zero_references() {
        let before = BTreeMap::from([
            ("a".into(), input(10., 10., 30)),
            ("b".into(), input(20., 5., 10)),
            ("c".into(), input(0., 0., 2)),
            ("d".into(), input(2., 3., 5)),
        ]);
        let after = BTreeMap::from([
            ("a".into(), input(5., 10., 30)),
            ("b".into(), input(15., 10., 10)),
            ("c".into(), input(1., 1., 2)),
            ("d".into(), input(3., 2., 5)),
        ]);
        let (t, d) = compare(
            "0.1.0",
            "0.2.0",
            &before,
            &after,
            "corpus/?release=0.2.0&baseline=0.1.0".into(),
            "details.json".into(),
        )
        .unwrap();
        assert_eq!(
            (t.baseline_cost, t.cost_decreases, t.cost_increases),
            (206., -50., 51.)
        );
        assert_eq!((t.lower, t.equal, t.higher), (1, 1, 2));
        assert_eq!(
            d.largest
                .iter()
                .map(|r| r.sample_id.as_str())
                .collect::<Vec<_>>(),
            ["b", "c"]
        );
        assert_eq!(
            d.smallest
                .iter()
                .map(|r| r.sample_id.as_str())
                .collect::<Vec<_>>(),
            ["c", "b"]
        );
        assert_eq!(d.smallest[0].percent, None);
        assert_eq!(d.largest[0].percent, Some(50.));
        assert_eq!(d.largest[0].before.nodes, 20.);
        assert_eq!(d.largest[0].after.nodes, 15.);
        assert_eq!(d.largest[0].contribution, Some(100. * (50. / 206.)));
    }
    #[test]
    fn examples_are_bounded_deterministic_and_do_not_change_the_population() {
        let before = (0..32)
            .map(|i| (format!("{i:02}"), input(1., 1., i)))
            .collect();
        let after = (0..32)
            .map(|i| (format!("{i:02}"), input(2., 1., i)))
            .collect();
        let (t, d) = compare("a", "b", &before, &after, "corpus/?a=b".into(), "d".into()).unwrap();
        assert_eq!(t.higher, 32);
        assert_eq!(
            (d.largest.len(), d.smallest.len()),
            (EXAMPLE_LIMIT, EXAMPLE_LIMIT)
        );
        assert_eq!(
            d.largest
                .iter()
                .map(|r| r.sample_id.as_str())
                .collect::<Vec<_>>(),
            ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09"]
        );
    }
    #[test]
    fn invalid_metrics_or_populations_fail_and_zero_total_is_not_a_percentage() {
        let before = BTreeMap::from([("a".into(), input(0., 0., 1))]);
        let after = BTreeMap::from([("a".into(), input(1., 1., 1))]);
        let (t, d) = compare("a", "b", &before, &after, "corpus/?a=b".into(), "d".into()).unwrap();
        assert_eq!(t.baseline_cost, 0.);
        assert_eq!(
            (d.largest[0].percent, d.largest[0].contribution),
            (None, None)
        );
        for bad in [
            BTreeMap::from([("other".into(), input(1., 1., 1))]),
            BTreeMap::from([("a".into(), input(-1., -1., 1))]),
            BTreeMap::from([("a".into(), input(f64::NAN, 1., 1))]),
            BTreeMap::from([("a".into(), input(1., 1., 2))]),
        ] {
            assert!(compare("a", "b", &before, &bad, String::new(), String::new()).is_err());
        }
        assert_eq!(
            input_url(
                "history/progression.html?stage=raw#progression-vectors",
                "a &/é"
            ),
            "history/progression.html?stage=raw&vector_sample=a%20%26%2F%C3%A9#progression-vectors"
        );
    }
}
