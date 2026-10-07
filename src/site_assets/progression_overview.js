// SPDX-License-Identifier: Apache-2.0
// Browser projection only: the catalog's typed evidence remains authoritative.
const costKinds = [
  {key: 'lower', label: 'Lower', color: '#5cdb9b'},
  {key: 'equal', label: 'Equal', color: '#8795a8'},
  {key: 'higher', label: 'Higher', color: '#ff7990'},
];
function costKind(delta) { return delta < 0 ? 'lower' : delta > 0 ? 'higher' : 'equal'; }
function costRatio(value, reference) { return reference === 0 ? null : 100 * value / reference; }
function checkedCost(value) {
  if (!Number.isFinite(value) || value < 0) throw Error('Cost must be a finite nonnegative number.');
  return value;
}
function costComparison(baseline, current, mode = 'baseline') {
  if (!['baseline', 'yosys'].includes(mode)) throw Error('Unknown cost comparison reference.');
  const before = indexSamples(mode === 'yosys' ? current.samples : baseline.samples, 'cost reference');
  const after = indexSamples(current.samples, 'current cost');
  const rows = [...after].filter(([key]) => before.has(key)).map(([key, sample]) => {
    const referenceSample = before.get(key);
    const old = checkedCost(mode === 'yosys' ? sample.yosys_abc_product : referenceSample.g8r_product);
    const now = checkedCost(sample.g8r_product), delta = now - old;
    return {key, label: sample.fn_key || referenceSample.fn_key || key, old, now, delta,
      percent: old === 0 ? null : 100 * delta / old, kind: costKind(delta)};
  });
  const old = rows.reduce((total, row) => total + row.old, 0);
  const now = rows.reduce((total, row) => total + row.now, 0);
  const counts = Object.fromEntries(costKinds.map(kind => [kind.key, rows.filter(row => row.kind === kind.key).length]));
  const added = [...after.keys()].filter(key => !before.has(key)).length;
  const removed = [...before.keys()].filter(key => !after.has(key)).length;
  return {rows, counts, old, now, delta: now - old, index: rows.length ? costRatio(now, old) : null,
    percent: rows.length && old !== 0 ? 100 * (now - old) / old : null, added, removed,
    complete: current.coverage === 'cohort_complete' &&
      (mode === 'yosys' || baseline.coverage === 'cohort_complete') && added === 0 && removed === 0};
}
function costOverview(generations, baselineId, currentId, mode = 'baseline') {
  const baseline = generations.find(g => g.generation_id === baselineId);
  const current = generations.find(g => g.generation_id === currentId);
  if (!baseline || !current) throw Error('Selected fixed-IR generation is unavailable.');
  const selected = costComparison(baseline, current, mode);
  const history = generations.filter(g => g.coverage === 'cohort_complete').map(g =>
    ({generation: g, ...costComparison(baseline, g, mode)})).filter(row => row.complete);
  const referenceLabel = mode === 'yosys' ? 'matched codegen+Yosys/ABC' : `${generationLabel(baseline)} G8r+ABC`;
  return {baseline, current, mode, selected, history, referenceLabel};
}
function costPlotLayout(xTitle, yTitle) {
  return {paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
    font: {color: '#e6edf3', family: 'ui-sans-serif, system-ui, sans-serif', size: 12},
    margin: {l: 66, r: 24, t: 32, b: 80}, height: 370, showlegend: false,
    xaxis: {title: {text: xTitle}, gridcolor: '#30363d', zerolinecolor: '#8795a8', automargin: true},
    yaxis: {title: {text: yTitle}, gridcolor: '#30363d', zerolinecolor: '#8795a8', automargin: true}};
}
function costTrendSpec(model) {
  const rows = model.history, x = rows.map((_, i) => i), values = rows.map(row => row.index);
  const finite = values.filter(Number.isFinite), lo = Math.min(100, ...finite), hi = Math.max(100, ...finite);
  const pad = Math.max((hi - lo) * .15, .01);
  const layout = costPlotLayout('Generations in order · equally spaced', 'Total cost index');
  const labelStep = Math.max(1, Math.ceil(rows.length / 6));
  const ticks = x.filter(i => i % labelStep === 0 || i === rows.length - 1);
  layout.xaxis = {...layout.xaxis, range: [-.3, Math.max(.3, rows.length - .7)],
    tickvals: ticks, ticktext: ticks.map(i => esc(generationLabel(rows[i].generation))), tickangle: -25};
  layout.yaxis = {...layout.yaxis, range: [lo - pad, hi + pad], tickformat: '.6~g'};
  layout.shapes = [{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 100, y1: 100,
    line: {color: '#8795a8', width: 1, dash: 'dot'}}];
  layout.annotations = [{xref: 'paper', yref: 'paper', x: 0, y: 1.1, xanchor: 'left', showarrow: false,
    text: model.mode === 'yosys' ? '100 = each generation’s Yosys/ABC' : `100 = ${esc(generationLabel(model.baseline))}`}];
  if (!finite.length) layout.annotations.push({xref: 'paper', yref: 'paper', x: .5, y: .5,
    showarrow: false, text: rows.length ? 'Index undefined: reference total is zero' : 'No complete, matched generations'});
  const trace = {type: 'scatter', mode: 'lines+markers', x, y: values, connectgaps: false,
    line: {color: '#d2a8ff', width: 2.5},
    marker: {color: '#d2a8ff', size: rows.map(row => row.generation.generation_id === model.current.generation_id ? 12 : 7),
      symbol: rows.map(row => generationIsGit(row.generation) ? 'diamond-open' : 'circle')},
    customdata: rows.map(row => row.generation.generation_id),
    text: rows.map(row => `${esc(generationLabel(row.generation))}<br>${esc(row.generation.event_time_utc || 'Date unavailable')} · DSO v${esc(row.generation.dso_version)}<br>Index: ${product(row.index)}<br>G8r+ABC cost: ${product(row.now)}<br>Reference cost: ${product(row.old)}<br>${row.rows.length} paired functions`),
    hovertemplate: '%{text}<extra></extra>'};
  return {traces: [trace], layout};
}
function costContributionSpec(comparison, units = 'absolute') {
  const field = units === 'percent' ? 'percent' : 'delta';
  const rows = comparison.rows.filter(row => Number.isFinite(row[field]))
    .sort((a, b) => a[field] - b[field] || a.key.localeCompare(b.key));
  const layout = costPlotLayout(units === 'percent' ? 'Cost change (%) · lower ← 0 → higher' : 'Cost change (nodes × depth) · lower ← 0 → higher', 'Functions, sorted by change');
  layout.margin.b = 62;
  layout.xaxis.tickformat = units === 'percent' ? '.4~g' : '~s';
  const extent = Math.max(1, ...rows.map(row => Math.abs(row[field])));
  layout.xaxis.range = [-extent * 1.08, extent * 1.08];
  layout.yaxis = {...layout.yaxis, range: [rows.length + .5, .5], tickformat: 'd', dtick: Math.max(1, Math.ceil(rows.length / 5))};
  layout.shapes = [{type: 'line', yref: 'paper', y0: 0, y1: 1, x0: 0, x1: 0, line: {color: '#8795a8', width: 1}}];
  if (!rows.length) layout.annotations = [{xref: 'paper', yref: 'paper', x: .5, y: .5,
    showarrow: false, text: comparison.rows.length ? 'All percentage changes are undefined; choose absolute cost' : 'No paired functions'}];
  const traces = costKinds.map(kind => {
    const selected = rows.map((row, rank) => ({row, rank})).filter(({row}) => row.kind === kind.key);
    return {type: 'scatter', mode: 'markers', name: kind.label,
      x: selected.map(({row}) => row[field]), y: selected.map(({rank}) => rank + 1),
      customdata: selected.map(({row}) => row.key), marker: {color: kind.color, size: 6, opacity: .85},
      text: selected.map(({row}) => `${esc(row.label)}<br>${row.key.slice(0, 12)}<br>Reference: ${product(row.old)}<br>Current: ${product(row.now)}<br>Change: ${product(row.delta)} (${percentage(row.percent)})`),
      hovertemplate: '%{text}<extra>%{fullData.name}</extra>'};
  });
  return {traces, layout, omitted: comparison.rows.length - rows.length};
}
function costHeadline(comparison) {
  if (!comparison.rows.length) return 'No paired functions';
  if (comparison.percent === null) return 'Zero reference cost';
  if (comparison.delta === 0) return 'Equal total cost';
  return `${product(Math.abs(comparison.percent))}% ${comparison.delta < 0 ? 'lower' : 'higher'} total cost`;
}
function costBreadthRows(model) {
  return model.history.map(row => {
    const g = row.generation, n = row.rows.length;
    const counts = costKinds.map(kind => `${row.counts[kind.key]} ${kind.label.toLowerCase()}`).join(' · ');
    return `<button type="button" class="cost-breadth-row" data-cost-generation="${esc(g.generation_id)}" aria-pressed="${g.generation_id === model.current.generation_id}" aria-label="${esc(generationLabel(g))}: ${counts}" title="${esc(g.event_time_utc || 'Date unavailable')} · ${counts}"><span class="cost-release">${esc(generationLabel(g))}</span><span class="cost-stack" aria-hidden="true">${costKinds.map(kind => `<span class="cost-${kind.key}" style="width:${n ? 100 * row.counts[kind.key] / n : 0}%"></span>`).join('')}</span><span class="cost-counts">${costKinds.map(kind => `<span class="cost-text-${kind.key}">${row.counts[kind.key]}</span>`).join(' / ')}</span></button>`;
  }).join('');
}
function renderGenerationPair(generations, baselineId, currentId) {
  const root = byId('progression-overview');
  if (!root) return;
  const mode = byId('progression-reference').value;
  const model = costOverview(generations, baselineId, currentId, mode), s = model.selected;
  const currentLabel = generationLabel(model.current), context = `${currentLabel} G8r+ABC vs ${model.referenceLabel}`;
  root.dataset.costReference = mode;
  root.dataset.rendered = 'loading';
  const previousTrend = byId('progression-trend');
  if (previousTrend && typeof Plotly !== 'undefined') Plotly.purge(previousTrend);
  const warning = !s.complete ? `<p class="coverage-warning">Incomplete fixed-IR warning: this selected comparison uses only ${s.rows.length} paired functions (${s.added} current-only, ${s.removed} reference-only). It is not a full-cohort result. Incomplete generations are never plotted in the historical charts.</p>` : '';
  byId('progression-summary').innerHTML = warning + `<article class="card"><div class="muted">${esc(context)}</div><div class="stat-value cost-text-${costKind(s.delta)}">${costHeadline(s)}</div><div class="meta">${product(s.old)} → ${product(s.now)} nodes × depth. ${s.percent === null ? 'A percentage is undefined when the reference total is zero.' : 'Ratio of summed costs; larger functions carry more weight.'}</div></article><article class="card"><div class="muted">How widespread is it?</div><div class="cost-summary-counts">${costKinds.map(kind => `<span class="cost-text-${kind.key}"><strong>${s.counts[kind.key]}</strong> ${kind.label.toLowerCase()}</span>`).join('')}</div><div class="meta">${s.rows.length} paired functions, each counted once. Equal cost need not mean equal nodes and depth.</div></article>`;
  byId('progression-chart').innerHTML = `<div class="progression-chart-grid"><article class="progression-chart-panel"><h3>How much did total cost change?</h3><p class="meta">Lower index is better · reference = 100</p><div id="progression-trend" class="progression-plot" aria-label="Indexed total cost by generation"></div></article><article class="progression-chart-panel"><h3>How many functions improved?</h3><p class="meta">Product cost vs ${esc(model.referenceLabel)}</p><div class="cost-legend">${costKinds.map(kind => `<span class="cost-text-${kind.key}">● ${kind.label}</span>`).join('')}</div><div class="cost-breadth">${costBreadthRows(model) || '<p class="muted">No complete, matched generations.</p>'}</div><p class="meta cost-chart-help">Counts: lower / equal / higher. Click a row or trend point to inspect that generation.</p></article></div><p class="meta cost-chart-help">Same canonical structural-hash cohort in every historical point. ${mode === 'yosys' ? 'Each point uses its own matched Yosys/ABC reference; changes in this gap can come from either implementation. This is not a direct release-to-release change.' : `Each point compares G8r+ABC directly with ${esc(generationLabel(model.baseline))}; no Yosys/ABC subtraction. DSO and runtime details remain in the coverage inventory.`} The indexed line uses a zoomed vertical range around the observed values; 100 marks parity.</p>`;
  const selectGeneration = id => {
    const select = byId('current-version'); select.value = id; select.dispatchEvent(new Event('change'));
  };
  root.querySelectorAll('[data-cost-generation]').forEach(button => button.onclick = () => selectGeneration(button.dataset.costGeneration));
  const breadth = root.querySelector('.cost-breadth'), selectedBar = breadth.querySelector('[aria-pressed="true"]');
  if (selectedBar) breadth.scrollTop = Math.max(0, selectedBar.getBoundingClientRect().bottom - breadth.getBoundingClientRect().bottom + 8);
  byId('progression-contribution-context').textContent = context;
  byId('progression-function-detail').innerHTML = '';
  const exactRows = [...s.rows].sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta) || a.key.localeCompare(b.key));
  byId('progression-table').innerHTML = `<div class="table-wrap"><table><caption>${esc(context)} · all ${s.rows.length} paired functions, ordered by absolute change</caption><thead><tr><th>Function</th><th>${esc(model.referenceLabel)}</th><th>${esc(currentLabel)} G8r+ABC</th><th>Cost change</th><th>Percent change</th></tr></thead><tbody>${exactRows.map(row => `<tr><td><button type="button" class="sample-link" data-cost-function="${row.key}">${esc(row.label)}</button></td><td>${product(row.old)}</td><td>${product(row.now)}</td><td class="cost-text-${row.kind}">${product(row.delta)}</td><td>${percentage(row.percent)}</td></tr>`).join('')}</tbody></table></div>`;
  const inspect = key => {
    const row = s.rows.find(row => row.key === key); if (!row) return;
    byId('progression-function-detail').innerHTML = `<div class="cost-function-detail"><h4>${esc(row.label)}</h4><code>${row.key}</code><p>${esc(model.referenceLabel)}: ${product(row.old)} → ${esc(currentLabel)} G8r+ABC: ${product(row.now)} nodes × depth</p><p class="cost-text-${row.kind}">Change: ${product(row.delta)} (${percentage(row.percent)})${row.percent === null ? ' · percentage undefined: zero reference cost' : ''}</p></div>`;
  };
  root.querySelectorAll('[data-cost-function]').forEach(button => button.onclick = () => inspect(button.dataset.costFunction));
  const revision = String(Number(root.dataset.plotRevision || 0) + 1); root.dataset.plotRevision = revision;
  const trend = costTrendSpec(model), contribution = costContributionSpec(s, byId('progression-change-units').value);
  byId('progression-contribution-status').textContent = `${s.rows.length - contribution.omitted} / ${s.rows.length} paired functions plotted, including equal costs. ${contribution.omitted ? `${contribution.omitted} zero-reference percentage changes are undefined; all remain in the counts and exact-cost table. Choose absolute cost to see them plotted.` : 'Hover or click a point to inspect exact costs.'}`;
  if (typeof Plotly === 'undefined') { root.dataset.rendered = 'unavailable'; byId('progression-contribution-status').textContent = 'Plotting library unavailable; exact costs remain in the table.'; return; }
  const trendPlot = byId('progression-trend'), contributionPlot = byId('progression-contributions');
  Promise.all([[trendPlot, trend], [contributionPlot, contribution]].map(([plot, spec]) =>
    Plotly.react(plot, spec.traces, spec.layout, {responsive: true, displaylogo: false, displayModeBar: false, scrollZoom: false})
  )).then(() => {
    if (revision !== root.dataset.plotRevision) return;
    for (const [plot, callback] of [[trendPlot, selectGeneration], [contributionPlot, inspect]]) {
      plot.removeAllListeners('plotly_click'); plot.on('plotly_click', event => {
        const key = event.points?.[0]?.customdata; if (key) callback(key);
      });
    }
    root.dataset.rendered = 'true';
  }).catch(error => { if (revision === root.dataset.plotRevision) { root.dataset.rendered = 'unavailable'; byId('progression-contribution-status').textContent = `Cost plot unavailable: ${error.message}`; } });
}
