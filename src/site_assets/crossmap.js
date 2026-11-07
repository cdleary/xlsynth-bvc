// SPDX-License-Identifier: Apache-2.0
'use strict';
(() => {
  const kinds = [
    ['native-g8r', 'g8r', 'area'], ['native-g8r', 'g8r', 'delay'],
    ['yosys-abc', 'g8r', 'area'], ['yosys-abc', 'g8r', 'delay'],
    ['yosys-abc', 'abc', 'area'], ['yosys-abc', 'abc', 'delay'],
  ];
  const key = point => `${point.source}:${point.mapper}:${point.goal}`;
  const keys = kinds.map(([source, mapper, goal]) => key({source, mapper, goal}));
  const colors = {area: '#48d8f0', delay: '#bc91ff', abcArea: '#f5bd4f', abcDelay: '#ff858d'};
  const point = (sample, source, mapper, goal) => sample?.points.find(p => p.source === source && p.mapper === mapper && p.goal === goal);
  const crossMapped = p => p.source === 'yosys-abc' && p.mapper === 'g8r';
  const usable = p => !!p && (!crossMapped(p) || p.verified);
  const positive = p => p.area > 0 && p.delay_ps > 0;
  const nearlyEqual = (a, b) => Math.abs(a - b) <= Math.max(1e-9, 1e-9 * Math.max(Math.abs(a), Math.abs(b)));
  const noLarger = (a, b) => a < b || nearlyEqual(a, b);
  const strictlySmaller = (a, b) => a < b && !nearlyEqual(a, b);
  const dominates = (a, b) => noLarger(a.area, b.area) && noLarger(a.delay_ps, b.delay_ps) && (strictlySmaller(a.area, b.area) || strictlySmaller(a.delay_ps, b.delay_ps));
  const frontier = points => points.filter(p => usable(p) && !points.some(q => usable(q) && dominates(q, p)));
  const sourceLabel = source => source === 'native-g8r' ? 'Native g8r AIG' : 'Yosys / ABC AIG';
  const goalLabel = p => p.mapper === 'abc' ? (p.goal === 'area' ? 'Area-only' : 'Timing-oriented (default)') : (p.goal === 'area' ? 'Area strategy' : 'Delay strategy');
  const label = p => `${p.source === 'native-g8r' ? 'Native' : 'Yosys'} AIG → ${p.mapper} · ${p.mapper === 'abc' && p.goal === 'delay' ? 'timing' : p.goal}`;
  function style(p, absolute = false) {
    const native = p.source === 'native-g8r', abc = p.mapper === 'abc';
    return {
      color: abc ? (p.goal === 'area' ? colors.abcArea : colors.abcDelay) : colors[p.goal],
      symbol: abc ? (p.goal === 'area' ? 'square-open' : 'x') : (p.goal === 'area' ? (native ? 'circle-open' : 'circle') : (native ? 'triangle-up-open' : 'triangle-up')),
      size: absolute ? (p.goal === 'area' ? 15 : 17) : (p.goal === 'area' ? 8 : 9),
      opacity: absolute ? 1 : .78,
      line: {width: native || abc ? 1.8 : .5},
    };
  }
  function validateCatalog(data) {
    const need = (condition, message) => { if (!condition) throw Error(message); };
    need(data?.schema_version === 1 && Array.isArray(data.runs), 'Unsupported mapper-isolation catalog');
    const runIds = new Set();
    for (const run of data.runs) {
      need(typeof run.id === 'string' && run.id.length > 0 && !runIds.has(run.id), 'Invalid or duplicate comparison run');
      runIds.add(run.id);
      need(typeof run.version === 'string' && typeof run.cohort_id === 'string' && Array.isArray(run.samples), 'Invalid comparison run metadata');
      need(Number.isSafeInteger(run.sample_count) && run.sample_count === run.samples.length && Number.isSafeInteger(run.expected_count) && run.expected_count >= run.sample_count, 'Comparison sample count mismatch');
      need(Number.isFinite(run.input_transition_ps) && run.input_transition_ps >= 0 && Number.isFinite(run.output_load_ff) && run.output_load_ff >= 0, 'Invalid timing constraints');
      let verified = 0;
      const sampleIds = new Set();
      for (const sample of run.samples) {
        need(typeof sample.id === 'string' && sample.id.length > 0 && !sampleIds.has(sample.id) && typeof sample.label === 'string', 'Invalid or duplicate comparison input');
        sampleIds.add(sample.id);
        need(Array.isArray(sample.points) && sample.points.length === 6, 'Each comparison input must contain six mapping choices');
        const seen = new Set();
        for (const p of sample.points) {
          need(keys.includes(key(p)) && !seen.has(key(p)), 'Invalid or duplicate mapping choice');
          seen.add(key(p));
          need(Number.isFinite(p.area) && p.area >= 0 && Number.isFinite(p.delay_ps) && p.delay_ps >= 0 && Number.isSafeInteger(p.cell_count) && p.cell_count >= 0 && typeof p.verified === 'boolean', 'Invalid mapping measurement');
          if (crossMapped(p) && p.verified) ++verified;
        }
      }
      need(Number.isSafeInteger(run.verified_points) && run.verified_points === verified, 'Cross-mapping proof count mismatch');
    }
    return data;
  }
  function sameAigRows(samples, referenceGoal) {
    const rows = [], counts = {totalSamples: samples.length, zeroReferenceSamples: 0, zeroCandidatePoints: 0, unverifiedPoints: 0, plottedPoints: 0};
    for (const sample of samples) {
      const ref = point(sample, 'yosys-abc', 'abc', referenceGoal);
      if (!positive(ref)) { ++counts.zeroReferenceSamples; continue; }
      for (const p of sample.points.filter(p => p.source === 'yosys-abc')) {
        if (!usable(p)) { ++counts.unverifiedPoints; continue; }
        if (!positive(p)) { ++counts.zeroCandidatePoints; continue; }
        rows.push({sample, point: p, x: p.area / ref.area, y: p.delay_ps / ref.delay_ps});
      }
    }
    counts.plottedPoints = rows.length;
    return {rows, counts};
  }
  function inputAigRows(samples) {
    const rows = [], counts = {totalEndpoints: samples.length * 2, zeroReferencePoints: 0, zeroCandidatePoints: 0, unverifiedPoints: 0, plottedPoints: 0};
    for (const sample of samples) for (const goal of ['area', 'delay']) {
      const ref = point(sample, 'native-g8r', 'g8r', goal), p = point(sample, 'yosys-abc', 'g8r', goal);
      if (!usable(p)) { ++counts.unverifiedPoints; continue; }
      if (!positive(ref)) { ++counts.zeroReferencePoints; continue; }
      if (!positive(p)) { ++counts.zeroCandidatePoints; continue; }
      rows.push({sample, point: p, x: p.area / ref.area, y: p.delay_ps / ref.delay_ps});
    }
    counts.plottedPoints = rows.length;
    return {rows, counts};
  }
  const formatter = new Intl.NumberFormat(undefined, {maximumSignificantDigits: 5});
  const fmt = value => formatter.format(value);
  function layout(x, y, {narrow = false, logarithmic = false} = {}) {
    return {
      paper_bgcolor: 'transparent', plot_bgcolor: 'transparent', font: {color: '#a9b9c7', family: 'system-ui,sans-serif', size: 11},
      margin: {l: narrow ? 65 : 78, r: 20, t: narrow ? 136 : 91, b: 73},
      legend: {orientation: 'h', x: 0, y: 1.035, yanchor: 'bottom', itemsizing: 'constant', font: {size: 10}},
      xaxis: {title: {text: x, standoff: 13}, type: logarithmic ? 'log' : 'linear', gridcolor: '#2b3c4b', zerolinecolor: '#647d92', ...(logarithmic ? {} : {rangemode: 'tozero'})},
      yaxis: {title: {text: y, standoff: 15}, type: logarithmic ? 'log' : 'linear', gridcolor: '#2b3c4b', zerolinecolor: '#647d92', ...(logarithmic ? {} : {rangemode: 'tozero'})},
      hovermode: 'closest',
    };
  }
  function overviewSpec(samples, mode, referenceGoal = 'delay', options = {}) {
    const normalized = mode === 'same-aig' ? sameAigRows(samples, referenceGoal) : inputAigRows(samples);
    const series = mode === 'same-aig' ? kinds.slice(2) : kinds.slice(2, 4);
    const data = series.map(([source, mapper, goal]) => {
      const p = {source, mapper, goal}, selected = normalized.rows.filter(row => key(row.point) === key(p));
      return {
        type: 'scatter', mode: 'markers', name: mode === 'same-aig' ? label(p) : `g8r ${goal} · matched strategy`, legendgroup: key(p),
        x: selected.map(row => row.x), y: selected.map(row => row.y), customdata: selected.map(row => row.sample.id),
        text: selected.map(row => `${row.sample.label}<br>${label(p)}<br>Area ${fmt(row.point.area)} · delay ${fmt(row.point.delay_ps)} ps`),
        hovertemplate: '%{text}<br>Area ratio %{x:.4g}<br>Delay ratio %{y:.4g}<extra></extra>', marker: style(p),
      };
    });
    const result = layout(mode === 'same-aig' ? 'Mapped area / same-input ABC area' : 'Area: Yosys AIG / native AIG', mode === 'same-aig' ? 'Delay / same-input ABC delay' : 'Delay: Yosys AIG / native AIG', {...options, logarithmic: true});
    result.shapes = [{type: 'line', xref: 'x', yref: 'paper', x0: 1, x1: 1, y0: 0, y1: 1, line: {color: '#8297a9', dash: 'dot', width: 1}}, {type: 'line', xref: 'paper', yref: 'y', x0: 0, x1: 1, y0: 1, y1: 1, line: {color: '#8297a9', dash: 'dot', width: 1}}];
    if (!normalized.rows.length) result.annotations = [{xref: 'paper', yref: 'paper', x: .5, y: .5, text: 'No positive ratios in this run; see the numeric table', showarrow: false}];
    return {data, layout: result, counts: normalized.counts};
  }
  function absoluteSpec(sample, options = {}) {
    const points = sample?.points || [], best = frontier(points);
    const data = kinds.map(([source, mapper, goal]) => {
      const p = points.find(p => key(p) === key({source, mapper, goal})), selected = usable(p) ? [p] : [];
      return {type: 'scatter', mode: 'markers', name: label({source, mapper, goal}), legendgroup: key({source, mapper, goal}),
        x: selected.map(p => p.area), y: selected.map(p => p.delay_ps), customdata: selected.map(p => key(p)),
        marker: style({source, mapper, goal}, true), hovertemplate: 'Area %{x:.5g}<br>Delay %{y:.5g} ps<extra>%{fullData.name}</extra>'};
    });
    data.push({type: 'scatter', mode: 'markers', name: 'Nondominated measured endpoint', showlegend: false,
      x: best.map(p => p.area), y: best.map(p => p.delay_ps), marker: {symbol: 'circle-open', size: 27, color: '#f0f5fa', line: {width: 1.2}}, hoverinfo: 'skip'});
    return {data, layout: layout('Mapped cell area (library units)', 'Nominal arrival delay (ps)', options)};
  }
  function resolveState(data, requested = {}) {
    const run = data.runs.find(r => r.id === requested.run) || data.runs.at(-1);
    if (!run) return {run: '', sample: '', reference: requested.reference === 'area' ? 'area' : 'delay'};
    const sample = run.samples.find(s => s.id === requested.sample) || run.samples.find(s => s.points.every(positive)) || run.samples[0];
    return {run: run.id, sample: sample?.id || '', reference: requested.reference === 'area' ? 'area' : 'delay'};
  }
  function stateFromUrl(url) { const params = new URL(url).searchParams; return {run: params.get('run'), sample: params.get('sample'), reference: params.get('reference')}; }
  function stateUrl(url, state) {
    const result = new URL(url);
    for (const field of ['run', 'sample', 'reference']) if (state[field]) result.searchParams.set(field, state[field]); else result.searchParams.delete(field);
    return result.href;
  }
  if (typeof module !== 'undefined') module.exports = {kinds, key, point, usable, nearlyEqual, dominates, frontier, style, validateCatalog, sameAigRows, inputAigRows, overviewSpec, absoluteSpec, resolveState, stateFromUrl, stateUrl};
  if (typeof document === 'undefined') return;
  const el = id => document.getElementById(id), set = (id, text) => { el(id).textContent = text; };
  const option = (value, text) => { const element = document.createElement('option'); element.value = value; element.textContent = text; return element; };
  const config = {responsive: true, displayModeBar: false};
  let catalog, state, rendering = false, pending = false, renderedOverview = '', page = 0;
  const runForState = () => catalog.runs.find(run => run.id === state.run);
  function updateLocation(mode) {
    const url = stateUrl(location.href, state);
    if (url !== location.href) history[mode === 'push' ? 'pushState' : 'replaceState'](null, '', url);
  }
  function navigate(next, {historyMode = 'push', scroll = false} = {}) {
    const previousRun = state?.run;
    state = resolveState(catalog, {...state, ...next});
    if (previousRun !== state.run) page = 0;
    updateLocation(historyMode);
    void schedule();
    if (scroll) el('selection').scrollIntoView({behavior: 'smooth', block: 'start'});
  }
  function fillPoints(sample) {
    set('input-title', sample.label); set('input-id', sample.id);
    const best = frontier(sample.points), body = el('point-rows'); body.replaceChildren();
    for (const [source, mapper, goal] of kinds) {
      const p = point(sample, source, mapper, goal), row = document.createElement('tr'); row.dataset.pointKey = key(p);
      const evidence = crossMapped(p) ? (p.verified ? 'Equivalence proved' : 'Proof unavailable') : 'Baseline measurement';
      const isBest = best.includes(p);
      for (const text of [sourceLabel(source), mapper, goalLabel(p), fmt(p.area), fmt(p.delay_ps), fmt(p.cell_count), evidence, usable(p) ? (isBest ? 'Nondominated' : 'Dominated') : 'Not ranked']) {
        const cell = document.createElement('td'); cell.textContent = text; row.append(cell);
      }
      const cue = document.createElement('span'); cue.className = 'point-cue'; cue.style.color = style(p).color;
      cue.textContent = mapper === 'abc' ? (goal === 'area' ? '□' : '×') : goal === 'area' ? (source === 'native-g8r' ? '○' : '●') : (source === 'native-g8r' ? '△' : '▲');
      cue.setAttribute('aria-hidden', 'true'); row.firstChild.prepend(cue);
      row.lastChild.className = isBest ? 'frontier' : 'dominated'; row.lastChild.dataset.frontier = String(isBest); body.append(row);
    }
    const zeros = sample.points.filter(p => !positive(p)).length, unproved = sample.points.filter(p => crossMapped(p) && !p.verified).length;
    set('input-status', `${sample.points.length} measured endpoints; ${best.length} nondominated. ${zeros} endpoints have zero area or delay and remain visible here.${unproved ? ` ${unproved} cross-mapped endpoints lack proof and are excluded from plots and ranking.` : ' Both new cross-mapped endpoints have equivalence proofs.'}`);
  }
  function fillSamples() {
    const run = runForState(), term = el('search').value.trim().toLocaleLowerCase();
    const matches = run.samples.filter(sample => `${sample.label} ${sample.id}`.toLocaleLowerCase().includes(term));
    page = Math.max(0, Math.min(page, Math.max(0, Math.ceil(matches.length / 50) - 1)));
    const body = el('sample-rows'); body.replaceChildren();
    for (const sample of matches.slice(page * 50, (page + 1) * 50)) {
      const row = document.createElement('tr'); row.classList.toggle('selected', sample.id === state.sample); row.dataset.sampleId = sample.id;
      const name = document.createElement('td'), button = document.createElement('button'), id = document.createElement('span');
      button.type = 'button'; button.className = 'sample-link'; button.textContent = sample.label;
      button.addEventListener('click', () => navigate({sample: sample.id}, {scroll: true}));
      id.className = 'sample-id'; id.textContent = sample.id; name.append(button, id); row.append(name);
      for (const [source, mapper, goal] of kinds) { const p = point(sample, source, mapper, goal), cell = document.createElement('td'); cell.textContent = `${fmt(p.area)} / ${fmt(p.delay_ps)}`; row.append(cell); }
      body.append(row);
    }
    set('table-coverage', matches.length ? `${fmt(page * 50 + 1)}–${fmt(Math.min((page + 1) * 50, matches.length))} of ${fmt(matches.length)} matching inputs · ${fmt(run.samples.length)} measured` : `No matching inputs · ${fmt(run.samples.length)} measured`);
    el('previous-page').disabled = page === 0; el('next-page').disabled = (page + 1) * 50 >= matches.length;
  }
  function accounting(id, counts) {
    const element = el(id);
    for (const [name, value] of Object.entries(counts)) element.dataset[name] = String(value);
    if (id === 'same-aig-note') set(id, `${fmt(counts.plottedPoints)} endpoint ratios shown. ${fmt(counts.zeroReferenceSamples)} inputs have a zero ABC reference; ${fmt(counts.zeroCandidatePoints)} other endpoint ratios are zero; ${fmt(counts.unverifiedPoints)} lack proof. All numeric measurements remain below.`);
    else set(id, `${fmt(counts.plottedPoints)} of ${fmt(counts.totalEndpoints)} matched-goal ratios shown. ${fmt(counts.zeroReferencePoints)} have a zero native reference; ${fmt(counts.zeroCandidatePoints)} other ratios are zero; ${fmt(counts.unverifiedPoints)} lack proof. Zero-over-zero ratios are undefined.`);
  }
  async function render() {
    const current = {...state}, run = catalog.runs.find(run => run.id === current.run), sample = run.samples.find(sample => sample.id === current.sample);
    el('crossmap').dataset.rendered = 'loading'; set('error', '');
    el('run').value = current.run; el('reference').value = current.reference;
    el('input').replaceChildren(...run.samples.map(sample => option(sample.id, sample.label))); el('input').value = current.sample;
    const cohortLabel = run.cohort_id === 'whole-functions-v1' ? 'whole functions' : run.cohort_id === 'mffc-v1' ? 'MFFCs' : run.cohort_id;
    set('coverage-scope', `Measured coverage: v${run.version} · ${fmt(run.sample_count)} ${cohortLabel} (${fmt(run.expected_count)} expected). Only the measured runs listed below are available for mapper isolation.`);
    set('coverage', `${fmt(run.sample_count)} / ${fmt(run.expected_count)} inputs · ${fmt(run.verified_points)} / ${fmt(run.sample_count * 2)} new cross-mapped endpoints verified`);
    set('constraints', `Pinned ASAP7 RVT SS standard cells; g8r v${run.version}. All six endpoints use the common library evaluator with ${fmt(run.input_transition_ps)} ps input transition and ${fmt(run.output_load_ff)} fF output load. Delay is a nominal library estimate before placement or routing.`);
    el('crossmap').dataset.sampleCount = String(run.sample_count);
    const overviewKey = `${run.id}:${current.reference}:${el('same-aig').clientWidth}`;
    if (overviewKey !== renderedOverview) {
      for (const mode of ['same-aig', 'input-aig']) {
        const spec = overviewSpec(run.samples, mode, current.reference, {narrow: el(mode).clientWidth < 650});
        await Plotly.react(mode, spec.data, spec.layout, config); accounting(`${mode}-note`, spec.counts);
        el(mode).removeAllListeners?.('plotly_click');
        el(mode).on('plotly_click', event => { const id = event.points?.[0]?.customdata; if (runForState().samples.some(sample => sample.id === id)) navigate({sample: id}, {scroll: true}); });
      }
      renderedOverview = overviewKey;
    }
    if (sample) fillPoints(sample); else { set('input-title', 'No measured inputs'); set('input-id', ''); set('input-status', ''); el('point-rows').replaceChildren(); }
    const absolute = absoluteSpec(sample, {narrow: el('absolute').clientWidth < 650});
    await Plotly.react('absolute', absolute.data, absolute.layout, config); fillSamples();
    if (current.run === state.run && current.sample === state.sample && current.reference === state.reference) el('crossmap').dataset.rendered = 'true';
  }
  async function schedule() {
    pending = true; if (rendering) return; rendering = true;
    try { while (pending) { pending = false; await render(); } }
    catch (error) { set('error', error.message); el('crossmap').dataset.rendered = 'error'; }
    finally { rendering = false; }
  }
  async function main() {
    const response = await fetch('crossmap-results.json');
    if (response.status === 404) { el('empty').hidden = false; el('crossmap').dataset.rendered = 'empty'; return; }
    if (!response.ok) throw Error(`HTTP ${response.status} loading mapper-isolation results`);
    catalog = validateCatalog(await response.json());
    if (!catalog.runs.length) { el('empty').hidden = false; el('crossmap').dataset.rendered = 'empty'; return; }
    el('results').hidden = false;
    el('run').replaceChildren(...catalog.runs.map(run => option(run.id, `v${run.version} · ${run.cohort_id} · ${fmt(run.sample_count)} inputs`)));
    state = resolveState(catalog, stateFromUrl(location.href)); updateLocation('replace');
    el('run').addEventListener('change', () => navigate({run: el('run').value, sample: ''}));
    el('reference').addEventListener('change', () => navigate({reference: el('reference').value}));
    el('input').addEventListener('change', () => navigate({sample: el('input').value}));
    el('search').addEventListener('input', () => { page = 0; fillSamples(); });
    el('previous-page').addEventListener('click', () => { --page; fillSamples(); });
    el('next-page').addEventListener('click', () => { ++page; fillSamples(); });
    window.addEventListener('popstate', () => { state = resolveState(catalog, stateFromUrl(location.href)); page = 0; updateLocation('replace'); void schedule(); });
    let timer; window.addEventListener('resize', () => { clearTimeout(timer); timer = setTimeout(() => { renderedOverview = ''; void schedule(); }, 180); });
    await schedule();
  }
  void main().catch(error => { set('error', error.message); el('crossmap').dataset.rendered = 'error'; });
})();
