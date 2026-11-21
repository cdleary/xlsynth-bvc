// SPDX-License-Identifier: Apache-2.0
// Execute the real navigation and shard loader; stub only dataset loading and rendering.
'use strict';
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const {createHash, webcrypto} = require('node:crypto');
const {TextDecoder} = require('node:util');
const BvcCostStats=require(require('node:path').resolve('src/site_assets/aggregation.js'));
const source = fs.readFileSync(0, 'utf8');
const copy = value => JSON.parse(JSON.stringify(value));
const hash = n => n.toString(16).padStart(64, '0');

function browser(search = '', fetchShard = () => new Promise(() => {}), options = {}) {
  const elements = new Map(), renders = [], datasetLoads = [];
  const element = id => {
    if (id === 'progression' && options.noProgression) return null;
    if (!elements.has(id)) elements.set(id, {
      value: '', checked: false, innerHTML: '', textContent: '', dataset: {}, toolbar: {}, listeners: new Map(),
      addEventListener(type, listener) { this.listeners.set(type, listener); }, replaceChildren() {}, querySelector() { return this.toolbar; },
    });
    return elements.get(id);
  };
  if (!options.noProgression) element('progression').dataset.datasetKey = 'comparison';
  const location = {href: 'https://example.invalid/history/progression.html' + search, search};
  const context = vm.createContext({
    document: {querySelector: () => ({content: './'}), getElementById: element},
    // Keep automatic main() pending; invoke the actual progression handler directly.
    fetch: url => url === './catalog.json' ? new Promise(() => {}) : fetchShard(url),
    crypto: options.noCrypto ? undefined : webcrypto, TextDecoder, URL, URLSearchParams, location, BvcCostStats,
    history: {replaceState(_state, _title, url) { location.href = String(url); }},
    captureRender: (...args) => renders.push(args),
    captureDataset: (...args) => datasetLoads.push(args),
    datasetSamples: options.datasetSamples || [],
  });
  vm.runInContext(source, context);
  vm.runInContext(`
    renderProgressionPair = (...args) => captureRender(...args);
    renderProgressionVectors = () => {};
    progressionInventory = () => '';
    loadSiteDataset = async (...args) => {
      captureDataset(...args);
      return {dataset: {samples: datasetSamples}};
    };
  `, context);
  return {context, element, location, renders, datasetLoads};
}

function legacyCatalog() {
  const hashes = ['a'.repeat(64), 'b'.repeat(64)];
  const generation = (id, version, complete) => ({
    generation_id: id, origin: {kind: 'crate_release'}, display_label: 'v' + version,
    crate_version: version, dso_version: '1.0.0',
    coverage: complete ? 'cohort_complete' : 'partial',
    observed_ir_count: complete ? 2 : 1, cohort_ir_count: 2,
    missing_cohort_ir_count: complete ? 0 : 1, extra_ir_count: 0,
    run_samples: hashes.slice(0, complete ? 2 : 1).map(structural_hash => ({structural_hash})),
  });
  return {
    datasets: [{logical_key: 'comparison'}],
    progression: {default_cohort_id: 'whole-functions-v1', cohorts: [{
      cohort_id: 'whole-functions-v1', dataset_key: 'comparison', display_label: 'Whole functions',
      cohort_ir_count: 2, cohort_ir_hashes: hashes,
      generations: [generation('partial', '0.65.0', false), generation('first', '0.66.0', true), generation('last', '0.67.0', true)],
    }]},
  };
}

function selection(page) {
  const selected = {baseline: page.element('baseline-version').value, current: page.element('current-version').value};
  const query = new URL(page.location.href).searchParams;
  assert.equal(query.get('aggregation'),page.element('aggregation').value);
  assert.equal(query.get('baseline'), selected.baseline);
  assert.equal(query.get('current'), selected.current);
  assert.equal(query.get('include_incomplete') === 'true', page.element('include-incomplete').checked);
  return selected;
}

async function navigate(search) {
  const page = browser(search);
  await page.context.progression(legacyCatalog());
  return selection(page);
}

const sample = (n, cost) => ({
  structural_hash: hash(n), fn_key: `function-${n}-λ`, source_sha256: hash(10 + n), ir_node_count: 7,
  g8r_nodes: cost, g8r_levels: 1, g8r_product: cost,
  yosys_abc_nodes: 100, yosys_abc_levels: 1, yosys_abc_product: 100, g8r_product_loss: cost - 100,
  g8r_abc_graph_logical_effort: 2.5, yosys_abc_graph_logical_effort: 3,
  g8r_raw_stats: {and_nodes: cost + 3, depth: 2, graph_logical_effort: 4.5},
  graph_le_estimator: {runtime_sha256: hash(30), stats_driver_version: 'test'},
  ir_action_id: hash(40 + n), g8r_stats_action_id: hash(50 + n), yosys_abc_stats_action_id: hash(60 + n),
});
const reference = bytes => ({
  bytes: bytes.byteLength, sha256: createHash('sha256').update(bytes).digest('hex'),
});
const response = bytes => ({
  ok: true, status: 200, arrayBuffer: async () => Uint8Array.from(bytes).buffer,
});

function shardedFixture(cohortId = 'whole-functions-v1', generationOffset = 0) {
  const catalog = legacyCatalog(), cohort = catalog.progression.cohorts[0], files = new Map(), expected = new Map();
  cohort.cohort_id = cohortId;
  cohort.cohort_ir_hashes = [hash(1), hash(2)];
  const rows = [[sample(1, 5)], [sample(2, 100), sample(1, 10)], [sample(1, 20), sample(2, 80)]];
  cohort.generations.forEach((generation, i) => {
    generation.generation_id = hash(100 + generationOffset + i);
    const samples = rows[i], url = `data/progression-samples/${generation.generation_id}.json`;
    const bytes = Buffer.from(JSON.stringify({schema_version: 1, cohort_id: cohort.cohort_id, generation_id: generation.generation_id, samples}));
    generation.run_samples_shard = {url, ...reference(bytes), sample_count: samples.length};
    delete generation.run_samples;
    files.set('./' + url, bytes);
    expected.set(generation.generation_id, copy(samples));
  });
  // Metadata and rows must retain their original order while the consumer sorts generations.
  cohort.generations.reverse();
  catalog.schema_version = 10;
  return {catalog, files, expected};
}

function editPayload(fixture, edit) {
  const cohort = fixture.catalog.progression.cohorts[0], generation = cohort.generations[0];
  const key = './' + generation.run_samples_shard.url, payload = JSON.parse(fixture.files.get(key));
  edit(payload);
  const bytes = Buffer.from(JSON.stringify(payload));
  fixture.files.set(key, bytes);
  Object.assign(generation.run_samples_shard, reference(bytes));
}

async function shardContract() {
  const fixture = shardedFixture(), original = copy(fixture.catalog), requested = [];
  const page = browser('', async url => { requested.push(url); return response(fixture.files.get(url)); });
  const hydrated = await page.context.loadProgressionSamples(fixture.catalog);
  assert.deepEqual(copy(fixture.catalog), original); // Hydration is atomic and leaves the fetched catalog immutable.
  assert.deepEqual(hydrated.cohorts[0].generations.map(g => g.generation_id), original.progression.cohorts[0].generations.map(g => g.generation_id));
  for (const generation of hydrated.cohorts[0].generations) {
    assert.deepEqual(copy(generation.run_samples), fixture.expected.get(generation.generation_id));
  }
  assert.deepEqual(requested.sort(), [...fixture.files.keys()].sort());
  const generations = page.context.releaseGenerations(hydrated.cohorts[0], []);
  assert.deepEqual(generations.map(g => g.generation_id), [hash(100), hash(101), hash(102)]);
  const totals = page.context.releaseStats(generations);
  assert.deepEqual(totals.map(g => g.g8r_total), [5, 110, 100]);
  const changes = page.context.compareSamples(generations[1].samples, generations[2].samples);
  assert.equal(changes.pairs.reduce((total, row) => total + row.delta, 0), -10);
  assert.deepEqual(copy(generations[1].samples), fixture.expected.get(hash(101)));

  // Every cohort is hydrated before the first render, regardless of fetch completion order.
  const secondary = shardedFixture('mffc-v1', 1000);
  fixture.catalog.progression.cohorts.push(secondary.catalog.progression.cohorts[0]);
  for (const [url, bytes] of secondary.files) fixture.files.set(url, bytes);
  const pending = new Map();
  const delayed = browser('', url => new Promise(resolve => pending.set(url, resolve)));
  const navigating = delayed.context.progression(fixture.catalog);
  assert.equal(pending.size, 6);
  assert.equal(delayed.renders.length, 0);
  assert.equal(delayed.datasetLoads.length, 0);
  for (const [url, resolve] of [...pending].reverse()) resolve(response(fixture.files.get(url)));
  await navigating;
  assert.deepEqual(selection(delayed), {baseline: hash(101), current: hash(102)});
  assert.equal(delayed.renders.length, 1);
  assert.deepEqual(copy(delayed.renders[0][0][1].samples), fixture.expected.get(hash(101)));
  delayed.element('progression-cohort').value = 'mffc-v1';
  delayed.element('progression-cohort').listeners.get('change')();
  assert.deepEqual(selection(delayed), {baseline: hash(1101), current: hash(1102)});
  assert.equal(delayed.element('progression').dataset.progressionCohort, 'mffc-v1');
  assert.deepEqual(copy(delayed.renders[1][0][1].samples), secondary.expected.get(hash(1101)));
  assert.equal(pending.size, 6); // Switching cohorts uses the already verified rows.

  // Pages without progression do not request its sample history.
  const other = browser('', () => { throw Error('Unexpected progression shard request'); }, {noProgression: true});
  await other.context.progression(fixture.catalog);

  // An explicitly verified empty generation must not borrow rows from the comparison fallback.
  const empty = shardedFixture();
  editPayload(empty, payload => { payload.samples = []; });
  const emptyGeneration = empty.catalog.progression.cohorts[0].generations[0];
  emptyGeneration.run_samples_shard.sample_count = emptyGeneration.observed_ir_count = 0;
  const emptyPage = browser('', async url => response(empty.files.get(url)));
  const emptyIndex = await emptyPage.context.loadProgressionSamples(empty.catalog);
  const fallbackRow = {...sample(1, 99), crate_version: emptyGeneration.crate_version, dso_version: emptyGeneration.dso_version};
  const emptyResult = emptyPage.context.releaseGenerations(emptyIndex.cohorts[0], [fallbackRow]);
  assert.equal(emptyResult.find(g => g.generation_id === emptyGeneration.generation_id).samples.length, 0);

  // Generations without explicit persisted samples retain the ordinary comparison fallback.
  const fallback = legacyCatalog(), fallbackRows = [];
  for (const generation of fallback.progression.cohorts[0].generations) {
    for (const row of generation.run_samples) fallbackRows.push({...row, crate_version: generation.crate_version, dso_version: generation.dso_version});
    delete generation.run_samples;
  }
  const legacy = browser('', () => { throw Error('Unexpected legacy shard request'); }, {datasetSamples: fallbackRows});
  await legacy.context.progression(fallback);
  assert.deepEqual(selection(legacy), {baseline: 'first', current: 'last'});
}

async function shardRejections() {
  const metadataCases = [
    f => f.catalog.progression.cohorts[0].generations.push(copy(f.catalog.progression.cohorts[0].generations[0])),
    f => f.catalog.progression.cohorts.push(copy(f.catalog.progression.cohorts[0])),
    f => { f.catalog.progression.cohorts[0].generations[0].generation_id = ''; },
    f => { f.catalog.progression.cohorts[0].generations[0].run_samples = [sample(1, 1)]; },
    f => { f.catalog.progression.cohorts[0].generations[0].run_samples = {}; },
    f => { f.catalog.progression.cohorts[0].cohort_ir_hashes[1] = hash(1); },
    ...['../samples.json', '/data/samples.json', 'https://example.invalid/samples.json', 'data/progression-samples/a.json?x=1'].map(url => f => { f.catalog.progression.cohorts[0].generations[0].run_samples_shard.url = url; }),
    f => { const g = f.catalog.progression.cohorts[0].generations; g[0].run_samples_shard.url = g[1].run_samples_shard.url; },
    ...[{bytes: 0}, {bytes: '1'}, {sha256: 'invalid'}, {sample_count: -1}, {sample_count: '2'}, {sample_count: 3}].map(fields => f => Object.assign(f.catalog.progression.cohorts[0].generations[0].run_samples_shard, fields)),
  ];
  for (const mutate of metadataCases) {
    const fixture = shardedFixture();
    mutate(fixture);
    const before = copy(fixture.catalog), requests = [];
    const page = browser('', url => { requests.push(url); return response(fixture.files.get(url)); });
    await assert.rejects(page.context.loadProgressionSamples(fixture.catalog));
    assert.deepEqual(copy(fixture.catalog), before);
    assert.deepEqual(requests, []); // Reject bad references before any fetch.
  }
  const payloadCases = [
    p => { p.schema_version = 2; }, p => { p.cohort_id = 'other'; }, p => { p.generation_id = hash(999); },
    p => { delete p.samples; }, p => { p.samples.pop(); }, p => { p.samples[1] = copy(p.samples[0]); },
    p => { p.samples[0].structural_hash = hash(999); }, p => { p.samples[0].structural_hash = 'A'.repeat(64); },
    p => { p.samples[0].structural_hash = ' ' + hash(1); }, p => { p.samples[0].structural_hash = 1; },
    p => { p.samples[0] = null; },
  ];
  for (const mutate of payloadCases) {
    const fixture = shardedFixture();
    editPayload(fixture, mutate);
    const before = copy(fixture.catalog), page = browser('', async url => response(fixture.files.get(url)));
    await assert.rejects(page.context.loadProgressionSamples(fixture.catalog));
    assert.deepEqual(copy(fixture.catalog), before);
  }
  for (const failure of ['http', 'bytes', 'digest', 'json', 'utf8', 'crypto']) {
    const fixture = shardedFixture(), generation = fixture.catalog.progression.cohorts[0].generations[0];
    const key = './' + generation.run_samples_shard.url;
    if (failure === 'bytes') generation.run_samples_shard.bytes++;
    if (failure === 'digest') generation.run_samples_shard.sha256 = 'f'.repeat(64);
    if (failure === 'json' || failure === 'utf8') {
      const bytes = failure === 'json' ? Buffer.from('{') : Buffer.from([0xff]);
      fixture.files.set(key, bytes);
      Object.assign(generation.run_samples_shard, reference(bytes));
    }
    const page = browser('', async url => failure === 'http' && url === key ? {ok: false, status: 404} : response(fixture.files.get(url)), {noCrypto: failure === 'crypto'});
    await assert.rejects(page.context.loadProgressionSamples(fixture.catalog));
  }
  // A failed load never reaches the comparison dataset or renders misleading zero-valued history.
  const fixture = shardedFixture(), failed = browser('', async () => ({ok: false, status: 404}));
  await failed.context.progression(fixture.catalog);
  assert.equal(failed.element('progression').dataset.progressionRendered, 'unavailable');
  assert.equal(failed.element('progression').toolbar.hidden, true);
  assert.equal(failed.renders.length, 0);
  assert.equal(failed.datasetLoads.length, 0);
}

(async () => {
  assert.deepEqual(await navigate('?cohort=whole-functions-v1'), {baseline: 'first', current: 'last'});
  assert.deepEqual(await navigate('?cohort=whole-functions-v1&current=partial&include_incomplete=true'), {baseline: 'last', current: 'partial'});
  assert.deepEqual(await navigate('?cohort=whole-functions-v1&current=first&baseline=first'), {baseline: 'first', current: 'first'});
  assert.deepEqual(await navigate('?cohort=whole-functions-v1&current=last&baseline=first'), {baseline: 'first', current: 'last'});
  const aggregatePage=browser('?aggregation=total');
  await aggregatePage.context.progression(legacyCatalog());
  assert.equal(aggregatePage.element('aggregation').value,'total');
  aggregatePage.element('aggregation').value='geomean';
  aggregatePage.element('aggregation').listeners.get('change')();
  assert.equal(new URL(aggregatePage.location.href).searchParams.get('aggregation'),'geomean');
  await shardContract();
  await shardRejections();
})().catch(error => { console.error(error); process.exitCode = 1; });
