// SPDX-License-Identifier: Apache-2.0
'use strict';
const assert = require('node:assert/strict');
const {
  kinds, key, point, usable, nearlyEqual, dominates, frontier, style, validateCatalog,
  sameAigRows, inputAigRows, overviewSpec, absoluteSpec, resolveState, stateFromUrl, stateUrl, coverageScope,
} = require('../src/site_assets/crossmap.js');
const sample = (id = 'one') => ({id, label: `Function ${id}`, points: kinds.map(([source, mapper, goal], i) => ({
  source, mapper, goal, area: [10, 11, 8, 12, 9, 10][i], delay_ps: [10, 9, 12, 8, 13, 10][i], cell_count: i + 1,
  verified: source === 'yosys-abc' && mapper === 'g8r',
}))});
const run = (id = 'run-one', samples = [sample()]) => ({id, version: '0.79.0', cohort_id: 'whole-functions-v1',
  expected_count: samples.length, sample_count: samples.length, verified_points: samples.flatMap(s => s.points).filter(p => p.source === 'yosys-abc' && p.mapper === 'g8r' && p.verified).length,
  input_transition_ps: 10, output_load_ff: 1, samples});
const catalog = {schema_version: 1, runs: [run()]};
assert.equal(validateCatalog(catalog), catalog);
assert.equal(validateCatalog({schema_version: 1, runs: []}).runs.length, 0);
assert.equal(usable(point(catalog.runs[0].samples[0], 'native-g8r', 'g8r', 'area')), true);
assert.equal(usable(point(catalog.runs[0].samples[0], 'yosys-abc', 'abc', 'delay')), true);
const first = sample();
assert.deepEqual(sameAigRows([first], 'delay').rows.map(r => [key(r.point), r.x, r.y]), [
  ['yosys-abc:g8r:area', .8, 1.2], ['yosys-abc:g8r:delay', 1.2, .8], ['yosys-abc:abc:area', .9, 1.3], ['yosys-abc:abc:delay', 1, 1],
]);
assert.deepEqual(inputAigRows([first]).rows.map(r => [r.x, r.y]), [[.8, 1.2], [12 / 11, 8 / 9]]);
assert.deepEqual(frontier(first.points).map(key), kinds.map(([source, mapper, goal]) => key({source, mapper, goal})).filter(k => k !== 'yosys-abc:abc:area'));
assert.equal(dominates(first.points[2], first.points[4]), true);
assert.equal(dominates(first.points[0], first.points[5]), false);
assert.equal(nearlyEqual(10, 10 + 5e-9), true);
assert.equal(dominates({area: 10, delay_ps: 10}, {area: 10 + 5e-9, delay_ps: 10}), false);
assert.equal(dominates({area: 10, delay_ps: 10}, {area: 10 + 5e-7, delay_ps: 10}), true);
assert.equal(dominates({area: 0, delay_ps: 0}, {area: 5e-10, delay_ps: 5e-10}), false);
const zero = sample('zero');
for (const p of zero.points) { p.area = 0; p.delay_ps = 0; }
point(zero, 'yosys-abc', 'abc', 'area').area = 3;
point(zero, 'yosys-abc', 'abc', 'area').delay_ps = 2;
assert.deepEqual(sameAigRows([first, zero], 'delay').counts, {totalSamples: 2, zeroReferenceSamples: 1, zeroCandidatePoints: 0, unverifiedPoints: 0, plottedPoints: 4});
assert.deepEqual(sameAigRows([zero], 'area').counts, {totalSamples: 1, zeroReferenceSamples: 0, zeroCandidatePoints: 3, unverifiedPoints: 0, plottedPoints: 1});
assert.deepEqual(inputAigRows([first, zero]).counts, {totalEndpoints: 4, zeroReferencePoints: 2, zeroCandidatePoints: 0, unverifiedPoints: 0, plottedPoints: 2});
const unproved = sample('unproved');
point(unproved, 'yosys-abc', 'g8r', 'delay').verified = false;
assert.equal(frontier(unproved.points).some(p => key(p) === 'yosys-abc:g8r:delay'), false);
assert.deepEqual(sameAigRows([unproved], 'delay').counts, {totalSamples: 1, zeroReferenceSamples: 0, zeroCandidatePoints: 0, unverifiedPoints: 1, plottedPoints: 3});
assert.deepEqual(inputAigRows([unproved]).counts, {totalEndpoints: 2, zeroReferencePoints: 0, zeroCandidatePoints: 0, unverifiedPoints: 1, plottedPoints: 1});
for (const samples of [[first], [zero], [first, zero, unproved]]) {
  for (const goal of ['area', 'delay']) {
    const c = sameAigRows(samples, goal).counts;
    assert.equal(c.plottedPoints + c.zeroCandidatePoints + c.unverifiedPoints + c.zeroReferenceSamples * 4, samples.length * 4);
  }
  const c = inputAigRows(samples).counts;
  assert.equal(c.plottedPoints + c.zeroCandidatePoints + c.zeroReferencePoints + c.unverifiedPoints, samples.length * 2);
}
const sameSpec = overviewSpec([first], 'same-aig', 'delay');
assert.deepEqual(sameSpec.data.map(t => t.x), [[.8], [1.2], [.9], [1]]);
assert.deepEqual(sameSpec.data.map(t => t.customdata), [['one'], ['one'], ['one'], ['one']]);
assert.deepEqual(sameSpec.data.map(t => [t.marker.color, t.marker.symbol]), [['#48d8f0', 'circle'], ['#bc91ff', 'triangle-up'], ['#f5bd4f', 'square-open'], ['#ff858d', 'x']]);
assert.equal(sameSpec.layout.xaxis.type, 'log');
assert.equal(sameSpec.layout.yaxis.type, 'log');
assert.deepEqual(sameSpec.layout.shapes.map(s => [s.xref, s.yref, s.x0, s.x1, s.y0, s.y1]), [['x', 'paper', 1, 1, 0, 1], ['paper', 'y', 0, 1, 1, 1]]);
const inputSpec = overviewSpec([first], 'input-aig');
assert.deepEqual(inputSpec.data.map(t => t.x), [[.8], [12 / 11]]);
const absolute = absoluteSpec(first);
assert.deepEqual(absolute.data.slice(0, 6).map(t => t.x), [[10], [11], [8], [12], [9], [10]]);
assert.deepEqual(absolute.data.slice(0, 6).map(t => t.y), [[10], [9], [12], [8], [13], [10]]);
assert.equal(absolute.data[6].showlegend, false);
assert.deepEqual(absolute.data[6].x, frontier(first.points).map(p => p.area));
assert.equal(absolute.layout.xaxis.type, 'linear');
assert.equal(absolute.layout.shapes, undefined);
assert.equal(style(first.points[0]).symbol, 'circle-open');
assert.equal(style(first.points[1]).symbol, 'triangle-up-open');
assert.equal(new Set(first.points.map(p => style(p).symbol)).size, 6);
const withZero = {schema_version: 1, runs: [run('old'), run('new', [zero, first])]};
assert.deepEqual(resolveState(withZero, {}), {run: 'new', sample: 'one', reference: 'delay'});
assert.deepEqual(resolveState(withZero, {run: 'old', sample: 'missing', reference: 'area'}), {run: 'old', sample: 'one', reference: 'area'});
assert.deepEqual(resolveState(withZero, {run: 'new', sample: 'zero', reference: 'bad'}), {run: 'new', sample: 'zero', reference: 'delay'});
assert.deepEqual(resolveState({runs: []}, {}), {run: '', sample: '', reference: 'delay'});
const url = 'https://example.invalid/tech-mapped-asap7/mapper-isolation.html?keep=value#selection';
const restored = {run: 'new', sample: 'zero', reference: 'area'};
assert.deepEqual(stateFromUrl(stateUrl(url, restored)), restored);
const parsed = new URL(stateUrl(url, restored));
assert.equal(parsed.searchParams.get('keep'), 'value');
assert.equal(parsed.hash, '#selection');
for (const mutate of [
  c => { c.schema_version = 2; },
  c => { c.runs.push(structuredClone(c.runs[0])); },
  c => { c.runs[0].sample_count = 2; },
  c => { c.runs[0].expected_count = 0; },
  c => { c.runs[0].verified_points = 6; },
  c => { c.runs[0].samples[0].points.pop(); },
  c => { c.runs[0].samples[0].points[0].source = 'yosys-abc'; },
  c => { c.runs[0].samples[0].points[0].area = Infinity; },
  c => { c.runs[0].samples[0].points[0].delay_ps = -1; },
  c => { c.runs[0].samples[0].points[0].cell_count = .5; },
  c => { c.runs[0].samples[0].points[0].verified = 1; },
]) {
  const invalid = structuredClone(catalog); mutate(invalid); assert.throws(() => validateCatalog(invalid));
}
assert.equal(coverageScope(null), 'No mapper-isolation runs have been published yet.');
assert.equal(coverageScope(run()), 'Measured coverage: v0.79.0 · 1 whole functions (1 expected). Only the measured runs listed below are available for mapper isolation.');
async function checkEmptyDom(response) {
  const elements = {
    'coverage-scope': {textContent: 'Loading measured coverage…'},
    results: {hidden: true}, empty: {hidden: true}, crossmap: {dataset: {rendered: 'loading'}}, error: {textContent: ''},
  };
  const source = require('node:fs').readFileSync(require.resolve('../src/site_assets/crossmap.js'), 'utf8');
  require('node:vm').runInNewContext(source, {document: {getElementById: id => elements[id]}, fetch: async () => response, Intl, URL});
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(elements, {
    'coverage-scope': {textContent: 'No mapper-isolation runs have been published yet.'},
    results: {hidden: true}, empty: {hidden: false}, crossmap: {dataset: {rendered: 'empty'}}, error: {textContent: ''},
  });
}
(async () => {
  await checkEmptyDom({status: 404, ok: false});
  await checkEmptyDom({status: 200, ok: true, json: async () => ({schema_version: 1, runs: []})});
  console.log('Crossmap measurement, coverage, frontier, plot, validation, URL and empty DOM semantics passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
