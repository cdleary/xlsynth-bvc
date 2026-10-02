// SPDX-License-Identifier: Apache-2.0
// Semantic tests for pairing and Plotly coordinates; stdin is the site app.
const assert = require('node:assert/strict');
const fs = require('node:fs');
global.document = {querySelector: () => ({content: ''}), getElementById: () => null};
const app = fs.readFileSync(0, 'utf8').split('async function main()')[0];
const api = new Function(app + '\nreturn {progressionVectors, vectorPlotSpec};')();
const hash = n => n.toString(16).padStart(64, '0');
const sample = (n, le, nodes, runtime = 'same') => ({
  structural_hash: hash(n), fn_key: `function-${n}`, g8r_graph_logical_effort: le,
  g8r_nodes: nodes, graph_le_estimator: {runtime_sha256: runtime, stats_driver_version: 'test'},
});
const old = [sample(1, 10, 100), sample(2, 10, 100), sample(3, 10, 100),
  sample(4, 10, 100), sample(5, null, 10), sample(6, 0, 0), sample(7, 10, 100), sample(9, 1, 1)];
const now = [sample(7, 10, 90), sample(6, 0, 0), sample(5, 1, 10), sample(4, 10 + 1e-10, 100),
  sample(3, 9, 110), sample(2, 11, 110, 'other'), sample(1, 9, 90), sample(8, 1, 1)];
now[6].fn_key = 'renamed-function';
const summary = api.progressionVectors({samples: old}, {samples: now});
assert.deepEqual(summary.counts, {improved: 2, regressed: 1, tradeoff: 1, unchanged: 2});
assert.equal(summary.paired, 7);
assert.equal(summary.missing, 1);
assert.equal(summary.added, 1);
assert.equal(summary.removed, 1);
assert.equal(summary.differentEstimators, 1);
assert.equal(summary.rows.find(row => row.key === hash(1)).label, 'renamed-function');
assert.throws(() => api.progressionVectors({samples: [old[0], old[0]]}, {samples: now}));
for (const bad of [null, undefined, NaN, Infinity, -1, '10']) {
  assert.equal(api.progressionVectors({samples: [sample(1, bad, 10)]}, {samples: [sample(1, 1, 10)]}).missing, 1);
}
const reverse = api.progressionVectors({samples: now}, {samples: old});
assert.deepEqual(reverse.counts, {improved: 1, regressed: 2, tradeoff: 1, unchanged: 2});
const zeroToPositive = api.progressionVectors({samples: [sample(1, 0, 0)]}, {samples: [sample(1, 5, 10)]});
assert.equal(zeroToPositive.rows[0].kind, 'regressed');
assert.equal(zeroToPositive.rows[0].magnitude, Infinity);

for (const scale of ['linear', 'log']) {
  const transform = x => scale === 'linear' ? x : Math.log10(1 + x);
  const spec = api.vectorPlotSpec(summary.rows, summary.rows, scale, 'versions');
  const improved = spec.traces.find(trace => trace.name === 'Improved');
  const index = improved.customdata.indexOf(hash(1));
  assert.deepEqual(improved.x.slice(index, index + 3), [transform(10), transform(9), null]);
  assert.deepEqual(improved.y.slice(index, index + 3), [transform(100), transform(90), null]);
  assert.deepEqual(improved.marker.symbol.slice(index, index + 3), ['circle', 'arrow', 'circle']);
  assert.deepEqual(improved.marker.size.slice(index, index + 3), [0, 10, 0]);
  assert.equal(improved.marker.angleref, 'previous');
  const tails = spec.traces.find(trace => trace.name === 'Improved baseline');
  const tailIndex = tails.customdata.indexOf(hash(1));
  assert.equal(tails.mode, 'markers');
  assert.equal(tails.x[tailIndex], transform(10));
  assert.equal(tails.y[tailIndex], transform(100));
  assert.deepEqual(tails.marker, {color: improved.marker.color, symbol: 'circle-open', size: 5, angleref: 'up'});
  const unchanged = spec.traces.find(trace => trace.name === 'Unchanged');
  assert.equal(unchanged.marker.angleref, 'up');
  const zeroIndex = unchanged.customdata.indexOf(hash(6));
  assert.equal(unchanged.x[zeroIndex], 0);
  assert.equal(unchanged.y[zeroIndex], 0);
  const singleton = api.vectorPlotSpec(summary.rows.filter(row => row.key === hash(6)), summary.rows, scale, 'versions');
  assert.equal(singleton.traces.length, 1);
  assert.deepEqual(singleton.traces[0].customdata, [hash(6)]);
  assert.equal(singleton.traces[0].marker.angleref, 'up');
  const filtered = api.vectorPlotSpec(summary.rows.filter(row => row.kind === 'improved'), summary.rows, scale, 'versions');
  assert.deepEqual(filtered.layout.xaxis, spec.layout.xaxis);
  assert.deepEqual(filtered.layout.yaxis, spec.layout.yaxis);
  const empty = api.vectorPlotSpec([], [], scale, 'empty');
  assert.deepEqual(empty.traces, []);
  assert.deepEqual(empty.layout.xaxis.range, [0, 1]);
  assert.equal(empty.layout.annotations.length, 1);
}
