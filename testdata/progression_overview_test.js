// SPDX-License-Identifier: Apache-2.0
// Semantic contract: exact cohort pairing, weighting, classification, and plot data.
const assert = require('node:assert/strict');
const fs = require('node:fs');
global.document = {querySelector: () => ({content: ''}), getElementById: () => null};
const app = fs.readFileSync(0, 'utf8').split('async function main()')[0];
const api = new Function(app + '\nreturn {costComparison,costOverview,costTrendSpec,costContributionSpec,costHeadline};')();
const hash = n => n.toString(16).padStart(64, '0');
const sample = (n, cost, reference = 100) => ({structural_hash: hash(n), fn_key: `function-${n}`, g8r_product: cost, yosys_abc_product: reference});
const generation = (id, samples, coverage = 'cohort_complete') => ({generation_id: id, display_label: id, samples, coverage});
const before = generation('before', [sample(1, 100), sample(2, 10), sample(3, 0), sample(4, 2)]);
const after = generation('after', [sample(4, 2), sample(3, 0, 0), sample(2, 20, 40), sample(1, 80, 160)]);
const pair = api.costComparison(before, after);
assert.deepEqual(pair.counts, {lower: 1, equal: 2, higher: 1});
assert.deepEqual([pair.old, pair.now, pair.delta], [112, 102, -10]);
assert.equal(pair.index, 100 * 102 / 112); // Ratio of totals, not average per-function ratios.
assert.equal(pair.percent, -1000 / 112);
assert.equal(pair.rows.find(row => row.key === hash(3)).percent, null);
assert.equal(pair.rows.find(row => row.key === hash(2)).percent, 100);
assert.equal(pair.complete, true);
assert.equal(api.costHeadline(pair), '8.93% lower total cost');
const reference = api.costComparison(before, after, 'yosys');
assert.deepEqual(reference.counts, {lower: 3, equal: 1, higher: 0});
assert.equal(reference.old, 300);
assert.equal(reference.index, 34);
// The reference mode does not subtract a changing backend gap from the release delta.
assert.equal(reference.delta, -198);
assert.equal(api.costComparison(after, before).delta, 10);
const git = {...after, generation_id: 'git', origin: {kind: 'git_revision'}, baseline_generation_id: 'before'};
assert.equal(api.costComparison(before, git).delta, pair.delta);
const partial = generation('partial', [sample(1, 80), sample(5, 10)], 'partial');
const partialPair = api.costComparison(before, partial);
assert.deepEqual([partialPair.added, partialPair.removed, partialPair.rows.length], [1, 3, 1]);
assert.deepEqual([partialPair.old, partialPair.now], [100, 80]);
assert.equal(partialPair.complete, false);
const model = api.costOverview([before, after, partial, git], 'before', 'after');
assert.deepEqual(model.history.map(row => row.generation.generation_id), ['before', 'after', 'git']);
assert.equal(model.selected.index, pair.index);
const trend = api.costTrendSpec(model);
assert.deepEqual(trend.traces[0].x, [0, 1, 2]); // Equal spacing, independent of timestamps.
assert.deepEqual(trend.traces[0].y, [100, pair.index, pair.index]);
assert.deepEqual(trend.traces[0].customdata, ['before', 'after', 'git']);
assert.deepEqual(trend.traces[0].marker.symbol, ['circle', 'circle', 'diamond-open']);
assert.deepEqual(trend.traces[0].marker.size, [7, 12, 7]);
assert.equal(trend.layout.shapes[0].y0, 100);
assert(trend.layout.yaxis.range[0] < pair.index && trend.layout.yaxis.range[1] > 100);
const changedSelection = api.costOverview([before, after], 'after', 'before');
assert.equal(changedSelection.selected.index, 100 * 112 / 102);
assert.equal(api.costOverview([before, after, partial], 'partial', 'after').history.length, 0);
assert.equal(api.costOverview([before, after, partial], 'partial', 'after', 'yosys').history.length, 2);
const mismatched = generation('mismatched', [sample(6, 100)]);
assert.equal(api.costOverview([before, mismatched], 'before', 'mismatched').history.length, 1);
assert.throws(() => api.costOverview([before], 'before', 'missing'));
assert.throws(() => api.costComparison(before, after, 'unknown'));
assert.throws(() => api.costComparison(before, generation('duplicate', [sample(1, 10), sample(1, 20)])));
for (const bad of [null, NaN, Infinity, -1, '20']) {
  assert.throws(() => api.costComparison(before, generation('bad', [sample(1, bad)])));
}
const absolute = api.costContributionSpec(pair);
assert.equal(absolute.omitted, 0);
assert.deepEqual(absolute.traces.map(trace => trace.x), [[-20], [0, 0], [10]]);
assert.deepEqual(absolute.traces.map(trace => trace.y), [[1], [2, 3], [4]]);
assert.equal(absolute.traces.flatMap(trace => trace.customdata).length, 4);
assert.equal(absolute.layout.xaxis.range[0], -absolute.layout.xaxis.range[1]);
const percent = api.costContributionSpec(pair, 'percent');
assert.equal(percent.omitted, 1);
assert.deepEqual(percent.traces.map(trace => trace.x), [[-20], [0], [100]]);
assert.equal(percent.traces.flatMap(trace => trace.customdata).length + percent.omitted, pair.rows.length);
const zero = generation('zero', [sample(1, 0, 0)]);
const positive = generation('positive', [sample(1, 1, 0)]);
for (const current of [zero, positive]) {
  const comparison = api.costComparison(zero, current);
  assert.equal(comparison.index, null);
  assert.equal(comparison.percent, null);
  assert.equal(api.costContributionSpec(comparison).omitted, 0);
  assert.equal(api.costContributionSpec(comparison, 'percent').omitted, 1);
  assert.equal(api.costContributionSpec(comparison, 'percent').layout.annotations.length, 1);
}
const zeroTrend = api.costTrendSpec(api.costOverview([zero, positive], 'zero', 'positive'));
assert.deepEqual(zeroTrend.traces[0].y, [null, null]);
assert.equal(zeroTrend.traces[0].connectgaps, false);
assert.equal(zeroTrend.layout.annotations.length, 2);
const equal = api.costComparison(before, before);
assert.deepEqual(equal.counts, {lower: 0, equal: 4, higher: 0});
assert.equal(api.costHeadline(equal), 'Equal total cost');
assert.equal(api.costComparison(before, generation('empty', [])).index, null);
// A zero individual reference must not discard its absolute contribution to the total.
const mixed = api.costComparison(generation('a', [sample(1, 100), sample(2, 0)]),
  generation('b', [sample(1, 80), sample(2, 10)]));
assert.equal(mixed.index, 90);
assert.deepEqual(mixed.counts, {lower: 1, equal: 0, higher: 1});
