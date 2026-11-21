// SPDX-License-Identifier: Apache-2.0
'use strict';
// Final web projection: all costs are nonnegative; zero has no finite log ratio.
(function(root) {
  const selectedAggregation = value => value === 'total' ? 'total' : 'geomean';
  const ratioPercent = ratio => Number.isFinite(ratio) && ratio >= 0 ? 100 * (ratio - 1) : null;
  function checkedCost(value) {
    if (!Number.isFinite(value) || value < 0) throw Error('Cost must be a finite nonnegative number.');
    return value;
  }
  function summarizePairs(pairs) {
    const zero = {positive_to_positive:0,zero_to_zero:0,positive_to_zero:0,zero_to_positive:0};
    let total = 0, sum = 0, correction = 0;
    for (const pair of pairs) {
      const before = checkedCost(pair.before), after = checkedCost(pair.after);
      total++;
      if (before === 0) zero[after === 0 ? 'zero_to_zero' : 'zero_to_positive']++;
      else if (after === 0) zero.positive_to_zero++;
      else {
        zero.positive_to_positive++;
        const value = Math.log(after) - Math.log(before), next = sum + value;
        correction += Math.abs(sum) >= Math.abs(value) ? (sum - next) + value : (value - next) + sum;
        sum = next;
      }
    }
    const eligible = zero.positive_to_positive, log_ratio = eligible ? (sum + correction) / eligible : null;
    return {total,eligible,zero_cost:zero,log_ratio,ratio:log_ratio === null ? null : Math.exp(log_ratio),
      percent:log_ratio === null ? null : 100 * Math.expm1(log_ratio)};
  }
  function fixedPositiveKeys(populations) {
    if (!populations.length) return [];
    const maps = populations.map(population => population instanceof Map ? population : new Map(population));
    for (const population of maps) for (const value of population.values()) checkedCost(value);
    return [...maps[0].keys()].filter(key => maps.every(population => population.has(key) && population.get(key) > 0)).sort();
  }
  const api = {selectedAggregation,ratioPercent,checkedCost,summarizePairs,fixedPositiveKeys};
  if (typeof module !== 'undefined') module.exports = api;
  root.BvcCostStats = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
