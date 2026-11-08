// SPDX-License-Identifier: Apache-2.0
'use strict';
const assert = require('node:assert/strict');
const stats = require('../src/site_assets/aggregation.js');
const near = (value, expected) => assert.ok(Math.abs(value - expected) < 1e-10, `${value} ≠ ${expected}`);
assert.equal(stats.selectedAggregation(null),'geomean');
assert.equal(stats.selectedAggregation('total'),'total');
const equalWeight = stats.summarizePairs([{before:1000,after:500},{before:1,after:2}]);
near(equalWeight.ratio,1);
near(equalWeight.percent,0);
const extremes = stats.summarizePairs([{before:1e-300,after:1e300},{before:1e300,after:1e-300}]);
near(extremes.ratio,1);
const zero = stats.summarizePairs([{before:0,after:0},{before:3,after:0},{before:0,after:4},{before:4,after:2}]);
assert.deepEqual(zero.zero_cost,{positive_to_positive:1,zero_to_zero:1,positive_to_zero:1,zero_to_positive:1});
assert.deepEqual([zero.total,zero.eligible],[4,1]);
near(zero.percent,-50);
assert.equal(stats.summarizePairs([{before:0,after:0}]).ratio,null);
assert.equal(stats.summarizePairs([]).percent,null);
const maps = [new Map([['a',1],['b',2],['c',3]]),new Map([['a',1],['b',0],['d',4]])];
assert.deepEqual(stats.fixedPositiveKeys(maps),['a']);
assert.deepEqual(stats.fixedPositiveKeys([]),[]);
for (const bad of [-1,NaN,Infinity,null,'1']) assert.throws(()=>stats.summarizePairs([{before:bad,after:1}]));
// A fixed positive cohort makes adjacent geometric ratios chain exactly.
const values = [[100,10,0],[80,20,5],[40,10,0]], keys=stats.fixedPositiveKeys(values.map(row=>new Map(row.map((value,i)=>[i,value]))));
assert.deepEqual(keys,[0,1]);
const ratio=(a,b)=>stats.summarizePairs(keys.map(key=>({before:a[key],after:b[key]}))).ratio;
near(ratio(values[0],values[1])*ratio(values[1],values[2]),ratio(values[0],values[2]));
