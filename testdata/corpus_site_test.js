// SPDX-License-Identifier: Apache-2.0
'use strict';
const assert = require('node:assert/strict');
const {names, pairRows, filterRows, summarize, lossRows, metricComparison, comparisonCsv} = require('../src/site_assets/corpus.js');
const row = {sample_id:'a',source_sha256:'1',top_fn_name:'whole',source_relpath:'whole.ir',input_kinds:['whole'],function_names:['whole'],ir_node_count:4,g8r_nodes:2,g8r_depth:3,g8r_le:1.5,yosys_nodes:3,yosys_depth:3,yosys_le:2};
const all = pairRows([row,{...row,sample_id:'b',g8r_nodes:4},{...row,sample_id:'c',g8r_nodes:3}]);
assert.deepEqual(summarize(all), {count:3,cost:27,reference:27,better:1,worse:1,unchanged:1});
assert.deepEqual(filterRows(all,{kind:'all',max:4,losses:true}).map(r=>r.sample_id),['b']);
assert.deepEqual(filterRows(all,{kind:'all',max:3,losses:false}),[]);
// The f32 adder is a whole function AND a deduplicated extracted MFFC.
const adder = {...row,sample_id:'adder',top_fn_name:'__mffc_345679b6c3fb1f66',input_kinds:['mffc','whole'],function_names:['__float32__add']};
const cone = {...row,sample_id:'cone',input_kinds:['k3'],function_names:[]};
const population = pairRows([adder,cone]);
for(const kind of ['whole','mffc']) assert.deepEqual(filterRows(population,{kind,max:null}).map(r=>r.sample_id),['adder']);
assert.deepEqual(filterRows(population,{kind:'all',max:null}).map(r=>r.sample_id),['adder','cone']);
assert.deepEqual(filterRows(population,{kind:'k3',max:null}).map(r=>r.sample_id),['cone']);
assert.deepEqual(filterRows(population,{kind:'all',max:null,search:'FLOAT32__ADD'}).map(r=>r.sample_id),['adder']);
assert.deepEqual(filterRows(population,{kind:'whole',max:null,search:'345679b6'}).map(r=>r.sample_id),['adder']);
assert.equal(names(adder),'__float32__add');
assert.equal(names(cone),'whole');
const compared = pairRows([row],[{...row,g8r_nodes:1,g8r_le:1}])[0];
assert.equal(compared.change,3);
assert.equal(compared.reference_le,1);
assert.throws(()=>pairRows([row],[]));
assert.throws(()=>pairRows([row],[{...row,source_sha256:'2'}]));
assert.throws(()=>pairRows([row],[{...row,top_fn_name:'different'}]));
// Raw comparisons must select both sides' raw measurements, including LE,
// while leaving the cached post-ABC rows intact.
const raw={...row,raw_nodes:4,raw_depth:5,raw_le:6};
const rawBaseline={...raw,raw_nodes:3,raw_depth:4,raw_le:5};
const rawPair=pairRows([raw],[rawBaseline],'raw')[0];
assert.deepEqual([rawPair.g8r_nodes,rawPair.g8r_depth,rawPair.g8r_le,rawPair.reference_nodes,rawPair.reference_depth,rawPair.reference_le,rawPair.cost,rawPair.reference_cost,rawPair.change],[4,5,6,3,4,5,20,12,8]);
assert.equal(raw.g8r_nodes,2);
assert.equal(rawBaseline.g8r_nodes,2);
assert.equal(pairRows([raw],null,'raw')[0].reference_cost,9);
assert.throws(()=>pairRows([row],[rawBaseline],'raw'));
assert.throws(()=>pairRows([raw],[row],'raw'));
assert.equal(summarize(pairRows([{...row,g8r_nodes:0,yosys_nodes:0}])).count,1);
assert.deepEqual(lossRows(all).map(r=>[r.sample_id,r.ir_node_count,r.change]),[['b',4,3]]);
// A node/depth tradeoff with net positive cost remains visible; nonpositive
// sizes/losses and non-finite coordinates cannot go on logarithmic axes.
const tradeoff=pairRows([{...row,g8r_nodes:1,g8r_depth:10}])[0];
assert.deepEqual(lossRows([tradeoff,...[-1,0,NaN,Infinity].map(n=>({...tradeoff,ir_node_count:n})),...[-1,0,NaN,Infinity].map(n=>({...tradeoff,change:n}))]),[tradeoff]);
assert.deepEqual(lossRows(filterRows(all,{kind:'all',max:3,losses:false})),[]);

// A 20% direct improvement can be a 30 pp improvement relative to a fixed reference.
const changes=metricComparison(120,150,100,100);
assert.ok(Math.abs(changes.percent+20)<1e-10);
assert.ok(Math.abs(changes.relative_pp+30)<1e-10);
// A changing codegen reference changes pp independently of the direct improvement.
assert.ok(Math.abs(metricComparison(120,150,120,100).relative_pp+50)<1e-10);
for(const stage of ['abc','raw']) {
  const a={...raw,g8r_nodes:15,g8r_depth:10,raw_nodes:30,raw_depth:10,yosys_nodes:10,yosys_depth:10};
  const b={...a,g8r_nodes:12,raw_nodes:24,yosys_nodes:12,g8r_le:null};
  const paired=pairRows([b],[a],stage)[0];
  assert.ok(Math.abs(paired.change_percent+20)<1e-10);
  const relativePp=stage==='abc'?-50:-100;
  assert.equal(paired.relative_change_pp,relativePp);
  assert.equal(pairRows([a],[a],stage)[0].relative_change_pp,0);
  // The fixture has one row and no commas in its fields.
  const [header, record] = comparisonCsv([paired]).trimEnd().split('\n')
    .map(line => line.slice(1,-1).split('","'));
  const cell = name => record[header.indexOf(name)];
  assert.deepEqual([cell('product_old'),cell('product_new')],stage==='abc'?['150','120']:['300','240']);
  assert.deepEqual([cell('product_old_yosys'),cell('product_new_yosys')],['100','120']);
  assert.ok(Math.abs(Number(cell('product_percent'))+20)<1e-10);
  assert.equal(cell('product_relative_pp'),String(relativePp));
  assert.equal(cell('le_new'),stage==='abc'?'':'6');
}
assert.equal(metricComparison(0,0,0,0).percent,null);
assert.equal(metricComparison(0,10,10,10).percent,-100);
assert.equal(metricComparison(null,10,null,10).relative_pp,null);
assert.throws(()=>pairRows([row],[row,{...row,sample_id:'extra'}]));
assert.throws(()=>pairRows([row,row],[row,row]));
assert.throws(()=>pairRows([row],[{...row,source_relpath:'another.ir'}]));
