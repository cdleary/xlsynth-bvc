// SPDX-License-Identifier: Apache-2.0
'use strict';
const assert = require('node:assert/strict');
const {kind, pairRows, filterRows, summarize} = require('../src/site_assets/corpus.js');
const row = {sample_id:'a',source_sha256:'1',top_fn_name:'whole',ir_node_count:4,g8r_nodes:2,g8r_depth:3,g8r_le:1.5,yosys_nodes:3,yosys_depth:3,yosys_le:2};
const all = pairRows([row,{...row,sample_id:'b',g8r_nodes:4},{...row,sample_id:'c',g8r_nodes:3}]);
assert.deepEqual(summarize(all), {count:3,cost:27,reference:27,better:1,worse:1,unchanged:1});
assert.deepEqual(filterRows(all,{kind:'all',max:4,losses:true}).map(r=>r.sample_id),['b']);
assert.deepEqual(filterRows(all,{kind:'all',max:3,losses:false}),[]);
assert.equal(kind({...row,top_fn_name:'__k3_cone_123'}),'k3');
assert.equal(kind({...row,top_fn_name:'__mffc_123'}),'mffc');
assert.equal(kind(row),'whole');
const compared = pairRows([row],[{...row,g8r_nodes:1,g8r_le:1}])[0];
assert.equal(compared.change,3);
assert.equal(compared.reference_le,1);
assert.throws(()=>pairRows([row],[]));
assert.throws(()=>pairRows([row],[{...row,source_sha256:'2'}]));
assert.throws(()=>pairRows([row],[{...row,top_fn_name:'different'}]));
assert.equal(summarize(pairRows([{...row,g8r_nodes:0,yosys_nodes:0}])).count,1);
