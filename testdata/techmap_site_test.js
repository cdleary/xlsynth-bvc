// SPDX-License-Identifier: Apache-2.0
'use strict';
const assert = require('node:assert/strict');
const {dominates,frontier,normalized,counts,normalizedSpec,absoluteSpec,overviewSamples,seriesStyles,objectiveLabel} = require('../src/site_assets/techmap.js');
const p=(tool,goal,area,delay_ps)=>({tool,goal,area,delay_ps,cell_count:3});
const abc=p('abc','delay',10,10),area=p('g8r','area',5,15),delay=p('g8r','delay',15,5);
const sample={id:'one',label:'One',points:[abc,area,delay]};
assert.equal(dominates(area,abc),false);
assert.equal(dominates(delay,abc),false);
assert.equal(dominates(abc,abc),false);
assert.deepEqual(frontier(sample.points),sample.points);
assert.deepEqual(counts([sample],'delay'),{paired:1,better:0,tradeoff:1,worse:0,equal:0,missing:0});
assert.deepEqual(normalized([sample],'delay').map(r=>[r.x,r.y]),[[1,1],[.5,1.5],[1.5,.5]]);
assert.deepEqual(normalized([{id:'zero',points:[p('abc','delay',0,10),area]}],'delay'),[]);
const win={id:'two',points:[abc,p('g8r','area',10,5)]};
assert.deepEqual(counts([win],'delay'),{paired:1,better:1,tradeoff:0,worse:0,equal:0,missing:0});
const missing={id:'missing',points:[area]};
assert.deepEqual(counts([missing],'delay'),{paired:0,better:0,tradeoff:0,worse:0,equal:0,missing:1});
const ratio=normalizedSpec([sample],'delay');
assert.deepEqual(ratio.data.map(t=>t.x),[[.5],[1.5],[],[1]]);
assert.deepEqual(ratio.data.map(t=>t.y),[[1.5],[.5],[],[1]]);
const absolute=absoluteSpec(sample);
assert.deepEqual(absolute.data.map(t=>t.x),[[5],[15],[],[10]]);
assert.deepEqual(absolute.data.map(t=>t.y),[[15],[5],[],[10]]);
assert.deepEqual(absolute.layout.shapes.map(line=>[line.x0,line.y0,line.x1,line.y1]),[[5,15,15,5]]);
const expectedSeries=[
  {key:'g8r:area',color:'#48d8f0',symbol:'circle',label:'XLS / g8r · Area objective'},
  {key:'g8r:delay',color:'#bc91ff',symbol:'triangle-up',label:'XLS / g8r · Delay objective'},
  {key:'abc:area',color:'#f5bd4f',symbol:'square-open',label:'Yosys / ABC · Area-only'},
  {key:'abc:delay',color:'#ff858d',symbol:'x',label:'Yosys / ABC · Timing-oriented (default)'},
];
const identities=spec=>spec.data.map(trace=>({key:trace.legendgroup,color:trace.marker.color,symbol:trace.marker.symbol,label:trace.name}));
assert.deepEqual(identities(ratio),expectedSeries);
assert.deepEqual(identities(absolute),expectedSeries);
assert.equal(new Set(Object.values(seriesStyles).map(s=>s.color)).size,4);
assert.equal(new Set(Object.values(seriesStyles).map(s=>s.symbol)).size,4);
const luminance=hex=>{const rgb=hex.slice(1).match(/../g).map(s=>parseInt(s,16)/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4);return rgb[0]*.2126+rgb[1]*.7152+rgb[2]*.0722;};
const background=luminance('#14202a');
for(const style of Object.values(seriesStyles))assert.ok((luminance(style.color)+.05)/(background+.05)>=5);
assert.equal(objectiveLabel('abc','area'),'Area-only');
assert.equal(objectiveLabel('abc','delay'),'Timing-oriented (default)');
assert.equal(objectiveLabel('g8r','delay'),'Delay objective');
const dense=normalizedSpec(Array.from({length:1100},(_,id)=>({...sample,id:String(id)})),'delay');
assert.deepEqual(identities(dense),expectedSeries);
assert.equal(dense.data[0].type,'scatter');
const denseFour=normalizedSpec(Array.from({length:1100},(_,id)=>({...sample,id:String(id),points:[...sample.points,p('abc','area',8,12)]})),'delay');
assert.equal(denseFour.data[0].type,'scattergl');
assert.deepEqual(identities(denseFour),expectedSeries);
const population=Array.from({length:8000},(_,id)=>({id}));
assert.equal(overviewSamples(population,true),population);
const bounded=overviewSamples(population,false);
assert.equal(bounded.length,2000);
assert.equal(bounded[0],population[0]);
assert.equal(bounded.at(-1),population.at(-1));
assert.equal(new Set(bounded.map(s=>s.id)).size,2000);
console.log('Techmap area-delay semantics passed');
