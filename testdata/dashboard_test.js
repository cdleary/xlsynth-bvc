// SPDX-License-Identifier: Apache-2.0
'use strict';
const assert=require('node:assert/strict');
const {change,share,headline,trendSpec:geomeanTrendSpec,transitionSpec:geomeanTransitionSpec,validateDetail,fullCorpusHref,measurementLinks,irComparisonLinks,techmapLinks,latestResults,selectedStage}=require('../src/site_assets/dashboard.js');
const trendSpec=(series,selected,narrow)=>geomeanTrendSpec(series,selected,narrow,'total');
const transitionSpec=(series,selected,narrow)=>geomeanTransitionSpec(series,selected,narrow,'total');
assert.equal(change(100,80),-20);
assert.equal(change(0,0),null);
assert.equal(headline(0),'Unchanged total cost');
assert.equal(headline(-20),'20% lower cost');
assert.equal(selectedStage(null),'raw');
assert.equal(selectedStage('raw'),'raw');
assert.equal(selectedStage('unrecognized'),'raw');
assert.equal(selectedStage('abc'),'abc');
const series={points:[{version:'0.1.0',cost:100,url:'first'},{version:'0.2.0',cost:80,url:'second'}],unavailable_versions:[]};
const spec=trendSpec(series);
assert.deepEqual(spec.data[0].y,[0,-20]);
assert.deepEqual(spec.data[0].customdata,['0.1.0','0.2.0']);
assert.deepEqual(spec.layout.xaxis.ticktext,['v0.1.0','v0.2.0']);
assert.ok(spec.layout.yaxis.range[0]<-20&&spec.layout.yaxis.range[1]>0);
const zero=trendSpec({points:[{version:'0.1.0',cost:0,url:'first'}],unavailable_versions:[]});
assert.deepEqual(zero.data[0].y,[null]);
assert.equal(zero.layout.annotations.length,1);
const gap=trendSpec({points:[{version:'0.65.0',cost:100,url:'first'},{version:'0.68.0',cost:80,url:'last'}],unavailable_versions:['0.66.0']});
assert.deepEqual(gap.data[0].y,[0,null,-20]);
assert.deepEqual(gap.data[0].text,['v0.65.0','v0.66.0','v0.68.0']);
assert.deepEqual(gap.data[0].customdata,['0.65.0',null,'0.68.0']);
assert.equal(gap.data[0].connectgaps,false);
const empty=trendSpec({points:[],unavailable_versions:['0.66.0']});
assert.deepEqual(empty.data[0].y,[null]);
assert.equal(empty.layout.annotations.length,1);
assert.equal(share(-5,100),-5);
assert.equal(share(1,0),null);
const paired={points:[{version:'0.1.0',cost:100,transition:null},{version:'0.2.0',cost:102,transition:{baseline_version:'0.1.0',baseline_cost:100,cost_decreases:-20,cost_increases:22,higher:1}}],unavailable_versions:['0.15.0']};
const strip=transitionSpec(paired,'0.2.0');
assert.deepEqual(strip.data.map(t=>t.name),['Decreases','Increases','Net']);
assert.deepEqual(strip.data.map(t=>t.y),[[null,-20,null],[null,22,null],[null,2,null]]);
assert.equal(strip.layout.barmode,'relative');
assert.deepEqual(strip.data.slice(0,2).map(t=>t.textposition),['none','none']);
const dense={points:Array.from({length:22},(_,i)=>({version:`0.${i+54}.0`,cost:100-i})),unavailable_versions:[]};
const phoneTicks=transitionSpec(dense,'0.75.0',true).layout.xaxis.tickvals;
assert.deepEqual(phoneTicks,[0,7,14,21]);
assert.deepEqual(strip.layout.xaxis.range,trendSpec(paired).layout.xaxis.range);
assert.deepEqual(strip.layout.shapes[0],{type:'rect',xref:'x',yref:'paper',x0:.65,x1:1.35,y0:0,y1:1,line:{width:0},fillcolor:'#a0b0c014',layer:'below'});
const zeroStrip=transitionSpec({points:[{version:'0.1.0',cost:0,transition:null},{version:'0.2.0',cost:1,transition:{baseline_version:'0.1.0',baseline_cost:0,cost_decreases:0,cost_increases:1}}],unavailable_versions:[]});
assert.deepEqual(zeroStrip.data.map(t=>t.y),[[null,null],[null,null],[null,null]]);
assert.equal(zeroStrip.layout.annotations.length,1);
const detail={schema_version:1,baseline_version:'0.1.0',version:'0.2.0',higher:1,limit:10,largest:[],smallest:[]};
assert.equal(validateDetail(detail,paired.points[1]),detail);
assert.throws(()=>validateDetail({...detail,version:'0.3.0'},paired.points[1]));
assert.throws(()=>validateDetail({...detail,higher:3},paired.points[1]));
assert.throws(()=>validateDetail({...detail,largest:Array(11).fill({})},paired.points[1]));
// Coverage is selected independently for the two comparison dimensions.
const full={crate_version:'0.77.0',full_count:32806,cohorts:[],techmap_cohorts:[]};
const mappedCoverage={id:'whole-functions-v1',label:'Whole functions',measured:3,total:187,complete:false,url:'tech-mapped-asap7/?release=0.79.0&cohort=whole-functions-v1'};
const mappedOnly={crate_version:'0.79.0',full_count:0,cohorts:[],techmap_cohorts:[mappedCoverage]};
const latest=latestResults([full,mappedOnly]);
assert.equal(latest.aig,full);
assert.equal(latest.techmap,mappedOnly);
assert.deepEqual(latestResults([]),{aig:undefined,techmap:undefined});
assert.deepEqual(irComparisonLinks(mappedOnly),[]);
assert.deepEqual(techmapLinks(full),[]);
const fullLinks=irComparisonLinks(full);
assert.equal(fullLinks.length,2);
assert.deepEqual(fullLinks.map(link=>link.count),[32806,32806]);
assert.deepEqual(fullLinks.map(link=>{const url=new URL(link.href,'https://example.test/');return [url.pathname,url.searchParams.get('release'),url.searchParams.get('stage'),url.searchParams.get('reference')];}),[['/corpus/','0.77.0','abc','yosys'],['/corpus/','0.77.0','raw','yosys']]);
assert.equal(fullCorpusHref(full,'abc'),fullLinks[0].href);
const retained={crate_version:'0.74.0',full_count:0,historical_abc_count:2,historical_abc_measurements:9,historical_raw_count:3,historical_raw_measurements:13,techmap_cohorts:[mappedCoverage]};
const retainedLinks=irComparisonLinks(retained);
assert.deepEqual(retainedLinks.map(({kind,stage,count,href})=>({kind,stage,count,href})),[
  {kind:'retained',stage:'abc',count:2,href:'history/ir-fn-g8r-abc-vs-codegen-yosys-abc/?crate_version=0.74.0'},
  {kind:'unpaired',stage:'abc',count:5,href:'history/dataset.html'},
  {kind:'retained',stage:'raw',count:3,href:'history/ir-fn-corpus-g8r-vs-yosys-abc/?crate_version=0.74.0'},
  {kind:'unpaired',stage:'raw',count:7,href:'history/dataset.html'},
]);
assert.deepEqual(measurementLinks(retained),retainedLinks);
assert.deepEqual(techmapLinks(retained),[{count:3,text:'Whole functions: 3 / 187 (partial)',href:mappedCoverage.url}]);

// Default plots use geometric ratios and a single unstacked adjacent-change bar.
const geometric={points:[{version:'0.1.0',cost:1000,geomean_ratio:1},{version:'0.2.0',cost:500,geomean_ratio:1.2,transition:{baseline_version:'0.1.0',baseline_cost:1000,cost_decreases:-500,cost_increases:0,geomean_ratio:1.2}}],unavailable_versions:['0.15.0']};
const geometricTrend=geomeanTrendSpec(geometric), geometricStrip=geomeanTransitionSpec(geometric);
assert.deepEqual(geometricTrend.data[0].y.map(n=>n===null?null:Math.round(n)),[0,20,null]);
assert.equal(geometricStrip.data.length,1);
assert.equal(geometricStrip.data[0].type,'bar');
assert.deepEqual(geometricStrip.data[0].y.map(n=>n===null?null:Math.round(n)),[null,20,null]);
assert.deepEqual(geometricStrip.data[0].marker.color,['#80e0b1','#ff9aa9','#80e0b1']);
assert.deepEqual(geomeanTrendSpec({...geometric,points:geometric.points.map(p=>({...p,geomean_ratio:null}))}).data[0].y,[null,null,null]);
assert.equal(geomeanTrendSpec({...geometric,points:geometric.points.map(p=>({...p,geomean_ratio:null}))}).layout.annotations.length,1);
