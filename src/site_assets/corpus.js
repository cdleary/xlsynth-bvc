// SPDX-License-Identifier: Apache-2.0
'use strict';
(() => {
  const kind = row => row.top_fn_name.startsWith('__k3_cone_') ? 'k3' : row.top_fn_name.startsWith('__mffc_') ? 'mffc' : 'whole';
  const product = row => row.g8r_nodes * row.g8r_depth;
  function pairRows(rows, baseline) {
    const lookup = baseline ? new Map(baseline.map(r => [r.sample_id, r])) : null;
    return rows.map(row => {
      const other = lookup?.get(row.sample_id);
      if (lookup && (!other || other.source_sha256 !== row.source_sha256 || other.top_fn_name !== row.top_fn_name)) throw Error('Release inputs do not match');
      const nodes = other ? other.g8r_nodes : row.yosys_nodes, depth = other ? other.g8r_depth : row.yosys_depth;
      return {...row, reference_nodes:nodes, reference_depth:depth, reference_le:other ? other.g8r_le : row.yosys_le,
        cost:product(row), reference_cost:nodes*depth, change:product(row)-nodes*depth};
    });
  }
  function filterRows(rows, options) {
    return rows.filter(r => (options.kind === 'all' || kind(r) === options.kind)
      && (options.max === null || r.ir_node_count <= options.max) && (!options.losses || r.change > 0));
  }
  function summarize(rows) {
    return rows.reduce((s,r) => ({count:s.count+1,cost:s.cost+r.cost,reference:s.reference+r.reference_cost,
      better:s.better+Number(r.change<0),worse:s.worse+Number(r.change>0),unchanged:s.unchanged+Number(r.change===0)}),
      {count:0,cost:0,reference:0,better:0,worse:0,unchanged:0});
  }
  if (typeof module !== 'undefined') module.exports = {kind,pairRows,filterRows,summarize};
  if (typeof document === 'undefined') return;
  const byId = id => document.getElementById(id), fmt = n => n.toLocaleString(undefined,{maximumFractionDigits:3});
  const load = async url => {const r=await fetch(url);if(!r.ok)throw Error(`${url}: HTTP ${r.status}`);return r.json()};
  const state = {catalog:null, cache:new Map(), epoch:0, selected:0, rows:[], rendering:false, pending:false};
  const color = row => row.change < 0 ? '#7de9ae' : row.change > 0 ? '#ff91ad' : '#9ca8b6';
  function layout(x,y,log=false) {
    return {paper_bgcolor:'rgba(0,0,0,0)',plot_bgcolor:'#111922',font:{color:'#dce7f0'},margin:{l:68,r:18,t:24,b:65},showlegend:false,
      xaxis:{title:x,type:log?'log':'linear',gridcolor:'#293847',zerolinecolor:'#789'},yaxis:{title:y,type:log?'log':'linear',gridcolor:'#293847',zerolinecolor:'#789'}};
  }
  async function selectSample(row) {
    const token=++state.selected;
    byId('selected').textContent=`${row.top_fn_name} · ${row.source_relpath} · selected ${fmt(row.cost)} / reference ${fmt(row.reference_cost)} · Δ ${fmt(row.change)}`;
    byId('ir').hidden=false; byId('ir').textContent='Loading verified IR…';
    try {const data=await load(row.ir_url);if(token===state.selected){if(typeof data[row.sample_id]!=='string')throw Error('IR is missing');byId('ir').textContent=data[row.sample_id]}}
    catch(e){if(token===state.selected)byId('ir').textContent=e.message}
  }
  async function scatter(name, rows, x, y, xLabel, yLabel, logarithmic, diagonal) {
    const valid=rows.filter(r=>Number.isFinite(x(r))&&Number.isFinite(y(r))&&(!logarithmic||(x(r)>0&&y(r)>0)));
    const traces=[{type:'scattergl',mode:'markers',x:valid.map(x),y:valid.map(y),customdata:valid.map(r=>r.sample_id),marker:{size:5,color:valid.map(color),opacity:.72},hovertemplate:diagonal?'Reference: %{x}<br>Selected: %{y}<extra></extra>':'Δ nodes: %{x}<br>Δ depth: %{y}<extra></extra>'}];
    if(diagonal&&valid.length){let lo=Infinity,hi=-Infinity;for(const r of valid){lo=Math.min(lo,x(r),y(r));hi=Math.max(hi,x(r),y(r))}traces.push({type:'scatter',mode:'lines',x:[lo,hi],y:[lo,hi],line:{color:'#83eeb5',dash:'dot',width:1},hoverinfo:'skip'})}
    const l=layout(xLabel,yLabel,logarithmic);l.title={text:`${valid.length.toLocaleString()} plotted inputs`,font:{size:12},x:.98,xanchor:'right'};
    await Plotly.react(name,traces,l,{responsive:true,displayModeBar:false});
    const el=byId(name);el.removeAllListeners?.('plotly_click');el.on('plotly_click',e=>{const row=state.rows.find(r=>r.sample_id===e.points?.[0]?.customdata);if(row)void selectSample(row)});
  }
  async function loadRelease(generation) {
    if(!state.cache.has(generation.crate_version)) {
      const rows=[];
      // Bound concurrent requests; never fetch every release or all source IR.
      for(let i=0;i<generation.shards.length;i+=4){const shards=generation.shards.slice(i,i+4);const values=await Promise.all(shards.map(s=>load(s.metrics)));values.forEach((part,j)=>part.forEach(r=>rows.push({...r,ir_url:shards[j].ir})))}
      if(rows.length!==state.catalog.sample_count)throw Error('Incomplete release data');
      state.cache.set(generation.crate_version,rows);
    }
    return state.cache.get(generation.crate_version);
  }
  async function render() {
    const token=state.epoch;byId('error').textContent='';byId('corpus-site').dataset.rendered='loading';
    byId('scope').textContent='Loading selected release…';
    byId('baseline').disabled=byId('reference').value!=='release';
    const version=byId('release').value, baselineVersion=byId('baseline').value, compare=byId('reference').value;
    const current=await loadRelease(state.catalog.generations.find(g=>g.crate_version===version));
    const baseline=compare==='release'?await loadRelease(state.catalog.generations.find(g=>g.crate_version===baselineVersion)):null;
    if(token!==state.epoch)return;
    for(const key of state.cache.keys())if(key!==version&&(!baseline||key!==baselineVersion))state.cache.delete(key);
    const maxValue=byId('max-ir').value;
    state.rows=filterRows(pairRows(current,baseline),{kind:byId('kind').value,max:maxValue===''?null:Math.max(0,Number(maxValue)),losses:byId('losses').checked});
    const rows=state.rows,s=summarize(rows),reference=baseline?`v${baselineVersion} G8r+ABC`:'codegen+Yosys/ABC';
    byId('scope').textContent=`${fmt(s.count)} / ${fmt(state.catalog.sample_count)} inputs · v${version} G8r+ABC versus ${reference}`;
    const pct=s.reference?`${((s.cost/s.reference-1)*100).toFixed(3)}%`:'undefined';
    byId('summary').replaceChildren(...[[`Aggregate cost change`,pct],['Lower / higher cost',`${fmt(s.better)} / ${fmt(s.worse)}`],['Unchanged cost',fmt(s.unchanged)],['Filtered inputs',fmt(s.count)]].map(([label,value])=>{const card=document.createElement('div');card.className='card';const title=document.createElement('span');title.textContent=label;const number=document.createElement('strong');number.textContent=value;card.append(title,number);return card}));
    ++state.selected;byId('selected').textContent='Click a point or outlier to inspect its verified source IR.';byId('ir').hidden=true;
    const body=byId('outliers');body.replaceChildren();
    for(const row of [...rows].filter(r=>r.change>0).sort((a,b)=>b.change-a.change||a.sample_id.localeCompare(b.sample_id)).slice(0,30)){
      const tr=document.createElement('tr'),first=document.createElement('td'),button=document.createElement('button');button.textContent=row.top_fn_name;button.addEventListener('click',()=>void selectSample(row));first.append(button);tr.append(first);
      for(const n of [row.cost,row.reference_cost,row.change]){const td=document.createElement('td');td.textContent=fmt(n);tr.append(td)}body.append(tr);
    }
    await scatter('product',rows,r=>r.reference_cost,r=>r.cost,`${reference} nodes × depth`,`v${version} nodes × depth`,true,true);
    await scatter('le',rows,r=>r.reference_le,r=>r.g8r_le,`${reference} graph LE`,`v${version} graph LE`,false,true);
    await scatter('nodes',rows,r=>r.reference_nodes,r=>r.g8r_nodes,`${reference} AND nodes`,`v${version} AND nodes`,true,true);
    await scatter('delta',rows,r=>r.g8r_nodes-r.reference_nodes,r=>r.g8r_depth-r.reference_depth,'Selected − reference AND nodes','Selected − reference depth',false,false);
    if(token!==state.epoch)return;
    const url=new URL(location.href);url.searchParams.set('release',version);url.searchParams.set('reference',compare);url.searchParams.set('baseline',baselineVersion);history.replaceState(null,'',url);
    byId('corpus-site').dataset.rendered='true';byId('corpus-site').dataset.sampleCount=String(rows.length);
  }
  async function requestRender(){
    ++state.epoch;state.pending=true;
    if(state.rendering)return;
    state.rendering=true;
    try {
      // Serialize Plotly mutations; rapid control changes coalesce into the latest view.
      while(state.pending){state.pending=false;try{await render()}catch(e){if(!state.pending){byId('error').textContent=e.message;byId('corpus-site').dataset.rendered='error'}}}
    } finally {state.rendering=false}
  }
  async function main(){
    state.catalog=await load('catalog.json');if(state.catalog.schema_version!==1||!state.catalog.generations.length)throw Error('Invalid corpus catalog');
    const versions=state.catalog.generations.map(g=>g.crate_version),query=new URLSearchParams(location.search);
    for(const name of ['release','baseline'])byId(name).replaceChildren(...versions.map(v=>{const o=document.createElement('option');o.value=v;o.textContent=`v${v}`;return o}));
    byId('release').value=versions.includes(query.get('release'))?query.get('release'):versions.at(-1);
    byId('baseline').value=versions.includes(query.get('baseline'))?query.get('baseline'):versions.at(-2)||versions[0];
    byId('reference').value=query.get('reference')==='release'?'release':'yosys';
    byId('coverage').textContent=`${versions.length} complete releases · ${fmt(state.catalog.sample_count)} identical frozen inputs each · post-ABC measurements · no partial samples`;
    for(const name of ['release','baseline','reference','kind','max-ir','losses'])byId(name).addEventListener('change',requestRender);
    await requestRender();
  }
  void main().catch(e=>{byId('error').textContent=e.message;byId('corpus-site').dataset.rendered='error'});
})();
