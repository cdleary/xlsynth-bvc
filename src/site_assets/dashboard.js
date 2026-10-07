// SPDX-License-Identifier: Apache-2.0
'use strict';
(() => {
  const change = (before, after) => before === 0 ? null : 100 * (after - before) / before;
  const headline = percent => percent === null ? 'Change undefined' : percent === 0 ? 'Unchanged total cost' : `${Math.abs(percent).toLocaleString(undefined,{maximumSignificantDigits:4})}% ${percent < 0 ? 'lower' : 'higher'} cost`;
  function trendSpec(trend) {
    const first = trend.points[0], y = trend.points.map(p => change(first.cost,p.cost));
    const finite = y.filter(Number.isFinite), lo = Math.min(0,...finite), hi = Math.max(0,...finite), pad = Math.max((hi-lo)*.18,.01);
    const x = trend.points.map((_,i)=>i), step = Math.max(1,Math.ceil(x.length/9)), ticks=x.filter(i=>i%step===0||i===x.length-1);
    return {data:[{type:'scatter',mode:'lines+markers',x,y,customdata:trend.points.map(p=>p.url),text:trend.points.map(p=>`v${p.version}`),line:{color:'#80e0b1',width:3},marker:{size:8,color:'#80e0b1'},connectgaps:false,hovertemplate:'%{text}<br>%{y:.4g}% cost change<extra></extra>'}],
      layout:{paper_bgcolor:'transparent',plot_bgcolor:'transparent',font:{color:'#a0b0c0',family:'system-ui,sans-serif'},showlegend:false,margin:{l:66,r:20,t:25,b:62},
        xaxis:{tickvals:ticks,ticktext:ticks.map(i=>`v${trend.points[i].version}`),tickangle:-20,range:[-.3,Math.max(.3,x.length-.7)],gridcolor:'#263441',zeroline:false},
        yaxis:{title:{text:'Cost change (%)'},range:[lo-pad,hi+pad],ticksuffix:'%',gridcolor:'#263441',zerolinecolor:'#8195a7',tickformat:'.4~g'},
        annotations:finite.length?[]:[{xref:'paper',yref:'paper',x:.5,y:.5,text:'Percentage undefined: first release has zero cost',showarrow:false}]}};
  }
  const resultHref = version => version.full_count ? `corpus/?release=${version.crate_version}` : version.cohorts[0]?.url || (version.historical_abc_count || version.historical_raw_count ? `history/${version.historical_abc_count?'ir-fn-g8r-abc-vs-codegen-yosys-abc':'ir-fn-corpus-g8r-vs-yosys-abc'}/?crate_version=${version.crate_version}` : 'history/dataset.html');
  function measurementLinks(version) {
    const links=[];
    for(const [pairs,measurements,path,pairLabel,measurementLabel] of [
      [version.historical_abc_count,version.historical_abc_measurements,'ir-fn-g8r-abc-vs-codegen-yosys-abc','post-ABC pairs','post-ABC one-sided measurements'],
      [version.historical_raw_count,version.historical_raw_measurements,'ir-fn-corpus-g8r-vs-yosys-abc','raw G8r pairs','raw-path one-sided measurements'],
    ]) {
      if(pairs)links.push({count:pairs,label:pairLabel,href:`history/${path}/?crate_version=${version.crate_version}`});
      // Each paired sample accounts for one G8r and one Yosys measurement.
      const oneSided=measurements-2*pairs;
      if(oneSided>0)links.push({count:oneSided,label:measurementLabel,href:'history/dataset.html'});
    }
    return links;
  }
  if(typeof module!=='undefined')module.exports={change,headline,trendSpec,resultHref,measurementLinks};
  if(typeof document==='undefined')return;
  const el=id=>document.getElementById(id), fmt=n=>n.toLocaleString();
  function link(text,href){const a=document.createElement('a');a.textContent=text;a.href=href;return a}
  async function main(){
    const response=await fetch('dashboard.json');if(!response.ok)throw Error(`Dashboard: HTTP ${response.status}`);
    const data=await response.json();if(data.schema_version!==1||!data.versions.length)throw Error('Invalid dashboard');
    const latest=data.versions.at(-1), full=data.trends.find(t=>t.id==='full-corpus');
    el('latest-version').textContent=`v${latest.crate_version}`;
    el('latest-coverage').textContent=latest.full_count?`${fmt(latest.full_count)} inputs · full-corpus evaluation complete`:'Historical results available · full-corpus backfill not included';
    el('latest-link').href=resultHref(latest);
    el('version-count').textContent=`${data.versions.length} releases`;
    el('version-range').textContent=`v${data.versions[0].crate_version} → v${latest.crate_version} · including partial historical coverage`;
    const now=full?.points.at(-1), prior=full?.points.at(-2);
    el('latest-change').textContent=prior?headline(change(prior.cost,now.cost)):'One full release';
    el('latest-comparison').textContent=prior?`v${now.version} vs v${prior.version} · ${fmt(full.count)} identical inputs · nodes × depth`:'A second completed release is needed for comparison.';
    el('compare-link').href=prior?`corpus/?release=${now.version}&reference=release&baseline=${prior.version}`:'corpus/';
    for(const version of [...data.versions].reverse()){
      const tr=document.createElement('tr'), release=document.createElement('td'), corpus=document.createElement('td'), coverage=document.createElement('td'), other=document.createElement('td');
      release.append(link(`v${version.crate_version}`,resultHref(version)));
      if(version.full_count){const badge=document.createElement('span');badge.className='badge';badge.textContent='COMPLETE';corpus.append(badge,`${fmt(version.full_count)} inputs`)}else{corpus.textContent='Not included';corpus.className='equal'}
      for(const c of version.cohorts){coverage.append(link(`${c.label}: ${fmt(c.measured)} / ${fmt(c.total)}`,c.url));if(!c.complete){const small=document.createElement('small');small.textContent='Partial · excluded from trend';coverage.append(small)}}
      if(!version.cohorts.length){coverage.textContent='No fixed-cohort results';coverage.className='equal'}
      for(const {count,label,href} of measurementLinks(version))other.append(link(`${fmt(count)} ${label}`,href));
      if(!other.childNodes.length)other.textContent='—';
      tr.append(release,corpus,coverage,other);el('version-rows').append(tr);
    }
    for(const trend of data.trends){const option=document.createElement('option');option.value=trend.id;option.textContent=`${trend.label} · ${fmt(trend.count)} inputs`;el('cohort').append(option)}
    const query=new URLSearchParams(location.search), selected=query.get('cohort');
    el('cohort').value=data.trends.some(t=>t.id===selected)?selected:data.trends.find(t=>t.id==='mffc-v1'&&t.points.length>1)?.id||data.trends[0].id;
    let rendering=false,pending=false;
    async function render(){pending=true;if(rendering)return;rendering=true;
      try{while(pending){pending=false;el('dashboard').dataset.rendered='loading';const trend=data.trends.find(t=>t.id===el('cohort').value), first=trend.points[0], last=trend.points.at(-1), percent=change(first.cost,last.cost);
        el('trend-headline').textContent=headline(percent);el('trend-headline').className=percent===null||percent===0?'equal':percent<0?'lower':'higher';
        el('trend-scope').textContent=`v${last.version} vs v${first.version} · ${fmt(trend.count)} identical inputs in every release`;
        el('trend-note').textContent=`0% = v${first.version}. Y-axis is zoomed to show changes; releases are equally spaced. ${trend.points.length} complete releases; partial generations are not plotted.`;
        el('breadth').replaceChildren(...[['lower','↓ Lower cost',last.lower],['equal','= Unchanged',last.equal],['higher','↑ Higher cost',last.higher]].map(([cls,label,n])=>{const span=document.createElement('span');span.className=cls;span.textContent=`${label}: ${fmt(n)}`;return span}));
        const spec=trendSpec(trend);await Plotly.react('trend',spec.data,spec.layout,{responsive:true,displayModeBar:false});
        el('trend').removeAllListeners?.('plotly_click');el('trend').on('plotly_click',event=>{const url=event.points?.[0]?.customdata;if(url)location.href=url});
        const url=new URL(location.href);url.searchParams.set('cohort',trend.id);history.replaceState(null,'',url);
        el('dashboard').dataset.rendered='true';
      }}finally{rendering=false}
    }
    const failed=e=>{el('error').textContent=e.message;el('dashboard').dataset.rendered='error'};
    el('cohort').addEventListener('change',()=>void render().catch(failed));await render();
  }
  void main().catch(e=>{el('error').textContent=e.message;el('dashboard').dataset.rendered='error'});
})();
