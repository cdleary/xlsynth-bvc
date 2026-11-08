// SPDX-License-Identifier: Apache-2.0
// Optional integration test: node scripts/test_techmap_site_browser.cjs URL [SCREENSHOT_DIR]
// Uses a disposable headless profile, never an interactive browser profile.
'use strict';
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),cp=require('node:child_process'),assert=require('node:assert/strict');
const {once}=require('node:events');
const {normalizedSpec,absoluteSpec,overviewSamples,counts}=require('../src/site_assets/techmap.js');
const url = new URL(process.argv[2]);
const screenshots = process.argv[3];
const requireWebgl=process.env.BVC_TECHMAP_REQUIRE_WEBGL==='1';
const forceSvg=process.env.BVC_TECHMAP_FORCE_SVG==='1';
assert.ok(!(requireWebgl&&forceSvg),'WebGL-required and forced-SVG checks must run separately');
const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'bvc-techmap-browser-'));
const chrome = cp.spawn(process.env.BVC_CHROME || 'google-chrome', [
  '--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
  '--disable-background-networking', '--disable-component-update', '--disable-default-apps',
  '--disable-sync', '--no-first-run', '--remote-debugging-pipe',
  `--user-data-dir=${profile}`, 'about:blank',
], {stdio: ['ignore', 'ignore', 'ignore', 'pipe', 'pipe']});
const browserClosed=once(chrome,'close');
let serial = 0, buffer = '', session;
const requests = new Map(), errors = [];
chrome.stdio[4].on('data', chunk => {
  buffer += chunk.toString();
  let end;
  while ((end = buffer.indexOf('\0')) >= 0) {
    const message = JSON.parse(buffer.slice(0, end));
    buffer = buffer.slice(end + 1);
    if (message.id) {
      const pending = requests.get(message.id);
      requests.delete(message.id);
      if (message.error) pending?.reject(Error(JSON.stringify(message.error)));
      else pending?.resolve(message.result);
    }
    if (message.method === 'Runtime.exceptionThrown') errors.push(message.params.exceptionDetails);
    if (message.method === 'Runtime.consoleAPICalled' && message.params.type === 'error') errors.push(message.params);
    if (message.method === 'Log.entryAdded' && message.params.entry.level === 'error') {
      const entry=message.params.entry;
      // Chrome requests this optional browser-tab icon even when the page declares none.
      if(!(entry.source==='network'&&entry.url&&new URL(entry.url).pathname==='/favicon.ico'))errors.push(entry);
    }
    if (message.method === 'Network.loadingFailed' && !message.params.canceled) errors.push(message.params);
  }
});
const send = (method, params = {}, browser = false) => new Promise((resolve, reject) => {
  const id = ++serial;
  requests.set(id, {resolve, reject});
  chrome.stdio[3].write(JSON.stringify({id, method, params, ...(session && !browser ? {sessionId:session} : {})}) + '\0');
});
const evaluate = async expression => {
  const result = await send('Runtime.evaluate', {expression, returnByValue:true, awaitPromise:true});
  if (result.exceptionDetails) throw Error(JSON.stringify(result.exceptionDetails));
  return result.result.value;
};
const waitFor = async expression => {
  const deadline = Date.now() + 60000;
  do {
    if (await evaluate(expression)) return;
    await new Promise(resolve => setTimeout(resolve, 100));
  } while (Date.now() < deadline);
  throw Error(`Timed out: ${expression}`);
};
const load = async relative => {
  const response = await fetch(new URL(relative, url));
  assert.equal(response.status, 200);
  return response.json();
};
const shot = async name => {
  if (!screenshots) return;
  fs.mkdirSync(screenshots, {recursive:true});
  const result = await send('Page.captureScreenshot', {format:'png'});
  fs.writeFileSync(path.join(screenshots, name + '.png'), Buffer.from(result.data, 'base64'));
};

const ready=()=>waitFor("document.getElementById('techmap')?.dataset.rendered==='true'");
const control=async(id,value)=>{
  await evaluate(`document.getElementById('techmap').dataset.rendered='loading';document.getElementById(${JSON.stringify(id)}).value=${JSON.stringify(value)};document.getElementById(${JSON.stringify(id)}).dispatchEvent(new Event('change'))`);
  await ready();
};
async function selectedRun(catalog){
  const values=await evaluate("[document.getElementById('release').value,document.getElementById('cohort').value]");
  const meta=catalog.runs.find(r=>r.version===values[0]&&r.cohort_id===values[1]);assert.ok(meta);
  const samples=[];
  for(const shard of meta.metric_shards)samples.push(...(await load(shard)).samples);
  return {...meta,samples};
}
async function checkPlots(run){
  const goal=await evaluate("document.getElementById('reference').value");
  const plotted=await evaluate("Number(document.getElementById('techmap').dataset.plottedInputs)");
  const selected=overviewSamples(run.samples,plotted===run.samples.length);
  assert.equal(plotted,selected.length);
  const measuredCount=await evaluate("document.getElementById('techmap').dataset.sampleCount??null");
  if(measuredCount!==null)assert.equal(Number(measuredCount),run.samples.length);
  const summary=counts(run.samples,goal);
  const summaryCounts=await evaluate("document.getElementById('summary').dataset.better===undefined?null:Object.fromEntries(['better','tradeoff','equal','worse','missing'].map(key=>[key,Number(document.getElementById('summary').dataset[key])]))");
  if(summaryCounts!==null)assert.deepEqual(summaryCounts,Object.fromEntries(['better','tradeoff','equal','worse','missing'].map(key=>[key,summary[key]])));
  const expected=normalizedSpec(selected,goal);
  assert.deepEqual(await evaluate("document.getElementById('normalized').data.map(trace=>[trace.x,trace.y])"),expected.data.map(trace=>[trace.x,trace.y]));
  assert.deepEqual(await evaluate("document.getElementById('normalized').data.map(t=>[t.name,t.legendgroup,t.marker.color,t.marker.symbol])"),expected.data.map(t=>[t.name,t.legendgroup,t.marker.color,t.marker.symbol]));
  assert.deepEqual(await evaluate('window.__bvcPolicyViolations'),[]);
  const renderer=await evaluate("(()=>{const plot=document.getElementById('normalized');return {types:plot.data.map(t=>t.type),failures:plot.querySelectorAll('.no-webgl').length,scenes:Object.values(plot._fullLayout._plots).filter(p=>p._scene).map(p=>({scatter:typeof p._scene.scatter2d,count:p._scene.count})),canvases:Array.from(plot.querySelectorAll('canvas.gl-canvas-context'),c=>({width:c.width,height:c.height}))};})()");
  assert.equal(renderer.failures,0,JSON.stringify(renderer));
  if(renderer.types.some(type=>type==='scattergl')){
    assert.ok(renderer.scenes.length>0,JSON.stringify(renderer));
    assert.ok(renderer.scenes.every(scene=>scene.scatter==='function'&&scene.count>0),JSON.stringify(renderer));
    assert.ok(renderer.canvases.length>0&&renderer.canvases.every(canvas=>canvas.width>0&&canvas.height>0),JSON.stringify(renderer));
  }
  if(forceSvg){
    assert.ok(renderer.types.every(type=>type==='scatter'));
    assert.equal(plotted,Math.min(run.samples.length,2000));
  }
  if(requireWebgl&&normalizedSpec(run.samples,goal).data.some(t=>t.type==='scattergl')){
    assert.equal(plotted,run.samples.length);
    assert.ok(renderer.types.every(type=>type==='scattergl'),JSON.stringify(renderer));
  }
  assert.ok(await evaluate("document.getElementById('sample-rows').children.length<=100"));
  assert.ok(await evaluate("document.getElementById('input').options.length<=201"));
}
async function checkLegend(){
  const toggle=await evaluate("(()=>{const node=document.querySelector('#normalized .legend .legendtoggle');if(!node)return null;const r=node.getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2};})()");
  if(!toggle)return;
  const click=async count=>{await send('Input.dispatchMouseEvent',{type:'mousePressed',...toggle,button:'left',clickCount:count});await send('Input.dispatchMouseEvent',{type:'mouseReleased',...toggle,button:'left',clickCount:count});};
  await click(1);await waitFor("document.getElementById('normalized').data[0].visible==='legendonly'");
  await click(1);await waitFor("document.getElementById('normalized').data[0].visible===true");
  const active=await evaluate("document.getElementById('normalized').data.filter(t=>t.x.length).length");
  if(active>1){
    await click(1);await click(2);
    await waitFor("(()=>{const data=document.getElementById('normalized').data;return data[0].visible===true&&data.slice(1).filter(t=>t.x.length).every(t=>t.visible==='legendonly');})()");
    await click(1);await click(2);
    await waitFor("document.getElementById('normalized').data.filter(t=>t.x.length).every(t=>t.visible===true)");
  }
  await evaluate("document.querySelectorAll('.notifier').forEach(node=>node.remove())");
}
async function checkLegendLayout(){
  for(const id of ['normalized','absolute']){
    const box=await evaluate(`(()=>{const plot=document.getElementById(${JSON.stringify(id)}),legend=plot.querySelector('.legend');if(!legend)return null;const outer=plot.getBoundingClientRect(),bounds=legend.getBoundingClientRect();return {top:bounds.top-outer.top,bottom:bounds.bottom-outer.top,gridTop:plot._fullLayout._size.t,right:bounds.right-outer.left,width:outer.width};})()`);
    if(box){assert.ok(box.top>=-2,JSON.stringify(box));assert.ok(box.bottom<=box.gridTop+2,JSON.stringify(box));assert.ok(box.right<=box.width+2,JSON.stringify(box));}
  }
}
async function checkDashboard(){
  const rootUrl=new URL('../',url),response=await fetch(new URL('dashboard.json',rootUrl));
  if(response.status===404)return; // A standalone leaf preview has no parent.
  assert.equal(response.status,200);const dashboard=await response.json(),latest=dashboard.versions.at(-1),latestAig=[...dashboard.versions].reverse().find(v=>v.full_count>0),latestTechmap=[...dashboard.versions].reverse().find(v=>v.techmap_cohorts?.length);
  await send('Emulation.setDeviceMetricsOverride',{width:1440,height:1100,deviceScaleFactor:1,mobile:false});
  await send('Page.navigate',{url:new URL('index.html',rootUrl).href});
  await waitFor("document.getElementById('dashboard')?.dataset.rendered==='true'");
  const stage=await evaluate("document.getElementById('stage').value");
  if(latestAig){
    assert.equal(await evaluate("document.getElementById('latest-aig-version').textContent"),`v${latestAig.crate_version}`);
    const target=new URL(await evaluate("document.getElementById('latest-aig-link').href"));
    assert.equal(target.pathname,new URL('corpus/',rootUrl).pathname);
    assert.equal(target.searchParams.get('release'),latestAig.crate_version);
    assert.equal(target.searchParams.get('stage'),'abc');
    assert.equal(target.searchParams.get('reference'),'yosys');
  }
  assert.deepEqual(await evaluate("Array.from(document.querySelectorAll('#version-rows tr'),r=>r.firstChild.textContent)"),[...dashboard.versions].reverse().map(v=>`v${v.crate_version}`));
  if(latestTechmap){
    assert.equal(await evaluate("document.getElementById('techmap-link').hidden"),false);
    assert.equal(await evaluate("document.getElementById('latest-techmap-version').textContent"),`v${latestTechmap.crate_version}`);
    const target=new URL(await evaluate("document.getElementById('latest-techmap-link').href"));
    assert.equal(target.pathname,new URL('tech-mapped-asap7/',rootUrl).pathname);
    assert.equal(target.searchParams.get('release'),latestTechmap.crate_version);
    assert.ok(latestTechmap.techmap_cohorts.some(c=>c.id===target.searchParams.get('cohort')));
    assert.equal(await evaluate("document.getElementById('techmap-link').href"),target.href);
  }
  if(!latest.full_count&&latest.techmap_cohorts?.length){
    assert.equal(await evaluate("document.querySelector('#version-rows tr [data-dimension=\"ir-aig\"]').querySelectorAll('a[href^=\"corpus/\"]').length"),0);
    const urls=await evaluate("Array.from(document.querySelector('#version-rows tr [data-dimension=\"techmap\"]').querySelectorAll('a'),a=>a.getAttribute('href'))");
    for(const cohort of latest.techmap_cohorts)assert.ok(urls.includes(cohort.url));
  }
  const cohort=await evaluate("document.getElementById('cohort').value"),series=dashboard.trends.find(t=>t.id===cohort).stages[stage];
  assert.deepEqual(await evaluate("Array.from(document.getElementById('release').options,o=>o.value)"),series.points.map(p=>p.version));
  assert.equal(await evaluate("document.getElementById('error').textContent"),'');
  assert.deepEqual(await evaluate('window.__bvcPolicyViolations'),[]);
  await shot('dashboard-techmap-desktop');
}
(async()=>{
  const target=await send('Target.createTarget',{url:'about:blank'},true);
  const attached=await send('Target.attachToTarget',{targetId:target.targetId,flatten:true},true);session=attached.sessionId;
  await send('Runtime.enable');await send('Page.enable');await send('Log.enable');await send('Network.enable');
  await send('Page.addScriptToEvaluateOnNewDocument',{source:"window.__bvcPolicyViolations=[];addEventListener('securitypolicyviolation',event=>window.__bvcPolicyViolations.push({directive:event.effectiveDirective,blocked:event.blockedURI,source:event.sourceFile,line:event.lineNumber}));"+(forceSvg?"const getContext=HTMLCanvasElement.prototype.getContext;HTMLCanvasElement.prototype.getContext=function(type,...args){return /^(webgl2?|experimental-webgl)$/.test(type)?null:getContext.call(this,type,...args);};":"")});
  await send('Emulation.setDeviceMetricsOverride',{width:1440,height:1100,deviceScaleFactor:1,mobile:false});
  await send('Page.navigate',{url:url.href});await ready();
  const catalog=await load('results.json');let run=await selectedRun(catalog);await checkPlots(run);await checkLegend();await shot('techmap-desktop');await evaluate("document.querySelector('.selection').scrollIntoView()");await shot('techmap-input');await evaluate('window.scrollTo(0,0)');
  const initialCohort=run.cohort_id,initialVersion=run.version;
  for(const meta of catalog.runs.filter(r=>r.version===initialVersion&&r.cohort_id!==initialCohort)){
    await control('cohort',meta.cohort_id);run=await selectedRun(catalog);await checkPlots(run);
  }
  await control('cohort',initialCohort);run=await selectedRun(catalog);
  if(run.supported_goals.includes('area')){await control('reference','area');await checkPlots(run);}
  if(run.samples.length>100){
    const before=await evaluate("document.querySelector('#sample-rows .sample-id').textContent");
    await evaluate("document.getElementById('next-page').click()");
    assert.notEqual(await evaluate("document.querySelector('#sample-rows .sample-id').textContent"),before);
  }
  if(run.samples.length>200){
    await evaluate("document.getElementById('next-page').click();document.getElementById('techmap').dataset.rendered='loading';document.querySelector('#sample-rows .sample-link').click()");await ready();
    const id=await evaluate("document.getElementById('input').value");assert.equal(id,run.samples[200].id);
  }
  const older=catalog.runs.find(r=>r.version==='0.75.0');
  if(older){
    await control('release','0.75.0');run=await selectedRun(catalog);const delayReference=run.supported_goals.includes('delay');
    assert.equal(await evaluate("document.querySelector('#reference option[value=delay]').disabled"),!delayReference);
    if(delayReference)await control('reference','delay');else assert.equal(await evaluate("document.getElementById('reference').value"),'area');
    assert.equal(run.samples.some(s=>s.points.some(p=>p.tool==='g8r'&&p.goal==='delay')),false);
    await checkPlots(run);
  }
  if(run.samples.length){
    const wanted=run.samples.at(-1);
    await evaluate(`document.getElementById('search').value=${JSON.stringify(wanted.id)};document.getElementById('search').dispatchEvent(new Event('input'))`);
    assert.equal(await evaluate("document.getElementById('sample-rows').children.length"),1);
    await evaluate("document.getElementById('techmap').dataset.rendered='loading';document.querySelector('#sample-rows .sample-link').click()");await ready();
    assert.equal(await evaluate("document.getElementById('input').value"),wanted.id);
    const absolute=await evaluate("document.getElementById('absolute').data.map(t=>[t.x,t.y])");
    const expectedAbsolute=absoluteSpec(wanted);
    assert.deepEqual(absolute,expectedAbsolute.data.map(t=>[t.x,t.y]));
    assert.deepEqual(await evaluate("document.getElementById('absolute').data.map(t=>[t.name,t.marker.color,t.marker.symbol])"),expectedAbsolute.data.map(t=>[t.name,t.marker.color,t.marker.symbol]));
    assert.deepEqual(await evaluate("Array.from(document.querySelectorAll('#point-rows .series-cue'),node=>getComputedStyle(node).color)"),expectedAbsolute.data.map(t=>{const hex=t.marker.color;return `rgb(${parseInt(hex.slice(1,3),16)}, ${parseInt(hex.slice(3,5),16)}, ${parseInt(hex.slice(5,7),16)})`;}));
  }
  await send('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});await new Promise(resolve=>setTimeout(resolve,350));await ready();await shot('techmap-mobile');await evaluate("document.querySelector('.selection').scrollIntoView()");await shot('techmap-mobile-input');
  await control('release',initialVersion);await control('cohort',initialCohort);run=await selectedRun(catalog);
  if(run.supported_goals.includes('delay'))await control('reference','delay');
  await evaluate('window.scrollTo(0,0)');await ready();await checkLegendLayout();await shot('techmap-mobile-four-series');
  await evaluate("document.querySelector('.selection').scrollIntoView()");await shot('techmap-mobile-four-input');
  await checkDashboard();assert.deepEqual(errors,[]);console.log('Techmap browser integration passed');
})().catch(error=>{console.error(error);process.exitCode=1;}).finally(async()=>{
  if(chrome.exitCode===null){try{await Promise.race([send('Browser.close',{},true),new Promise(resolve=>setTimeout(resolve,3000))]);}catch{}if(chrome.exitCode===null)chrome.kill();}
  await browserClosed;
  await fs.promises.rm(profile,{recursive:true,force:true,maxRetries:20,retryDelay:100});
});
