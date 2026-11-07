// SPDX-License-Identifier: Apache-2.0
// Optional integration test: node scripts/test_crossmap_site_browser.cjs URL [SCREENSHOT_DIR]
// Uses a disposable headless profile, never an interactive browser profile.
'use strict';
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),cp=require('node:child_process'),assert=require('node:assert/strict');
const {once}=require('node:events');
const url=new URL(process.argv[2]),screenshots=process.argv[3];
const profile=fs.mkdtempSync(path.join(os.tmpdir(),'bvc-crossmap-browser-'));
const chrome=cp.spawn(process.env.BVC_CHROME||'google-chrome',[
  '--headless=new','--no-sandbox','--disable-gpu','--disable-dev-shm-usage',
  '--disable-background-networking','--disable-component-update','--disable-default-apps',
  '--disable-sync','--no-first-run','--remote-debugging-pipe',`--user-data-dir=${profile}`,'about:blank',
],{stdio:['ignore','ignore','ignore','pipe','pipe']});
const closed=once(chrome,'close'),requests=new Map(),errors=[];
let serial=0,buffer='',session;
chrome.stdio[4].on('data',chunk=>{
  buffer+=chunk.toString();let end;
  while((end=buffer.indexOf('\0'))>=0){
    const message=JSON.parse(buffer.slice(0,end));buffer=buffer.slice(end+1);
    if(message.id){const pending=requests.get(message.id);requests.delete(message.id);if(message.error)pending?.reject(Error(JSON.stringify(message.error)));else pending?.resolve(message.result);}
    if(message.method==='Runtime.exceptionThrown')errors.push(message.params.exceptionDetails);
    if(message.method==='Runtime.consoleAPICalled'&&message.params.type==='error')errors.push(message.params);
    if(message.method==='Log.entryAdded'&&message.params.entry.level==='error'){
      const entry=message.params.entry;
      if(!(entry.source==='network'&&entry.url&&new URL(entry.url).pathname==='/favicon.ico'))errors.push(entry);
    }
    if(message.method==='Network.loadingFailed'&&!message.params.canceled)errors.push(message.params);
  }
});
const send=(method,params={},browser=false)=>new Promise((resolve,reject)=>{
  const id=++serial;requests.set(id,{resolve,reject});
  chrome.stdio[3].write(JSON.stringify({id,method,params,...(session&&!browser?{sessionId:session}:{})})+'\0');
});
const evaluate=async expression=>{
  const response=await send('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});
  if(response.exceptionDetails)throw Error(JSON.stringify(response.exceptionDetails));
  return response.result.value;
};
const waitFor=async expression=>{
  const deadline=Date.now()+60000;
  do{if(await evaluate(expression))return;await new Promise(resolve=>setTimeout(resolve,100));}while(Date.now()<deadline);
  throw Error(`Timed out: ${expression}`);
};
const ready=()=>waitFor("document.getElementById('crossmap')?.dataset.rendered==='true'");
const shot=async name=>{
  if(!screenshots)return;fs.mkdirSync(screenshots,{recursive:true});
  const response=await send('Page.captureScreenshot',{format:'png'});
  fs.writeFileSync(path.join(screenshots,name+'.png'),Buffer.from(response.data,'base64'));
};
const click=async position=>{
  await send('Input.dispatchMouseEvent',{type:'mouseMoved',...position});
  await send('Input.dispatchMouseEvent',{type:'mousePressed',...position,button:'left',clickCount:1});
  await send('Input.dispatchMouseEvent',{type:'mouseReleased',...position,button:'left',clickCount:1});
};
const control=async(id,value,type='change')=>{
  await evaluate(`document.getElementById(${JSON.stringify(id)}).value=${JSON.stringify(value)};document.getElementById(${JSON.stringify(id)}).dispatchEvent(new Event(${JSON.stringify(type)}))`);
  await ready();
};
const key=point=>`${point.source}:${point.mapper}:${point.goal}`;
const pointKeys=['native-g8r:g8r:area','native-g8r:g8r:delay','yosys-abc:g8r:area','yosys-abc:g8r:delay','yosys-abc:abc:area','yosys-abc:abc:delay'].sort();
const point=(sample,source,mapper,goal)=>sample.points.find(p=>p.source===source&&p.mapper===mapper&&p.goal===goal);
const sortedCoordinates=coordinates=>coordinates.sort((a,b)=>a[0].localeCompare(b[0])||a[1]-b[1]||a[2]-b[2]);
const plotted=async id=>evaluate(`document.getElementById(${JSON.stringify(id)}).data.flatMap(trace=>trace.x.map((x,index)=>[trace.customdata[index],x,trace.y[index]]))`);
async function checkSelected(run,id){
  const sample=run.samples.find(s=>s.id===id);assert.ok(sample,'selected input belongs to this run');
  assert.equal(await evaluate("document.getElementById('input').value"),id);
  assert.equal(new URL(await evaluate('location.href')).searchParams.get('sample'),id);
  assert.deepEqual(await evaluate("Array.from(document.querySelectorAll('#point-rows tr'),row=>row.dataset.pointKey).sort()"),pointKeys);
  const expected=sample.points.map(p=>[p.area,p.delay_ps]).sort((a,b)=>a[0]-b[0]||a[1]-b[1]);
  const actual=await evaluate("document.getElementById('absolute').data.filter(trace=>trace.showlegend!==false).flatMap(trace=>trace.x.map((x,index)=>[x,trace.y[index]])).sort((a,b)=>a[0]-b[0]||a[1]-b[1])");
  assert.deepEqual(actual,expected);
  assert.equal(await evaluate("document.getElementById('absolute').data.filter(trace=>trace.showlegend!==false).length"),6);
  assert.equal(await evaluate("document.getElementById('input-id').textContent"),id);
  const compare=(a,b)=>Math.abs(a-b)<=1e-9*Math.max(1,Math.abs(a),Math.abs(b))?0:Math.sign(a-b);
  const frontier=sample.points.filter(p=>!sample.points.some(q=>{const area=compare(q.area,p.area),delay=compare(q.delay_ps,p.delay_ps);return area<=0&&delay<=0&&(area<0||delay<0);}));
  assert.deepEqual(await evaluate("Array.from(document.querySelectorAll('#point-rows tr')).filter(row=>row.lastChild.dataset.frontier==='true').map(row=>row.dataset.pointKey).sort()"),frontier.map(key).sort());
  assert.deepEqual(await evaluate("document.getElementById('absolute').data.filter(trace=>trace.showlegend===false).flatMap(trace=>trace.x.map((x,index)=>[x,trace.y[index]])).sort((a,b)=>a[0]-b[0]||a[1]-b[1])"),frontier.map(p=>[p.area,p.delay_ps]).sort((a,b)=>a[0]-b[0]||a[1]-b[1]));
}
async function checkOverview(run){
  const reference=await evaluate("document.getElementById('reference').value");
  const expectedSame=[],expectedInput=[];
  const sameCounts={zeroReferenceSamples:0,zeroCandidatePoints:0,unverifiedPoints:0,plottedPoints:0};
  const inputCounts={zeroReferencePoints:0,zeroCandidatePoints:0,unverifiedPoints:0,plottedPoints:0};
  for(const sample of run.samples){
    const ref=point(sample,'yosys-abc','abc',reference);
    if(ref.area===0||ref.delay_ps===0)++sameCounts.zeroReferenceSamples;
    else for(const candidate of sample.points.filter(p=>p.source==='yosys-abc')){
      if(candidate.mapper==='g8r'&&!candidate.verified)++sameCounts.unverifiedPoints;
      else if(candidate.area===0||candidate.delay_ps===0)++sameCounts.zeroCandidatePoints;
      else expectedSame.push([sample.id,candidate.area/ref.area,candidate.delay_ps/ref.delay_ps]);
    }
    for(const goal of ['area','delay']){
      const cross=point(sample,'yosys-abc','g8r',goal),native=point(sample,'native-g8r','g8r',goal);
      if(!cross.verified)++inputCounts.unverifiedPoints;
      else if(native.area===0||native.delay_ps===0)++inputCounts.zeroReferencePoints;
      else if(cross.area===0||cross.delay_ps===0)++inputCounts.zeroCandidatePoints;
      else expectedInput.push([sample.id,cross.area/native.area,cross.delay_ps/native.delay_ps]);
    }
  }
  sameCounts.plottedPoints=expectedSame.length;inputCounts.plottedPoints=expectedInput.length;
  assert.deepEqual(sortedCoordinates(await plotted('same-aig')),sortedCoordinates(expectedSame));
  assert.deepEqual(sortedCoordinates(await plotted('input-aig')),sortedCoordinates(expectedInput));
  assert.equal(await evaluate("Number(document.getElementById('crossmap').dataset.sampleCount)"),run.samples.length);
  for(const id of ['same-aig','input-aig']){
    const state=await evaluate(`(()=>{const plot=document.getElementById(${JSON.stringify(id)});return {xaxis:plot.layout.xaxis.type,yaxis:plot.layout.yaxis.type,noWebgl:plot.querySelectorAll('.no-webgl').length,points:plot.data.reduce((n,t)=>n+t.x.length,0),note:Object.fromEntries(Object.entries(document.getElementById(${JSON.stringify(id+'-note')}).dataset)),visible:document.getElementById(${JSON.stringify(id+'-note')}).getBoundingClientRect().height>0};})()`);
    assert.equal(state.xaxis,'log');assert.equal(state.yaxis,'log');assert.equal(state.noWebgl,0);assert.equal(Number(state.note.plottedPoints),state.points);assert.equal(state.visible,true);
    for(const [key,value] of Object.entries(id==='same-aig'?sameCounts:inputCounts))assert.equal(Number(state.note[key]),value,`${id}: ${key}`);
  }
  assert.deepEqual(await evaluate('window.__bvcPolicyViolations'),[]);
}
async function checkLegend(){
  const before=await evaluate("document.getElementById('same-aig').data.map(t=>t.visible??true)");
  const position=await evaluate("(()=>{const node=document.querySelector('#same-aig .legend .legendtoggle');if(!node)return null;const r=node.getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2};})()");
  assert.ok(position,'overview exposes a legend control');
  await click(position);await waitFor("document.getElementById('same-aig').data[0].visible==='legendonly'");
  await click(position);await waitFor("document.getElementById('same-aig').data[0].visible===true");
  assert.deepEqual(await evaluate("document.getElementById('same-aig').data.map(t=>t.visible??true)"),before);
}
async function checkPlotSelection(run){
  await evaluate("window.scrollTo(0,0);window.__crossmapClick=null;document.getElementById('same-aig').on('plotly_click',event=>window.__crossmapClick=event.points[0].customdata)");
  const position=await evaluate("(()=>{const plot=document.getElementById('same-aig'),trace=plot.data.find(t=>t.x.length),x=trace.x[0],y=trace.y[0],r=plot.getBoundingClientRect(),axis=plot._fullLayout;return {x:r.left+axis.xaxis._offset+axis.xaxis.l2p(Math.log10(x)),y:r.top+axis.yaxis._offset+axis.yaxis.l2p(Math.log10(y))};})()");
  await click(position);await waitFor('window.__crossmapClick!==null');await ready();
  const id=await evaluate('window.__crossmapClick');assert.ok(run.samples.some(sample=>sample.id===id));await checkSelected(run,id);
}
async function main(){
  const target=await send('Target.createTarget',{url:'about:blank'},true),attached=await send('Target.attachToTarget',{targetId:target.targetId,flatten:true},true);session=attached.sessionId;
  await send('Runtime.enable');await send('Page.enable');await send('Log.enable');await send('Network.enable');
  await send('Page.addScriptToEvaluateOnNewDocument',{source:"window.__bvcPolicyViolations=[];addEventListener('securitypolicyviolation',event=>window.__bvcPolicyViolations.push({directive:event.effectiveDirective,blocked:event.blockedURI,source:event.sourceFile,line:event.lineNumber}));"});
  const response=await fetch(new URL('crossmap-results.json',url));assert.equal(response.status,200);const catalog=await response.json();assert.equal(catalog.schema_version,1);
  assert.ok(catalog.runs.length>0);
  for(const run of catalog.runs){
    assert.equal(run.sample_count,run.samples.length);assert.ok(run.samples.length>0);assert.equal(run.verified_points,2*run.samples.length);
    for(const sample of run.samples){assert.deepEqual(sample.points.map(key).sort(),pointKeys);for(const p of sample.points){assert.ok(Number.isFinite(p.area)&&p.area>=0);assert.ok(Number.isFinite(p.delay_ps)&&p.delay_ps>=0);assert.ok(Number.isInteger(p.cell_count)&&p.cell_count>=0);}}
  }
  await send('Emulation.setDeviceMetricsOverride',{width:1440,height:1100,deviceScaleFactor:1,mobile:false});
  await send('Page.navigate',{url:url.href});await ready();
  const policy=await evaluate("Object.fromEntries(document.querySelector('meta[http-equiv=Content-Security-Policy]').content.split(';').map(part=>part.trim().split(/ +/)).filter(parts=>parts[0]).map(([name,...values])=>[name,values]))");
  assert.deepEqual(policy['script-src'],["'self'"]);
  assert.equal(await evaluate("Array.from(document.scripts,script=>new URL(script.src).pathname.split('/').at(-1)).filter(name=>name==='plotly-strict-2.35.2.min.js').length"),1);
  const selectedRun=await evaluate("document.getElementById('run').value"),run=catalog.runs.find(r=>r.id===selectedRun);assert.ok(run);
  await checkOverview(run);await checkLegend();await checkPlotSelection(run);await send('Input.dispatchMouseEvent',{type:'mouseMoved',x:10,y:10});await evaluate("document.querySelectorAll('.notifier').forEach(node=>node.remove());window.scrollTo(0,0)");await new Promise(resolve=>setTimeout(resolve,300));await evaluate("document.querySelectorAll('.notifier,.plotly-notifier,.notifier-container').forEach(node=>node.remove())");await shot('crossmap-desktop');
  await control('reference','area');await checkOverview(run);await control('reference','delay');await checkOverview(run);
  const wanted=run.samples.at(-1);
  await control('search',wanted.id,'input');
  assert.equal(await evaluate("document.getElementById('sample-rows').children.length"),1);
  await evaluate("document.querySelector('#sample-rows button,#sample-rows a').click()");await ready();await checkSelected(run,wanted.id);
  const saved=await evaluate('location.href');
  await send('Page.navigate',{url:saved});await ready();await checkSelected(run,wanted.id);await checkOverview(run);
  const other=run.samples.find(sample=>sample.id!==wanted.id);
  if(other){await control('input',other.id);await checkSelected(run,other.id);await evaluate('history.back()');await waitFor(`new URL(location.href).searchParams.get('sample')===${JSON.stringify(wanted.id)}`);await ready();await checkSelected(run,wanted.id);}
  const zero=run.samples.find(sample=>sample.points.some(p=>p.area===0||p.delay_ps===0));
  if(zero){await control('input',zero.id);await checkSelected(run,zero.id);}
  await control('search','','input');
  await control('input',wanted.id);await evaluate("document.getElementById('selection').scrollIntoView()");await shot('crossmap-input');
  await send('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});
  const longest=run.samples.reduce((a,b)=>a.label.length>=b.label.length?a:b);
  await control('input',longest.id);await checkSelected(run,longest.id);
  await evaluate('window.scrollTo(0,0)');await new Promise(resolve=>setTimeout(resolve,400));await ready();
  const mobile=await evaluate('({width:innerWidth,content:document.documentElement.scrollWidth,title:document.getElementById("input-title").textContent,titleWidth:document.getElementById("input-title").getBoundingClientRect().width,titleContent:document.getElementById("input-title").scrollWidth,titleBox:document.getElementById("input-title").clientWidth})');
  assert.equal(mobile.width,390,`mobile layout viewport keeps the device width: ${JSON.stringify(mobile)}`);
  assert.ok(mobile.content<=mobile.width,`mobile page has no horizontal overflow: ${JSON.stringify(mobile)}`);
  assert.ok(mobile.titleContent<=mobile.titleBox+1,`mobile function title fits its panel: ${JSON.stringify(mobile)}`);
  for(const id of ['same-aig','input-aig','absolute']){
    const bounds=await evaluate(`(()=>{const plot=document.getElementById(${JSON.stringify(id)}),legend=plot.querySelector('.legend'),outer=plot.getBoundingClientRect(),box=legend.getBoundingClientRect();return {top:box.top-outer.top,bottom:box.bottom-outer.top,gridTop:plot._fullLayout._size.t,right:box.right-outer.left,width:outer.width};})()`);
    assert.ok(bounds.top>=-2&&bounds.bottom<=bounds.gridTop+2&&bounds.right<=bounds.width+2,`${id}: ${JSON.stringify(bounds)}`);
  }
  await shot('crossmap-mobile');await evaluate("document.getElementById('selection').scrollIntoView()");await shot('crossmap-mobile-input');
  assert.deepEqual(await evaluate('window.__bvcPolicyViolations'),[]);assert.deepEqual(errors,[]);
  console.log(`Crossmap browser integration passed (${run.samples.length} inputs)`);
}
const timer=setTimeout(()=>{console.error('Crossmap browser test timed out');process.exitCode=1;chrome.kill('SIGKILL');},180000);
Promise.race([main(),new Promise((_,reject)=>chrome.once('error',reject))]).catch(error=>{console.error(error);process.exitCode=1;}).finally(async()=>{
  clearTimeout(timer);
  if(chrome.exitCode===null){try{await Promise.race([send('Browser.close',{},true),new Promise(resolve=>setTimeout(resolve,3000))]);}catch{}if(chrome.exitCode===null)chrome.kill();}
  await closed;await fs.promises.rm(profile,{recursive:true,force:true,maxRetries:20,retryDelay:100});
});
