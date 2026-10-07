// SPDX-License-Identifier: Apache-2.0
// Optional integration test: node scripts/test_dashboard_browser.cjs URL [SCREENSHOT_DIR]
'use strict';
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),cp=require('node:child_process'),assert=require('node:assert/strict');
const {once}=require('node:events');
const {trendSpec,resultHref}=require('../src/site_assets/dashboard.js');
const url=new URL(process.argv[2]),screenshots=process.argv[3];
const profile=fs.mkdtempSync(path.join(os.tmpdir(),'bvc-dashboard-browser-'));
const chrome=cp.spawn(process.env.BVC_CHROME||'google-chrome',['--headless=new','--no-sandbox','--disable-gpu','--disable-dev-shm-usage','--disable-background-networking','--disable-component-update','--disable-default-apps','--disable-sync','--no-first-run','--remote-debugging-pipe',`--user-data-dir=${profile}`,'about:blank'],{stdio:['ignore','ignore','ignore','pipe','pipe']});
let serial=0,buffer='',session;
const requests=new Map(),errors=[],resources=[];
chrome.stdio[4].on('data',chunk=>{buffer+=chunk.toString();let end;while((end=buffer.indexOf('\0'))>=0){const message=JSON.parse(buffer.slice(0,end));buffer=buffer.slice(end+1);if(message.id){const pending=requests.get(message.id);requests.delete(message.id);if(message.error)pending?.reject(Error(JSON.stringify(message.error)));else pending?.resolve(message.result)}if(message.method==='Runtime.exceptionThrown')errors.push(message.params.exceptionDetails);if(message.method==='Network.requestWillBeSent')resources.push(message.params.request.url)}});
const send=(method,params={},browser=false)=>new Promise((resolve,reject)=>{const id=++serial;requests.set(id,{resolve,reject});chrome.stdio[3].write(JSON.stringify({id,method,params,...(session&&!browser?{sessionId:session}:{})})+'\0')});
const evaluate=async expression=>{const r=await send('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value};
const waitFor=async expression=>{const deadline=Date.now()+60000;do{if(await evaluate(expression))return;await new Promise(r=>setTimeout(r,100))}while(Date.now()<deadline);throw Error(`Timed out: ${expression}`)};
const ready=()=>waitFor("document.getElementById('dashboard')?.dataset.rendered==='true'");
const shot=async name=>{if(!screenshots)return;fs.mkdirSync(screenshots,{recursive:true});const r=await send('Page.captureScreenshot',{format:'png'});fs.writeFileSync(path.join(screenshots,`${name}.png`),Buffer.from(r.data,'base64'))};
async function main(){
  const response=await fetch(new URL('dashboard.json',url));assert.equal(response.status,200);const data=await response.json();
  const {targetId}=await send('Target.createTarget',{url:'about:blank'},true);session=(await send('Target.attachToTarget',{targetId,flatten:true},true)).sessionId;
  await send('Runtime.enable');await send('Page.enable');await send('Network.enable');
  await send('Emulation.setDeviceMetricsOverride',{width:1440,height:1180,deviceScaleFactor:1,mobile:false});await send('Page.navigate',{url:url.href});await ready();
  assert.equal(await evaluate("document.getElementById('latest-version').textContent"),`v${data.versions.at(-1).crate_version}`);
  assert.equal(await evaluate("document.getElementById('latest-link').getAttribute('href')"),resultHref(data.versions.at(-1)));
  assert.deepEqual(await evaluate("Array.from(document.querySelectorAll('#version-rows tr'),r=>r.firstChild.textContent)"),[...data.versions].reverse().map(v=>`v${v.crate_version}`));
  assert.deepEqual(await evaluate("Array.from(document.getElementById('cohort').options,o=>o.value)"),data.trends.map(t=>t.id));
  await shot('dashboard-desktop');
  for(const trend of data.trends){
    await evaluate(`document.getElementById('cohort').value=${JSON.stringify(trend.id)};document.getElementById('cohort').dispatchEvent(new Event('change'))`);await ready();
    assert.deepEqual(await evaluate("document.getElementById('trend').data[0].y"),trendSpec(trend).data[0].y);
    assert.deepEqual(await evaluate("document.getElementById('trend').data[0].customdata"),trend.points.map(p=>p.url));
    assert.equal(trend.points.at(-1).lower+trend.points.at(-1).equal+trend.points.at(-1).higher,trend.count);
  }
  // The splash only fetches the small dashboard projection, never sample shards.
  assert.deepEqual([...new Set(resources.filter(r=>new URL(r).pathname.endsWith('.json')).map(r=>new URL(r).pathname))],[new URL('dashboard.json',url).pathname]);
  const links=await evaluate("Array.from(document.querySelectorAll('a[href]'),a=>a.href)");
  assert.equal(links.some(href=>['history/','history/index.html'].some(p=>new URL(href).pathname===new URL(p,url).pathname)),false);
  for(const href of new Set(links)){const r=await fetch(href);assert.equal(r.status,200,href);await r.body.cancel()}
  // Verify every generated detail page, including nested campaign routes, has
  // exactly the same global destinations and no link back to a second overview.
  const catalogResponse=await fetch(new URL('history/catalog.json',url));assert.equal(catalogResponse.status,200);
  const catalog=await catalogResponse.json();
  const detailPages=['corpus/','history/progression.html','history/releases.html','history/runs.html','history/dataset.html','history/mffc-discrepancies.html','history/ir-fn-g8r-abc-vs-codegen-yosys-abc/','history/ir-fn-corpus-g8r-vs-yosys-abc/',...catalog.runs.map(r=>'history/'+r.page_url)];
  for(const page of detailPages){
    const target=new URL(page,url),response=await fetch(target);assert.equal(response.status,200);const html=await response.text();
    const parsed=await evaluate(`(()=>{const doc=new DOMParser().parseFromString(${JSON.stringify(html)},'text/html');return {nav:Array.from(doc.querySelectorAll('nav[aria-label="Primary navigation"] a'),a=>[a.textContent,a.getAttribute('href')]),back:Array.from(doc.querySelectorAll('a')).filter(a=>a.textContent.trim()==='← Results').map(a=>a.getAttribute('href'))}})()`);
    assert.deepEqual(parsed.nav.map(([label,href])=>[label,new URL(href,target).href]),[['Results','index.html'],['Latest','index.html#latest'],['All versions','index.html#versions'],['Progression','history/progression.html']].map(([label,href])=>[label,new URL(href,url).href]));
    assert.ok(parsed.back.every(href=>new URL(href,target).href===new URL('index.html',url).href));
  }
  for(const oldLanding of ['history/','history/index.html']){
    await send('Page.navigate',{url:new URL(oldLanding,url).href});await ready();
    assert.equal(await evaluate('location.pathname'),new URL('index.html',url).pathname);
    assert.equal(await evaluate("document.getElementById('home-overview')"),null);
  }
  await send('Emulation.setDeviceMetricsOverride',{width:390,height:1000,deviceScaleFactor:1,mobile:true});await evaluate('window.dispatchEvent(new Event("resize"))');
  await new Promise(r=>setTimeout(r,500));
  assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true);await shot('dashboard-mobile');
  await send('Emulation.setDeviceMetricsOverride',{width:1440,height:1180,deviceScaleFactor:1,mobile:false});
  const historical=data.trends.find(t=>t.id!=='full-corpus'&&t.points.length>1);
  if(historical){
    for(const point of [historical.points[0],historical.points.at(-1)]){
      const target=new URL(point.url,url);
      await send('Page.navigate',{url:target.href});
      await waitFor("document.getElementById('progression')?.dataset.progressionRendered==='true'");
      assert.equal(await evaluate("document.getElementById('progression-cohort').value"),historical.id);
      assert.equal(await evaluate("document.getElementById('current-version').value"),target.searchParams.get('current'));
      assert.equal(await evaluate("document.getElementById('baseline-version').value"),target.searchParams.get('baseline'));
    }
    await shot('progression-navigation');
    await evaluate("document.querySelector('.bvc-navigation-home').click()");await ready();
    assert.equal(await evaluate('location.pathname'),new URL('index.html',url).pathname);
  }
  const trend=data.trends.find(t=>t.id==='full-corpus'),last=trend.points.at(-1);
  await evaluate(`document.getElementById('cohort').value='full-corpus';document.getElementById('cohort').dispatchEvent(new Event('change'))`);await ready();await shot('dashboard-full-corpus');
  await evaluate(`document.getElementById('trend').emit('plotly_click',{points:[{customdata:${JSON.stringify(last.url)}}]})`);
  await waitFor("document.getElementById('corpus-site')?.dataset.rendered==='true'");
  assert.equal(await evaluate("document.getElementById('release').value"),last.version);
  await shot('corpus-navigation');
  await evaluate("Array.from(document.querySelectorAll('.bvc-navigation a')).find(a=>a.textContent==='All versions').click()");await ready();
  assert.equal(await evaluate('location.hash'),'#versions');
  assert.deepEqual(errors,[]);
  console.log(JSON.stringify({versions:data.versions.map(v=>v.crate_version),cohorts:data.trends.map(t=>({id:t.id,count:t.count,releases:t.points.length})),desktop:true,mobile:true,links:true,pointNavigation:true,errors},null,2));
}
const timer=setTimeout(()=>{console.error('Dashboard browser test timed out');chrome.kill('SIGKILL');process.exitCode=1},180000);
main().catch(e=>{console.error(e);process.exitCode=1}).finally(async()=>{clearTimeout(timer);const exited=once(chrome,'exit');chrome.kill('SIGTERM');await exited;await fs.promises.rm(profile,{recursive:true,force:true,maxRetries:10,retryDelay:200})});
