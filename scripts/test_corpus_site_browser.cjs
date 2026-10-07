// SPDX-License-Identifier: Apache-2.0
// Optional integration test: node scripts/test_corpus_site_browser.cjs URL [SCREENSHOT_DIR]
// Uses a disposable headless profile, never an interactive browser profile.
'use strict';
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const cp = require('node:child_process');
const assert = require('node:assert/strict');
const {once} = require('node:events');
const {pairRows, filterRows} = require('../src/site_assets/corpus.js');

const url = new URL(process.argv[2]);
const screenshots = process.argv[3];
const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'bvc-corpus-browser-'));
const chrome = cp.spawn(process.env.BVC_CHROME || 'google-chrome', [
  '--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
  '--disable-background-networking', '--disable-component-update', '--disable-default-apps',
  '--disable-sync', '--no-first-run', '--remote-debugging-pipe',
  `--user-data-dir=${profile}`, 'about:blank',
], {stdio: ['ignore', 'ignore', 'ignore', 'pipe', 'pipe']});
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
const control = async (id, value) => evaluate(`document.getElementById(${JSON.stringify(id)}).value=${JSON.stringify(value)};document.getElementById(${JSON.stringify(id)}).dispatchEvent(new Event('change'))`);
const ready = () => waitFor("document.getElementById('corpus-site')?.dataset.rendered==='true'");
async function checkPlots(rows) {
  const lengths = await evaluate("Object.fromEntries(['product','le','nodes','delta'].map(id=>[id,document.getElementById(id).data[0].x.length]))");
  assert.deepEqual(lengths, {
    product:rows.filter(r=>r.cost>0 && r.reference_cost>0).length,
    le:rows.filter(r=>Number.isFinite(r.g8r_le) && Number.isFinite(r.reference_le)).length,
    nodes:rows.filter(r=>r.g8r_nodes>0 && r.reference_nodes>0).length,
    delta:rows.length,
  });
  assert.equal(await evaluate("Number(document.getElementById('corpus-site').dataset.sampleCount)"), rows.length);
  assert.deepEqual(await evaluate("document.getElementById('delta').data[0].x"), rows.map(r=>r.g8r_nodes-r.reference_nodes));
}

async function main() {
  const catalog = await load('catalog.json');
  const versions = catalog.generations.map(g=>g.crate_version);
  const {targetId} = await send('Target.createTarget', {url:'about:blank'}, true);
  session = (await send('Target.attachToTarget', {targetId, flatten:true}, true)).sessionId;
  await send('Runtime.enable');
  await send('Page.enable');
  await send('Emulation.setDeviceMetricsOverride', {width:1440,height:1100,deviceScaleFactor:1,mobile:false});
  await send('Page.navigate', {url:url.href});
  await ready();
  assert.deepEqual(await evaluate("Array.from(document.getElementById('release').options,o=>o.value)"), versions);
  const allRows = new Map();
  for (const generation of catalog.generations) {
    const rows = [];
    for (const shard of generation.shards) rows.push(...await load(shard.metrics));
    assert.equal(rows.length, catalog.sample_count);
    allRows.set(generation.crate_version, rows);
    await control('release', generation.crate_version);
    await ready();
    await checkPlots(pairRows(rows));
  }
  await shot('corpus-desktop');
  const newest = versions.at(-1), baseline = versions[0];
  await control('baseline', baseline);
  await control('reference', 'release');
  await ready();
  const paired = pairRows(allRows.get(newest), allRows.get(baseline));
  await checkPlots(paired);
  await shot('corpus-release-comparison');
  // Rapid edits exercise coalescing while a Plotly render is in flight.
  await control('kind', 'mffc');
  await control('kind', 'k3');
  await control('kind', 'whole');
  await ready();
  await checkPlots(filterRows(paired, {kind:'whole',max:null,losses:false}));
  await control('kind', 'all');
  await ready();
  const sample = paired[0], generation = catalog.generations.at(-1);
  const index = allRows.get(newest).findIndex(row=>row.sample_id===sample.sample_id);
  let offset = 0;
  const shard = generation.shards.find(s=>{offset+=s.sample_count;return index<offset});
  const expectedIr = (await load(shard.ir))[sample.sample_id];
  await evaluate(`document.getElementById('delta').emit('plotly_click',{points:[{customdata:${JSON.stringify(sample.sample_id)}}]})`);
  await waitFor(`document.getElementById('ir').textContent===${JSON.stringify(expectedIr)}`);
  await send('Emulation.setDeviceMetricsOverride', {width:390,height:1100,deviceScaleFactor:1,mobile:true});
  await evaluate("document.getElementById('ir').hidden=true;window.scrollTo(0,0);window.dispatchEvent(new Event('resize'))");
  await new Promise(resolve=>setTimeout(resolve,1000));
  assert.equal(await evaluate('innerWidth'), 390, 'mobile viewport expanded to fit overflowing content');
  assert(await evaluate('document.body.scrollWidth<=innerWidth'), 'mobile layout overflows');
  await shot('corpus-mobile');
  assert.equal(await evaluate("document.getElementById('error').textContent"), '');
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({releases:versions,samples_each:catalog.sample_count,desktop:true,mobile:true,paired:true,filters:true,ir:true,errors}));
}
const timeout = new Promise((_, reject)=>{
  const timer = setTimeout(()=>reject(Error('Browser test timed out')), 180000);
  timer.unref();
});
Promise.race([main(), timeout, new Promise((_,reject)=>chrome.once('error',reject))])
  .catch(error=>{console.error(error);process.exitCode=1})
  .finally(async()=>{
    if (chrome.exitCode===null && chrome.pid) {const closed=once(chrome,'exit');chrome.kill();await closed;}
    await fs.promises.rm(profile,{recursive:true,force:true,maxRetries:10,retryDelay:200});
  });
