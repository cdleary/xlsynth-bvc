// SPDX-License-Identifier: Apache-2.0
// Execute the real navigation handler; stub only dataset loading and rendering.
'use strict';
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(0, 'utf8');

async function navigate(search) {
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, {
      value: '', checked: false, innerHTML: '', textContent: '', dataset: {},
      addEventListener() {}, replaceChildren() {},
    });
    return elements.get(id);
  };
  element('progression').dataset.datasetKey = 'comparison';
  const location = {href: 'https://example.invalid/history/progression.html' + search, search};
  const context = vm.createContext({
    document: {querySelector: () => ({content: './'}), getElementById: element},
    // Keep automatic main() initialization pending and invoke progression directly.
    fetch: () => new Promise(() => {}),
    URL, URLSearchParams, location,
    history: {replaceState(_state, _title, url) { location.href = String(url); }},
  });
  vm.runInContext(source, context);
  vm.runInContext(`
    renderProgressionPair = () => {};
    renderProgressionVectors = () => {};
    progressionInventory = () => '';
    loadSiteDataset = async () => ({dataset: {samples: []}});
  `, context);
  const hashes = ['a'.repeat(64), 'b'.repeat(64)];
  const generation = (id, version, complete) => ({
    generation_id: id, origin: {kind: 'crate_release'}, display_label: 'v' + version,
    crate_version: version, dso_version: '1.0.0',
    coverage: complete ? 'cohort_complete' : 'partial',
    observed_ir_count: complete ? 2 : 1, cohort_ir_count: 2,
    missing_cohort_ir_count: complete ? 0 : 1, extra_ir_count: 0,
    run_samples: hashes.slice(0, complete ? 2 : 1).map(structural_hash => ({structural_hash})),
  });
  await context.progression({
    datasets: [{logical_key: 'comparison'}],
    progression: {default_cohort_id: 'whole-functions-v1', cohorts: [{
      cohort_id: 'whole-functions-v1', dataset_key: 'comparison', display_label: 'Whole functions',
      cohort_ir_count: 2, cohort_ir_hashes: hashes,
      generations: [generation('partial', '0.65.0', false), generation('first', '0.66.0', true), generation('last', '0.67.0', true)],
    }]},
  });
  const selected = {baseline: element('baseline-version').value, current: element('current-version').value};
  const query = new URL(location.href).searchParams;
  assert.equal(query.get('baseline'), selected.baseline);
  assert.equal(query.get('current'), selected.current);
  assert.equal(query.get('include_incomplete') === 'true', element('include-incomplete').checked);
  return selected;
}

(async () => {
  assert.deepEqual(await navigate('?cohort=whole-functions-v1'), {baseline: 'first', current: 'last'});
  assert.deepEqual(await navigate('?cohort=whole-functions-v1&current=partial&include_incomplete=true'), {baseline: 'last', current: 'partial'});
  assert.deepEqual(await navigate('?cohort=whole-functions-v1&current=first&baseline=first'), {baseline: 'first', current: 'first'});
  assert.deepEqual(await navigate('?cohort=whole-functions-v1&current=last&baseline=first'), {baseline: 'first', current: 'last'});
})().catch(error => { console.error(error); process.exitCode = 1; });
