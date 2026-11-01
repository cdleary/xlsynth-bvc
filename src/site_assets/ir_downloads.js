// SPDX-License-Identifier: Apache-2.0
'use strict';
// Downloads and their metadata are server-rendered; JavaScript only fills in
// the URL in the example command, including immutable publication prefixes.
const publicationRoot = new URL('../', location.href);
publicationRoot.search = '';
publicationRoot.hash = '';
if (['http:', 'https:'].includes(publicationRoot.protocol)) {
  const quoted = "'" + publicationRoot.href.replaceAll("'", "'\\''") + "'";
  const command = document.getElementById('import-command');
  command.textContent = command.textContent.replace('SITE_URL', quoted);
  document.getElementById('publication-root').textContent = publicationRoot.href;
}
