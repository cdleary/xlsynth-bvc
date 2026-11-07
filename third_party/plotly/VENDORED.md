# Vendored Plotly Assets

The static publication embeds Plotly so interactive charts remain hermetic and
work without a CDN dependency.

- Upstream project: `https://github.com/plotly/plotly.js`
- Version: `v2.35.2`
- Bundle source: `https://cdn.plot.ly/plotly-2.35.2.min.js`
- Bundle SHA-256: `6d21266ce1bd7d9e5ab4e115989c70c20de0382fd973a8f26ab58619eba4d603`
- Primary license source: `https://raw.githubusercontent.com/plotly/plotly.js/v2.35.2/LICENSE`
- Primary license SHA-256: `67a26cf80f03ff388f26945dfc1f6caed1a0746ff43871190339b3aee49b94bb`
- Bundle notice source: `https://cdn.plot.ly/plotly-2.35.2.min.js.LICENSE.txt`
- Bundle notice SHA-256: `9144a55454d7944bd0695179ec85956de3d08a64c5227ffa2d45da186ec8445a`

Refresh these files only from the pinned versioned sources above. Verify all
three digests before accepting an update. Both the Plotly MIT license and the
bundle's third-party notices are copied into every generated static site.

The tech-mapped comparison uses the same version's strict bundle, whose WebGL
renderer avoids function constructors and works with the page's `script-src 'self'`
policy. Other publication pages retain the bundle listed above.

- Strict bundle source: `https://cdn.plot.ly/plotly-strict-2.35.2.min.js`
- Strict bundle SHA-256: `06dc893a77f745c3c07873b4030ded816256fe3797db524e6985c9b214fd532f`
- Strict bundle notice source: `https://cdn.plot.ly/plotly-strict-2.35.2.min.js.LICENSE.txt`
- Strict bundle notice SHA-256: `9144a55454d7944bd0695179ec85956de3d08a64c5227ffa2d45da186ec8445a`

The strict bundle uses the same primary MIT license and third-party notices.
Refresh it only from these pinned versioned sources and verify both digests.
