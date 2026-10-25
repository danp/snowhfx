const fs = require('node:fs');
const { performance } = require('node:perf_hooks');
const { featureURLs, jsonResponse, tick, startMap } = require('./map-harness.cjs');

const filename = process.argv[2] || 'cmd/api/featuredata/features.bin';
const bytes = fs.readFileSync(filename);

async function openMap(southwest, northeast) {
  const app = startMap(async url => {
    if (url === featureURLs.sidewalks) {
      return { ok: true, arrayBuffer: async () => bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length) };
    }
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    return jsonResponse({ reports: [] });
  });
  if (southwest) app.setViewport(southwest, northeast);
  await tick();
  if (app.errors.length) throw new Error(JSON.stringify(app.errors));
  return app;
}

(async () => {
  const views = {};
  for (const [name, southwest, northeast] of [
    ['downtown', [44.64, -63.59], [44.66, -63.57]],
    ['southEnd', [44.625, -63.59], [44.635, -63.575]],
    ['coleHarbour', [44.66, -63.5], [44.68, -63.48]]
  ]) {
    const app = await openMap(southwest, northeast);
    views[name] = app.metrics.polylinesCreated;
  }
  const app = await openMap();
  app.evaluate(`setCommunityReports(Array.from({ length: 1000 }, (_, i) => ({
    segment_key: 'unmatched-' + i, created_at: '2026-01-01T00:00:00Z'
  })))`);
  const query = `for (let i = 0; i < 10000; i++) {
    hasReportForFeature('sidewalks', 0, i % allSegments[0].features.length);
  }`;
  app.evaluate(query);
  const samples = [];
  for (let i = 0; i < 5; i++) {
    const start = performance.now();
    app.evaluate(query);
    samples.push(performance.now() - start);
  }
  samples.sort((a, b) => a - b);
  console.log(JSON.stringify({
    filename,
    features: app.evaluate('allSegments.reduce((n, segment) => n + segment.features.length, 0)'),
    polylinesCreated: views,
    reportChecks: { queries: 10000, reports: 1000, medianMs: +samples[2].toFixed(2) }
  }, null, 2));
})().catch(error => { console.error(error); process.exitCode = 1; });
