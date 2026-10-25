const assert = require('node:assert/strict');
const test = require('node:test');
const { featureHashes, featureURLs, featureManifest, featuresResponse, jsonResponse, tick, startMap } = require('./map-harness.cjs');

for (const failure of ['network error', 'HTTP error', 'invalid JSON', 'pending request']) {
  test(`routes and controls initialize with event API ${failure}`, async () => {
    const app = startMap(async url => {
      if (url.endsWith('/current-event')) {
        if (failure === 'network error') throw new Error('offline');
        if (failure === 'HTTP error') return { ok: false, status: 503 };
        if (failure === 'invalid JSON') return { ok: true, json: async () => { throw new SyntaxError('invalid JSON'); } };
        return new Promise(() => {});
      }
      if (url === featureURLs.sidewalks) return featuresResponse();
      return jsonResponse({ reports: [] });
    });
    await tick();
    assert.equal(app.evaluate('allSegments[0].features[0].title'), 'Sidewalk');
    assert.equal(app.evaluate('window.priorities[1].Deadline'), null);
    assert.match(app.getElement('priorities-header').innerHTML, /Unavailable/);
    assert.ok(app.getElement('about-toggle').listeners.has('click'));
    assert.equal(app.evaluate('typeof reportActions.openCommunityFromIndices'), 'function');
    assert.equal(app.errors.some(args => args[0] === 'Error initializing map:'), false);
  });
}

test('event data arriving after routes updates deadlines and popups', async () => {
  let resolveEvent;
  const event = new Promise(resolve => { resolveEvent = resolve; });
  const app = startMap(async url => {
    if (url.endsWith('/current-event')) return event;
    if (url === featureURLs.sidewalks) return featuresResponse();
    return jsonResponse({ reports: [] });
  });
  await tick();
  assert.match(app.evaluate('getPopupContent(0, 0)'), /Unavailable/);
  resolveEvent(jsonResponse({ state: 'ended', max_end_time: '2026-02-10T12:00:00Z' }));
  await tick();
  assert.equal(app.evaluate('window.priorities[1].Deadline.toISOString()'), '2026-02-11T00:00:00.000Z');
  assert.doesNotMatch(app.evaluate('getPopupContent(0, 0)'), /Unavailable/);
  assert.equal(app.errors.length, 0);
});

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

function assertCycling(app) {
  assert.equal(app.evaluate('currentDatasetMode'), 'cycling');
  assert.equal(app.evaluate('currentDatasetCode'), 1);
  assert.equal(app.evaluate('allSegments[0].features[0].title'), 'Bikeway');
  assert.equal(app.evaluate('routeTable[1].route'), 'Bikeway');
  assert.equal(app.evaluate('communityReports[0].segment_key'), 'Bikeway');
}

test('late initial dataset cannot overwrite the selected dataset or route table', async () => {
  const sidewalks = deferred();
  const app = startMap(async url => {
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    if (url === featureURLs.sidewalks) return sidewalks.promise;
    if (url === featureURLs.cycling) return featuresResponse('Bikeway');
    return jsonResponse({ reports: [{ segment_key: 'Bikeway' }] });
  });
  await tick();
  await app.click('cycling');
  assertCycling(app);
  sidewalks.resolve(featuresResponse());
  await tick();
  assertCycling(app);
  assert.equal(app.errors.length, 0);
});

for (const outcome of ['success', 'failure']) {
  test(`late report ${outcome} cannot overwrite the selected dataset's reports`, async () => {
    const reports = deferred();
    const app = startMap(async url => {
      if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
      if (url === featureURLs.sidewalks) return featuresResponse();
      if (url === featureURLs.cycling) return featuresResponse('Bikeway');
      if (url.includes('dataset_mode=sidewalks')) return reports.promise;
      return jsonResponse({ reports: [{ segment_key: 'Bikeway' }] });
    });
    await tick();
    await app.click('cycling');
    assertCycling(app);
    if (outcome === 'success') reports.resolve(jsonResponse({ reports: [{ segment_key: 'Sidewalk' }] }));
    else reports.reject(new Error('offline'));
    await tick();
    assertCycling(app);
  });
}

test('switching back to the same mode still discards an older request', async () => {
  const cycling = deferred();
  const app = startMap(async url => {
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    if (url === featureURLs.sidewalks) return featuresResponse();
    if (url === featureURLs.cycling) return cycling.promise;
    return jsonResponse({ reports: [] });
  });
  await tick();
  const oldLoad = app.click('cycling');
  await app.click('sidewalks');
  cycling.resolve(featuresResponse('Bikeway'));
  await oldLoad;
  assert.equal(app.evaluate('currentDatasetMode'), 'sidewalks');
  assert.equal(app.evaluate('allSegments[0].features[0].title'), 'Sidewalk');
  assert.equal(app.evaluate('routeTable[1].route'), 'Sidewalk');
});

test('failed replacement keeps the current map and can be retried', async () => {
  let fail = true;
  const app = startMap(async url => {
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    if (url === featureURLs.sidewalks) return featuresResponse();
    if (url === featureURLs.cycling) return fail ? { ok: false, status: 503 } : featuresResponse('Bikeway');
    return jsonResponse({ reports: [{ segment_key: 'Bikeway' }] });
  });
  await tick();
  await app.click('cycling');
  assert.equal(app.evaluate('currentDatasetMode'), 'sidewalks');
  assert.equal(app.evaluate('allSegments[0].features[0].title'), 'Sidewalk');
  assert.equal(app.evaluate('routeTable[1].route'), 'Sidewalk');
  fail = false;
  await app.click('cycling');
  assertCycling(app);
});

test('polls at separate intervals, pauses while hidden, and refreshes on return', async () => {
  let events = 0, reports = 0;
  const app = startMap(async (url, options) => {
    if (url === featureURLs.sidewalks) return featuresResponse();
    assert.equal(options.cache, 'no-store');
    if (url.endsWith('/current-event')) {
      events++;
      return jsonResponse({ state: 'active' });
    }
    reports++;
    return jsonResponse({ reports: [] });
  });
  await tick();
  assert.equal(events, 1);
  assert.equal(reports, 1);
  await app.advance(5 * 60 * 1000);
  assert.equal(events, 2);
  assert.equal(reports, 3);
  await app.setHidden(true);
  await app.advance(10 * 60 * 1000);
  assert.equal(events, 2);
  assert.equal(reports, 3);
  await app.setHidden(false);
  assert.equal(events, 3);
  assert.equal(reports, 4);
});

test('failed refreshes retain deadlines and reports, show last success, and recover', async () => {
  let fail = false;
  const app = startMap(async url => {
    if (url === featureURLs.sidewalks) return featuresResponse();
    if (fail) throw new Error('offline');
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'ended', max_end_time: '2026-02-10T12:00:00Z' });
    return jsonResponse({ reports: [{ segment_key: 'Sidewalk' }] });
  });
  await tick();
  fail = true;
  await app.advance(5 * 60 * 1000);
  assert.equal(app.evaluate('window.priorities[1].Deadline.toISOString()'), '2026-02-11T00:00:00.000Z');
  assert.equal(app.evaluate('communityReports[0].segment_key'), 'Sidewalk');
  assert.match(app.getElement('refresh-status').textContent, /Weather refresh failed; last checked/);
  assert.match(app.getElement('refresh-status').textContent, /Reports refresh failed; last checked/);
  fail = false;
  await app.setHidden(true);
  await app.setHidden(false);
  assert.equal(app.getElement('refresh-status').hidden, true);
});

test('event polling updates an open popup without replacing its polyline', async () => {
  let ended = false;
  const app = startMap(async url => {
    if (url === featureURLs.sidewalks) return featuresResponse();
    if (url.endsWith('/current-event')) return jsonResponse(ended
      ? { state: 'ended', max_end_time: '2000-01-01T00:00:00Z' }
      : { state: 'active' });
    return jsonResponse({ reports: [] });
  });
  await tick();
  app.evaluate('globalThis.openLine = renderedSegments[0].layers[0]; openLine.openPopup()');
  assert.doesNotMatch(app.evaluate('openLine.popupContent'), /popup-311-button/);
  ended = true;
  await app.advance(5 * 60 * 1000);
  assert.equal(app.evaluate('openLine === renderedSegments[0].layers[0]'), true);
  assert.match(app.evaluate('openLine.popupContent'), /popup-311-button/);
});

test('report polling updates an open popup and replaces expired reports', async () => {
  let reports = [{ segment_key: 'Sidewalk', conditions: ['icy'], created_at: new Date().toISOString() }];
  const app = startMap(async url => {
    if (url === featureURLs.sidewalks) return featuresResponse();
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    return jsonResponse({ reports });
  });
  await tick();
  app.evaluate('renderedSegments[0].layers[0].openPopup()');
  assert.match(app.evaluate('renderedSegments[0].layers[0].popupContent'), /Icy/);
  reports = [];
  await app.advance(2 * 60 * 1000);
  assert.match(app.evaluate('renderedSegments[0].layers[0].popupContent'), /None yet/);
});

test('hanging requests time out and retry without overlapping on visibility changes', async () => {
  let events = 0;
  const pending = deferred();
  const app = startMap(async url => {
    if (url === featureURLs.sidewalks) return featuresResponse();
    if (url.endsWith('/current-event')) {
      events++;
      return events === 1 ? pending.promise : jsonResponse({ state: 'active' });
    }
    return jsonResponse({ reports: [] });
  });
  await tick();
  await app.setHidden(true);
  await app.setHidden(false);
  assert.equal(events, 1);
  await app.advance(10000);
  assert.equal(events, 2);
  assert.equal(app.evaluate('currentEventSummary.state'), 'active');
  pending.resolve(jsonResponse({ state: 'ended' }));
  await tick();
  assert.equal(app.evaluate('currentEventSummary.state'), 'active');
});

test('dataset switches invalidate a pending report poll', async () => {
  let sidewalkRequests = 0;
  const pending = deferred();
  const app = startMap(async url => {
    if (url === featureURLs.sidewalks) return featuresResponse();
    if (url === featureURLs.cycling) return featuresResponse('Bikeway');
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    if (url.includes('dataset_mode=sidewalks') && ++sidewalkRequests === 2) return pending.promise;
    return jsonResponse({ reports: [{ segment_key: url.includes('cycling') ? 'Bikeway' : 'Sidewalk' }] });
  });
  await tick();
  await app.advance(2 * 60 * 1000);
  await app.click('cycling');
  pending.resolve(jsonResponse({ reports: [{ segment_key: 'Sidewalk' }] }));
  await tick();
  assertCycling(app);
});

test('uses one revalidated API manifest and cacheable hash URLs for both datasets', async () => {
  const app = startMap(async url => {
    if (url === featureURLs.sidewalks) return featuresResponse();
    if (url === featureURLs.cycling) return featuresResponse('Bikeway');
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    return jsonResponse({ reports: [{ segment_key: 'Bikeway' }] });
  });
  await tick();
  await app.click('cycling');
  assertCycling(app);
  const manifests = app.requests.filter(request => request.url.endsWith('/features/manifest.json'));
  assert.equal(manifests.length, 1);
  assert.equal(manifests[0].options.cache, 'no-cache');
  for (const url of Object.values(featureURLs)) {
    const request = app.requests.find(request => request.url === url);
    assert.ok(request, `missing feature request to ${url}`);
    assert.equal(request.options?.cache, undefined, 'binary requests should use normal browser caching');
  }
  assert.equal(app.requests.some(request => request.url === 'features.bin' || request.url === 'features_cycling.bin'), false);
  assert.equal(app.errors.length, 0);
});

test('a failed manifest is retried on the next dataset selection', async () => {
  let attempts = 0;
  const app = startMap(async url => {
    if (url === featureURLs.cycling) return featuresResponse('Bikeway');
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    return jsonResponse({ reports: [{ segment_key: 'Bikeway' }] });
  }, async () => {
    if (++attempts === 1) return { ok: false, status: 503 };
    return jsonResponse(featureManifest);
  });
  await tick();
  assert.equal(app.evaluate('allSegments.length'), 0);
  assert.ok(app.getElement('about-toggle').listeners.has('click'));
  await app.click('cycling');
  assertCycling(app);
  assert.equal(attempts, 2);
});

for (const manifest of [
  { ...featureManifest, format_version: 5 },
  { format_version: 4, datasets: { sidewalks: { sha256: '../other' } } }
]) {
  test(`rejects invalid feature manifest ${JSON.stringify(manifest)}`, async () => {
    const app = startMap(async url => {
      if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
      return jsonResponse({ reports: [] });
    }, async () => jsonResponse(manifest));
    await tick();
    assert.equal(app.evaluate('allSegments.length'), 0);
    assert.equal(app.requests.some(request => request.url.endsWith('.bin')), false);
    assert.ok(app.errors.some(args => args[0] === 'Error loading dataset:'));
  });
}

test('a shared pending manifest still honors the latest dataset selection', async () => {
  const manifest = deferred();
  const app = startMap(async url => {
    if (url === featureURLs.cycling) return featuresResponse('Bikeway');
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    return jsonResponse({ reports: [{ segment_key: 'Bikeway' }] });
  }, () => manifest.promise);
  const selected = app.click('cycling');
  manifest.resolve(jsonResponse(featureManifest));
  await selected;
  assertCycling(app);
  assert.equal(app.requests.filter(request => request.url.endsWith('/features/manifest.json')).length, 1);
  assert.equal(app.requests.some(request => request.url === featureURLs.sidewalks), false);
});

test('report index preserves newest-first order and removes old entries on refresh', async () => {
  let reports = [
    { segment_key: 'Sidewalk', id: 'old', created_at: '2026-01-01T00:00:00Z' },
    { segment_key: 'Other', id: 'other', created_at: '2026-01-04T00:00:00Z' },
    { segment_key: 'Sidewalk', id: 'new', created_at: '2026-01-03T00:00:00Z' },
    { segment_key: 'Sidewalk', id: 'expired', created_at: '2026-01-05T00:00:00Z', expires_at: '2000-01-01T00:00:00Z' }
  ];
  const app = startMap(async url => {
    if (url === featureURLs.sidewalks) return featuresResponse();
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    return jsonResponse({ reports });
  });
  await tick();
  assert.deepEqual(Array.from(app.evaluate("reportsForFeature('sidewalks', 0, 0).map(report => report.id)")), ['new', 'old']);
  assert.equal(app.evaluate("hasReportForFeature('sidewalks', 0, 0)"), true);
  reports = [];
  await app.advance(2 * 60 * 1000);
  assert.equal(app.evaluate("reportsForFeature('sidewalks', 0, 0).length"), 0);
  assert.equal(app.evaluate("hasReportForFeature('sidewalks', 0, 0)"), false);
});

test('report lookups do not scan report entries', async () => {
  const app = startMap(async url => {
    if (url === featureURLs.sidewalks) return featuresResponse();
    if (url.endsWith('/current-event')) return jsonResponse({ state: 'active' });
    return jsonResponse({ reports: [] });
  });
  await tick();
  app.evaluate(`globalThis.reportKeyReads = 0;
    setCommunityReports([{ get segment_key() { reportKeyReads++; return 'Sidewalk'; } }]);
    reportKeyReads = 0;
    for (let i = 0; i < 1000; i++) {
      hasReportForFeature('sidewalks', 0, 0);
      reportsForFeature('sidewalks', 0, 0);
    }`);
  assert.equal(app.evaluate('reportKeyReads'), 0);
});
