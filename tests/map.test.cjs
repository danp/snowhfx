const assert = require('node:assert/strict');
const { webcrypto } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../index.html'), 'utf8');
const script = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)][0][1];
const featureHashes = { sidewalks: 'a'.repeat(64), cycling: 'b'.repeat(64) };
const featureURLs = Object.fromEntries(Object.entries(featureHashes)
  .map(([mode, hash]) => [mode, `http://localhost:3001/api/v1/features/${hash}.bin`]));
const featureManifest = {
  format_version: 4,
  datasets: Object.fromEntries(Object.entries(featureHashes).map(([mode, sha256]) => [mode, { sha256 }]))
};

function element(dataset = {}) {
  const classes = new Set();
  const listeners = new Map();
  return {
    dataset, listeners, value: '', textContent: '', innerHTML: '',
    classList: {
      contains: name => classes.has(name),
      add: name => classes.add(name),
      remove: (...names) => names.forEach(name => classes.delete(name)),
      toggle: (name, active) => active ? classes.add(name) : classes.delete(name)
    },
    addEventListener: (name, callback) => listeners.set(name, callback),
    setAttribute() {}, focus() {}, querySelectorAll: () => []
  };
}

function featuresResponse(title = 'Sidewalk') {
  const base = Buffer.alloc(16);
  const titleBytes = Buffer.from(title);
  const data = Buffer.concat([
    Buffer.from('SHFX'), Buffer.from([4, 1]), base,
    Buffer.from([1, 1, titleBytes.length]), titleBytes,
    Buffer.from([0, 1, 1]), // route: no maintainer, one name piece
    Buffer.from([0, 0, 2, 2, 1]), // segment bounds, one feature
    Buffer.from([1, 1, 1, 1, 1, 0, 1, 2, 0, 0, 2, 2])
  ]);
  return { ok: true, arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.length) };
}

const jsonResponse = payload => ({ ok: true, json: async () => payload });
const tick = () => new Promise(resolve => setImmediate(resolve));

function fakeClock() {
  let now = 0;
  let nextId = 0;
  const timers = new Map();
  return {
    setTimeout(callback, delay) {
      const id = ++nextId;
      timers.set(id, { at: now + delay, callback });
      return id;
    },
    clearTimeout: id => timers.delete(id),
    async advance(ms) {
      const target = now + ms;
      while (true) {
        const next = [...timers].sort((a, b) => a[1].at - b[1].at)[0];
        if (!next || next[1].at > target) break;
        now = next[1].at;
        timers.delete(next[0]);
        next[1].callback();
        await tick();
      }
      now = target;
    }
  };
}

function startMap(fetch, fetchManifest = async () => jsonResponse(featureManifest)) {
  const clock = fakeClock();
  const documentListeners = new Map();
  const elements = new Map();
  const getElement = id => {
    if (!elements.has(id)) elements.set(id, element());
    return elements.get(id);
  };
  const buttons = ['sidewalks', 'cycling'].map(mode => element({ mode, full: mode }));
  getElement('mobile-controls').querySelectorAll = () => buttons;
  const layer = () => ({
    layers: [],
    addTo(parent) { parent.addLayer?.(this); return this; },
    addLayer(child) { this.layers.push(child); },
    removeLayer(child) { this.layers = this.layers.filter(value => value !== child); },
    clearLayers() { this.layers = []; },
    eachLayer(callback) { this.layers.forEach(callback); },
    bindPopup(callback) { this.popup = callback; return this; },
    setStyle() {}, bringToFront() {},
    isPopupOpen() { return !!this.popupOpen; },
    setPopupContent(content) { this.popupContent = content; },
    openPopup() { this.popupOpen = true; this.popupContent = this.popup(); }
  });
  const map = {
    on() {}, removeLayer() {}, fitBounds() {}, setView() {}, closePopup() {},
    getBounds: () => ({ intersects: () => true })
  };
  const storage = new Map();
  const errors = [];
  const requests = [];
  const context = vm.createContext({
    fetch: (url, options) => new Promise((resolve, reject) => {
      requests.push({ url, options });
      options?.signal?.addEventListener('abort', () => reject(new Error('aborted')), { once: true });
      const request = url.endsWith('/features/manifest.json') ? fetchManifest : fetch;
      Promise.resolve(request(url, options)).then(resolve, reject);
    }),
    TextDecoder, URLSearchParams, AbortController, crypto: webcrypto,
    setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
    console: { error: (...args) => errors.push(args) },
    location: { hostname: 'localhost' }, navigator: {},
    matchMedia: () => ({ matches: false, addEventListener() {} }),
    addEventListener() {},
    localStorage: { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value) },
    document: {
      hidden: false, getElementById: getElement, querySelectorAll: () => [],
      addEventListener(name, callback) {
        if (!documentListeners.has(name)) documentListeners.set(name, []);
        documentListeners.get(name).push(callback);
      }
    },
    L: {
      map: () => map, tileLayer: layer, layerGroup: layer, polyline: layer,
      latLngBounds: () => ({ extend() {} }),
      control: () => ({ addTo() { this.onAdd(); } }),
      DomUtil: { create: () => element() },
      DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} }
    }
  });
  context.window = context;
  vm.runInContext(script, context, { filename: 'index.html' });
  return {
    errors, getElement, requests, advance: clock.advance,
    async setHidden(hidden) {
      context.document.hidden = hidden;
      documentListeners.get('visibilitychange').forEach(callback => callback());
      await tick();
    },
    evaluate: expression => vm.runInContext(expression, context),
    click: mode => buttons.find(button => button.dataset.mode === mode).listeners.get('click')()
  };
}

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
