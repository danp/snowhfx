const assert = require('node:assert/strict');
const { webcrypto } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../index.html'), 'utf8');
const script = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)][0][1];

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

function startMap(fetch) {
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
    bindPopup(callback) { this.popup = callback; return this; }
  });
  const map = {
    on() {}, removeLayer() {}, fitBounds() {}, setView() {},
    getBounds: () => ({ intersects: () => true })
  };
  const storage = new Map();
  const errors = [];
  const context = vm.createContext({
    fetch, TextDecoder, URLSearchParams, crypto: webcrypto,
    console: { error: (...args) => errors.push(args) },
    location: { hostname: 'localhost' }, navigator: {},
    matchMedia: () => ({ matches: false, addEventListener() {} }),
    addEventListener() {},
    localStorage: { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value) },
    document: { getElementById: getElement, querySelectorAll: () => [], addEventListener() {} },
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
    errors, getElement,
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
      if (url.endsWith('features.bin')) return featuresResponse();
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
    if (url.endsWith('features.bin')) return featuresResponse();
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
