const { webcrypto } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
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

function geometryResponse(lines) {
  const values = [];
  const unsigned = value => {
    while (value >= 128) {
      values.push((value % 128) | 128);
      value = Math.floor(value / 128);
    }
    values.push(value);
  };
  const signed = value => unsigned(value < 0 ? -2 * value - 1 : 2 * value);
  const points = lines.flat();
  unsigned(0); unsigned(0); // routes and string pieces
  signed(Math.round(Math.min(...points.map(point => point[1])) * 1e6));
  signed(Math.round(Math.min(...points.map(point => point[0])) * 1e6));
  signed(Math.round(Math.max(...points.map(point => point[1])) * 1e6));
  signed(Math.round(Math.max(...points.map(point => point[0])) * 1e6));
  unsigned(lines.length);
  for (const line of lines) {
    values.push(0, 0, 1, 0, 0); // no strings, priority 1, travelways, no route
    unsigned(line.length);
    let lon = 0, lat = 0;
    for (const point of line) {
      const nextLon = Math.round(point[1] * 1e6), nextLat = Math.round(point[0] * 1e6);
      signed(nextLon - lon); signed(nextLat - lat);
      lon = nextLon; lat = nextLat;
    }
  }
  const data = Buffer.concat([Buffer.from('SHFX\x04\x01'), Buffer.alloc(16), Buffer.from(values)]);
  return { ok: true, arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.length) };
}

const jsonResponse = payload => ({ ok: true, json: async () => payload });
const tick = () => new Promise(resolve => setImmediate(resolve));

function bounds(southwest, northeast) {
  const result = {
    minLat: Infinity, minLon: Infinity, maxLat: -Infinity, maxLon: -Infinity,
    extend(value) {
      if (Array.isArray(value)) {
        this.minLat = Math.min(this.minLat, value[0]);
        this.minLon = Math.min(this.minLon, value[1]);
        this.maxLat = Math.max(this.maxLat, value[0]);
        this.maxLon = Math.max(this.maxLon, value[1]);
      } else {
        this.extend([value.minLat, value.minLon]);
        this.extend([value.maxLat, value.maxLon]);
      }
      return this;
    },
    intersects(other) {
      return this.minLat <= other.maxLat && this.maxLat >= other.minLat
        && this.minLon <= other.maxLon && this.maxLon >= other.minLon;
    },
    pad(ratio) {
      const lat = (this.maxLat - this.minLat) * ratio;
      const lon = (this.maxLon - this.minLon) * ratio;
      return bounds([this.minLat - lat, this.minLon - lon], [this.maxLat + lat, this.maxLon + lon]);
    }
  };
  if (southwest) result.extend(southwest);
  if (northeast) result.extend(northeast);
  return result;
}

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
  const metrics = { polylinesCreated: 0, styleUpdates: 0, drawSequence: 0 };
  const layer = () => ({
    layers: [],
    addTo(parent) { parent.addLayer?.(this); return this; },
    addLayer(child) { this.layers.push(child); },
    removeLayer(child) { this.layers = this.layers.filter(value => value !== child); },
    clearLayers() { this.layers = []; },
    eachLayer(callback) { this.layers.forEach(callback); },
    bindPopup(callback) { this.popup = callback; return this; },
    setStyle(style) { this.style = style; metrics.styleUpdates++; },
    bringToFront() { this.front = true; this.drawOrder = ++metrics.drawSequence; },
    isPopupOpen() { return !!this.popupOpen; },
    setPopupContent(content) { this.popupContent = content; },
    openPopup() { this.popupOpen = true; this.popupContent = this.popup(); }
  });
  const mapListeners = new Map();
  let viewport = bounds([-90, -180], [90, 180]);
  const map = {
    on: (name, callback) => mapListeners.set(name, callback),
    removeLayer() {}, fitBounds() {}, setView() {}, closePopup() {},
    getBounds: () => viewport,
    getCenter: () => ({ lat: 0, lng: 0 }), getZoom: () => 12
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
      map: () => map, tileLayer: layer, layerGroup: layer,
      polyline(coords, style) {
        metrics.polylinesCreated++;
        return Object.assign(layer(), { coords, style, drawOrder: ++metrics.drawSequence });
      },
      latLngBounds: bounds,
      control: () => ({ addTo() { this.onAdd(); } }),
      DomUtil: { create: () => element() },
      DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} }
    }
  });
  context.window = context;
  vm.runInContext(script, context, { filename: 'index.html' });
  return {
    errors, getElement, requests, metrics, advance: clock.advance,
    setViewport(southwest, northeast) {
      viewport = bounds(southwest, northeast);
      mapListeners.get('moveend')?.();
    },
    async setHidden(hidden) {
      context.document.hidden = hidden;
      documentListeners.get('visibilitychange').forEach(callback => callback());
      await tick();
    },
    evaluate: expression => vm.runInContext(expression, context),
    click: mode => buttons.find(button => button.dataset.mode === mode).listeners.get('click')()
  };
}


module.exports = { featureHashes, featureURLs, featureManifest, featuresResponse, geometryResponse, jsonResponse, tick, startMap };
