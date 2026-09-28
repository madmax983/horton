// The live telemetry page: a simulated flight recorder streaming frames to
// the logger Worker (logger-worker.mjs), and what the logger reports back.
//
// The device keeps every frame the logger has not acknowledged and sends
// them again after the logger restarts (at-least-once delivery). Frames
// are a pure function of their tick (recorder.mjs), so "keeping" them is
// remembering the newest acknowledged tick.

import { frameAt } from './recorder.mjs';
import { SensorCharts, el, fmt, statusLine, tile } from './sensor-charts.mjs';

const $ = (id) => document.getElementById(id);
/** Most frames in flight (sent, not yet acknowledged). */
const WINDOW = 512;
const CHART_TICKS = 600;

const charts = new SensorCharts({ charts: $('charts'), tip: $('tip'), note: $('band-note') });
const state = {
  worker: null,
  ready: false,
  running: false,
  rate: 50, // frames per second; 0 = as fast as the logger acknowledges
  acked: -1, // newest tick acknowledged (durable)
  sent: -1, // newest tick sent
  budget: 0,
  acks: [], // [time, tick] of recent acknowledgements, for the rate
  status: null,
  replayed: 0,
  boots: 0,
  windowPending: false,
  lastTickSeen: null,
};
// For the browser test: the device's view, read-only, and the crash button.
window.liveStation = {
  get acked() { return state.acked; },
  get ready() { return state.ready; },
  get status() { return state.status; },
  get boots() { return state.boots; },
};

function log(msg) {
  const li = el('li');
  li.append(el('time', {}, new Date().toLocaleTimeString()), el('span', {}, msg));
  $('log').prepend(li);
  while ($('log').children.length > 60) $('log').lastChild.remove();
}

function showState() {
  const inFlight = state.sent - state.acked;
  $('state').textContent = !state.ready
    ? 'The logger is starting: recovering the flash…'
    : state.running
      ? `Streaming ${state.rate ? `${state.rate} frames/s` : 'as fast as the logger acknowledges'}; ${inFlight} frame${inFlight === 1 ? '' : 's'} in flight.`
      : 'The stream is paused. Everything acknowledged is on disk.';
  $('toggle').textContent = state.running ? 'Pause the stream' : 'Start the stream';
  for (const b of document.querySelectorAll('[data-rate]')) b.setAttribute('aria-pressed', String(Number(b.dataset.rate) === state.rate));
}

function startWorker() {
  state.ready = false;
  state.windowPending = false;
  showState();
  const w = new Worker('logger-worker.mjs', { type: 'module' });
  state.worker = w;
  w.onmessage = (e) => onMessage(w, e.data);
  w.onerror = (e) => {
    $('error').textContent = `The logger failed: ${e.message}`;
  };
  w.postMessage({ type: 'start' });
}

function onMessage(w, m) {
  if (w !== state.worker) return; // a message from a logger we killed
  switch (m.type) {
    case 'ready': {
      state.ready = true;
      state.status = m.status;
      state.replayed = m.replayed;
      state.boots = m.boots;
      const newest = m.status.newestTick ?? -1;
      // A device that outlived the logger resends from its last
      // acknowledgement; a fresh page picks up after the newest durable tick.
      if (state.acked < 0) state.acked = newest;
      const resend = state.sent - state.acked;
      state.sent = state.acked;
      log(
        `logger started (boot ${fmt.format(m.boots)}): recovered the flash, replayed ${m.replayed} WAL record${m.replayed === 1 ? '' : 's'}; ` +
          `newest durable tick ${newest < 0 ? '—' : fmt.format(newest)}` +
          (resend > 0 ? `; the device resends ${resend} unacknowledged frame${resend === 1 ? '' : 's'}` : ''),
      );
      renderTiles();
      showState();
      break;
    }
    case 'ack':
      state.acked = Math.max(state.acked, m.upTo);
      state.acks.push([performance.now(), m.upTo]);
      state.status = { ...state.status, ...m.stats };
      break;
    case 'status':
      state.status = m.status;
      renderTiles();
      break;
    case 'window':
      state.windowPending = false;
      if (m.rows.length) charts.setRows(m.rows);
      break;
    case 'log':
      log(m.msg);
      break;
    case 'verified':
      renderVerify(m.report, m.ms);
      break;
    case 'exported': {
      const url = URL.createObjectURL(new Blob([m.bytes], { type: 'application/octet-stream' }));
      const a = el('a', { href: url, download: 'flash.img' });
      document.body.append(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 10_000);
      log(`downloaded flash.img (${fmt.format(m.bytes.length)} bytes): open it in the dump viewer`);
      break;
    }
    case 'reset':
      w.terminate();
      state.acked = -1;
      state.sent = -1;
      charts.setRows([]);
      $('verify-out').replaceChildren();
      log('erased the flash and the archive');
      startWorker();
      break;
    case 'error':
      $('error').textContent = `The logger stopped (${m.during}): ${m.message}`;
      state.running = false;
      showState();
      break;
    default:
  }
}

// The device: sends frames at the chosen rate, never more than WINDOW
// ahead of the last acknowledgement.
let last = performance.now();
setInterval(() => {
  const now = performance.now();
  const dt = (now - last) / 1000;
  last = now;
  if (!state.ready || !state.running) return;
  const room = WINDOW - (state.sent - state.acked);
  let n;
  if (state.rate === 0) n = room >= WINDOW / 4 ? Math.min(room, 128) : 0;
  else {
    state.budget = Math.min(state.budget + state.rate * dt, WINDOW);
    n = Math.min(Math.floor(state.budget), room);
    state.budget -= n;
  }
  if (n <= 0) return;
  const frames = [];
  for (let i = 0; i < n; i++) frames.push(frameAt(state.sent + 1 + i));
  state.sent += n;
  state.worker.postMessage({ type: 'frames', frames });
}, 40);

// The view: the chart four times a second, the tiles once a second.
setInterval(() => {
  if (!state.ready || state.windowPending || state.acked < 0) return;
  if (state.lastTickSeen === state.acked && !state.running) return;
  state.lastTickSeen = state.acked;
  state.windowPending = true;
  state.worker.postMessage({ type: 'window', count: CHART_TICKS });
}, 250);
setInterval(() => {
  if (state.ready) state.worker.postMessage({ type: 'status' });
  showState();
}, 1000);

function ackRate() {
  const now = performance.now();
  state.acks = state.acks.filter(([t]) => now - t < 3000);
  if (state.acks.length < 2) return 0;
  const [t0, k0] = state.acks[0];
  const [t1, k1] = state.acks.at(-1);
  return t1 > t0 ? ((k1 - k0) * 1000) / (t1 - t0) : 0;
}

function renderTiles() {
  const s = state.status;
  if (!s) return;
  const mib = (s.archiveBytes / (1 << 20)).toFixed(1);
  $('tiles').replaceChildren(
    tile('Newest durable tick', s.newestTick === null ? '—' : fmt.format(s.newestTick), `acknowledged up to ${state.acked < 0 ? '—' : fmt.format(state.acked)}`),
    tile('Frames per second', fmt.format(Math.round(ackRate())), 'written, flushed and acknowledged'),
    tile('On flash (OPFS)', `${s.slotsUsed} of ${s.slotsUsed + s.slotsFree}`, 'table slots in use; the hot window'),
    tile('In IndexedDB', fmt.format(s.archiveTables), `archived tables, ${mib} MiB`),
    tile('Logger starts', fmt.format(state.boots), `${fmt.format(state.replayed)} WAL records replayed at the last one`),
  );
}

function renderVerify(r, ms) {
  const span = r.firstTick === null ? 'no frames yet' : `ticks ${fmt.format(r.firstTick)} to ${fmt.format(r.upTo)}`;
  const good = r.complete;
  const text = good
    ? `Whole: every frame in ${span} is on flash or in IndexedDB`
    : `${fmt.format(r.missingCount)} tick${r.missingCount === 1 ? '' : 's'} not whole: ${r.missing.join(', ')}`;
  const facts = el('div', { class: 'facts num' });
  const fact = (t) => facts.append(el('span', {}, t));
  fact(`${fmt.format(r.flashTicks)} ticks whole on flash`);
  fact(`${fmt.format(r.archiveOnlyTicks)} only in the archive`);
  fact(`${fmt.format(r.tables)} archived tables read back (${(r.archiveBytes / (1 << 20)).toFixed(1)} MiB)`);
  fact(`${r.badValues} bad values`);
  fact(`${Math.round(ms)} ms`);
  $('verify-out').replaceChildren(statusLine(good, text), facts);
  log(good ? `verified: ${span} whole` : `verify found ${r.missingCount} ticks not whole`);
}

$('toggle').addEventListener('click', () => {
  state.running = !state.running;
  last = performance.now();
  showState();
});
for (const b of document.querySelectorAll('[data-rate]')) {
  b.addEventListener('click', () => {
    state.rate = Number(b.dataset.rate);
    state.budget = 0;
    showState();
  });
}
$('crash').addEventListener('click', () => {
  if (!state.worker) return;
  const inFlight = state.sent - state.acked;
  state.worker.terminate();
  state.worker = null;
  state.ready = false;
  log(`killed the logger mid-stream with ${inFlight} frame${inFlight === 1 ? '' : 's'} unacknowledged`);
  setTimeout(startWorker, 300);
});
$('verify').addEventListener('click', () => {
  if (state.ready) state.worker.postMessage({ type: 'verify' });
});
$('export').addEventListener('click', () => {
  if (state.ready) state.worker.postMessage({ type: 'export' });
});
$('reset').addEventListener('click', () => {
  if (!state.ready || !window.confirm('Erase the logged flash and the IndexedDB archive?')) return;
  state.running = false;
  state.ready = false;
  state.worker.postMessage({ type: 'reset' });
});

if (!navigator.storage?.getDirectory) {
  $('error').textContent = 'This browser has no Origin Private File System, which the logger needs for its flash.';
} else {
  startWorker();
}
