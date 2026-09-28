// The ground station page: load a flash dump, open it with horton (wasm),
// and draw what is on the flash. All data access goes through
// ground-station.mjs, the same wrapper test.mjs checks.

import { GroundStation } from './ground-station.mjs';

const SENSORS = 4;
const Y_MIN = 15; // the recorder's readings are 15.00..34.99 °C
const Y_MAX = 35;
const CHART_H = 72;
const AXIS_W = 34;
const NS = 'http://www.w3.org/2000/svg';

const $ = (id) => document.getElementById(id);
const fmt = new Intl.NumberFormat('en-US');
const el = (tag, attrs = {}, text) => {
  const n = tag.startsWith('svg:') ? document.createElementNS(NS, tag.slice(4)) : document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  if (text !== undefined) n.textContent = text;
  return n;
};

const gs = await GroundStation.load(await (await fetch('ground_station.wasm')).arrayBuffer());

let summary = null;
let windowSize = 1000;
let rows = [];

// ── Loading ──────────────────────────────────────────────────────────

async function openBytes(bytes, name) {
  $('error').textContent = '';
  try {
    const recovered = gs.open(new Uint8Array(bytes));
    summary = gs.summary();
    summary.recovered = recovered;
    summary.name = name;
  } catch (e) {
    summary = null;
    $('report').hidden = true;
    $('error').textContent = `${name}: ${e.message}`;
    return;
  }
  $('report').hidden = false;
  renderTiles();
  renderIntegrity();
  renderLayout();
  setupWindow();
}

$('pick').addEventListener('click', () => $('file').click());
$('file').addEventListener('change', async (e) => {
  const f = e.target.files[0];
  if (f) await openBytes(await f.arrayBuffer(), f.name);
});
const drop = $('drop');
for (const ev of ['dragenter', 'dragover']) {
  document.addEventListener(ev, (e) => {
    e.preventDefault();
    drop.classList.add('over');
  });
}
for (const ev of ['dragleave', 'drop']) {
  document.addEventListener(ev, () => drop.classList.remove('over'));
}
document.addEventListener('drop', async (e) => {
  e.preventDefault();
  const f = e.dataTransfer.files[0];
  if (f) await openBytes(await f.arrayBuffer(), f.name);
});

// build.sh copies the latest recording next to the page when there is one.
fetch('flash.img', { method: 'HEAD' }).then((r) => {
  if (!r.ok) return;
  $('sample').hidden = false;
  $('sample').addEventListener('click', async () => {
    await openBytes(await (await fetch('flash.img', { cache: 'no-store' })).arrayBuffer(), 'flash.img');
  });
}, () => {});

// ── Summary ──────────────────────────────────────────────────────────

function tile(label, value, sub) {
  const t = el('div', { class: 'card tile' });
  t.append(el('div', { class: 'label' }, label), el('div', { class: 'value num' }, value));
  if (sub) t.append(el('div', { class: 'sub' }, sub));
  return t;
}

function renderTiles() {
  const s = summary;
  const span = s.newestTick === null ? 0 : s.newestTick - s.oldestTick + 1;
  $('tiles').replaceChildren(
    tile('Newest tick', s.newestTick === null ? '—' : fmt.format(s.newestTick), s.name),
    tile(
      'Ticks on flash',
      fmt.format(span),
      s.newestTick === null ? 'erased chip' : `${fmt.format(s.oldestTick)} to ${fmt.format(s.newestTick)}; older ones are archived`,
    ),
    tile('Boots', s.boots === null ? '—' : fmt.format(s.boots), 'the recorder counts its own restarts'),
    tile('Replayed from the WAL', fmt.format(s.recovered), 'writes that were only in the log when the dump was taken'),
  );
}

const ICON_OK = 'M3 8.5l3 3 7-7';
const ICON_BAD = 'M4 4l8 8M12 4l-8 8';

function renderIntegrity() {
  const v = gs.verify();
  const good = v.badValues === 0 && v.outOfOrder === 0 && v.tornFrames === 0 && v.invariants;
  const status = el('div', { class: `status ${good ? 'good' : 'bad'}` });
  const icon = el('svg:svg', { width: 16, height: 16, viewBox: '0 0 16 16', 'aria-hidden': 'true' });
  icon.append(el('svg:path', { d: good ? ICON_OK : ICON_BAD, stroke: 'currentColor', 'stroke-width': 2, fill: 'none', 'stroke-linecap': 'round' }));
  status.append(
    icon,
    el('span', {}, good
      ? 'Intact: every value matches its key, and every frame is whole'
      : 'Problems found: see below'),
  );
  const facts = el('div', { class: 'facts num' });
  const fact = (t) => facts.append(el('span', {}, t));
  fact(`${fmt.format(v.entries)} entries`);
  fact(`${fmt.format(v.wholeFrames)} whole frames`);
  fact(`${fmt.format(v.alarms)} alarms`);
  fact(`${fmt.format(v.traces)} debug traces`);
  fact(`${v.badValues} bad values`);
  fact(`${v.tornFrames} torn frames`);
  fact(`${v.outOfOrder} keys out of order`);
  fact(v.invariants ? 'database invariants hold' : `invariants: ${v.invariantError}`);
  if (v.boundaryReadings) {
    fact(`the oldest frame has ${v.boundaryReadings} of 4 readings here; the rest went to the archive with its table`);
  }
  $('integrity').replaceChildren(status, facts);
}

function renderLayout() {
  const s = summary;
  const wrap = el('div');
  const p = el('p', { class: 'num', style: 'color:var(--ink-2);font-size:13px;margin-bottom:12px' },
    `Manifest: ring of ${s.manifestCopies} copies · WAL: blocks ${s.wal[0]}–${s.wal[1] - 1} · ` +
    `tables: blocks ${s.tables[0]}–${s.tables[1] - 1}, ${s.slots.total} slots of ${s.slots.blocks} blocks, ` +
    `${s.slots.used} used`);
  const bars = el('div', { class: 'layout-bars' });
  const perLevel = s.slots.total / s.levels.length;
  s.levels.forEach((n, i) => {
    const track = el('div', { class: 'track' });
    track.append(el('div', { class: 'bar', style: `width:${(n / perLevel) * 100}%` }));
    bars.append(el('span', {}, `L${i}`), track, el('span', { class: 'num' }, `${n} of ${perLevel}`));
  });
  wrap.append(p, bars);
  $('layout').replaceChildren(wrap);
}

// ── Window and charts ───────────────────────────────────────────────

function setupWindow() {
  const end = $('end');
  if (summary.newestTick === null) {
    $('charts').replaceChildren(el('p', { style: 'color:var(--ink-2)' }, 'No readings on this flash.'));
    $('rows').replaceChildren();
    return;
  }
  end.min = summary.oldestTick;
  end.max = summary.newestTick;
  end.value = summary.newestTick;
  $('lk-tick').value = summary.newestTick;
  $('lk-tick').max = summary.newestTick;
  load();
}

for (const b of document.querySelectorAll('[data-size]')) {
  b.addEventListener('click', () => {
    windowSize = Number(b.dataset.size);
    load();
  });
}
$('end').addEventListener('input', () => load());
$('newest').addEventListener('click', () => {
  $('end').value = summary.newestTick;
  load();
});
let resizeTimer;
new ResizeObserver(() => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => summary && rows.length && draw(), 100);
}).observe($('charts'));

function load() {
  for (const b of document.querySelectorAll('[data-size]')) {
    b.setAttribute('aria-pressed', String(Number(b.dataset.size) === windowSize));
  }
  const endTick = Number($('end').value);
  const from = Math.max(summary.oldestTick, endTick - windowSize + 1);
  rows = gs.frames(from, endTick - from + 1, summary.newestTick);
  const gaps = rows.filter((r) => r.sensors.every((x) => x === null)).length;
  const alarms = rows.filter((r) => r.alarm !== null).length;
  const live = rows.filter((r) => r.live).length;
  const traces = rows.filter((r) => r.trace).length;
  $('window-label').textContent =
    `Ticks ${fmt.format(from)} to ${fmt.format(endTick)}: ${fmt.format(rows.length - gaps)} frames, ` +
    `${gaps} ticks without readings, ${alarms} alarms, ${traces} debug traces stored ` +
    `(${live} still live at tick ${fmt.format(summary.newestTick)}; they expire after ${gs.ttlTicks} ticks)`;
  draw();
  renderTable();
}

/** Buckets rows so there are at most ~one bucket per 2px. */
function buckets(width) {
  const n = rows.length;
  const per = Math.max(1, Math.ceil(n / Math.max(1, Math.floor(width / 2))));
  const out = [];
  for (let i = 0; i < n; i += per) {
    const slice = rows.slice(i, i + per);
    const b = { start: slice[0].tick, end: slice.at(-1).tick, sensors: [] };
    for (let s = 0; s < SENSORS; s++) {
      const vals = slice.map((r) => r.sensors[s]).filter((x) => x !== null);
      b.sensors.push(vals.length
        ? { min: Math.min(...vals), max: Math.max(...vals), mean: vals.reduce((a, c) => a + c, 0) / vals.length }
        : null);
    }
    out.push(b);
  }
  return { per, out };
}

let geom = null;

function draw() {
  const charts = $('charts');
  const width = Math.max(240, charts.clientWidth);
  const plotW = width - AXIS_W;
  const { per, out } = buckets(plotW);
  const first = rows[0].tick;
  const span = Math.max(1, rows.at(-1).tick - first + 1);
  const x = (tick) => AXIS_W + ((tick - first + 0.5) / span) * plotW;
  const y = (c) => 4 + (1 - (c - Y_MIN) / (Y_MAX - Y_MIN)) * (CHART_H - 8);
  geom = { first, span, plotW, x };
  $('band-note').textContent = per > 1
    ? `each point is the mean of ${per} ticks; the band is their min–max`
    : '';

  // Tick spans without readings, shared by every sensor row.
  const gapRuns = [];
  let runStart = null;
  rows.forEach((r, i) => {
    const empty = r.sensors.every((v) => v === null);
    if (empty && runStart === null) runStart = r.tick;
    if ((!empty || i === rows.length - 1) && runStart !== null) {
      gapRuns.push([runStart, empty ? r.tick : r.tick - 1]);
      runStart = null;
    }
  });

  const nodes = [];
  for (let s = 0; s < SENSORS; s++) {
    const wrap = el('div', { class: 'chart' });
    const name = el('div', { class: 'name' });
    name.append(el('i', { style: `background:var(--s${s})` }), el('span', {}, `Sensor s${s} · °C`));
    const svg = el('svg:svg', {
      viewBox: `0 0 ${width} ${CHART_H}`, height: CHART_H, role: 'img',
      'aria-label': `Sensor s${s} temperature over ticks ${first} to ${first + span - 1}`,
    });
    for (const g of [Y_MIN, 25, Y_MAX]) {
      svg.append(el('svg:line', { x1: AXIS_W, x2: width, y1: y(g), y2: y(g), stroke: g === Y_MIN ? 'var(--axis)' : 'var(--grid)', 'stroke-width': 1 }));
      svg.append(el('svg:text', { x: AXIS_W - 6, y: y(g) + 4, 'text-anchor': 'end', class: 'axis-text' }, String(g)));
    }
    for (const [a, b] of gapRuns) {
      svg.append(el('svg:rect', { x: x(a) - (0.5 / span) * plotW, y: 0, width: Math.max(1, ((b - a + 1) / span) * plotW), height: CHART_H, fill: 'var(--gap)' }));
    }
    // Min–max band, then the mean line; both break where there is no data.
    let band = '';
    let line = '';
    let top = [];
    let bottom = [];
    const flush = () => {
      if (top.length > 1 && per > 1) band += `M${top.join('L')}L${bottom.reverse().join('L')}Z`;
      top = [];
      bottom = [];
    };
    let pen = false;
    for (const b of out) {
      const v = b.sensors[s];
      const cx = x((b.start + b.end) / 2).toFixed(1);
      if (!v) {
        flush();
        pen = false;
        continue;
      }
      top.push(`${cx},${y(v.max).toFixed(1)}`);
      bottom.push(`${cx},${y(v.min).toFixed(1)}`);
      line += `${pen ? 'L' : 'M'}${cx},${y(v.mean).toFixed(1)}`;
      pen = true;
    }
    flush();
    if (band) svg.append(el('svg:path', { d: band, fill: `var(--s${s})`, 'fill-opacity': 0.18 }));
    svg.append(el('svg:path', { d: line, fill: 'none', stroke: `var(--s${s})`, 'stroke-width': 2, 'stroke-linejoin': 'round', 'stroke-linecap': 'round' }));
    svg.append(el('svg:line', { class: 'cross', x1: 0, x2: 0, y1: 0, y2: CHART_H, stroke: 'var(--ink-2)', 'stroke-width': 1, visibility: 'hidden' }));
    wrap.append(name, svg);
    nodes.push(wrap);
  }

  // Alarm strip with the tick axis.
  const strip = el('svg:svg', { viewBox: `0 0 ${width} 34`, height: 34, 'aria-hidden': 'true' });
  strip.append(el('svg:line', { x1: AXIS_W, x2: width, y1: 10, y2: 10, stroke: 'var(--axis)' }));
  for (const r of rows) {
    if (r.alarm === null) continue;
    const cx = x(r.tick);
    strip.append(el('svg:rect', { x: cx - 4, y: 6, width: 8, height: 8, fill: 'var(--warning)', transform: `rotate(45 ${cx} 10)` }));
  }
  const ticks = Math.max(1, Math.min(5, Math.floor(plotW / 120)));
  for (let i = 0; i <= ticks; i++) {
    const t = Math.round(first + (i / ticks) * (span - 1));
    strip.append(el('svg:text', { x: x(t), y: 30, 'text-anchor': i === 0 ? 'start' : i === ticks ? 'end' : 'middle', class: 'axis-text' }, fmt.format(t)));
  }
  strip.append(el('svg:line', { class: 'cross', x1: 0, x2: 0, y1: 0, y2: 16, stroke: 'var(--ink-2)', 'stroke-width': 1, visibility: 'hidden' }));
  const stripWrap = el('div', { class: 'chart' });
  stripWrap.append(el('div', { class: 'name' }, 'Alarms · tick'), strip);
  nodes.push(stripWrap);

  charts.replaceChildren(...nodes);
  charts.tabIndex = 0;
  charts.setAttribute('aria-label', 'Sensor charts. Use the left and right arrow keys to read values.');
}

// ── Crosshair and tooltip ───────────────────────────────────────────

let focusIndex = null;

function showAt(index) {
  if (!geom || index === null) return;
  focusIndex = Math.max(0, Math.min(rows.length - 1, index));
  const r = rows[focusIndex];
  const cx = geom.x(r.tick);
  for (const line of $('charts').querySelectorAll('.cross')) {
    line.setAttribute('x1', cx);
    line.setAttribute('x2', cx);
    line.setAttribute('visibility', 'visible');
  }
  const tip = $('tip');
  tip.replaceChildren(el('div', { class: 't num' }, `tick ${fmt.format(r.tick)}`));
  r.sensors.forEach((v, s) => {
    const row = el('div', { class: 'row' });
    row.append(el('i', { style: `background:var(--s${s})` }), el('b', {}, v === null ? '—' : `${v.toFixed(2)} °C`), el('span', {}, `s${s}`));
    tip.append(row);
  });
  if (r.alarm !== null) tip.append(el('div', { class: 't', style: 'margin:4px 0 0' }, `alarm code ${r.alarm}`));
  if (r.trace) tip.append(el('div', { class: 't', style: 'margin:4px 0 0' }, r.live ? 'debug trace: live' : 'debug trace: expired'));
  tip.hidden = false;
  const card = tip.parentElement.getBoundingClientRect();
  const charts = $('charts').getBoundingClientRect();
  const left = charts.left - card.left + cx;
  const flip = left + tip.offsetWidth + 16 > card.width;
  tip.style.left = `${flip ? left - tip.offsetWidth - 12 : left + 12}px`;
  tip.style.top = `${charts.top - card.top + 18}px`;
}

function hide() {
  $('tip').hidden = true;
  for (const line of $('charts').querySelectorAll('.cross')) line.setAttribute('visibility', 'hidden');
}

$('charts').addEventListener('pointermove', (e) => {
  if (!geom) return;
  const px = e.clientX - $('charts').getBoundingClientRect().left;
  if (px < AXIS_W) return hide();
  const tick = geom.first + Math.floor(((px - AXIS_W) / geom.plotW) * geom.span);
  showAt(tick - geom.first);
});
$('charts').addEventListener('pointerleave', hide);
$('charts').addEventListener('blur', hide);
$('charts').addEventListener('keydown', (e) => {
  const step = e.shiftKey ? 10 : 1;
  if (e.key === 'ArrowRight') showAt((focusIndex ?? -1) + step);
  else if (e.key === 'ArrowLeft') showAt((focusIndex ?? rows.length) - step);
  else if (e.key === 'Escape') hide();
  else return;
  e.preventDefault();
});

// ── Point read and table ────────────────────────────────────────────

$('lookup').addEventListener('submit', (e) => {
  e.preventDefault();
  const tick = Number($('lk-tick').value);
  const sensor = Number($('lk-sensor').value);
  const out = $('lookup-out');
  try {
    const r = gs.reading(tick, sensor);
    out.textContent = r === null
      ? `No reading for s${sensor} at tick ${fmt.format(tick)} on this flash (purged, archived, or not yet recorded).`
      : `s${sensor} at tick ${fmt.format(tick)}: ${r.celsius.toFixed(2)} °C · ${r.intact ? 'intact' : 'does not match its key'}`;
  } catch (err) {
    out.textContent = err.message;
  }
});

function renderTable() {
  const body = $('rows');
  const frag = document.createDocumentFragment();
  for (const r of rows.slice(-200).reverse()) {
    const tr = el('tr');
    tr.append(el('td', {}, fmt.format(r.tick)));
    for (const v of r.sensors) tr.append(el('td', v === null ? { class: 'none' } : {}, v === null ? '—' : v.toFixed(2)));
    tr.append(el('td', r.alarm === null ? { class: 'none' } : {}, r.alarm === null ? '—' : `code ${r.alarm}`));
    tr.append(el('td', r.trace ? {} : { class: 'none' }, r.trace ? (r.live ? 'live' : 'expired') : '—'));
    frag.append(tr);
  }
  body.replaceChildren(frag);
}
