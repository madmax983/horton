// The ground station page: load a flash dump, open it with horton (wasm),
// and draw what is on the flash. All data access goes through
// ground-station.mjs, the same wrapper test.mjs checks.

import { GroundStation } from './ground-station.mjs';
import { SensorCharts, el, fmt, statusLine, tile } from './sensor-charts.mjs';

const $ = (id) => document.getElementById(id);

const gs = await GroundStation.load(await (await fetch('ground_station.wasm')).arrayBuffer());

let summary = null;
let windowSize = 1000;
let rows = [];
const charts = new SensorCharts({ charts: $('charts'), tip: $('tip'), note: $('band-note') });

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

function renderIntegrity() {
  const v = gs.verify();
  const good = v.badValues === 0 && v.outOfOrder === 0 && v.tornFrames === 0 && v.invariants;
  const status = statusLine(good, good
    ? 'Intact: every value matches its key, and every frame is whole'
    : 'Problems found: see below');
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
  if (v.splitFrames) {
    fact(`${fmt.format(v.splitFrames)} frame${v.splitFrames === 1 ? ' is' : 's are'} split with the archive: ` +
      'a table boundary cut the frame, and the other part left with its table');
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
    charts.setRows([]);
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
  charts.setRows(rows);
  renderTable();
}

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
