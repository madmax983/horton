// Page helpers shared by the dump viewer (app.mjs) and the live logger
// (live.mjs): DOM building, number formatting, stat tiles, and the four
// sensor charts with their alarm strip, crosshair and tooltip.

const SENSORS = 4;
const Y_MIN = 15; // the recorder's readings are 15.00..34.99 °C
const Y_MAX = 35;
const CHART_H = 72;
const AXIS_W = 34;
const NS = 'http://www.w3.org/2000/svg';

export const fmt = new Intl.NumberFormat('en-US');

/** Builds an element (`svg:` prefix for SVG). Text goes in as textContent. */
export function el(tag, attrs = {}, text) {
  const n = tag.startsWith('svg:') ? document.createElementNS(NS, tag.slice(4)) : document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  if (text !== undefined) n.textContent = text;
  return n;
}

/** A stat tile: label, big value, optional note. */
export function tile(label, value, sub) {
  const t = el('div', { class: 'card tile' });
  t.append(el('div', { class: 'label' }, label), el('div', { class: 'value num' }, value));
  if (sub) t.append(el('div', { class: 'sub' }, sub));
  return t;
}

/** A status line: a check or a cross, then text. Never color alone. */
export function statusLine(good, text) {
  const status = el('div', { class: `status ${good ? 'good' : 'bad'}` });
  const icon = el('svg:svg', { width: 16, height: 16, viewBox: '0 0 16 16', 'aria-hidden': 'true' });
  icon.append(el('svg:path', {
    d: good ? 'M3 8.5l3 3 7-7' : 'M4 4l8 8M12 4l-8 8',
    stroke: 'currentColor', 'stroke-width': 2, fill: 'none', 'stroke-linecap': 'round',
  }));
  status.append(icon, el('span', {}, text));
  return status;
}

/**
 * Four small charts (one per sensor) and an alarm strip over a window of
 * frame rows, sharing one crosshair and tooltip. `charts` is the container,
 * `tip` the tooltip element (positioned inside the card), `note` where the
 * "each point is the mean of N ticks" note goes.
 */
export class SensorCharts {
  constructor({ charts, tip, note }) {
    this.charts = charts;
    this.tip = tip;
    this.note = note;
    this.rows = [];
    this.geom = null;
    this.focus = null;

    charts.addEventListener('pointermove', (e) => {
      if (!this.geom) return;
      const px = e.clientX - charts.getBoundingClientRect().left;
      if (px < AXIS_W) return this.hide();
      const tick = this.geom.first + Math.floor(((px - AXIS_W) / this.geom.plotW) * this.geom.span);
      this.showAt(tick - this.geom.first);
    });
    charts.addEventListener('pointerleave', () => this.hide());
    charts.addEventListener('blur', () => this.hide());
    charts.addEventListener('keydown', (e) => {
      const step = e.shiftKey ? 10 : 1;
      if (e.key === 'ArrowRight') this.showAt((this.focus ?? -1) + step);
      else if (e.key === 'ArrowLeft') this.showAt((this.focus ?? this.rows.length) - step);
      else if (e.key === 'Escape') this.hide();
      else return;
      e.preventDefault();
    });
    let timer;
    new ResizeObserver(() => {
      clearTimeout(timer);
      timer = setTimeout(() => this.rows.length && this.draw(), 100);
    }).observe(charts);
  }

  /** Replaces the rows and redraws. */
  setRows(rows) {
    this.rows = rows;
    if (rows.length) this.draw();
    else {
      this.hide();
      this.charts.replaceChildren();
    }
  }

  /** Buckets rows so there are at most ~one bucket per 2px. */
  #buckets(width) {
    const { rows } = this;
    const per = Math.max(1, Math.ceil(rows.length / Math.max(1, Math.floor(width / 2))));
    const out = [];
    for (let i = 0; i < rows.length; i += per) {
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

  draw() {
    // A redraw moves every mark, so a tooltip left from before would point
    // at nothing (and, after a resize, stick out of the card).
    this.hide();
    const { rows, charts } = this;
    const width = Math.max(240, charts.clientWidth);
    const plotW = width - AXIS_W;
    const { per, out } = this.#buckets(plotW);
    const first = rows[0].tick;
    const span = Math.max(1, rows.at(-1).tick - first + 1);
    const x = (tick) => AXIS_W + ((tick - first + 0.5) / span) * plotW;
    const y = (c) => 4 + (1 - (c - Y_MIN) / (Y_MAX - Y_MIN)) * (CHART_H - 8);
    this.geom = { first, span, plotW, x };
    if (this.note) {
      this.note.textContent = per > 1 ? `each point is the mean of ${per} ticks; the band is their min–max` : '';
    }

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

  showAt(index) {
    if (!this.geom || index === null || !this.rows.length) return;
    this.focus = Math.max(0, Math.min(this.rows.length - 1, index));
    const r = this.rows[this.focus];
    const cx = this.geom.x(r.tick);
    for (const line of this.charts.querySelectorAll('.cross')) {
      line.setAttribute('x1', cx);
      line.setAttribute('x2', cx);
      line.setAttribute('visibility', 'visible');
    }
    const { tip } = this;
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
    const charts = this.charts.getBoundingClientRect();
    const left = charts.left - card.left + cx;
    const flip = left + tip.offsetWidth + 16 > card.width;
    tip.style.left = `${Math.max(0, flip ? left - tip.offsetWidth - 12 : left + 12)}px`;
    tip.style.top = `${charts.top - card.top + 18}px`;
  }

  hide() {
    this.tip.hidden = true;
    for (const line of this.charts.querySelectorAll('.cross')) line.setAttribute('visibility', 'hidden');
  }
}
