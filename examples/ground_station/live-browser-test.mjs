// Live logger browser test, in headless Chromium with a real OPFS file and
// real IndexedDB:
//
// 1. stream frames as fast as the logger acknowledges them until tables
//    have gone to IndexedDB;
// 2. kill the logger Worker mid-stream (the page's crash button), let it
//    recover while the device resends what was unacknowledged;
// 3. crash the whole tab (chrome://crash: the renderer and the Worker die
//    mid-write), then open a new tab on the same storage and prove every
//    frame acknowledged before the crash survived, and the whole history
//    is on flash or in IndexedDB;
// 4. download flash.img and open it with the dump viewer's module;
// 5. erase everything, and check the logger starts fresh.
//
//   ./build.sh && node live-browser-test.mjs

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { chromium as loadChromium, serve } from './serve.mjs';
import { GroundStation } from './web/ground-station.mjs';

const chromium = await loadChromium();
const server = await serve();
const browser = await chromium.launch();
const context = await browser.newContext({ acceptDownloads: true });
let checks = 0;
const ok = (cond, msg) => {
  assert.ok(cond, msg);
  checks++;
};

async function open() {
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => m.type() === 'error' && errors.push(m.text()));
  await page.goto(`${server.url}live.html`);
  await page.waitForFunction(() => window.liveStation?.ready, null, { timeout: 30_000 });
  return { page, errors };
}
const station = (page) => page.evaluate(() => ({ acked: window.liveStation.acked, status: window.liveStation.status }));

try {
  // 1. Stream until at least two tables are in IndexedDB.
  let { page, errors } = await open();
  await page.click('[data-rate="0"]');
  await page.click('#toggle');
  await page.waitForFunction(() => (window.liveStation.status?.archivedTables ?? 0) >= 2, null, { timeout: 60_000 });
  let s = await station(page);
  console.log(`streamed to tick ${s.acked}; ${s.status.archivedTables} tables archived to IndexedDB`);
  ok(s.acked > 3000, 'frames were acknowledged');

  // 2. Kill the logger mid-stream; it recovers and the stream goes on.
  const beforeKill = s.acked;
  await page.click('#crash');
  await page.waitForFunction(() => window.liveStation.ready, null, { timeout: 30_000 });
  await page.waitForFunction((t) => window.liveStation.acked > t + 1000, beforeKill, { timeout: 30_000 });
  const log = await page.$$eval('#log li', (ls) => ls.map((l) => l.textContent).join('\n'));
  ok(/killed the logger mid-stream/.test(log) && /logger started \(boot 2\)/.test(log), 'the logger restarted as boot 2');
  ok(errors.length === 0, `no console errors: ${errors.join('; ')}`);

  // 3. Crash the whole tab while frames are in flight.
  s = await station(page);
  const ackedBeforeCrash = s.acked;
  const crashed = new Promise((r) => page.on('crash', r));
  await page.goto('chrome://crash').catch(() => {});
  await crashed;
  console.log(`crashed the tab mid-stream; the last acknowledged tick was ${ackedBeforeCrash}`);

  ({ page, errors } = await open());
  s = await station(page);
  console.log(`new tab: recovered to tick ${s.status.newestTick} (boot ${await page.evaluate(() => window.liveStation.boots)})`);
  ok(s.status.newestTick >= ackedBeforeCrash, `newest durable tick ${s.status.newestTick} ≥ last acknowledged ${ackedBeforeCrash}`);

  await page.click('#verify');
  await page.waitForSelector('#verify-out .status', { timeout: 60_000 });
  const verdict = await page.textContent('#verify-out');
  console.log(`verify: ${verdict.replace(/(\d) /g, '$1 ').slice(0, 160)}`);
  ok(/^Whole: every frame in ticks 0 to/.test(verdict), `history whole: ${verdict}`);
  ok(/[1-9][\d,]* only in the archive/.test(verdict), 'part of the history lives only in IndexedDB');

  // 4. The logger's flash is a recorder flash image.
  const download = page.waitForEvent('download');
  await page.click('#export');
  const file = await (await download).path();
  const gs = await GroundStation.load(await readFile(new URL('./web/ground_station.wasm', import.meta.url)));
  gs.open(new Uint8Array(await readFile(file)));
  const v = gs.verify();
  const sum = gs.summary();
  ok(v.invariants && v.badValues === 0 && v.tornFrames === 0, `the dump viewer finds the export intact: ${JSON.stringify(v)}`);
  ok(sum.newestTick >= ackedBeforeCrash, 'the export holds every acknowledged tick');
  ok(sum.boots >= 3, `every logger start was counted (${sum.boots})`);
  console.log(`dump viewer: the downloaded flash.img opens intact, newest tick ${sum.newestTick}, boot ${sum.boots}`);
  ok(errors.length === 0, `no console errors: ${errors.join('; ')}`);

  // At phone width the page still fits (the charts redraw on resize).
  await page.setViewportSize({ width: 390, height: 800 });
  const fits = await page
    .waitForFunction(() => document.documentElement.scrollWidth <= 390, null, { timeout: 5000 })
    .then(() => true, () => false);
  ok(fits, 'the live page fits a phone');

  // 5. Erase everything.
  page.once('dialog', (d) => d.accept());
  await page.click('#reset');
  await page.waitForFunction(() => window.liveStation.ready && window.liveStation.status?.newestTick === null, null, { timeout: 30_000 });
  s = await station(page);
  ok(s.status.archiveTables === 0, 'the archive is empty after a reset');
} finally {
  await browser.close();
  server.close();
}
console.log(`LIVE BROWSER OK: ${checks} checks`);
