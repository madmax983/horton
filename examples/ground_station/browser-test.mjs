// Browser test: serves web/, opens it in headless Chromium, loads the
// recording through the page's own button, and checks what a person would
// see. Complements test.mjs, which checks the data itself.
//
//   ./build.sh                       # builds the module, copies the recording
//   npm install --no-save playwright # or have it installed globally
//   node browser-test.mjs

import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { execSync } from 'node:child_process';
import { join, extname } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';

const { chromium } = await import('playwright').catch(() =>
  import(pathToFileURL(join(execSync('npm root -g').toString().trim(), 'playwright', 'index.mjs')).href),
);

const root = fileURLToPath(new URL('./web/', import.meta.url));
const types = { '.html': 'text/html', '.mjs': 'text/javascript', '.wasm': 'application/wasm' };
const server = createServer(async (req, res) => {
  const path = new URL(req.url, 'http://x').pathname.replace(/^\/$/, '/index.html');
  try {
    const body = await readFile(join(root, path.slice(1)));
    res.writeHead(200, { 'content-type': types[extname(path)] ?? 'application/octet-stream' });
    res.end(req.method === 'HEAD' ? undefined : body);
  } catch {
    res.writeHead(404).end();
  }
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const url = `http://127.0.0.1:${server.address().port}/`;

const browser = await chromium.launch();
let checks = 0;
try {
  for (const colorScheme of ['light', 'dark']) {
    const page = await browser.newPage({ viewport: { width: 1100, height: 900 }, colorScheme });
    const errors = [];
    page.on('pageerror', (e) => errors.push(e.message));
    page.on('console', (m) => m.type() === 'error' && errors.push(m.text()));
    await page.goto(url);
    await page.click('#sample'); // needs web/flash.img: run ./build.sh after recording
    await page.waitForSelector('#report:not([hidden])');

    assert.match(await page.textContent('#integrity'), /Intact/);
    assert.match(await page.textContent('#window-label'), /^Ticks [\d,]+ to [\d,]+: [\d,]+ frames/);
    assert.equal(await page.locator('#charts .chart').count(), 5, 'four sensors and the alarm strip');
    checks += 3;

    // Hovering a chart shows one tooltip with all four sensors.
    const box = await page.locator('#charts').boundingBox();
    await page.mouse.move(box.x + box.width * 0.6, box.y + 40);
    assert.equal(await page.locator('#tip .row').count(), 4);
    checks++;

    // The newest tick's point read is intact.
    await page.click('[data-size="200"]');
    await page.click('#lookup button');
    assert.match(await page.textContent('#lookup-out'), /°C · intact$/);
    checks++;

    // The table view holds the window's newest ticks, newest first.
    await page.click('#table-view summary');
    const firstTick = Number((await page.textContent('#rows tr:first-child td')).replaceAll(',', ''));
    assert.equal(firstTick, Number(await page.inputValue('#end')));
    checks++;

    // Phone width: no horizontal scroll.
    await page.setViewportSize({ width: 390, height: 800 });
    await page.waitForTimeout(250);
    assert.ok((await page.evaluate(() => document.documentElement.scrollWidth)) <= 390, 'fits a phone');
    checks++;

    assert.deepEqual(errors, [], `${colorScheme}: console errors`);
    checks++;
    await page.close();
  }

  // A file that is not a recorder dump is refused with a message.
  const page = await browser.newPage();
  await page.goto(url);
  await page.setInputFiles('#file', { name: 'notes.txt', mimeType: 'text/plain', buffer: Buffer.from('hello') });
  assert.match(await page.textContent('#error'), /notes\.txt: a recorder flash dump is/);
  assert.ok(await page.locator('#report').isHidden());
  checks += 2;
} finally {
  await browser.close();
  server.close();
}
console.log(`BROWSER OK: ${checks} checks`);
