// Browser test: serves web/, opens it in headless Chromium, loads the
// recording through the page's own button, and checks what a person would
// see. Complements test.mjs, which checks the data itself.
//
//   ./build.sh                       # builds the module, copies the recording
//   npm install --no-save playwright # or have it installed globally
//   node browser-test.mjs

import assert from 'node:assert/strict';
import { chromium as loadChromium, serve } from './serve.mjs';

const chromium = await loadChromium();
const server = await serve();
const { url } = server;

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
    // The charts redraw on a debounced ResizeObserver, so wait for the
    // layout to settle; if it never fits, name what sticks out.
    await page.setViewportSize({ width: 390, height: 800 });
    const fits = await page
      .waitForFunction(() => document.documentElement.scrollWidth <= 390, null, { timeout: 5000 })
      .then(() => true, () => false);
    const culprit = fits
      ? ''
      : await page.evaluate(() =>
          [...document.querySelectorAll('body *')]
            .filter((el) => el.getBoundingClientRect().right > 391)
            .map((el) => `${el.tagName.toLowerCase()}${el.id ? `#${el.id}` : ''}`)
            .slice(0, 5)
            .join(', '),
        );
    assert.ok(fits, `fits a phone (overflowing: ${culprit})`);
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
