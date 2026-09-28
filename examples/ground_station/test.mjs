// Ground station test: opens a real flight recorder flash dump through the
// same wrapper the page uses, and checks what it reads independently of
// the wasm (the recorder's value hash is reimplemented here in BigInt).
//
//   node test.mjs <flash.img> [--newest N]
//
// Build the module first (./build.sh). CI runs this on a clean recording
// and on one killed with SIGKILL mid-write.

import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';
import { GroundStation } from './web/ground-station.mjs';

const args = process.argv.slice(2);
const imagePath = args[0];
const newestFlag = args.indexOf('--newest');
const expectNewest = newestFlag >= 0 ? Number(args[newestFlag + 1]) : null;
if (!imagePath) {
  console.error('usage: node test.mjs <flash.img> [--newest N]');
  process.exit(2);
}

const wasm = readFileSync(new URL('./web/ground_station.wasm', import.meta.url));
const gs = await GroundStation.load(wasm);
const image = new Uint8Array(readFileSync(imagePath));

// The recorder's value(): FNV-1a over the key, then a murmur finalizer.
const M = (1n << 64n) - 1n;
function value(key) {
  let h = 0xcbf29ce484222325n;
  for (const b of key) h = ((h ^ BigInt(b)) * 0x100000001b3n) & M;
  h ^= h >> 33n;
  h = (h * 0xff51afd7ed558ccdn) & M;
  h ^= h >> 33n;
  return h;
}
function key(tick, kind, id) {
  const k = new Uint8Array(10);
  new DataView(k.buffer).setBigUint64(0, BigInt(tick));
  k[8] = kind.charCodeAt(0);
  k[9] = id;
  return k;
}
const celsius = (tick, sensor) => 15 + Number(value(key(tick, 'r', sensor)) % 2000n) / 100;

let checks = 0;
const ok = (cond, msg) => {
  assert.ok(cond, msg);
  checks++;
};

// 1. Open and summarize.
const before = image.slice();
const recovered = gs.open(image);
ok(image.every((b, i) => b === before[i]), 'open must not modify the caller\'s bytes');
const s = gs.summary();
console.log(
  `opened: ${recovered} WAL records replayed, boot ${s.boots}, ticks ${s.oldestTick}..=${s.newestTick} on flash,` +
    ` ${s.slots.used}/${s.slots.total} table slots, levels [${s.levels}]`,
);
ok(s.newestTick !== null && s.oldestTick !== null, 'the recording holds ticks');
ok(s.boots >= 1, 'the boot counter is present');
if (expectNewest !== null) ok(s.newestTick === expectNewest, `newest tick ${s.newestTick}, expected ${expectNewest}`);

// 2. Full integrity walk.
const v = gs.verify();
console.log(
  `verify: ${v.entries} entries, ${v.readings} readings, ${v.alarms} alarms, ${v.traces} traces;` +
    ` ${v.wholeFrames} whole frames, ${v.tornFrames} torn` +
    (v.splitFrames ? ` (+${v.splitFrames} split with the archive)` : '') +
    `, ${v.badValues} bad values, invariants ${v.invariants}`,
);
ok(v.badValues === 0, 'every value matches its key');
ok(v.outOfOrder === 0, 'keys come back in order');
ok(v.tornFrames === 0, 'frames are atomic: all four readings or none');
ok(v.invariants, `invariants: ${v.invariantError}`);
ok(v.readings === v.wholeFrames * 4 + v.splitReadings, 'readings are whole frames or split with the archive');

// 3. The newest window, cross-checked against the hash.
const window = Math.min(2000, s.newestTick - s.oldestTick + 1);
const from = s.newestTick - window + 1;
const rows = gs.frames(from, window, s.newestTick);
ok(rows.length === window, 'one row per tick');
let present = 0;
let gaps = 0;
for (const r of rows) {
  const have = r.sensors.filter((x) => x !== null).length;
  // A frame split with the archive keeps a prefix or a suffix here.
  const mask = r.sensors.reduce((m, x, i) => (x === null ? m : m | (1 << i)), 0);
  const split = have > 0 && have < 4 && ((mask & (mask + 1)) === 0 || (mask | (mask - 1)) === 0b1111);
  ok(have === 0 || have === 4 || split, `tick ${r.tick}: ${have} of 4 readings`);
  if (have === 0 || split) {
    gaps++;
    continue;
  }
  present++;
  r.sensors.forEach((c, i) => ok(Math.abs(c - celsius(r.tick, i)) < 1e-9, `tick ${r.tick} s${i}: ${c}`));
  if (r.live) ok(r.trace, 'a live trace is a stored trace');
  if (r.trace) ok(r.live === s.newestTick - r.tick < gs.ttlTicks, `tick ${r.tick}: trace liveness`);
}
ok(present > 0, 'the window has frames');
console.log(`frames: ${present} of ${window} newest ticks present, ${gaps} purged (glitch windows)`);

// 4. Point reads agree with the scan.
const last = rows.findLast((r) => r.sensors[0] !== null);
for (let sensor = 0; sensor < 4; sensor++) {
  const p = gs.reading(last.tick, sensor);
  ok(p && p.intact && p.celsius === last.sensors[sensor], `point read s${sensor}`);
}
ok(gs.reading(s.newestTick + 1000, 0) === null, 'a future tick is absent');

// 5. Bad dumps fail cleanly, never trap.
assert.throws(() => gs.open(new Uint8Array(1234)), /512 sectors/);
checks++;
// Noise has no valid manifest copy: rejected by name, and the station is
// left closed rather than half-open.
const noise = new Uint8Array(gs.imageLen);
for (let i = 0; i < noise.length; i++) noise[i] = (i * 2654435761) >>> 24;
assert.throws(() => gs.open(noise), /CorruptManifest/);
assert.throws(() => gs.summary(), /not open/);
checks += 2;
const blank = new Uint8Array(gs.imageLen).fill(0xff);
gs.open(blank);
ok(gs.summary().newestTick === null, 'an erased chip opens empty');

console.log(`GROUND STATION OK: ${checks} checks`);
