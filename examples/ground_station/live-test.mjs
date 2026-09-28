// Live logger power-cut test, in Node. The same LiveLogger the Worker
// runs, over memory that loses power partway through a write, and an
// archive store that can lose power between storing a table and horton
// committing it away. After every cut: a new module (all RAM gone), boot
// from what is durable, prove every acknowledged frame is whole on flash
// or in the archive, then let the device resend what was not acknowledged.
//
//   node live-test.mjs [cuts] [--seed N] [--export DIR]
//
// --export writes the final flash image (DIR/flash.img) and the archive as
// recorder objects (DIR/archive/tables/*.hrt), so CI can open them with
// the dump viewer and the native recorder's `restore`.

import { readFileSync, mkdirSync, writeFileSync } from 'node:fs';
import assert from 'node:assert/strict';
import { LiveLogger, PowerCut } from './web/live-logger.mjs';
import { frameAt } from './web/recorder.mjs';
import { GroundStation } from './web/ground-station.mjs';

const args = process.argv.slice(2);
const flag = (name) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : undefined;
};
const CUTS = Number(args.find((a) => /^\d+$/.test(a)) ?? 40);
let seed = BigInt(flag('--seed') ?? 0x5eed);
const exportDir = flag('--export');

// splitmix64: a small deterministic PRNG, so a failing seed reproduces.
function rand(n) {
  seed = (seed + 0x9e3779b97f4a7c15n) & ((1n << 64n) - 1n);
  let z = seed;
  z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & ((1n << 64n) - 1n);
  z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & ((1n << 64n) - 1n);
  return Number((z ^ (z >> 31n)) % BigInt(n));
}

const BLOCK = 4096;

/** Flash in memory. `cutAfter(ops, tear)` loses power on the ops-th write
 *  or flush from now: a write lands only `tear`/256 of the way. */
class Flash {
  constructor(len) {
    this.bytes = new Uint8Array(len).fill(0xff);
    this.ops = 0;
    this.cutAt = null;
    this.tear = 0;
    this.powered = true;
  }
  cutAfter(ops, tear) {
    this.cutAt = this.ops + ops;
    this.tear = tear;
  }
  restore() {
    this.powered = true;
    this.cutAt = null;
  }
  #spend() {
    if (!this.powered) throw new PowerCut();
    this.ops++;
    if (this.cutAt === this.ops) {
      this.powered = false;
      return false;
    }
    return true;
  }
  read(id, dst) {
    if (!this.powered) throw new PowerCut();
    dst.set(this.bytes.subarray(id * BLOCK, id * BLOCK + dst.length));
  }
  write(id, src) {
    const whole = this.#spend();
    const len = whole ? src.length : Math.floor((src.length * this.tear) / 256);
    this.bytes.set(src.subarray(0, len), id * BLOCK);
    if (!whole) throw new PowerCut();
  }
  flush() {
    if (!this.#spend()) throw new PowerCut();
  }
}

/** The archive (IndexedDB in the browser). A cut can land just before a
 *  put is durable, or just after it but before horton commits. */
class Archive {
  constructor() {
    this.objects = new Map();
    this.cut = null; // 'before' | 'after'
    this.puts = 0;
  }
  async put(id, bytes) {
    if (this.cut === 'before') {
      this.cut = null;
      throw new PowerCut();
    }
    this.objects.set(id, bytes.slice());
    this.puts++;
    if (this.cut === 'after') {
      this.cut = null;
      throw new PowerCut();
    }
  }
  async get(id) {
    return this.objects.get(id);
  }
  async list() {
    return [...this.objects.keys()].sort((a, b) => a - b);
  }
}

const wasm = await WebAssembly.compile(readFileSync(new URL('./web/live.wasm', import.meta.url)));
const flash = new Flash(0);
const archive = new Archive();
let acked = -1; // the newest tick the logger acknowledged
let checks = 0;
const log = [];

async function boot() {
  flash.restore();
  const logger = await LiveLogger.load(wasm, { device: flash, archive, log: (m) => log.push(m) });
  if (flash.bytes.length === 0) flash.bytes = new Uint8Array(logger.flashLen).fill(0xff);
  const replayed = await logger.open();
  return { logger, replayed };
}

async function check(logger, when) {
  // Every durable tick (up to the newest, which may be one the device
  // never heard acknowledged) must be whole across flash and archive: a
  // frame is all-or-nothing, and nothing acknowledged is missing.
  const h = await logger.verifyHistory();
  const s = logger.stats();
  assert.ok(s.newestTick === null || h.complete, `${when}: newest ${s.newestTick}, acked ${acked}; missing ${h.missingCount}: ${h.detail.join(' | ')}; bad values ${h.badValues}`);
  assert.ok(acked < 0 || h.firstTick === 0, `${when}: history starts at ${h.firstTick}`);
  assert.ok((s.newestTick ?? -1) >= acked, `${when}: newest ${s.newestTick} is behind acknowledged ${acked}`);
  const f = logger.verifyFlash();
  if (f.tornFrames && process.env.LIVE_DEBUG) {
    const sum = logger.x.lv_summary();
    const o = new Float64Array(logger.x.memory.buffer, logger.x.lv_out_ptr(), sum);
    const [oldest, newest] = [o[3], o[4]];
    for (let from = oldest; from <= newest; from += 4000) {
      for (const r of logger.frames(from, Math.min(4000, newest - from + 1), newest)) {
        const have = r.sensors.map((x) => (x === null ? '.' : 'x')).join('');
        if (have !== 'xxxx' && have !== '....') console.log(`partial frame on flash: tick ${r.tick} sensors ${have}; oldest ${oldest}, newest ${newest}`);
      }
    }
    for (const id of await archive.list()) {
      const b = archive.objects.get(id);
      const dv = new DataView(b.buffer, b.byteOffset);
      const tickAt = (off) => Number(dv.getBigUint64(off));
      // Header (format.rs): magic 8, id 4, blocks 4, max seq 8, min seq 8, entries 4,
      // rdel 4, then first and last key, each a u16 length and 10 bytes.
      console.log(`archive table ${id}: tick ${tickAt(42)} s${b[51]} .. tick ${tickAt(54)} s${b[63]}`);
    }
  }
  assert.ok(f.invariants && f.badValues === 0 && f.outOfOrder === 0 && f.tornFrames === 0, `${when}: flash ${JSON.stringify(f)}`);
  checks += 4;
  return h;
}

/** The device: sends every tick after the last acknowledged one, in order
 *  (at-least-once), acknowledging each once `record` resolves. */
async function stream(logger, ticks) {
  for (let n = 0; n < ticks; n++) {
    const t = acked + 1;
    await logger.record(frameAt(t));
    acked = t;
  }
}

let replays = 0;
let cutsDuringArchive = 0;
let tornWrites = 0;
for (let cut = 0; cut < CUTS; cut++) {
  const { logger, replayed } = await boot();
  replays += replayed;
  await check(logger, `boot ${cut}`);
  // Arm a cut somewhere in the next few hundred writes; now and then,
  // put it at the archive store instead.
  const where = rand(8);
  if (where === 0) archive.cut = 'before';
  else if (where === 1) archive.cut = 'after';
  else {
    flash.cutAfter(1 + rand(600), rand(256));
    tornWrites++;
  }
  const putsBefore = archive.puts;
  try {
    await stream(logger, 400 + rand(1600));
    await logger.archiveCold();
  } catch (e) {
    if (!e?.powerCut) throw e;
    if (archive.puts !== putsBefore || where < 2) cutsDuringArchive++;
    continue;
  }
  flash.restore();
  archive.cut = null;
}

// Final boot: nothing armed, run on until tables are well into the archive.
const { logger } = await boot();
await stream(logger, 4000);
await logger.archiveCold();
const history = await check(logger, 'final');
const s = logger.stats();
console.log(
  `live logger: ${CUTS} power cuts (${tornWrites} torn flash writes, ${cutsDuringArchive} around an archive store), ` +
    `${replays} WAL records replayed across boots`,
);
console.log(
  `history: ticks 0..${history.upTo} whole, ${history.flashTicks} on flash, ${history.archiveOnlyTicks} only in the archive ` +
    `(${history.tables} tables, ${(history.archiveBytes / 1024).toFixed(0)} KiB); ${s.slotsUsed} slots used on flash`,
);
assert.ok(history.tables >= 3, 'tables reached the archive');
assert.ok(history.archiveOnlyTicks > 0, 'some history lives only in the archive');
checks += 2;

// The flash is a recorder flash image: the dump viewer opens it.
const gs = await GroundStation.load(readFileSync(new URL('./web/ground_station.wasm', import.meta.url)));
gs.open(flash.bytes);
const v = gs.verify();
const sum = gs.summary();
assert.ok(v.invariants && v.badValues === 0 && v.tornFrames === 0, 'the viewer finds the flash intact');
assert.equal(sum.newestTick, acked, 'the viewer sees the newest acknowledged tick');
assert.equal(sum.boots, CUTS + 1, 'every boot was counted');
checks += 3;
console.log(`dump viewer: opens the logger's flash, newest tick ${sum.newestTick}, boot ${sum.boots}, intact`);

if (exportDir) {
  mkdirSync(`${exportDir}/archive/tables`, { recursive: true });
  writeFileSync(`${exportDir}/flash.img`, flash.bytes);
  for (const [id, bytes] of archive.objects) {
    writeFileSync(`${exportDir}/archive/tables/${String(id).padStart(10, '0')}.hrt`, bytes);
  }
  console.log(`exported flash.img and ${archive.objects.size} archive objects to ${exportDir}`);
}
console.log(`LIVE LOGGER OK: ${checks} checks`);
