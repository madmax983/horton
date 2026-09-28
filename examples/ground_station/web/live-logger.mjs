// The live logger's JavaScript half: the wasm module (live/src/lib.rs) over
// a block device and an archive store the caller provides. The Worker
// (logger-worker.mjs) gives it an OPFS file and IndexedDB; the Node test
// (live-test.mjs) gives it memory that can lose power mid-write. Both run
// this same code.
//
//   device  = { read(id, dst), write(id, src), flush() }        synchronous
//   archive = { put(id, bytes), get(id), list() }                async

export const RECORDED = 0;
export const COLD_WAITING = 1;
export const NEEDS_ARCHIVE = 2;

const BLOCK = 4096;

/**
 * Thrown by a test device to stop the world mid-operation, like pulling
 * the plug. It unwinds straight through the wasm module, which is then
 * dead: load a new one over the same device to "reboot".
 */
export class PowerCut extends Error {
  constructor() {
    super('power cut');
    this.powerCut = true;
  }
}

export class LiveLogger {
  /**
   * @param {BufferSource | WebAssembly.Module} wasm
   * @param {{ device: object, archive: object, log?: (msg: string) => void }} io
   */
  static async load(wasm, io) {
    const module = wasm instanceof WebAssembly.Module ? wasm : await WebAssembly.compile(wasm);
    const logger = new LiveLogger(io);
    const bytes = (ptr, len) => new Uint8Array(logger.x.memory.buffer, ptr, len);
    // Each import reports failure as a status code, which horton turns
    // into Error::Device. A PowerCut is not a failure: it ends the world.
    const guard = (fn) => {
      try {
        fn();
        return 0;
      } catch (e) {
        if (e?.powerCut) throw e;
        logger.hostError = String(e?.message ?? e);
        return 1;
      }
    };
    const instance = await WebAssembly.instantiate(module, {
      horton_host: {
        lv_read: (id, dst, len) => guard(() => io.device.read(Number(id), bytes(dst, len))),
        lv_write: (id, src, len) => guard(() => io.device.write(Number(id), bytes(src, len))),
        lv_flush: () => guard(() => io.device.flush()),
      },
    });
    // A trap means the module panicked (a bug): name where, from the
    // message its panic handler left in the error buffer.
    const x = instance.exports;
    const panicked = (e, name) => {
      if (e instanceof WebAssembly.RuntimeError) {
        const msg = new TextDecoder().decode(new Uint8Array(x.memory.buffer, x.lv_error_ptr(), x.lv_error_len()));
        e.message = `${e.message} in ${name}: ${msg}`;
      }
      return e;
    };
    logger.x = Object.fromEntries(Object.entries(x).map(([name, f]) => [
      name,
      typeof f !== 'function' ? f : (...a) => {
        try {
          return f(...a);
        } catch (e) {
          throw panicked(e, name);
        }
      },
    ]));
    logger.flashLen = Number(logger.x.lv_flash_len());
    return logger;
  }

  constructor({ device, archive, log }) {
    this.device = device;
    this.archive = archive;
    this.log = log ?? (() => {});
    this.hostError = null;
    this.coldWaiting = false;
    // Recording and archiving share the staged-table slot in the module,
    // so they run one at a time: a frame that arrives while an archive
    // write is awaited waits its turn.
    this.lock = Promise.resolve();
  }

  #exclusive(fn) {
    const run = this.lock.then(fn);
    this.lock = run.catch(() => {});
    return run;
  }

  #out(n) {
    return new Float64Array(this.x.memory.buffer, this.x.lv_out_ptr(), n);
  }

  #check(rc, what) {
    if (rc >= 0) return rc;
    const msg = new TextDecoder().decode(
      new Uint8Array(this.x.memory.buffer, this.x.lv_error_ptr(), this.x.lv_error_len()),
    );
    const host = this.hostError ? ` (host: ${this.hostError})` : '';
    throw new Error(`${msg || `${what} failed`}${host}`);
  }

  /**
   * Opens the flash as the device would on boot, and counts the boot
   * (archiving first if the flash has no room). Resolves to the WAL
   * records replayed.
   */
  open() {
    return this.#exclusive(async () => {
      const replayed = this.#check(this.x.lv_open(), 'open');
      for (let attempt = 0; ; attempt++) {
        if (this.#check(this.x.lv_count_boot(), 'count boot') !== NEEDS_ARCHIVE) break;
        if ((await this.#archivePass()) === 0 && attempt > 0) {
          throw new Error('the flash is full and nothing is cold enough to archive');
        }
      }
      return replayed;
    });
  }

  /**
   * Records one frame durably; resolves once it is safe to acknowledge.
   * When the flash has no room until cold tables leave, archives first.
   * @param {{ tick: number, readings: bigint[], alarm: number | null }} frame
   */
  record(frame) {
    return this.#exclusive(async () => {
      for (let attempt = 0; ; attempt++) {
        const input = new BigUint64Array(this.x.memory.buffer, this.x.lv_frame_ptr(), this.x.lv_frame_len());
        input[0] = BigInt(frame.tick);
        frame.readings.forEach((r, i) => { input[1 + i] = r; });
        input[5] = frame.alarm === null ? (1n << 64n) - 1n : BigInt(frame.alarm);
        const rc = this.#check(this.x.lv_record(), 'record');
        if (rc === NEEDS_ARCHIVE) {
          if ((await this.#archivePass()) === 0 && attempt > 0) {
            throw new Error(`tick ${frame.tick}: the flash is full and nothing is cold enough to archive`);
          }
          continue;
        }
        if (rc === COLD_WAITING) this.coldWaiting = true;
        return rc;
      }
    });
  }

  /** Moves every cold table to the archive. Resolves to how many left the flash. */
  archiveCold() {
    return this.#exclusive(() => this.#archivePass());
  }

  // Stage, store (awaited: the only asynchronous step, between two
  // horton calls), commit. Resolves to tables committed away.
  async #archivePass() {
    const seen = new Set();
    let moved = 0;
    for (;;) {
      if (this.#check(this.x.lv_archive_next(), 'archive') === 0) break;
      const [id, level, len, first, last] = this.#out(5);
      if (seen.has(id)) break; // refused this pass already; compaction must merge it first
      seen.add(id);
      // Copy out before awaiting: the buffer is the module's.
      const object = new Uint8Array(this.x.memory.buffer, this.x.lv_archive_ptr(), len).slice();
      await this.archive.put(id, object);
      const committed = this.#check(this.x.lv_archive_commit(), 'archive commit');
      if (committed === 1) {
        moved++;
        this.log(`archived table ${id} (L${level}, ticks ${first}–${last}, ${len / BLOCK} blocks)`);
      } else if (committed === 0) {
        this.log(`table ${id} was merged while being archived; the archive keeps a spare copy`);
      } else {
        this.log(`table ${id} stays on flash: archiving it now would bring deleted data back`);
      }
    }
    this.coldWaiting = false;
    return moved;
  }

  stats() {
    const n = this.#check(this.x.lv_stats(), 'stats');
    const o = Array.from(this.#out(n));
    return {
      frames: o[0], flushes: o[1], compactionSteps: o[2], archivedTables: o[3],
      archivedBlocks: o[4], archiveRefusals: o[5], stalls: o[6],
      newestTick: Number.isNaN(o[7]) ? null : o[7], slotsUsed: o[8], slotsFree: o[9],
    };
  }

  /** Frame rows for the charts; same layout as GroundStation.frames. */
  frames(from, count, now) {
    const rows = this.#check(this.x.lv_frames(BigInt(from), count, BigInt(now)), 'frames');
    const o = this.#out(rows * 8);
    const orNull = (v) => (Number.isNaN(v) ? null : v);
    const out = [];
    for (let i = 0; i < rows; i++) {
      const r = o.subarray(i * 8, (i + 1) * 8);
      out.push({
        tick: r[0],
        sensors: [orNull(r[1]), orNull(r[2]), orNull(r[3]), orNull(r[4])],
        alarm: orNull(r[5]),
        trace: r[6] === 1,
        live: r[7] === 1,
      });
    }
    return out;
  }

  /** Every entry on flash checked; same layout as GroundStation.verify. */
  verifyFlash() {
    const n = this.#check(this.x.lv_verify(), 'verify');
    const o = Array.from(this.#out(n));
    return { entries: o[0], badValues: o[4], outOfOrder: o[5], tornFrames: o[7], invariants: o[8] === 1, splitFrames: o[10] };
  }

  #coverage() {
    const [first, len, entries, bad, table] = this.#out(5);
    const bits = new Uint8Array(this.x.memory.buffer, this.x.lv_cover_ptr(), len).slice();
    return { first: Number.isNaN(first) ? null : first, bits, entries, bad, table };
  }

  /**
   * Proves nothing acknowledged is lost: marks which parts of every tick's
   * frame the flash and each archived table hold (reading each table back
   * through horton's ingest), then checks every tick from the first to
   * `upTo` (default: the newest on flash) is whole somewhere, with its
   * alarm on alarm ticks.
   */
  verifyHistory(upTo) {
    return this.#exclusive(async () => {
      const parts = [];
      this.#check(this.x.lv_cover(), 'cover');
      parts.push({ where: 'flash', ...this.#coverage() });
      const ids = await this.archive.list();
      let archiveBytes = 0;
      for (const id of ids) {
        const object = await this.archive.get(id);
        archiveBytes += object.length;
        if (object.length > this.x.lv_archive_cap()) throw new Error(`archived table ${id} is too large to read back`);
        new Uint8Array(this.x.memory.buffer, this.x.lv_archive_ptr(), object.length).set(object);
        this.#check(this.x.lv_archive_cover(object.length), `archived table ${id}`);
        parts.push({ where: 'archive', ...this.#coverage() });
      }
      const flash = parts[0];
      let first = Infinity;
      let last = -1;
      for (const p of parts) {
        if (p.first === null) continue;
        first = Math.min(first, p.first);
        last = Math.max(last, p.first + p.bits.length - 1);
      }
      const newest = this.stats().newestTick;
      const end = upTo ?? newest ?? last;
      const report = {
        firstTick: Number.isFinite(first) ? first : null,
        upTo: end,
        tables: ids.length,
        archiveBytes,
        badValues: parts.reduce((a, p) => a + p.bad, 0),
        flashTicks: 0,
        archiveOnlyTicks: 0,
        missing: [],
        missingCount: 0,
        detail: [],
      };
      if (!Number.isFinite(first) || end === null || end < first) return { ...report, complete: report.badValues === 0 && end === null };
      const all = new Uint8Array(end - first + 1);
      const onFlash = new Uint8Array(end - first + 1);
      for (const p of parts) {
        if (p.first === null) continue;
        for (let i = 0; i < p.bits.length; i++) {
          const t = p.first + i - first;
          if (t < 0 || t >= all.length) continue;
          all[t] |= p.bits[i];
          if (p === flash) onFlash[t] |= p.bits[i];
        }
      }
      for (let i = 0; i < all.length; i++) {
        const t = first + i;
        const alarm = t % 250 === 0 ? 0x10 : 0;
        const need = 0x0f | alarm;
        if ((all[i] & need) !== need) {
          report.missingCount++;
          if (report.missing.length < 10) {
            report.missing.push(t);
            // Which parts of the frame each source holds, for diagnosis.
            const holders = parts
              .filter((p) => p.first !== null && t >= p.first && t < p.first + p.bits.length && p.bits[t - p.first])
              .map((p) => `${p.where}${p.where === 'archive' ? ` table ${p.table}` : ''}: ${p.bits[t - p.first].toString(2).padStart(5, '0')}`);
            report.detail.push(`tick ${t}: ${holders.join('; ') || 'nowhere'}`);
          }
        } else if ((onFlash[i] & need) === need) report.flashTicks++;
        else report.archiveOnlyTicks++;
      }
      report.complete = report.missingCount === 0 && report.badValues === 0;
      return report;
    });
  }
}
