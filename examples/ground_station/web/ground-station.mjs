// A thin wrapper over the ground station's wasm exports (src/lib.rs).
// The page (app.mjs) and the Node test (test.mjs) both use it, so the test
// exercises the same code path as the browser.

/** Columns per frame row: tick, s0..s3, alarm, trace, live. */
export const FRAME_COLS = 8;

export class GroundStation {
  /** @param {BufferSource | WebAssembly.Module} wasm the compiled module or its bytes */
  static async load(wasm) {
    const module = wasm instanceof WebAssembly.Module ? wasm : await WebAssembly.compile(wasm);
    // No imports: the module cannot call out, so it can never re-enter.
    const instance = await WebAssembly.instantiate(module, {});
    return new GroundStation(instance.exports);
  }

  constructor(x) {
    this.x = x;
    this.imageLen = Number(x.gs_image_len());
    this.ttlTicks = Number(x.gs_ttl_ticks());
  }

  // Views are made fresh on every read: a view of wasm memory goes stale
  // if the memory ever grows.
  #out(n) {
    return new Float64Array(this.x.memory.buffer, this.x.gs_out_ptr(), n);
  }

  #check(rc, what) {
    if (rc >= 0) return rc;
    throw new Error(this.#lastError() || `${what} failed`);
  }

  /**
   * Copies a flash dump in and opens it. The caller's bytes are copied, so
   * recovery's writes never reach them. Returns the WAL records replayed.
   * @param {Uint8Array} bytes
   */
  open(bytes) {
    if (bytes.length !== this.imageLen) {
      throw new Error(
        `a recorder flash dump is ${this.imageLen} bytes (512 sectors of 4 KiB); this is ${bytes.length}`,
      );
    }
    new Uint8Array(this.x.memory.buffer, this.x.gs_image_ptr(), this.imageLen).set(bytes);
    return this.#check(this.x.gs_open(), 'open');
  }

  summary() {
    const n = this.#check(this.x.gs_summary(), 'summary');
    const o = Array.from(this.#out(n));
    const orNull = (v) => (Number.isNaN(v) ? null : v);
    return {
      recovered: o[0],
      maxSeq: o[1],
      boots: orNull(o[2]),
      oldestTick: orNull(o[3]),
      newestTick: orNull(o[4]),
      slots: { total: o[5], blocks: o[6], used: o[7], free: o[8] },
      manifestCopies: o[9],
      wal: [o[10], o[11]],
      tables: [o[12], o[13]],
      levels: o.slice(14),
    };
  }

  /**
   * One row per tick in [from, from + count): sensors in °C (null where the
   * flash has none), the alarm code or null, and whether the debug trace is
   * stored and still live at `now`.
   */
  frames(from, count, now) {
    const rows = this.#check(this.x.gs_frames(BigInt(from), count, BigInt(now)), 'frames');
    const o = this.#out(rows * FRAME_COLS);
    const orNull = (v) => (Number.isNaN(v) ? null : v);
    const out = [];
    for (let i = 0; i < rows; i++) {
      const r = o.subarray(i * FRAME_COLS, (i + 1) * FRAME_COLS);
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

  /** A point read: `{ celsius, intact }`, or null when absent. */
  reading(tick, sensor) {
    const found = this.#check(this.x.gs_reading(BigInt(tick), sensor), 'reading');
    if (found === 0) return null;
    const [celsius, intact] = this.#out(2);
    return { celsius, intact: intact === 1 };
  }

  /**
   * Checks every entry; see `verify` in src/common.rs. `splitFrames` are
   * frames partly archived (a table boundary cut them), holding
   * `splitReadings` readings here; `tornFrames` are anything else partial.
   */
  verify() {
    const n = this.#check(this.x.gs_verify(), 'verify');
    const o = Array.from(this.#out(n));
    const invariants = o[8] === 1;
    return {
      entries: o[0],
      readings: o[1],
      alarms: o[2],
      traces: o[3],
      badValues: o[4],
      outOfOrder: o[5],
      wholeFrames: o[6],
      tornFrames: o[7],
      splitReadings: o[9],
      splitFrames: o[10],
      invariants,
      invariantError: invariants ? null : this.#lastError(),
    };
  }

  #lastError() {
    return new TextDecoder().decode(
      new Uint8Array(this.x.memory.buffer, this.x.gs_error_ptr(), this.x.gs_error_len()),
    );
  }
}
