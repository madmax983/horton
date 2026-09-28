// The live logger's Worker: horton's flash is an OPFS file, its archive an
// IndexedDB store. OPFS's sync access handle (Worker-only) has synchronous
// read, write and flush, which is exactly what the module's three block
// device imports need. IndexedDB is asynchronous, and live-logger.mjs only
// awaits it between two horton calls.
//
// Messages are handled strictly one after another, so frames are recorded
// in the order they arrive and acknowledged only once durable.

import { LiveLogger } from './live-logger.mjs';

const FLASH = 'horton-live-flash.img';
const DB = 'horton-live';
const STORE = 'tables';
const BLOCK = 4096;

let logger = null;
let handle = null;
let idb = null;

const post = (msg, transfer) => self.postMessage(msg, transfer ?? []);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function openFlash(len) {
  const root = await navigator.storage.getDirectory();
  const file = await root.getFileHandle(FLASH, { create: true });
  // The handle is exclusive. After a crash the dead Worker's handle can
  // take a moment to be released, so retry briefly.
  for (let attempt = 0; ; attempt++) {
    try {
      handle = await file.createSyncAccessHandle();
      break;
    } catch (e) {
      if (attempt >= 50) throw e;
      await sleep(100);
    }
  }
  // A new file is fresh flash: erased (0xFF), the recorder's partition size.
  const size = handle.getSize();
  if (size < len) {
    const erased = new Uint8Array(64 * BLOCK).fill(0xff);
    for (let at = size; at < len; at += erased.length) {
      handle.write(erased.subarray(0, Math.min(erased.length, len - at)), { at });
    }
    handle.flush();
  }
}

const device = {
  read(id, dst) {
    const n = handle.read(dst, { at: id * BLOCK });
    if (n < dst.length) dst.fill(0xff, n);
  },
  write(id, src) {
    const n = handle.write(src, { at: id * BLOCK });
    if (n !== src.length) throw new Error(`short write: ${n} of ${src.length} bytes`);
  },
  flush() {
    handle.flush();
  },
};

function openArchive() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB, 1);
    req.onupgradeneeded = () => req.result.createObjectStore(STORE);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function tx(mode, fn) {
  return new Promise((resolve, reject) => {
    // 'strict': complete fires only once the data is on disk, so a table
    // is committed away from flash only after its copy is durable.
    const t = idb.transaction(STORE, mode, { durability: 'strict' });
    const result = fn(t.objectStore(STORE));
    t.oncomplete = () => resolve(result.result);
    t.onerror = () => reject(t.error);
    t.onabort = () => reject(t.error ?? new Error('IndexedDB transaction aborted'));
  });
}

const archive = {
  put: (id, bytes) => tx('readwrite', (s) => s.put(bytes, id)),
  get: (id) => tx('readonly', (s) => s.get(id)),
  list: () => tx('readonly', (s) => s.getAllKeys()),
};

async function archiveBytes() {
  let bytes = 0;
  await tx('readonly', (s) => {
    const req = s.openCursor();
    req.onsuccess = () => {
      const c = req.result;
      if (!c) return;
      bytes += c.value.byteLength;
      c.continue();
    };
    return req;
  });
  return bytes;
}

async function status() {
  const s = logger.stats();
  return { ...s, archiveTables: (await archive.list()).length, archiveBytes: await archiveBytes() };
}

const handlers = {
  async start() {
    const wasm = await (await fetch('live.wasm')).arrayBuffer();
    idb = await openArchive();
    logger = await LiveLogger.load(wasm, { device, archive, log: (msg) => post({ type: 'log', msg }) });
    await openFlash(logger.flashLen);
    const replayed = await logger.open();
    const sum = logger.x.lv_summary();
    const boots = new Float64Array(logger.x.memory.buffer, logger.x.lv_out_ptr(), sum)[2];
    post({ type: 'ready', replayed, boots, status: await status() });
  },

  async frames({ frames }) {
    for (const f of frames) {
      await logger.record(f);
      if (logger.coldWaiting) await logger.archiveCold();
    }
    post({ type: 'ack', upTo: frames.at(-1).tick, stats: logger.stats() });
  },

  async status() {
    post({ type: 'status', status: await status() });
  },

  async window({ count }) {
    const newest = logger.stats().newestTick;
    const rows = newest === null ? [] : logger.frames(Math.max(0, newest - count + 1), Math.min(count, newest + 1), newest);
    post({ type: 'window', rows });
  },

  async verify() {
    const started = performance.now();
    const report = await logger.verifyHistory();
    post({ type: 'verified', report, ms: performance.now() - started });
  },

  async export() {
    const bytes = new Uint8Array(handle.getSize());
    handle.read(bytes, { at: 0 });
    post({ type: 'exported', bytes }, [bytes.buffer]);
  },

  async reset() {
    handle?.close();
    handle = null;
    idb?.close();
    const root = await navigator.storage.getDirectory();
    await root.removeEntry(FLASH).catch(() => {});
    await new Promise((resolve, reject) => {
      const req = indexedDB.deleteDatabase(DB);
      req.onsuccess = resolve;
      req.onerror = () => reject(req.error);
      req.onblocked = resolve;
    });
    post({ type: 'reset' });
  },
};

let queue = Promise.resolve();
self.onmessage = (e) => {
  queue = queue
    .then(() => handlers[e.data.type](e.data))
    .catch((err) => post({ type: 'error', message: String(err?.message ?? err), during: e.data.type }));
};
