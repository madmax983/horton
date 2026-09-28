// The flight recorder's key schema and telemetry, mirrored in JavaScript
// from examples/flight_recorder/format.rs. The simulated device (live.mjs)
// sends frames built here, and the tests check horton's reads against it.

export const SENSORS = 4;
/** An alarm event joins the frame every this many ticks. */
export const EVENT_EVERY = 250;

const MASK = (1n << 64n) - 1n;

/** The key of `kind` ('r' reading, 'e' event, 'd' debug) at tick `t`. */
export function key(tick, kind, id) {
  const k = new Uint8Array(10);
  new DataView(k.buffer).setBigUint64(0, BigInt(tick));
  k[8] = kind.charCodeAt(0);
  k[9] = id;
  return k;
}

/**
 * The value the recorder stores under `key`, as a u64 (its 8 bytes read
 * little-endian): FNV-1a over the key, then a murmur finalizer. Every
 * value is a hash of its key, so a checker needs no copy of the data.
 */
export function value(key) {
  let h = 0xcbf29ce484222325n;
  for (const b of key) h = ((h ^ BigInt(b)) * 0x100000001b3n) & MASK;
  h ^= h >> 33n;
  h = (h * 0xff51afd7ed558ccdn) & MASK;
  h ^= h >> 33n;
  return h;
}

/** A reading's temperature, for display: 15.00 to 34.99 °C. */
export function celsius(tick, sensor) {
  return 15 + Number(value(key(tick, 'r', sensor)) % 2000n) / 100;
}

/** The alarm code the device raises at tick `t`, or null. */
export function alarmAt(tick) {
  return tick % EVENT_EVERY === 0 ? (tick / EVENT_EVERY) % 7 : null;
}

/**
 * The frame a recorder device sends at tick `t`: four readings (each a
 * u64, the value's 8 bytes) and the alarm code, if any.
 */
export function frameAt(tick) {
  const readings = [];
  for (let s = 0; s < SENSORS; s++) readings.push(value(key(tick, 'r', s)));
  return { tick, readings, alarm: alarmAt(tick) };
}
