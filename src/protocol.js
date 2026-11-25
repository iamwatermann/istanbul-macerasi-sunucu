'use strict';
// Binary player-state protocol (little endian).
// Client -> server  STATE (19 bytes):
//   u8 type=1 | u16 seq | f32 x | f32 y | f32 z | u16 yaw (0..65535 = 0..2PI) | u8 anim | u8 veh
// Server -> client  SNAPSHOT batch (7 + 19*n bytes):
//   u8 type=3 | u32 server_ms (mod 2^32) | u8 tick (mod 256) | u8 count | count x ENTRY
//   ENTRY (19 bytes): u8 slot | u16 seq | f32 x | f32 y | f32 z | u16 yaw | u8 anim | u8 veh
const T_STATE = 1, T_SNAPSHOT = 3;
const STATE_LEN = 19, ENTRY_LEN = 19, SNAP_HDR = 7;
const COORD_LIMIT = 100000;

function decodeState(buf) {
  if (!buf || buf.length !== STATE_LEN || buf[0] !== T_STATE) return null;
  const s = {
    seq: buf.readUInt16LE(1),
    x: buf.readFloatLE(3), y: buf.readFloatLE(7), z: buf.readFloatLE(11),
    yaw: buf.readUInt16LE(15), anim: buf[17], veh: buf[18],
  };
  for (const k of ['x', 'y', 'z']) if (!Number.isFinite(s[k]) || Math.abs(s[k]) > COORD_LIMIT) return null;
  return s;
}
function encodeState(s) {
  const b = Buffer.alloc(STATE_LEN);
  b[0] = T_STATE; b.writeUInt16LE(s.seq & 0xffff, 1);
  b.writeFloatLE(s.x, 3); b.writeFloatLE(s.y, 7); b.writeFloatLE(s.z, 11);
  b.writeUInt16LE(s.yaw & 0xffff, 15); b[17] = s.anim & 0xff; b[18] = s.veh & 0xff;
  return b;
}
function encodeSnapshot(serverMs, tick, entries) {
  const b = Buffer.alloc(SNAP_HDR + ENTRY_LEN * entries.length);
  b[0] = T_SNAPSHOT; b.writeUInt32LE(serverMs >>> 0, 1); b[5] = tick & 0xff; b[6] = entries.length;
  let o = SNAP_HDR;
  for (const e of entries) {
    b[o] = e.slot; b.writeUInt16LE(e.seq & 0xffff, o + 1);
    b.writeFloatLE(e.x, o + 3); b.writeFloatLE(e.y, o + 7); b.writeFloatLE(e.z, o + 11);
    b.writeUInt16LE(e.yaw & 0xffff, o + 15); b[o + 17] = e.anim; b[o + 18] = e.veh;
    o += ENTRY_LEN;
  }
  return b;
}
function decodeSnapshot(b) {
  if (!b || b.length < SNAP_HDR || b[0] !== T_SNAPSHOT) return null;
  const n = b[6]; if (b.length !== SNAP_HDR + n * ENTRY_LEN) return null;
  const out = { serverMs: b.readUInt32LE(1), tick: b[5], entries: [] };
  let o = SNAP_HDR;
  for (let i = 0; i < n; i++, o += ENTRY_LEN) out.entries.push({
    slot: b[o], seq: b.readUInt16LE(o + 1), x: b.readFloatLE(o + 3), y: b.readFloatLE(o + 7), z: b.readFloatLE(o + 11),
    yaw: b.readUInt16LE(o + 15), anim: b[o + 17], veh: b[o + 18] });
  return out;
}
module.exports = { T_STATE, T_SNAPSHOT, STATE_LEN, decodeState, encodeState, encodeSnapshot, decodeSnapshot };
