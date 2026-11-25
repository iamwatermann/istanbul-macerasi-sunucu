'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs'); const os = require('os'); const path = require('path');
const { IstanbulServer } = require('../src/server');
const { TestClient } = require('./client');
const P = require('../src/protocol');
const codes = require('../src/codes');
const C = require('../src/constants.json');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const reportLog = path.join(os.tmpdir(), `istnet-reports-${process.pid}.log`);
let srv, url;
test.before(async () => {
  srv = new IstanbulServer({ port: 0, host: '127.0.0.1', log: null, graceMs: 800, emptyRoomMs: 600, countdownMs: 200, knockTimeoutMs: 1500, reportLog });
  const port = await srv.start(); url = `ws://127.0.0.1:${port}`;
});
test.after(async () => { await srv.stop(); });

async function client(opts) { const c = new TestClient(url, opts); await c.open(); await c.hello(); return c; }
async function roomWith(n, mode = 'dostluk') {
  const host = await client();
  host.send({ t: 'create', mode }); const r = await host.wait('room_joined');
  const guests = [];
  for (let i = 1; i < n; i++) {
    const g = await client();
    g.send({ t: 'join', code: r.code });
    const k = await host.wait('knock'); host.send({ t: 'admit', kid: k.kid, ok: true });
    await g.wait('room_joined'); guests.push(g);
  }
  return { host, guests, room: r, all: [host, ...guests] };
}

test('binary state protocol roundtrip and size', () => {
  const b = P.encodeState({ seq: 7, x: 1.5, y: -2, z: 300.25, yaw: 1234, anim: 3, veh: 2 });
  assert.strictEqual(b.length, 19);
  const s = P.decodeState(b);
  assert.deepStrictEqual([s.seq, s.x, s.y, s.z, s.yaw, s.anim, s.veh], [7, 1.5, -2, 300.25, 1234, 3, 2]);
  const bad = Buffer.from(b); bad.writeFloatLE(NaN, 3); assert.strictEqual(P.decodeState(bad), null);
  const snap = P.encodeSnapshot(123456, 9, [{ slot: 2, seq: 1, x: 1, y: 2, z: 3, yaw: 5, anim: 1, veh: 0 }]);
  assert.strictEqual(snap.length, 7 + 19);
  assert.strictEqual(P.decodeSnapshot(snap).entries[0].slot, 2);
});

test('room codes: alphabet, normalisation, icons', () => {
  for (let i = 0; i < 200; i++) { const c = codes.genCode(); assert.match(c, /^[A-Z2-9]{6}$/); assert.ok(![...c].some(ch => 'IOQWX01'.includes(ch))); }
  assert.strictEqual(codes.normalizeCode('ab cd-ef'), 'ABCDEF');
  assert.strictEqual(codes.normalizeCode('şçü234'), 'SCU234');
  assert.strictEqual(codes.normalizeCode('ABC'), null);
  assert.strictEqual(codes.iconsKey([1, 2, 3, 99]), null);
  assert.strictEqual(codes.iconsKey([1, 2, 3, 4]), '1-2-3-4');
});

test('version check rejects old and new clients', async () => {
  const old = new TestClient(url, { v: 0 }); await old.open();
  old.send({ t: 'hello', v: 0, secret: old.secret, nick: [0, 0], avatar: {} });
  const e = await old.wait('error'); assert.strictEqual(e.code, 'version_old');
  const neu = new TestClient(url, { v: 99 }); await neu.open();
  neu.send({ t: 'hello', v: 99, secret: neu.secret, nick: [0, 0], avatar: {} });
  assert.strictEqual((await neu.wait('error')).code, 'version_new');
});

test('must say hello first; profile must be preset ids only', async () => {
  const c = new TestClient(url); await c.open();
  c.send({ t: 'create', mode: 'dostluk' }); assert.strictEqual((await c.wait('error')).code, 'hello_first');
  c.send({ t: 'hello', v: 1, secret: c.secret, nick: ['Ahmet', 'Yılmaz'], avatar: {} });
  assert.strictEqual((await c.wait('error')).code, 'bad_profile');
  c.close();
});

test('create room, stranger must knock, host admits / denies, max 6', async () => {
  const { host, guests, room } = await roomWith(6);
  assert.strictEqual(room.icons.length, 4);
  const seventh = await client();
  seventh.send({ t: 'join', code: room.code });
  assert.strictEqual((await seventh.wait('join_denied')).reason, 'full');
  // a denied knock
  guests[4].send({ t: 'leave' }); await host.wait('player_left');
  const stranger = await client();
  stranger.send({ t: 'join', icons: room.icons }); await stranger.wait('waiting_approval');
  const k = await host.wait('knock'); host.send({ t: 'admit', kid: k.kid, ok: false });
  assert.strictEqual((await stranger.wait('join_denied')).reason, 'host_said_no');
  [host, ...guests, seventh, stranger].forEach(c => c.close());
});

test('friends on host list are admitted automatically; friends_only refuses strangers', async () => {
  const friend = await client();
  const host = await client();
  host.send({ t: 'create', mode: 'dostluk', approval: 'friends_only', friends: [friend.pid] });
  const r = await host.wait('room_joined');
  friend.send({ t: 'join', code: r.code.toLowerCase() }); await friend.wait('room_joined');
  const stranger = await client(); stranger.send({ t: 'join', code: r.code });
  assert.strictEqual((await stranger.wait('join_denied')).reason, 'friends_only');
  [friend, host, stranger].forEach(c => c.close());
});

test('wrong codes are rate limited per connection', async () => {
  const c = await client();
  for (let i = 0; i < 8; i++) { c.send({ t: 'join', code: 'ZZZZZZ' }); await c.wait('join_denied'); }
  await c.wait('_closed');
  assert.strictEqual(c.closed.code, 1008);
});

test('state sync is relayed as compact binary snapshots, not echoed to sender', async () => {
  const { host, guests } = await roomWith(3);
  host.state({ seq: 1, x: 10, y: 0, z: 5, yaw: 100, anim: 1 });
  await sleep(200);
  const g = guests[0].snaps.flatMap(s => s.entries);
  assert.ok(g.some(e => e.slot === 0 && e.x === 10 && e.z === 5));
  assert.ok(!host.snaps.flatMap(s => s.entries).some(e => e.slot === 0));
  [host, ...guests].forEach(c => c.close());
});

test('shared flags, counters, builds, seats', async () => {
  const { host, guests: [a, b] } = await roomWith(3);
  host.send({ t: 'flag', k: 'quest.galata.anahtar', v: true });
  assert.strictEqual((await a.wait(m => m.t === 'flag' && m.k === 'quest.galata.anahtar')).v, true);
  a.send({ t: 'flag_add', k: 'ortak.simit', n: 3 }); await b.wait(m => m.t === 'flag' && m.v === 3);
  b.send({ t: 'flag_add', k: 'ortak.simit', n: 2 }); assert.strictEqual((await host.wait(m => m.t === 'flag' && m.k === 'ortak.simit' && m.v === 5)).v, 5);
  host.send({ t: 'flag', k: 'BAD KEY', v: 1 }); assert.strictEqual((await host.wait('error')).code, 'bad_flag');
  a.send({ t: 'build', op: 'place', kind: 12, plot: 3, pos: [1, 0, 2], r: 1, cid: 'x1' });
  const ack = await a.wait('build_ack'); const ev = await b.wait(m => m.t === 'build' && m.op === 'place');
  assert.strictEqual(ev.b.id, ack.id);
  b.send({ t: 'build', op: 'remove', id: ack.id }); assert.strictEqual((await b.wait('error')).code, 'not_yours');
  a.send({ t: 'seat', veh: 'vapur_1', capacity: 6 }); a.send({ t: 'seat', veh: 'vapur_1' }); b.send({ t: 'seat', veh: 'vapur_1', seat: 0 });
  assert.strictEqual((await b.wait('seat_denied')).reason, 'full');
  b.send({ t: 'seat', veh: 'vapur_1' });
  const s = await host.wait(m => m.t === 'seats' && m.seats.filter(x => x >= 0).length === 2);
  assert.deepStrictEqual(s.seats.slice(0, 2), [1, 2]);
  // late joiner gets full snapshot
  const late = await client(); late.send({ t: 'join', code: (await host.wait('room_joined', 100, true)).code });
  const k = await host.wait('knock'); host.send({ t: 'admit', kid: k.kid, ok: true });
  const snap = await late.wait('room_joined');
  assert.strictEqual(snap.flags['ortak.simit'], 5); assert.strictEqual(snap.builds.length, 1); assert.deepStrictEqual(snap.seats.vapur_1.slice(0, 2), [1, 2]);
  [host, a, b, late].forEach(c => c.close());
});

test('preset chat only, rate limited, blocked senders hidden, report logged without personal data', async () => {
  const { host, guests: [a, b], room } = await roomWith(3);
  a.send({ t: 'chat', p: 0, e: 3 });
  const m = await b.wait('chat'); assert.deepStrictEqual([m.from, m.p, m.e], [1, 0, 3]);
  a.send({ t: 'chat', p: 999 }); assert.strictEqual((await a.wait('error')).code, 'bad_chat');
  a.send({ t: 'chat', text: 'adresim ...' }); assert.strictEqual((await a.wait(x => x.t === 'error' && x.code === 'bad_chat')).code, 'bad_chat');
  a.send({ t: 'chat', p: 1 }); a.send({ t: 'chat', p: 1 }); a.send({ t: 'chat', p: 1 });
  assert.ok(await a.wait(x => x.t === 'error' && x.code === 'chat_slow_down'));
  b.send({ t: 'block', slot: 1 }); await b.wait('blocked');
  await sleep(1600); b.msgs.length = 0; host.msgs.length = 0;
  a.send({ t: 'chat', p: 4 });
  await host.wait(x => x.t === 'chat' && x.p === 4);
  await sleep(150);
  assert.ok(!b.msgs.some(x => x.t === 'chat'));
  host.send({ t: 'report', slot: 1, reason: 0 }); await host.wait('reported');
  const line = fs.readFileSync(reportLog, 'utf8').trim().split('\n').pop(); const rec = JSON.parse(line);
  assert.strictEqual(rec.target, a.pid); assert.ok(!('ip' in rec)); assert.ok(Array.isArray(rec.recent_chat));
  // host blocking = kick + ban from this room
  host.send({ t: 'block', slot: 1 }); await a.wait('kicked');
  a.send({ t: 'join', code: room.code }); assert.strictEqual((await a.wait('join_denied')).reason, 'not_found');
  [host, a, b].forEach(c => c.close());
});

test('mutual friend requests create friendship', async () => {
  const { host, guests: [a] } = await roomWith(2);
  host.send({ t: 'friend_req', slot: 1 }); await a.wait('friend_req');
  a.send({ t: 'friend_req', slot: 0 });
  const fa = await a.wait('friend_added'); const fh = await host.wait('friend_added');
  assert.strictEqual(fa.pid, host.pid); assert.strictEqual(fh.pid, a.pid);
  [host, a].forEach(c => c.close());
});

test('reconnection with resume token keeps slot; timeout frees it; host migrates', async () => {
  const { host, guests: [a, b], room } = await roomWith(3);
  const joined = await a.wait('room_joined', 100, true);
  a.close(); await host.wait(m => m.t === 'player_away' && m.slot === 1);
  const a2 = new TestClient(url, { secret: a.secret }); await a2.open(); await a2.hello();
  a2.send({ t: 'resume', code: room.code, token: joined.resume });
  const r = await a2.wait('room_joined'); assert.strictEqual(r.slot, 1); assert.strictEqual(r.resumed, true);
  await host.wait(m => m.t === 'player_back' && m.slot === 1);
  // wrong identity cannot steal a token
  const thief = await client(); thief.send({ t: 'resume', code: room.code, token: r.resume }); await thief.wait('resume_failed');
  // host drops -> host migrates to slot 1
  host.close(); const hc = await a2.wait('host_changed'); assert.strictEqual(hc.slot, 1);
  await b.wait(m => m.t === 'player_left' && m.slot === 0 && m.reason === 'timeout', 3000);
  [a2, b, thief].forEach(c => c.close());
});

test('empty rooms expire', async () => {
  const host = await client(); host.send({ t: 'create', mode: 'rakip' }); const r = await host.wait('room_joined');
  host.send({ t: 'leave' }); await host.wait('room_left');
  await sleep(2000);
  assert.ok(!srv.rooms.has(r.code));
  host.close();
});

test('rival race: server validates order + proximity, computes ranking', async () => {
  const { host, guests: [a, b] } = await roomWith(3, 'rakip');
  const pts = [[0, 0, 0], [50, 0, 0], [100, 0, 0]];
  host.send({ t: 'rival_start', kind: 'race', points: pts, dur: 20 });
  await a.wait('rival_countdown'); await a.wait('rival_go', 2000);
  // cheater claims checkpoint 1 while standing at 0
  b.state({ seq: 1, x: 0, y: 0, z: 0 }); await sleep(60); b.send({ t: 'rival_claim', i: 1 });
  assert.strictEqual((await b.wait('claim_rejected')).reason, 'wrong_order');
  b.send({ t: 'rival_claim', i: 0 }); await b.wait(m => m.t === 'rival_score' && m.slot === 2);
  b.send({ t: 'rival_claim', i: 1 }); assert.strictEqual((await b.wait(m => m.t === 'claim_rejected' && m.i === 1)).reason, 'too_far');
  // a: honest, moves steadily
  let seq = 1;
  for (let x = 0; x <= 100; x += 2) {
    a.state({ seq: seq++, x, y: 0, z: 0 }); await sleep(60);
    if (x % 50 === 0) a.send({ t: 'rival_claim', i: x / 50 });
  }
  await host.wait(m => m.t === 'rival_score' && m.slot === 1 && m.finished);
  // host teleports to the end -> rejected as teleport
  host.state({ seq: 1, x: 0, y: 0, z: 0 }); await sleep(60); host.send({ t: 'rival_claim', i: 0 }); await host.wait(m => m.t === 'rival_score' && m.slot === 0);
  host.state({ seq: 2, x: 50, y: 0, z: 0 }); await sleep(20); host.send({ t: 'rival_claim', i: 1 });
  assert.strictEqual((await host.wait(m => m.t === 'claim_rejected' && m.i === 1)).reason, 'teleport');
  host.send({ t: 'rival_stop' });
  const end = await a.wait('rival_end');
  assert.strictEqual(end.ranking[0].slot, 1); assert.strictEqual(end.ranking[0].finished, true);
  assert.deepStrictEqual(end.ranking.map(r => r.place), [1, 2, 3]);
  [host, a, b].forEach(c => c.close());
});

test('rival collect: first claim wins, results when all taken', async () => {
  const { host, guests: [a] } = await roomWith(2, 'rakip');
  host.send({ t: 'rival_start', kind: 'collect', points: [[0, 0, 0], [3, 0, 0]], dur: 15 });
  await a.wait('rival_go', 2000);
  host.state({ seq: 1, x: 0, y: 0, z: 0 }); a.state({ seq: 1, x: 1, y: 0, z: 0 }); await sleep(60);
  host.send({ t: 'rival_claim', i: 0 }); await host.wait(m => m.t === 'rival_score');
  a.send({ t: 'rival_claim', i: 0 }); assert.strictEqual((await a.wait('claim_rejected')).reason, 'taken');
  a.send({ t: 'rival_claim', i: 1 });
  const end = await host.wait('rival_end');
  assert.strictEqual(end.ranking.length, 2); assert.ok(end.ranking.every(r => r.score === 1));
  [host, a].forEach(c => c.close());
});

test('constants file matches the Godot addon copy', () => {
  const gd = path.join(__dirname, '../../addons/istanbul_net/net_constants.gd');
  if (!fs.existsSync(gd)) return; // standalone server repo: addon not present
  const txt = fs.readFileSync(gd, 'utf8');
  assert.deepStrictEqual(JSON.parse(txt.slice(txt.indexOf('const DATA := ') + 14)), C);
});

test('outfit change is validated and broadcast', async () => {
  const { host, guests: [a] } = await roomWith(2);
  a.send({ t: 'outfit', avatar: { shirt: 5, hat: 2 } });
  const u = await host.wait('player_update'); assert.strictEqual(u.slot, 1); assert.strictEqual(u.avatar.shirt, 5);
  a.send({ t: 'outfit', avatar: { shirt: 999 } }); assert.strictEqual((await a.wait('error')).code, 'bad_profile');
  [host, a].forEach(c => c.close());
});

test('health endpoint and oversize frames are refused', async () => {
  const http = require('http');
  const body = await new Promise(r => http.get(url.replace('ws', 'http') + '/health', res => { let d = ''; res.on('data', c => d += c); res.on('end', () => r(JSON.parse(d))); }));
  assert.strictEqual(body.ok, true); assert.strictEqual(body.protocol, 1);
  const c = await client(); c.ws.send('x'.repeat(5000)); await c.wait('_closed'); assert.strictEqual(c.closed.code, 1009);
});
