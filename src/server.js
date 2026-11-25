'use strict';
/*
 * İstanbul Macerası — relay + room server (protocol v1).
 * Server-authoritative for: room membership, admission, shared world state (flags, counters,
 * builds, vehicle seats), rival events (timer, checkpoint/pickup validation, scores, results),
 * chat validation (preset ids only), block/report/kick.
 * Client-authoritative (with plausibility checks) for: its own avatar movement.
 */
const http = require('http');
const https = require('https');
const fs = require('fs');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const C = require('./constants.json');
const P = require('./protocol');
const { TokenBucket } = require('./ratelimit');
const codes = require('./codes');

const DEFAULTS = {
  port: 8787, host: '0.0.0.0',
  tickHz: 15,                 // snapshot broadcast rate
  maxPayload: 2048,           // bytes per frame
  graceMs: 60_000,            // keep a dropped player's slot for reconnection
  emptyRoomMs: 120_000,       // delete empty rooms after this
  idleRoomMs: 30 * 60_000,    // close rooms with no messages for this long
  maxRoomAgeMs: 6 * 3600_000, // hard room lifetime
  knockTimeoutMs: 60_000,     // host must answer a join request within this
  maxRooms: 5000,
  maxConnsPerIp: 64,          // carrier-grade NAT puts many phones behind one IP
  joinFailPerIpPer10Min: 30,
  maxSpeed: 70,               // m/s; faster = teleport (rival claims ignored briefly)
  rivalRadius: 6,
  countdownMs: 3000,
  pingIntervalMs: 20_000,
  reportLog: process.env.REPORT_LOG || 'reports.log',
  tlsCert: null, tlsKey: null,
  log: (ev, data) => console.log(JSON.stringify({ ts: new Date().toISOString(), ev, ...data })),
};

const KEY_RE = /^[A-Za-z0-9_.:\-]{1,64}$/;
const VEH_RE = /^[a-z0-9_\-]{1,32}$/;
const isInt = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
const randToken = () => crypto.randomBytes(16).toString('hex');

function validAvatar(a) {
  if (!a || typeof a !== 'object' || Array.isArray(a)) return null;
  const out = {};
  for (const [k, n] of Object.entries(C.avatar_fields)) {
    const v = a[k] === undefined ? 0 : a[k];
    if (!isInt(v, 0, n - 1)) return null;
    out[k] = v;
  }
  return out;
}
function validNick(n) {
  if (!Array.isArray(n) || n.length !== 2) return null;
  if (!isInt(n[0], 0, C.nick_adjectives.length - 1) || !isInt(n[1], 0, C.nick_nouns.length - 1)) return null;
  return [n[0], n[1]];
}
function validFlagValue(v) {
  if (typeof v === 'boolean') return true;
  if (typeof v === 'number') return Number.isFinite(v);
  if (typeof v === 'string') return v.length <= 64;
  return false;
}
function validPoint(p) {
  return Array.isArray(p) && p.length === 3 && p.every(n => typeof n === 'number' && Number.isFinite(n) && Math.abs(n) < 100000);
}

class Room {
  constructor(srv, code, icons, mode, opts) {
    this.srv = srv; this.code = code; this.icons = icons; this.mode = mode;
    this.approval = opts.approval === 'friends_only' ? 'friends_only' : 'knock';
    this.friendPids = new Set(opts.friends || []);
    this.bannedPids = new Set(opts.blocked || []);
    this.created = Date.now(); this.lastActivity = Date.now(); this.emptySince = null;
    this.players = new Array(C.max_players).fill(null);
    this.hostSlot = -1;
    this.knocks = new Map(); // kid -> {conn, expires}
    this.nextKid = 1;
    this.flags = new Map();
    this.builds = new Map(); this.nextBuildId = 1;
    this.vehicles = new Map(); // veh -> array of slot|-1
    this.friendReqs = new Set();
    this.rival = null; this.tick = 0;
  }
  present() { return this.players.filter(p => p && p.conn); }
  count() { return this.players.filter(Boolean).length; }
  freeSlot() { return this.players.findIndex(p => p === null); }
  publicPlayer(p) { return { slot: p.slot, nick: p.nick, avatar: p.avatar, pid: p.pid, away: !p.conn, host: p.slot === this.hostSlot }; }
  snapshot(forSlot) {
    return {
      code: this.code, icons: this.icons, mode: this.mode, approval: this.approval,
      slot: forSlot, host: this.hostSlot,
      players: this.players.filter(Boolean).map(p => this.publicPlayer(p)),
      flags: Object.fromEntries(this.flags),
      builds: [...this.builds.values()],
      seats: Object.fromEntries([...this.vehicles].map(([k, v]) => [k, v])),
      rival: this.rivalPublic(),
    };
  }
  rivalPublic() {
    const r = this.rival; if (!r) return null;
    return { id: r.id, kind: r.kind, points: r.points, radius: r.radius, state: r.state, start_at: r.startAt, end_at: r.endAt,
      scores: Object.fromEntries([...r.progress].map(([s, g]) => [s, { score: g.score, ms: g.finishMs }])), taken: [...r.taken] };
  }
  broadcast(msg, exceptSlot = -1, filter = null) {
    const data = JSON.stringify(msg);
    for (const p of this.players) {
      if (!p || !p.conn || p.slot === exceptSlot) continue;
      if (filter && !filter(p)) continue;
      p.conn.sendRaw(data);
    }
  }
}

class IstanbulServer {
  constructor(opts = {}) {
    this.o = { ...DEFAULTS, ...opts };
    this.rooms = new Map(); this.iconIndex = new Map();
    this.ipConns = new Map(); this.ipJoinFails = new Map();
    this.stats = { connections: 0, messages: 0, bytesIn: 0, rejected: 0 };
    this.started = Date.now();
  }
  log(ev, data = {}) { if (this.o.log) this.o.log(ev, data); }

  start() {
    return new Promise((resolve) => {
      const handler = (req, res) => this.http(req, res);
      this.httpServer = (this.o.tlsCert && this.o.tlsKey)
        ? https.createServer({ cert: fs.readFileSync(this.o.tlsCert), key: fs.readFileSync(this.o.tlsKey) }, handler)
        : http.createServer(handler);
      this.wss = new WebSocketServer({ server: this.httpServer, maxPayload: this.o.maxPayload, perMessageDeflate: false });
      this.wss.on('connection', (ws, req) => this.onConnection(ws, req));
      this.tickTimer = setInterval(() => this.tick(), Math.round(1000 / this.o.tickHz));
      this.houseTimer = setInterval(() => this.housekeeping(), 1000);
      this.pingTimer = setInterval(() => this.pingAll(), this.o.pingIntervalMs);
      this.httpServer.listen(this.o.port, this.o.host, () => {
        this.port = this.httpServer.address().port;
        this.log('listening', { port: this.port, tls: !!this.o.tlsCert, protocol: C.protocol_version });
        if (this.o.lan) this.startLanBeacon();
        resolve(this.port);
      });
    });
  }
  stop() {
    clearInterval(this.tickTimer); clearInterval(this.houseTimer); clearInterval(this.pingTimer);
    if (this.lanTimer) { clearInterval(this.lanTimer); this.lanSock.close(); }
    for (const r of this.rooms.values()) if (r.rival && r.rival.timer) clearTimeout(r.rival.timer);
    for (const ws of this.wss.clients) ws.terminate();
    return new Promise(res => this.wss.close(() => this.httpServer.close(() => res())));
  }
  http(req, res) {
    if (req.url === '/health') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true, protocol: C.protocol_version, rooms: this.rooms.size,
        players: [...this.rooms.values()].reduce((a, r) => a + r.present().length, 0), uptime_s: Math.round((Date.now() - this.started) / 1000),
        rss_mb: Math.round(process.memoryUsage().rss / 1048576), cpu_ms: Math.round((process.cpuUsage().user + process.cpuUsage().system) / 1000) }));
      return;
    }
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('İstanbul Macerası oyun sunucusu. WebSocket ile bağlanın.\n');
  }
  startLanBeacon() {
    const dgram = require('dgram');
    const os = require('os');
    this.lanSock = dgram.createSocket('udp4');
    this.lanSock.bind(() => {
      this.lanSock.setBroadcast(true);
      const msg = Buffer.from(JSON.stringify({ istnet: C.protocol_version, port: this.port, tls: !!this.o.tlsCert, name: os.hostname().slice(0, 24) }));
      const send = () => this.lanSock.send(msg, this.o.lanPort || 47821, this.o.lanAddr || '255.255.255.255', () => {});
      send(); this.lanTimer = setInterval(send, 2000);
      this.log('lan_beacon', { port: this.o.lanPort || 47821 });
    });
  }

  // ---------------- connections ----------------
  onConnection(ws, req) {
    // Behind a platform proxy (Fly/Render/Caddy) the client IP is in X-Forwarded-For / Fly-Client-IP.
    const ip = (this.o.trustProxy && (req.headers['fly-client-ip'] || (req.headers['x-forwarded-for'] || '').split(',')[0].trim()))
      || req.socket.remoteAddress || '?';
    const n = (this.ipConns.get(ip) || 0) + 1;
    if (n > this.o.maxConnsPerIp) { ws.close(1013, 'too_many'); this.stats.rejected++; return; }
    this.ipConns.set(ip, n);
    this.stats.connections++;
    const conn = {
      id: this.stats.connections, ws, ip, alive: true, hello: false, pid: null, nick: null, avatar: null,
      room: null, slot: -1, knock: null, joinFails: 0,
      bState: new TokenBucket(30, 45), bCtl: new TokenBucket(20, 40), bChat: new TokenBucket(1 / 1.5, 3), strikes: 0,
      sendRaw: (d) => { if (ws.readyState === 1) ws.send(d); },
      send: (m) => { if (ws.readyState === 1) ws.send(JSON.stringify(m)); },
    };
    ws.on('pong', () => { conn.alive = true; });
    ws.on('message', (data, isBinary) => this.onMessage(conn, data, isBinary));
    ws.on('close', () => this.onClose(conn));
    ws.on('error', () => {});
  }
  pingAll() {
    for (const ws of this.wss.clients) {
      if (ws._dead) { ws.terminate(); continue; }
      ws._dead = true; ws.once('pong', () => { ws._dead = false; });
      try { ws.ping(); } catch (e) { /* ignore */ }
    }
  }
  onClose(conn) {
    const n = (this.ipConns.get(conn.ip) || 1) - 1;
    if (n <= 0) this.ipConns.delete(conn.ip); else this.ipConns.set(conn.ip, n);
    if (conn.knock) { const r = conn.knock.room; r.knocks.delete(conn.knock.kid); this.notifyHost(r, { t: 'knock_cancel', kid: conn.knock.kid }); conn.knock = null; }
    const room = conn.room;
    if (room) {
      const p = room.players[conn.slot];
      if (p && p.conn === conn) {
        if (conn.leaving) this.removePlayer(room, p, 'left');
        else {
          p.conn = null; p.awaySince = Date.now();
          room.broadcast({ t: 'player_away', slot: p.slot });
          this.log('player_away', { room: room.code, slot: p.slot });
          if (room.hostSlot === p.slot) this.migrateHost(room);
        }
      }
    }
  }
  err(conn, code, extra = {}) { conn.send({ t: 'error', code, ...extra }); }
  strike(conn, why) {
    conn.strikes++;
    if (conn.strikes > 50) { this.log('kick_abuse', { why }); conn.ws.close(1008, 'abuse'); }
  }

  onMessage(conn, data, isBinary) {
    this.stats.messages++; this.stats.bytesIn += data.length;
    if (isBinary) {
      if (!conn.room || conn.slot < 0) return;
      if (!conn.bState.take()) { this.strike(conn, 'state_rate'); return; }
      const s = P.decodeState(data);
      if (!s) { this.strike(conn, 'bad_state'); return; }
      this.onState(conn, s);
      return;
    }
    if (!conn.bCtl.take()) { this.err(conn, 'rate_limited'); this.strike(conn, 'ctl_rate'); return; }
    let m;
    try { m = JSON.parse(data.toString('utf8')); } catch (e) { this.strike(conn, 'bad_json'); return; }
    if (!m || typeof m.t !== 'string') return;
    if (!conn.hello && m.t !== 'hello' && m.t !== 'ping') { this.err(conn, 'hello_first'); return; }
    const h = this.handlers[m.t];
    if (!h) { this.err(conn, 'unknown', { what: m.t }); return; }
    if (conn.room) conn.room.lastActivity = Date.now();
    try { h.call(this, conn, m); } catch (e) { this.log('handler_error', { t: m.t, err: String(e && e.stack || e) }); }
  }

  // ---------------- room helpers ----------------
  newRoomCode() {
    for (let i = 0; i < 50; i++) {
      const code = codes.genCode(); const icons = codes.genIcons();
      if (!this.rooms.has(code) && !this.iconIndex.has(codes.iconsKey(icons))) return { code, icons };
    }
    return null;
  }
  addPlayer(room, conn, token) {
    const slot = room.freeSlot();
    const p = { slot, pid: conn.pid, nick: conn.nick, avatar: conn.avatar, conn, token: token || randToken(),
      awaySince: 0, state: null, dirty: false, lastTeleport: 0, blockedPids: new Set(conn.blockedPids || []), chatLog: [] };
    room.players[slot] = p; conn.room = room; conn.slot = slot; room.emptySince = null;
    if (room.hostSlot < 0) room.hostSlot = slot;
    room.friendReqs.forEach(k => { if (k.startsWith(slot + '>') || k.endsWith('>' + slot)) room.friendReqs.delete(k); });
    conn.send({ t: 'room_joined', resume: p.token, ...room.snapshot(slot) });
    room.broadcast({ t: 'player_joined', player: room.publicPlayer(p) }, slot);
    this.log('player_joined', { room: room.code, slot, pid: p.pid, mode: room.mode, count: room.count() });
    return p;
  }
  removePlayer(room, p, reason) {
    room.players[p.slot] = null;
    for (const [veh, seats] of room.vehicles) {
      const i = seats.indexOf(p.slot);
      if (i >= 0) { seats[i] = -1; room.broadcast({ t: 'seats', veh, seats }); }
    }
    if (p.conn) { p.conn.room = null; p.conn.slot = -1; }
    room.broadcast({ t: 'player_left', slot: p.slot, reason });
    this.log('player_left', { room: room.code, slot: p.slot, reason });
    if (room.rival && room.rival.progress.has(p.slot)) room.rival.progress.get(p.slot).gone = true;
    if (room.hostSlot === p.slot) this.migrateHost(room);
    if (room.present().length === 0 && !room.emptySince) room.emptySince = Date.now();
  }
  migrateHost(room) {
    const next = room.present().map(p => p.slot).sort((a, b) => a - b)[0];
    room.hostSlot = next === undefined ? -1 : next;
    if (room.hostSlot >= 0) {
      room.broadcast({ t: 'host_changed', slot: room.hostSlot });
      for (const [kid, k] of room.knocks) this.notifyHost(room, { t: 'knock', kid, nick: k.conn.nick, avatar: k.conn.avatar, pid: k.conn.pid });
    }
  }
  notifyHost(room, msg) { const h = room.players[room.hostSlot]; if (h && h.conn) h.conn.send(msg); }
  closeRoom(room, reason) {
    for (const p of room.players) if (p && p.conn) { p.conn.send({ t: 'room_closed', reason }); p.conn.room = null; p.conn.slot = -1; }
    for (const k of room.knocks.values()) { k.conn.send({ t: 'join_denied', reason: 'closed' }); k.conn.knock = null; }
    if (room.rival && room.rival.timer) clearTimeout(room.rival.timer);
    this.rooms.delete(room.code); this.iconIndex.delete(codes.iconsKey(room.icons));
    this.log('room_closed', { room: room.code, reason });
  }
  isHost(conn) { return conn.room && conn.room.hostSlot === conn.slot; }
  joinFail(conn, reason) {
    const now = Date.now();
    let e = this.ipJoinFails.get(conn.ip);
    if (!e || now - e.since > 600_000) e = { since: now, n: 0 };
    e.n++; this.ipJoinFails.set(conn.ip, e);
    conn.joinFails++;
    conn.send({ t: 'join_denied', reason });
    if (conn.joinFails >= 8) conn.ws.close(1008, 'too_many_attempts');
  }
  joinBlocked(conn) {
    const e = this.ipJoinFails.get(conn.ip);
    return e && Date.now() - e.since <= 600_000 && e.n >= this.o.joinFailPerIpPer10Min;
  }

  // ---------------- state ----------------
  onState(conn, s) {
    const p = conn.room.players[conn.slot]; if (!p) return;
    const now = Date.now();
    if (p.state) {
      const dt = Math.max(0.03, (now - p.state.t) / 1000);
      const d = Math.hypot(s.x - p.state.x, s.y - p.state.y, s.z - p.state.z);
      if (d / dt > this.o.maxSpeed) p.lastTeleport = now;
    }
    p.state = { ...s, t: now }; p.dirty = true;
  }
  tick() {
    const now = Date.now();
    for (const room of this.rooms.values()) {
      const dirty = room.players.filter(p => p && p.dirty && p.state);
      if (!dirty.length) continue;
      room.tick = (room.tick + 1) & 0xff;
      for (const rcv of room.players) {
        if (!rcv || !rcv.conn) continue;
        const entries = dirty.filter(p => p.slot !== rcv.slot).map(p => ({ slot: p.slot, ...p.state }));
        if (entries.length) rcv.conn.sendRaw(P.encodeSnapshot(now, room.tick, entries));
      }
      for (const p of dirty) p.dirty = false;
    }
  }
  housekeeping() {
    const now = Date.now();
    for (const room of [...this.rooms.values()]) {
      for (const p of room.players) if (p && !p.conn && now - p.awaySince > this.o.graceMs) this.removePlayer(room, p, 'timeout');
      for (const [kid, k] of room.knocks) if (now > k.expires) {
        room.knocks.delete(kid); k.conn.knock = null; k.conn.send({ t: 'join_denied', reason: 'no_answer' });
        this.notifyHost(room, { t: 'knock_cancel', kid });
      }
      if (room.present().length === 0) {
        if (!room.emptySince) room.emptySince = now;
        if (now - room.emptySince > this.o.emptyRoomMs) { this.closeRoom(room, 'empty'); continue; }
      }
      if (now - room.lastActivity > this.o.idleRoomMs) { this.closeRoom(room, 'idle'); continue; }
      if (now - room.created > this.o.maxRoomAgeMs) { this.closeRoom(room, 'expired'); continue; }
    }
  }

  // ---------------- rival ----------------
  endRival(room) {
    const r = room.rival; if (!r || r.state === 'ended') return;
    if (r.timer) clearTimeout(r.timer);
    r.state = 'ended';
    const rows = [...r.progress].map(([slot, g]) => ({ slot, nick: g.nick, score: g.score, ms: g.finishMs, last: g.lastMs, finished: g.finishMs != null }));
    if (r.kind === 'race') rows.sort((a, b) => (b.finished - a.finished) || (a.finished ? a.ms - b.ms : b.score - a.score) || (a.last - b.last));
    else rows.sort((a, b) => (b.score - a.score) || (a.last - b.last));
    rows.forEach((row, i) => { row.place = i + 1; delete row.last; });
    r.results = rows;
    room.broadcast({ t: 'rival_end', id: r.id, kind: r.kind, ranking: rows });
    this.log('rival_end', { room: room.code, id: r.id, kind: r.kind, ranking: rows });
  }
  claimCheck(room, p, point) {
    if (!p.state || Date.now() - p.state.t > 1500) return 'no_state';
    if (Date.now() - p.lastTeleport < 1500) return 'teleport';
    const d = Math.hypot(p.state.x - point[0], p.state.y - point[1], p.state.z - point[2]);
    if (d > room.rival.radius * 1.5) return 'too_far';
    return null;
  }
}

// ---------------- message handlers ----------------
IstanbulServer.prototype.handlers = {
  ping(conn, m) { conn.send({ t: 'pong', c: m.c, s: Date.now() }); },

  hello(conn, m) {
    if (conn.hello) return this.err(conn, 'already');
    if (!Number.isInteger(m.v) || m.v < C.min_protocol_version) { this.err(conn, 'version_old', { min: C.min_protocol_version }); return conn.ws.close(4001, 'version_old'); }
    if (m.v > C.protocol_version) { this.err(conn, 'version_new', { server: C.protocol_version }); return conn.ws.close(4002, 'version_new'); }
    if (typeof m.secret !== 'string' || !/^[0-9a-f]{32,64}$/.test(m.secret)) return this.err(conn, 'bad_identity');
    const nick = validNick(m.nick), avatar = validAvatar(m.avatar);
    if (!nick || !avatar) return this.err(conn, 'bad_profile');
    // Public id is derived from a device secret, so nobody can impersonate a friend without the secret.
    conn.pid = crypto.createHash('sha256').update('istnet:' + m.secret).digest('hex').slice(0, 16);
    conn.nick = nick; conn.avatar = avatar; conn.hello = true;
    conn.blockedPids = Array.isArray(m.blocked) ? m.blocked.filter(x => typeof x === 'string').slice(0, 500) : [];
    conn.send({ t: 'welcome', pid: conn.pid, v: C.protocol_version, s: Date.now(), tick_hz: this.o.tickHz, max_players: C.max_players });
  },

  create(conn, m) {
    if (conn.room || conn.knock) return this.err(conn, 'already_in_room');
    if (!C.modes.includes(m.mode)) return this.err(conn, 'bad_mode');
    if (this.rooms.size >= this.o.maxRooms) return this.err(conn, 'server_full');
    const c = this.newRoomCode(); if (!c) return this.err(conn, 'server_full');
    const friends = Array.isArray(m.friends) ? m.friends.filter(x => typeof x === 'string' && x.length <= 32).slice(0, 500) : [];
    const room = new Room(this, c.code, c.icons, m.mode, { approval: m.approval, friends, blocked: conn.blockedPids });
    this.rooms.set(c.code, room); this.iconIndex.set(codes.iconsKey(c.icons), c.code);
    this.log('room_created', { room: c.code, icons: c.icons, mode: m.mode, approval: room.approval });
    this.addPlayer(room, conn);
  },

  join(conn, m) {
    if (conn.room || conn.knock) return this.err(conn, 'already_in_room');
    if (this.joinBlocked(conn)) return conn.send({ t: 'join_denied', reason: 'rate_limited' });
    let code = null;
    if (m.icons !== undefined) { const k = codes.iconsKey(m.icons); code = k && this.iconIndex.get(k); }
    else code = codes.normalizeCode(m.code);
    const room = code && this.rooms.get(code);
    if (!room) return this.joinFail(conn, 'not_found');
    if (room.bannedPids.has(conn.pid)) return this.joinFail(conn, 'not_found'); // don't reveal the block
    if (room.players.some(p => p && p.pid === conn.pid)) return conn.send({ t: 'join_denied', reason: 'already_there' });
    if (room.freeSlot() < 0) return conn.send({ t: 'join_denied', reason: 'full' });
    // Friends on the host's approved friend list get in directly; everyone else must be admitted by the host.
    const friendOfRoom = room.friendPids.has(conn.pid);
    if (friendOfRoom) return this.addPlayer(room, conn);
    if (room.approval === 'friends_only') return this.joinFail(conn, 'friends_only');
    if (room.knocks.size >= 3) return conn.send({ t: 'join_denied', reason: 'busy' });
    const kid = room.nextKid++;
    room.knocks.set(kid, { conn, expires: Date.now() + this.o.knockTimeoutMs });
    conn.knock = { room, kid };
    conn.send({ t: 'waiting_approval', timeout_ms: this.o.knockTimeoutMs });
    this.notifyHost(room, { t: 'knock', kid, nick: conn.nick, avatar: conn.avatar, pid: conn.pid });
    this.log('knock', { room: room.code, kid });
  },

  admit(conn, m) {
    if (!this.isHost(conn)) return this.err(conn, 'not_host');
    const room = conn.room; const k = room.knocks.get(m.kid);
    if (!k) return this.err(conn, 'no_such_knock');
    room.knocks.delete(m.kid); k.conn.knock = null;
    if (m.ok === true && room.freeSlot() >= 0) {
      this.log('admitted', { room: room.code, kid: m.kid });
      if (m.remember === true) room.friendPids.add(k.conn.pid);
      this.addPlayer(room, k.conn);
    } else {
      this.log('denied', { room: room.code, kid: m.kid });
      k.conn.send({ t: 'join_denied', reason: 'host_said_no' });
    }
  },

  cancel_join(conn) {
    if (!conn.knock) return;
    const { room, kid } = conn.knock; room.knocks.delete(kid); conn.knock = null;
    this.notifyHost(room, { t: 'knock_cancel', kid });
  },

  resume(conn, m) {
    if (conn.room) return this.err(conn, 'already_in_room');
    const room = this.rooms.get(codes.normalizeCode(m.code));
    const p = room && room.players.find(x => x && x.token === m.token && typeof m.token === 'string');
    if (!p || p.pid !== conn.pid) return conn.send({ t: 'resume_failed' });
    if (p.conn && p.conn !== conn) { p.conn.leaving = false; p.conn.room = null; p.conn.ws.close(4003, 'replaced'); }
    p.conn = conn; p.awaySince = 0; conn.room = room; conn.slot = p.slot; room.emptySince = null;
    p.token = randToken();
    if (room.hostSlot < 0) room.hostSlot = p.slot;
    conn.send({ t: 'room_joined', resumed: true, resume: p.token, ...room.snapshot(p.slot) });
    room.broadcast({ t: 'player_back', slot: p.slot }, p.slot);
    this.log('player_resumed', { room: room.code, slot: p.slot });
  },

  leave(conn) {
    if (conn.knock) return this.handlers.cancel_join.call(this, conn);
    const room = conn.room; if (!room) return;
    const p = room.players[conn.slot];
    if (p) this.removePlayer(room, p, 'left');
    conn.send({ t: 'room_left' });
  },

  // ---- shared world state (Dostluk) ----
  flag(conn, m) {
    const room = conn.room; if (!room) return this.err(conn, 'no_room');
    if (typeof m.k !== 'string' || !KEY_RE.test(m.k) || !validFlagValue(m.v)) return this.err(conn, 'bad_flag');
    if (!room.flags.has(m.k) && room.flags.size >= 512) return this.err(conn, 'too_many_flags');
    room.flags.set(m.k, m.v);
    room.broadcast({ t: 'flag', k: m.k, v: m.v, by: conn.slot });
    this.log('flag', { room: room.code, k: m.k, v: m.v, by: conn.slot });
  },
  flag_add(conn, m) {
    const room = conn.room; if (!room) return this.err(conn, 'no_room');
    if (typeof m.k !== 'string' || !KEY_RE.test(m.k) || !isInt(m.n, -100, 100)) return this.err(conn, 'bad_flag');
    const cur = room.flags.get(m.k);
    if (cur !== undefined && typeof cur !== 'number') return this.err(conn, 'bad_flag');
    if (cur === undefined && room.flags.size >= 512) return this.err(conn, 'too_many_flags');
    const v = (cur || 0) + m.n; room.flags.set(m.k, v);
    room.broadcast({ t: 'flag', k: m.k, v, by: conn.slot });
    this.log('flag_add', { room: room.code, k: m.k, n: m.n, v, by: conn.slot });
  },
  build(conn, m) {
    const room = conn.room; if (!room) return this.err(conn, 'no_room');
    if (m.op === 'place') {
      if (!isInt(m.kind, 0, 9999) || !isInt(m.plot, 0, 255) || !validPoint(m.pos) || !isInt(m.r || 0, 0, 3)) return this.err(conn, 'bad_build');
      if (room.builds.size >= 2000) return this.err(conn, 'too_many_builds');
      const b = { id: room.nextBuildId++, kind: m.kind, plot: m.plot, pos: m.pos, r: m.r || 0, by: conn.slot };
      room.builds.set(b.id, b);
      room.broadcast({ t: 'build', op: 'place', b, cid: undefined });
      conn.send({ t: 'build_ack', cid: m.cid, id: b.id });
      this.log('build_place', { room: room.code, id: b.id, kind: b.kind, by: conn.slot });
    } else if (m.op === 'remove') {
      const b = room.builds.get(m.id); if (!b) return this.err(conn, 'no_build');
      // Only the builder or the host may remove (prevents griefing a friend's work).
      if (b.by !== conn.slot && !this.isHost(conn)) return this.err(conn, 'not_yours');
      room.builds.delete(m.id);
      room.broadcast({ t: 'build', op: 'remove', id: m.id, by: conn.slot });
    } else this.err(conn, 'bad_build');
  },
  seat(conn, m) {
    const room = conn.room; if (!room) return this.err(conn, 'no_room');
    if (typeof m.veh !== 'string' || !VEH_RE.test(m.veh)) return this.err(conn, 'bad_vehicle');
    let seats = room.vehicles.get(m.veh);
    if (!seats) {
      if (room.vehicles.size >= 64) return this.err(conn, 'too_many_vehicles');
      const cap = isInt(m.capacity, 1, 16) ? m.capacity : 4;
      seats = new Array(cap).fill(-1); room.vehicles.set(m.veh, seats);
    }
    const want = m.seat === undefined ? -1 : m.seat;
    if (!isInt(want, -1, seats.length - 1)) return this.err(conn, 'bad_seat');
    const cur = seats.indexOf(conn.slot);
    let idx;
    if (want >= 0) idx = (seats[want] === -1 || seats[want] === conn.slot) ? want : -1;
    else idx = cur >= 0 ? cur : seats.indexOf(-1);
    if (idx < 0) return conn.send({ t: 'seat_denied', veh: m.veh, reason: 'full' });
    // leave any other seat first (atomic move)
    for (const [veh, s] of room.vehicles) { const i = s.indexOf(conn.slot); if (i >= 0 && !(veh === m.veh && i === idx)) { s[i] = -1; if (veh !== m.veh) room.broadcast({ t: 'seats', veh, seats: s }); } }
    seats[idx] = conn.slot;
    room.broadcast({ t: 'seats', veh: m.veh, seats });
    this.log('seat', { room: room.code, veh: m.veh, seat: idx, slot: conn.slot });
  },
  unseat(conn) {
    const room = conn.room; if (!room) return;
    for (const [veh, s] of room.vehicles) { const i = s.indexOf(conn.slot); if (i >= 0) { s[i] = -1; room.broadcast({ t: 'seats', veh, seats: s }); } }
  },
  outfit(conn, m) {
    const room = conn.room; const a = validAvatar(m.avatar); if (!a) return this.err(conn, 'bad_profile');
    conn.avatar = a; if (!room) return;
    room.players[conn.slot].avatar = a;
    room.broadcast({ t: 'player_update', slot: conn.slot, avatar: a });
  },

  // ---- safety ----
  chat(conn, m) {
    const room = conn.room; if (!room) return this.err(conn, 'no_room');
    const p = m.p === undefined ? -1 : m.p, e = m.e === undefined ? -1 : m.e;
    if (!isInt(p, -1, C.phrases.length - 1) || !isInt(e, -1, C.emojis.length - 1) || (p < 0 && e < 0)) return this.err(conn, 'bad_chat');
    if (!conn.bChat.take()) return this.err(conn, 'chat_slow_down');
    const me = room.players[conn.slot];
    me.chatLog.push({ p, e, at: Date.now() }); if (me.chatLog.length > 20) me.chatLog.shift();
    room.broadcast({ t: 'chat', from: conn.slot, p, e }, -1, (r) => !r.blockedPids.has(me.pid));
    this.log('chat', { room: room.code, from: conn.slot, p, e });
  },
  block(conn, m) {
    const room = conn.room; if (!room) return this.err(conn, 'no_room');
    const target = isInt(m.slot, 0, C.max_players - 1) && room.players[m.slot];
    if (!target || target.slot === conn.slot) return this.err(conn, 'bad_target');
    room.players[conn.slot].blockedPids.add(target.pid);
    for (const k of [...room.friendReqs]) if (k === `${conn.slot}>${target.slot}` || k === `${target.slot}>${conn.slot}`) room.friendReqs.delete(k);
    conn.send({ t: 'blocked', slot: target.slot, pid: target.pid });
    this.log('block', { room: room.code, by: conn.slot, target: target.slot });
    if (this.isHost(conn)) { room.bannedPids.add(target.pid); this.handlers.kick.call(this, conn, { slot: target.slot, _fromBlock: true }); }
  },
  unblock(conn, m) {
    const room = conn.room; if (!room) return;
    const target = isInt(m.slot, 0, C.max_players - 1) && room.players[m.slot];
    if (target) room.players[conn.slot].blockedPids.delete(target.pid);
    conn.send({ t: 'unblocked', slot: m.slot });
  },
  report(conn, m) {
    const room = conn.room; if (!room) return this.err(conn, 'no_room');
    const target = isInt(m.slot, 0, C.max_players - 1) && room.players[m.slot];
    if (!target || !isInt(m.reason, 0, C.report_reasons.length - 1)) return this.err(conn, 'bad_target');
    // No IPs, no names, no free text: only anonymous ids + preset chat ids.
    const rec = { ts: new Date().toISOString(), room_hash: crypto.createHash('sha256').update(room.code + room.created).digest('hex').slice(0, 12),
      reporter: conn.pid, target: target.pid, reason: C.report_reasons[m.reason], recent_chat: target.chatLog.map(c => [c.p, c.e]) };
    try { fs.appendFileSync(this.o.reportLog, JSON.stringify(rec) + '\n'); } catch (e) { this.log('report_write_failed', {}); }
    conn.send({ t: 'reported', slot: target.slot });
    this.log('report', { room: room.code, by: conn.slot, target: target.slot, reason: rec.reason });
  },
  kick(conn, m) {
    if (!this.isHost(conn)) return this.err(conn, 'not_host');
    const room = conn.room; const target = isInt(m.slot, 0, C.max_players - 1) && room.players[m.slot];
    if (!target || target.slot === conn.slot) return this.err(conn, 'bad_target');
    room.bannedPids.add(target.pid);
    const tc = target.conn;
    this.removePlayer(room, target, 'kicked');
    if (tc) tc.send({ t: 'kicked' });
  },
  friends(conn, m) { // host updates auto-admit list
    if (!this.isHost(conn) || !Array.isArray(m.list)) return this.err(conn, 'not_host');
    conn.room.friendPids = new Set(m.list.filter(x => typeof x === 'string').slice(0, 500));
  },
  friend_req(conn, m) {
    const room = conn.room; if (!room) return this.err(conn, 'no_room');
    const target = isInt(m.slot, 0, C.max_players - 1) && room.players[m.slot];
    if (!target || target.slot === conn.slot || !target.conn) return this.err(conn, 'bad_target');
    const me = room.players[conn.slot];
    if (target.blockedPids.has(me.pid) || me.blockedPids.has(target.pid)) return; // silently ignored
    room.friendReqs.add(`${conn.slot}>${target.slot}`);
    if (room.friendReqs.has(`${target.slot}>${conn.slot}`)) {
      room.friendReqs.delete(`${conn.slot}>${target.slot}`); room.friendReqs.delete(`${target.slot}>${conn.slot}`);
      conn.send({ t: 'friend_added', pid: target.pid, nick: target.nick, avatar: target.avatar, slot: target.slot });
      target.conn.send({ t: 'friend_added', pid: me.pid, nick: me.nick, avatar: me.avatar, slot: me.slot });
      this.log('friends_made', { room: room.code, a: conn.slot, b: target.slot });
    } else target.conn.send({ t: 'friend_req', from: conn.slot });
  },

  // ---- Rakip: server-authoritative events ----
  rival_start(conn, m) {
    if (!this.isHost(conn)) return this.err(conn, 'not_host');
    const room = conn.room;
    if (room.rival && room.rival.state !== 'ended') return this.err(conn, 'rival_running');
    if (!C.rival_kinds.includes(m.kind)) return this.err(conn, 'bad_rival');
    if (!Array.isArray(m.points) || m.points.length < 1 || m.points.length > 64 || !m.points.every(validPoint)) return this.err(conn, 'bad_rival');
    const dur = isInt(m.dur, 10, 900) ? m.dur : 120;
    const radius = (typeof m.radius === 'number' && m.radius >= 1 && m.radius <= 30) ? m.radius : this.o.rivalRadius;
    const now = Date.now();
    const r = { id: (room.rival ? room.rival.id : 0) + 1, kind: m.kind, points: m.points, radius, state: 'countdown',
      startAt: now + this.o.countdownMs, endAt: now + this.o.countdownMs + dur * 1000, progress: new Map(), taken: new Set() };
    for (const p of room.present()) r.progress.set(p.slot, { nick: p.nick, next: 0, score: 0, finishMs: null, lastMs: 0 });
    room.rival = r;
    r.timer = setTimeout(() => {
      r.state = 'running'; room.broadcast({ t: 'rival_go', id: r.id });
      r.timer = setTimeout(() => this.endRival(room), dur * 1000);
    }, this.o.countdownMs);
    room.broadcast({ t: 'rival_countdown', ...room.rivalPublic(), now });
    this.log('rival_start', { room: room.code, id: r.id, kind: r.kind, points: r.points.length, dur });
  },
  rival_claim(conn, m) {
    const room = conn.room; const r = room && room.rival;
    if (!r || r.state !== 'running') return conn.send({ t: 'claim_rejected', i: m.i, reason: 'not_running' });
    const g = r.progress.get(conn.slot); if (!g) return conn.send({ t: 'claim_rejected', i: m.i, reason: 'not_in_event' });
    if (!isInt(m.i, 0, r.points.length - 1)) return conn.send({ t: 'claim_rejected', i: m.i, reason: 'bad_index' });
    const p = room.players[conn.slot];
    if (r.kind === 'race') {
      if (g.finishMs != null) return;
      if (m.i !== g.next) return conn.send({ t: 'claim_rejected', i: m.i, reason: 'wrong_order' });
      const why = this.claimCheck(room, p, r.points[m.i]);
      if (why) { this.log('claim_rejected', { room: room.code, slot: conn.slot, i: m.i, why }); return conn.send({ t: 'claim_rejected', i: m.i, reason: why }); }
      g.next++; g.score = g.next; g.lastMs = Date.now() - r.startAt;
      if (g.next === r.points.length) g.finishMs = g.lastMs;
      room.broadcast({ t: 'rival_score', slot: conn.slot, score: g.score, i: m.i, finished: g.finishMs != null, ms: g.finishMs });
      const active = [...r.progress].filter(([s, x]) => !x.gone && room.players[s] && room.players[s].conn);
      if (active.every(([, x]) => x.finishMs != null)) this.endRival(room);
    } else {
      if (r.taken.has(m.i)) return conn.send({ t: 'claim_rejected', i: m.i, reason: 'taken' });
      const why = this.claimCheck(room, p, r.points[m.i]);
      if (why) { this.log('claim_rejected', { room: room.code, slot: conn.slot, i: m.i, why }); return conn.send({ t: 'claim_rejected', i: m.i, reason: why }); }
      r.taken.add(m.i); g.score++; g.lastMs = Date.now() - r.startAt;
      room.broadcast({ t: 'rival_score', slot: conn.slot, score: g.score, i: m.i });
      if (r.taken.size === r.points.length) this.endRival(room);
    }
  },
  rival_stop(conn) { if (!this.isHost(conn)) return this.err(conn, 'not_host'); if (conn.room.rival) this.endRival(conn.room); },
};

module.exports = { IstanbulServer, Room, DEFAULTS };

if (require.main === module) {
  const env = process.env;
  const srv = new IstanbulServer({
    port: parseInt(env.PORT || '8787', 10),
    host: env.HOST || '0.0.0.0',
    tlsCert: env.TLS_CERT || null, tlsKey: env.TLS_KEY || null,
    lan: env.LAN === '1' || process.argv.includes('--lan'),
    trustProxy: env.TRUST_PROXY === '1',
    graceMs: env.GRACE_MS ? +env.GRACE_MS : DEFAULTS.graceMs,
    emptyRoomMs: env.EMPTY_ROOM_MS ? +env.EMPTY_ROOM_MS : DEFAULTS.emptyRoomMs,
    reportLog: env.REPORT_LOG || 'reports.log',
    maxConnsPerIp: env.MAX_CONNS_PER_IP ? +env.MAX_CONNS_PER_IP : DEFAULTS.maxConnsPerIp,
    maxRooms: env.MAX_ROOMS ? +env.MAX_ROOMS : DEFAULTS.maxRooms,
  });
  srv.start();
  const shutdown = () => { srv.log('shutdown', {}); srv.stop().then(() => process.exit(0)); };
  process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown);
}
