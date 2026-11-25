'use strict';
// Minimal Node client used by unit tests and the load test.
const WebSocket = require('ws');
const crypto = require('crypto');
const P = require('../src/protocol');

class TestClient {
  constructor(url, opts = {}) {
    this.url = url; this.msgs = []; this.snaps = []; this.waiters = [];
    this.secret = opts.secret || crypto.randomBytes(16).toString('hex');
    this.nick = opts.nick || [0, 0]; this.avatar = opts.avatar || {};
    this.v = opts.v || 1; this.blocked = opts.blocked || [];
    this.onSnap = null;
  }
  open() {
    return new Promise((res, rej) => {
      this.ws = new WebSocket(this.url, { rejectUnauthorized: false });
      this.ws.on('open', res); this.ws.on('error', rej);
      this.ws.on('close', (code, reason) => { this.closed = { code, reason: String(reason) }; this._push({ t: '_closed', code }); });
      this.ws.on('message', (d, bin) => {
        if (bin) { const s = P.decodeSnapshot(d); if (s) { this.snaps.push(s); if (this.onSnap) this.onSnap(s); } return; }
        this._push(JSON.parse(d.toString()));
      });
    });
  }
  _push(m) {
    this.msgs.push(m);
    for (const w of [...this.waiters]) if (w.pred(m)) { this.waiters.splice(this.waiters.indexOf(w), 1); clearTimeout(w.timer); w.res(m); }
  }
  send(m) { this.ws.send(JSON.stringify(m)); }
  state(s) { this.ws.send(P.encodeState({ seq: 0, yaw: 0, anim: 0, veh: 0, ...s })); }
  wait(pred, ms = 3000, fromStart = false) {
    if (typeof pred === 'string') { const t = pred; pred = (m) => m.t === t; }
    if (fromStart) { const m = this.msgs.find(pred); if (m) return Promise.resolve(m); }
    return new Promise((res, rej) => {
      const w = { pred, res, timer: setTimeout(() => { this.waiters.splice(this.waiters.indexOf(w), 1); rej(new Error('timeout waiting')); }, ms) };
      this.waiters.push(w);
    });
  }
  async hello() {
    const p = this.wait('welcome');
    this.send({ t: 'hello', v: this.v, secret: this.secret, nick: this.nick, avatar: this.avatar, blocked: this.blocked });
    const w = await p; this.pid = w.pid; return w;
  }
  close() { try { this.ws.terminate(); } catch (e) {} }
}
module.exports = { TestClient };
