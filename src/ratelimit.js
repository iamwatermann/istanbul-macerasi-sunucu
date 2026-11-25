'use strict';
// Simple token bucket. rate = tokens per second, burst = max tokens.
class TokenBucket {
  constructor(rate, burst, now = Date.now()) {
    this.rate = rate; this.burst = burst; this.tokens = burst; this.last = now;
  }
  take(n = 1, now = Date.now()) {
    const dt = Math.max(0, now - this.last) / 1000;
    this.last = now;
    this.tokens = Math.min(this.burst, this.tokens + dt * this.rate);
    if (this.tokens >= n) { this.tokens -= n; return true; }
    return false;
  }
}
module.exports = { TokenBucket };
