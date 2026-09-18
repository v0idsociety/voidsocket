// xherz

import { createHash, randomBytes, randomFillSync } from 'node:crypto';
import { connect as tlsConnect } from 'node:tls';
import { connect as netConnect } from 'node:net';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const CRLF = Buffer.from([0x0D, 0x0A, 0x0D, 0x0A]);
const EMPTY = Buffer.alloc(0);
const CLOSE_FRAME = Buffer.from([0x88, 0x82, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8]);
const PONG_EMPTY = Buffer.from([0x8A, 0x80, 0x00, 0x00, 0x00, 0x00]);
const PING_EMPTY = Buffer.from([0x89, 0x80, 0x00, 0x00, 0x00, 0x00]);
const DEFAULT_CIPHERS = 'TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256';

const POOL_SIZE = 4096;
const pool = Buffer.allocUnsafe(POOL_SIZE);
let poolOff = POOL_SIZE;

let seed = (Date.now() ^ (Math.random() * 0x7FFFFFFF)) | 0;
function fastMask32() {
    seed ^= seed << 13;
    seed ^= seed >>> 17;
    seed ^= seed << 5;
    return seed | 0;
}

function nextMask(buf, off) {
    if (poolOff + 4 > POOL_SIZE) {
        randomFillSync(pool);
        poolOff = 0;
    }
    const m0 = pool[poolOff++];
    const m1 = pool[poolOff++];
    const m2 = pool[poolOff++];
    const m3 = pool[poolOff++];
    buf[off] = m0;
    buf[off + 1] = m1;
    buf[off + 2] = m2;
    buf[off + 3] = m3;
    return (m0 | (m1 << 8) | (m2 << 16) | (m3 << 24));
}

const FRAME_POOL_SIZE = 64;
const _framePool2 = [];
const _framePool4 = [];
const _framePool10 = [];

for (let i = 0; i < FRAME_POOL_SIZE; i++) {
    _framePool2.push(Buffer.allocUnsafe(6));
    _framePool4.push(Buffer.allocUnsafe(8));
    _framePool10.push(Buffer.allocUnsafe(14));
}

function getHeaderBuf(hs) {
    if (hs === 6 && _framePool2.length > 0) return _framePool2.pop();
    if (hs === 8 && _framePool4.length > 0) return _framePool4.pop();
    if (hs === 14 && _framePool10.length > 0) return _framePool10.pop();
    return null;
}

function returnHeaderBuf(buf) {
    const len = buf.length;
    if (len === 6 && _framePool2.length < FRAME_POOL_SIZE) _framePool2.push(buf);
    else if (len === 8 && _framePool4.length < FRAME_POOL_SIZE) _framePool4.push(buf);
    else if (len === 14 && _framePool10.length < FRAME_POOL_SIZE) _framePool10.push(buf);
}

function mask32(buf, off, len, m32) {
    const byteOff = buf.byteOffset + off;
    if ((byteOff & 3) === 0) {
        const u32 = new Uint32Array(buf.buffer, byteOff, len >>> 2);
        const end4 = u32.length & ~3;
        let i = 0;
        for (; i < end4; i += 4) {
            u32[i] ^= m32;
            u32[i + 1] ^= m32;
            u32[i + 2] ^= m32;
            u32[i + 3] ^= m32;
        }
        for (; i < u32.length; i++) {
            u32[i] ^= m32;
        }
    } else {
        const dv = new DataView(buf.buffer, byteOff, len);
        let i = 0;
        const end16 = len & ~15;
        for (; i < end16; i += 16) {
            dv.setUint32(i, dv.getUint32(i, true) ^ m32, true);
            dv.setUint32(i + 4, dv.getUint32(i + 4, true) ^ m32, true);
            dv.setUint32(i + 8, dv.getUint32(i + 8, true) ^ m32, true);
            dv.setUint32(i + 12, dv.getUint32(i + 12, true) ^ m32, true);
        }
        const end4 = len & ~3;
        for (; i < end4; i += 4) {
            dv.setUint32(i, dv.getUint32(i, true) ^ m32, true);
        }
    }
    for (let i = len & ~3; i < len; i++) {
        buf[off + i] ^= (m32 >>> ((i & 3) << 3)) & 0xFF;
    }
}

function parseFrames(b, onFrame) {
    let o = 0;
    const l = b.length;
    while (l - o >= 2) {
        const b1 = b[o + 1];
        if (b1 < 126) {
            const total = 2 + b1;
            if (l - o < total) break;
            const b0 = b[o];
            o += total;
            onFrame(b1 === 0 ? EMPTY : b.subarray(o - b1, o), b0 & 0x0F, (b0 & 0x80) !== 0);
            continue;
        }
        let plen = b1 & 0x7F;
        let hs = 2;
        if (plen === 126) {
            if (l - o < 4) break;
            plen = (b[o + 2] << 8) | b[o + 3];
            hs = 4;
        } else if (plen === 127) {
            if (l - o < 10) break;
            plen = b.readUInt32BE(o + 6);
            hs = 10;
        }
        if (b1 & 0x80) {
            if (l - o < hs + 4) break;
            hs += 4;
        }
        const total = hs + plen;
        if (l - o < total) break;
        const b0 = b[o];
        const payload = plen === 0 ? EMPTY : b.subarray(o + hs, o + total);
        if (b1 & 0x80) mask32(payload, 0, plen, b.readInt32LE(o + hs - 4));
        o += total;
        onFrame(payload, b0 & 0x0F, (b0 & 0x80) !== 0);
    }
    return o;
}

function pack(op, data, mask = true, fast = true) {
    const isStr = typeof data === 'string';
    const isBuf = !isStr && Buffer.isBuffer(data);
    const len = isStr ? Buffer.byteLength(data) : (isBuf ? data.length : (data ? data.byteLength || 0 : 0));
    const hs = (len <= 125 ? 2 : len <= 65535 ? 4 : 10) + (mask ? 4 : 0);
    const f = Buffer.allocUnsafe(hs + len);
    f[0] = 0x80 | op;

    if (len <= 125) {
        f[1] = (mask ? 0x80 : 0) | len;
    } else if (len <= 65535) {
        f[1] = (mask ? 0x80 : 0) | 126;
        f[2] = len >> 8;
        f[3] = len & 0xFF;
    } else {
        f[1] = (mask ? 0x80 : 0) | 127;
        f[2] = 0; f[3] = 0; f[4] = 0; f[5] = 0;
        f[6] = (len >>> 24) & 0xFF;
        f[7] = (len >> 16) & 0xFF;
        f[8] = (len >> 8) & 0xFF;
        f[9] = len & 0xFF;
    }

    if (mask) {
        const mo = hs - 4;
        let m32;
        if (fast) {
            m32 = fastMask32();
            f.writeInt32LE(m32, mo);
        } else {
            m32 = nextMask(f, mo);
        }

        if (len > 0) {
            if (isStr) f.write(data, hs, 'utf8');
            else if (isBuf) data.copy(f, hs);
            else if (data) Buffer.from(data.buffer || data, data.byteOffset || 0, len).copy(f, hs);
            mask32(f, hs, len, m32);
        }
    } else if (len > 0) {
        if (isStr) f.write(data, hs, 'utf8');
        else if (isBuf) data.copy(f, hs);
        else if (data) Buffer.from(data.buffer || data, data.byteOffset || 0, len).copy(f, hs);
    }
    return f;
}

function packHeader(op, len, mask = true, fast = true) {
    const hs = (len <= 125 ? 2 : len <= 65535 ? 4 : 10) + (mask ? 4 : 0);
    let hdr = getHeaderBuf(hs);
    if (!hdr) hdr = Buffer.allocUnsafe(hs);
    hdr[0] = 0x80 | op;

    if (len <= 125) {
        hdr[1] = (mask ? 0x80 : 0) | len;
    } else if (len <= 65535) {
        hdr[1] = (mask ? 0x80 : 0) | 126;
        hdr[2] = len >> 8;
        hdr[3] = len & 0xFF;
    } else {
        hdr[1] = (mask ? 0x80 : 0) | 127;
        hdr[2] = 0; hdr[3] = 0; hdr[4] = 0; hdr[5] = 0;
        hdr[6] = (len >>> 24) & 0xFF;
        hdr[7] = (len >> 16) & 0xFF;
        hdr[8] = (len >> 8) & 0xFF;
        hdr[9] = len & 0xFF;
    }

    let m32 = 0;
    if (mask) {
        const mo = hs - 4;
        if (fast) {
            m32 = fastMask32();
            hdr.writeInt32LE(m32, mo);
        } else {
            m32 = nextMask(hdr, mo);
        }
    }
    return { hdr, m32, hs };
}

class VoidSocket {
    constructor(url, opts = {}) {
        if (Array.isArray(url)) {
            this._urls = url.map(u => typeof u === 'string' ? new URL(u) : u);
            this._urlIndex = 0;
            this.url = this._urls[0];
        } else {
            this._urls = null;
            this._urlIndex = 0;
            this.url = typeof url === 'string' ? new URL(url) : url;
        }

        this.opts = opts;
        this.fast = opts.fastMask ?? true;
        this.autoPong = opts.autoPong !== false;
        this.isOpen = false;
        this.isClosed = false;
        this.saturated = false;

        this.reconnect = opts.reconnect !== false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = opts.maxReconnectAttempts ?? Infinity;
        this.reconnectDelay = opts.reconnectDelay ?? 500;
        this.maxReconnectDelay = opts.maxReconnectDelay ?? 10000;

        this._pingInterval = opts.pingInterval ?? 30000;
        this._pingTimeout = opts.pingTimeout ?? 10000;
        this._autoPing = opts.autoPing !== false;
        this._pingTimer = null;
        this._pongTimer = null;
        this._awaitingPong = false;
        this._lastPongTime = 0;

        this._json = opts.json === true;

        this._queueEnabled = opts.queue !== false;
        this._maxQueueSize = opts.maxQueueSize ?? 512;
        this._queue = [];

        const rl = opts.rateLimit;
        this._rlEnabled = !!rl;
        this._rlCount = rl ? rl.count : 0;
        this._rlWindow = rl ? rl.window : 0;
        this._rlTokens = rl ? rl.count : 0;
        this._rlTimer = null;
        this._rlQueue = [];

        this._corked = false;

        this._statsEnabled = opts.stats !== false;
        this._msgIn = 0;
        this._msgOut = 0;
        this._bytesIn = 0;
        this._bytesOut = 0;
        this._reconnectCount = 0;
        this._connectedAt = 0;
        this._lastLatency = 0;
        this._pingSentAt = 0;

        this.recv = null;
        this.online = null;
        this.offline = null;
        this.error = null;
        this.onPing = null;
        this.flushed = null;
        this.reconnecting = null;

        this.beforeSend = null;
        this.afterRecv = null;

        this._sock = null;
        this._key = randomBytes(16).toString('base64');
        this._expect = createHash('sha1').update(this._key + GUID).digest('base64');
        this._timer = null;
        this._reconnectTimer = null;
        this._killed = false;

        this._rbInitSize = opts.bufferSize || 65536;
        this._rb = Buffer.allocUnsafe(this._rbInitSize);
        this._rbLen = 0;

        this._frags = null;
        this._fragOp = 0;

        this._ackWaiters = [];

        if (this._rlEnabled) {
            this._rlTimer = setInterval(() => {
                this._rlTokens = this._rlCount;
                this._drainRlQueue();
            }, this._rlWindow);
            if (this._rlTimer.unref) this._rlTimer.unref();
        }

        this._init();
    }

    get stats() {
        return {
            messagesIn: this._msgIn,
            messagesOut: this._msgOut,
            bytesIn: this._bytesIn,
            bytesOut: this._bytesOut,
            latency: this._lastLatency,
            uptime: this._connectedAt > 0 ? Date.now() - this._connectedAt : 0,
            reconnects: this._reconnectCount,
            queueSize: this._queue.length,
            saturated: this.saturated
        };
    }

    _startPing() {
        this._stopPing();
        if (!this._autoPing || this._pingInterval <= 0) return;
        this._pingTimer = setInterval(() => {
            if (!this.isOpen || !this._sock || this._sock.destroyed) return;
            this._awaitingPong = true;
            this._pingSentAt = Date.now();
            this._sock.write(PING_EMPTY);
            this._pongTimer = setTimeout(() => {
                if (this._awaitingPong) {
                    if (this.error) this.error(new Error('ping timeout'));
                    this._onClose(1006);
                }
            }, this._pingTimeout);
            if (this._pongTimer.unref) this._pongTimer.unref();
        }, this._pingInterval);
        if (this._pingTimer.unref) this._pingTimer.unref();
    }

    _stopPing() {
        if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
        if (this._pongTimer) { clearTimeout(this._pongTimer); this._pongTimer = null; }
        this._awaitingPong = false;
    }

    _onPong() {
        if (this._awaitingPong) {
            this._awaitingPong = false;
            this._lastPongTime = Date.now();
            this._lastLatency = this._lastPongTime - this._pingSentAt;
            if (this._pongTimer) { clearTimeout(this._pongTimer); this._pongTimer = null; }
        }
    }

    _enqueue(data) {
        if (this._queue.length >= this._maxQueueSize) {
            this._queue.shift();
        }
        this._queue.push(data);
    }

    _flushQueue() {
        if (this._queue.length === 0) return;
        const q = this._queue.splice(0, this._queue.length);
        for (let i = 0; i < q.length; i++) {
            this.send(q[i]);
        }
    }

    _rlCheck(data, cb) {
        if (!this._rlEnabled) return true;
        if (this._rlTokens > 0) {
            this._rlTokens--;
            return true;
        }
        this._rlQueue.push({ data, cb });
        return false;
    }

    _drainRlQueue() {
        while (this._rlQueue.length > 0 && this._rlTokens > 0) {
            const item = this._rlQueue.shift();
            this._rlTokens--;
            this._sendDirect(item.data, item.cb);
        }
    }

    _nextUrl() {
        if (!this._urls || this._urls.length <= 1) return;
        this._urlIndex = (this._urlIndex + 1) % this._urls.length;
        this.url = this._urls[this._urlIndex];
    }

    cork() {
        if (this._sock && !this._sock.destroyed) {
            this._corked = true;
            this._sock.cork();
        }
    }

    uncork() {
        if (this._sock && !this._sock.destroyed && this._corked) {
            this._corked = false;
            this._sock.uncork();
        }
    }

    _init() {
        const s = this.url.protocol === 'wss:';
        const host = this.opts.host || this.url.hostname;
        const port = parseInt(this.opts.port || this.url.port) || (s ? 443 : 80);
        const path = (this.url.pathname || '/') + (this.url.search || '');
        const hostHdr = this.opts.headers?.Host || this.opts.headers?.host || this.opts.servername || this.url.hostname;

        let req = `GET ${path} HTTP/1.1\r\nHost: ${hostHdr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${this._key}\r\nSec-WebSocket-Version: 13\r\n`;
        if (this.opts.headers) {
            for (const [k, v] of Object.entries(this.opts.headers)) {
                const l = k.toLowerCase();
                if (l !== 'host' && l !== 'upgrade' && l !== 'connection' && l !== 'sec-websocket-key' && l !== 'sec-websocket-version') {
                    req += `${k}: ${v}\r\n`;
                }
            }
        }
        if (this.opts.protocols) {
            req += `Sec-WebSocket-Protocol: ${Array.isArray(this.opts.protocols) ? this.opts.protocols.join(', ') : this.opts.protocols}\r\n`;
        }
        req += '\r\n';
        const reqBuf = Buffer.from(req);

        if (this.opts.createSocket) {
            this._sock = this.opts.createSocket(this.url);
            const writeHdr = () => { if (this._sock && !this._sock.destroyed) this._sock.write(reqBuf); };
            if (this._sock.connecting) this._sock.once(s ? 'secureConnect' : 'connect', writeHdr);
            else writeHdr();
        } else if (s) {
            this._sock = tlsConnect({
                host, port,
                servername: this.opts.servername || this.url.hostname,
                rejectUnauthorized: this.opts.rejectUnauthorized ?? true,
                minVersion: this.opts.minVersion || 'TLSv1.2',
                ciphers: this.opts.ciphers || DEFAULT_CIPHERS,
                ALPNProtocols: this.opts.ALPNProtocols || ['http/1.1'],
                ...(this.opts.secureContext ? { secureContext: this.opts.secureContext } : {}),
                ...(this.opts.maxVersion ? { maxVersion: this.opts.maxVersion } : {}),
                ...(this.opts.session ? { session: this.opts.session } : {}),
                ...(this.opts.tls || {})
            });
            this._sock.once('secureConnect', () => { if (this._sock && !this._sock.destroyed) this._sock.write(reqBuf); });
        } else {
            this._sock = netConnect({ host, port });
            this._sock.once('connect', () => { if (this._sock && !this._sock.destroyed) this._sock.write(reqBuf); });
        }

        this._sock.setNoDelay(this.opts.noDelay !== false);
        if (this.opts.keepAlive !== false) this._sock.setKeepAlive(true, this.opts.keepAliveInterval || 10000);
        const frag = this.opts.maxSendFragment ?? 2048;
        if (frag && this._sock.setMaxSendFragment) this._sock.setMaxSendFragment(frag);
        this._sock.setTimeout(0);

        this._sock.on('data', d => this._onData(d));
        this._sock.on('error', e => { if (this.error) this.error(e); });
        this._sock.on('close', () => this._onClose());
        this._sock.on('drain', () => {
            this.saturated = false;
            if (this.flushed) this.flushed();
        });

        const to = this.opts.handshakeTimeout ?? this.opts.connectTimeout ?? 5000;
        if (to > 0) {
            this._timer = setTimeout(() => {
                if (!this.isOpen) {
                    if (this.error) this.error(new Error('timeout'));
                    this._onClose(1006);
                }
            }, to);
        }
    }

    _checkAck(payload, op) {
        const len = this._ackWaiters.length;
        if (len === 0) return;
        for (let i = 0; i < len; i++) {
            const w = this._ackWaiters[i];
            if (w.match(payload, op)) {
                this._ackWaiters.splice(i, 1);
                clearTimeout(w.timer);
                w.resolve(payload);
                break;
            }
        }
    }

    _emitRecv(payload, op) {
        if (this._statsEnabled) {
            this._msgIn++;
            this._bytesIn += payload.length;
        }

        if (this.afterRecv) {
            payload = this.afterRecv(payload, op);
            if (payload === false || payload === null || payload === undefined) return;
        }

        this._checkAck(payload, op);

        if (this.recv) {
            if (this._json && op === 1) {
                try { this.recv(JSON.parse(payload), op); } catch (_) { this.recv(payload, op); }
            } else {
                this.recv(payload, op);
            }
        }
    }

    _onData(c) {
        if (!this.isOpen) return this._parseHandshake(c);

        let b = c;
        if (this._rbLen > 0) {
            this._pushRb(c);
            b = this._rb.subarray(0, this._rbLen);
        }

        let o = 0;
        const l = b.length;

        while (l - o >= 2) {
            const b1 = b[o + 1];

            if (b1 < 126) {
                const total = 2 + b1;
                if (l - o < total) break;

                const b0 = b[o];
                const op = b0 & 0x0F;
                o += total;

                if (op === 1 || op === 2) {
                    const payload = b1 === 0 ? EMPTY : b.subarray(o - b1, o);
                    if (b0 >= 128) {
                        this._emitRecv(payload, op);
                    } else {
                        this._fragOp = op;
                        this._frags = [Buffer.from(payload)];
                    }
                } else if (op === 0) {
                    const payload = b1 === 0 ? EMPTY : b.subarray(o - b1, o);
                    if (this._frags) {
                        this._frags.push(Buffer.from(payload));
                        if (b0 >= 128) {
                            const full = this._frags.length === 1 ? this._frags[0] : Buffer.concat(this._frags);
                            this._frags = null;
                            this._emitRecv(full, this._fragOp);
                        }
                    }
                } else if (op === 9) {
                    const payload = b1 === 0 ? EMPTY : b.subarray(o - b1, o);
                    if (this.autoPong && this._sock && !this._sock.destroyed) {
                        if (b1 === 0) this._sock.write(PONG_EMPTY);
                        else this._sock.write(pack(10, payload, true, this.fast));
                    }
                    if (this.onPing) this.onPing(payload);
                    else if (this.ping && this.ping !== VoidSocket.prototype.ping) this.ping(payload);
                } else if (op === 10) {
                    this._onPong();
                } else if (op === 8) {
                    const code = b1 >= 2 ? (b[o - b1] << 8) | b[o - b1 + 1] : 1005;
                    this._onClose(code);
                    return;
                }
                continue;
            }

            const b0 = b[o];
            const op = b0 & 0x0F;
            const masked = (b1 & 0x80) !== 0;
            let plen = b1 & 0x7F;
            let hs = 2;

            if (plen === 126) {
                if (l - o < 4) break;
                plen = (b[o + 2] << 8) | b[o + 3];
                hs = 4;
            } else if (plen === 127) {
                if (l - o < 10) break;
                if (b.readUInt32BE(o + 2) !== 0) { this.kill(); return; }
                plen = b.readUInt32BE(o + 6);
                hs = 10;
            }

            if (masked) {
                if (l - o < hs + 4) break;
                hs += 4;
            }

            const total = hs + plen;
            if (l - o < total) break;

            let payload;
            if (plen === 0) {
                payload = EMPTY;
            } else {
                payload = b.subarray(o + hs, o + total);
                if (masked) {
                    const m32 = b.readInt32LE(o + hs - 4);
                    mask32(payload, 0, plen, m32);
                }
            }

            o += total;

            if (op === 1 || op === 2) {
                if (b0 >= 128) {
                    this._emitRecv(payload, op);
                } else {
                    this._fragOp = op;
                    this._frags = [Buffer.from(payload)];
                }
            } else if (op === 0) {
                if (this._frags) {
                    this._frags.push(Buffer.from(payload));
                    if (b0 >= 128) {
                        const full = this._frags.length === 1 ? this._frags[0] : Buffer.concat(this._frags);
                        this._frags = null;
                        this._emitRecv(full, this._fragOp);
                    }
                }
            } else if (op === 9) {
                if (this.autoPong && this._sock && !this._sock.destroyed) {
                    if (plen === 0) this._sock.write(PONG_EMPTY);
                    else this._sock.write(pack(10, payload, true, this.fast));
                }
                if (this.onPing) this.onPing(payload);
                else if (this.ping && this.ping !== VoidSocket.prototype.ping) this.ping(payload);
            } else if (op === 10) {
                this._onPong();
            } else if (op === 8) {
                const code = plen >= 2 ? (payload[0] << 8) | payload[1] : 1005;
                this._onClose(code);
                return;
            }
        }

        if (this._rbLen > 0) {
            if (o < l) {
                this._rb.copy(this._rb, 0, o, l);
                this._rbLen = l - o;
            } else {
                this._rbLen = 0;
                if (this._rb.length > this._rbInitSize) {
                    this._rb = Buffer.allocUnsafe(this._rbInitSize);
                }
            }
        } else if (o < l) {
            const rem = l - o;
            if (rem > this._rb.length) {
                this._rb = Buffer.allocUnsafe(Math.max(this._rb.length << 1, rem));
            }
            c.copy(this._rb, 0, o, l);
            this._rbLen = rem;
        }
    }

    _pushRb(chunk) {
        const cl = chunk.length;
        if (this._rbLen + cl > this._rb.length) {
            const nb = Buffer.allocUnsafe(Math.max(this._rb.length << 1, this._rbLen + cl));
            this._rb.copy(nb, 0, 0, this._rbLen);
            this._rb = nb;
        }
        chunk.copy(this._rb, this._rbLen);
        this._rbLen += cl;
    }

    _parseHandshake(chunk) {
        this._pushRb(chunk);
        const idx = this._rb.indexOf(CRLF, 0);
        if (idx === -1 || idx >= this._rbLen) return;

        const hdr = this._rb.toString('latin1', 0, idx);
        const tail = Buffer.from(this._rb.subarray(idx + 4, this._rbLen));

        if (!hdr.includes('101')) {
            if (this.error) this.error(new Error('upgrade failed'));
            this._onClose(1006);
            return;
        }

        if (this.opts.skipAcceptCheck !== true) {
            const lines = hdr.split('\r\n');
            let accept = '';
            for (let i = 1; i < lines.length; i++) {
                const l = lines[i];
                const colon = l.indexOf(':');
                if (colon !== -1 && l.substring(0, colon).trim().toLowerCase() === 'sec-websocket-accept') {
                    accept = l.substring(colon + 1).trim();
                    break;
                }
            }

            if (accept !== this._expect) {
                if (this.error) this.error(new Error('bad accept'));
                this._onClose(1006);
                return;
            }
        }

        this.isOpen = true;
        this.isClosed = false;
        this.reconnectAttempts = 0;
        this._connectedAt = Date.now();
        if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
        if (this._timer) { clearTimeout(this._timer); this._timer = null; }

        this._startPing();

        if (this._queueEnabled) this._flushQueue();

        if (this.online) this.online();

        this._rbLen = 0;
        if (tail.length > 0) this._onData(tail);
    }

    _onClose(code = 1006) {
        if (!this.isClosed) {
            this.isClosed = true;
            this.isOpen = false;
            this.saturated = false;
            this._stopPing();
            if (this._timer) { clearTimeout(this._timer); this._timer = null; }
            this._frags = null;
            this._fragOp = 0;
            this._rbLen = 0;

            if (this._sock) {
                if (!this._sock.destroyed) {
                    try { this._sock.write(CLOSE_FRAME); } catch (_) {}
                    this._sock.destroy();
                }
                this._sock.removeAllListeners();
                this._sock = null;
            }

            if (this._ackWaiters.length > 0) {
                const err = new Error('connection closed');
                for (let i = 0; i < this._ackWaiters.length; i++) {
                    clearTimeout(this._ackWaiters[i].timer);
                    this._ackWaiters[i].reject(err);
                }
                this._ackWaiters.length = 0;
            }

            if (this.offline) this.offline(code);

            if (this.reconnect && !this._killed && this.reconnectAttempts < this.maxReconnectAttempts) {
                this._reconnectCount++;

                if (this._urls && this.reconnectAttempts > 0 && this.reconnectAttempts % this._urls.length === 0) {
                    this._nextUrl();
                }

                const delay = Math.min(this.reconnectDelay * Math.pow(1.5, this.reconnectAttempts), this.maxReconnectDelay) + ((Math.random() * 200) | 0);
                this.reconnectAttempts++;
                if (this.reconnecting) this.reconnecting(this.reconnectAttempts, delay);
                this._reconnectTimer = setTimeout(() => {
                    this._reconnectTimer = null;
                    if (!this._killed) {
                        this._key = randomBytes(16).toString('base64');
                        this._expect = createHash('sha1').update(this._key + GUID).digest('base64');
                        this.isOpen = false;
                        this.isClosed = false;
                        this._init();
                    }
                }, delay);
            }
        }
    }

    _sendDirect(data, cb) {
        if (!this.isOpen || !this._sock || this._sock.destroyed) {
            if (cb) cb(new Error('not open'));
            return false;
        }

        if (this.beforeSend) {
            const result = this.beforeSend(data);
            if (result === false) return false;
            if (result !== undefined && result !== true) data = result;
        }

        const isStr = typeof data === 'string';
        const isObj = !isStr && !Buffer.isBuffer(data) && typeof data === 'object' && data !== null && !ArrayBuffer.isView(data);

        let frame;
        if (this._json && isObj) {
            frame = pack(1, JSON.stringify(data), true, this.fast);
        } else {
            frame = pack(isStr ? 1 : 2, data, true, this.fast);
        }

        const ok = this._sock.write(frame, cb);
        if (!ok) this.saturated = true;

        if (this._statsEnabled) {
            this._msgOut++;
            this._bytesOut += frame.length;
        }

        return ok;
    }

    send(data, cb) {
        if (!this.isOpen || !this._sock || this._sock.destroyed) {
            if (this._queueEnabled) {
                this._enqueue(data);
                return false;
            }
            if (cb) cb(new Error('not open'));
            return false;
        }

        if (this._rlEnabled && !this._rlCheck(data, cb)) {
            return false;
        }

        return this._sendDirect(data, cb);
    }

    raw(buf, cb) {
        if (this.isOpen && this._sock && !this._sock.destroyed) {
            const ok = this._sock.write(buf, cb);
            if (!ok) this.saturated = true;
            if (this._statsEnabled) {
                this._msgOut++;
                this._bytesOut += buf.length;
            }
            return ok;
        }
        if (cb) cb(new Error('not open'));
        return false;
    }

    json(obj, cb) {
        if (!this.isOpen || !this._sock || this._sock.destroyed) {
            if (this._queueEnabled) {
                this._enqueue(obj);
                return false;
            }
            if (cb) cb(new Error('not open'));
            return false;
        }

        if (this.beforeSend) {
            const result = this.beforeSend(obj);
            if (result === false) return false;
            if (result !== undefined && result !== true) obj = result;
        }

        const frame = pack(1, JSON.stringify(obj), true, this.fast);
        const ok = this._sock.write(frame, cb);
        if (!ok) this.saturated = true;

        if (this._statsEnabled) {
            this._msgOut++;
            this._bytesOut += frame.length;
        }

        return ok;
    }

    sendAck(data, matcher, opts = {}) {
        const timeout = opts.timeout ?? 5000;
        const retries = opts.retries ?? 3;
        const matchFn = typeof matcher === 'function' ? matcher :
                        typeof matcher === 'string' ? buf => buf.includes(matcher) :
                        typeof matcher === 'number' ? (_, op) => op === matcher :
                        matcher instanceof RegExp ? buf => matcher.test(buf.toString()) :
                        () => true;

        return new Promise((resolve, reject) => {
            let attempts = 0;
            const trySend = () => {
                attempts++;
                if (!this.isOpen || !this._sock || this._sock.destroyed) {
                    return reject(new Error('not open'));
                }
                this.send(data);
                const waiter = {
                    match: matchFn,
                    resolve,
                    reject,
                    timer: setTimeout(() => {
                        const idx = this._ackWaiters.indexOf(waiter);
                        if (idx !== -1) this._ackWaiters.splice(idx, 1);
                        if (attempts < retries && this.isOpen && this._sock && !this._sock.destroyed) {
                            trySend();
                        } else {
                            reject(new Error('ack timeout'));
                        }
                    }, timeout)
                };
                this._ackWaiters.push(waiter);
            };
            trySend();
        });
    }

    expectAck(matcher, timeout = 5000) {
        const matchFn = typeof matcher === 'function' ? matcher :
                        typeof matcher === 'string' ? buf => buf.includes(matcher) :
                        typeof matcher === 'number' ? (_, op) => op === matcher :
                        matcher instanceof RegExp ? buf => matcher.test(buf.toString()) :
                        () => true;

        return new Promise((resolve, reject) => {
            if (!this.isOpen || !this._sock || this._sock.destroyed) {
                return reject(new Error('not open'));
            }
            const waiter = {
                match: matchFn,
                resolve,
                reject,
                timer: setTimeout(() => {
                    const idx = this._ackWaiters.indexOf(waiter);
                    if (idx !== -1) this._ackWaiters.splice(idx, 1);
                    reject(new Error('ack timeout'));
                }, timeout)
            };
            this._ackWaiters.push(waiter);
        });
    }

    flush() {
        if (!this.saturated || !this._sock || this._sock.destroyed) return Promise.resolve();
        return new Promise(resolve => this._sock.once('drain', resolve));
    }

    ping(payload = EMPTY, cb) {
        if (this.isOpen && this._sock && !this._sock.destroyed) {
            return this._sock.write(pack(9, payload, true, this.fast), cb);
        }
        if (cb) cb(new Error('not open'));
        return false;
    }

    pong(payload = EMPTY, cb) {
        if (this.isOpen && this._sock && !this._sock.destroyed) {
            if (!payload || payload.length === 0) return this._sock.write(PONG_EMPTY, cb);
            return this._sock.write(pack(10, payload, true, this.fast), cb);
        }
        if (cb) cb(new Error('not open'));
        return false;
    }

    reconnect() {
        if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
        this._killed = false;
        this._onClose(1000);
        this.isClosed = false;
        this._key = randomBytes(16).toString('base64');
        this._expect = createHash('sha1').update(this._key + GUID).digest('base64');
        this._init();
    }

    close(code = 1000) {
        this._killed = true;
        if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
        this._stopPing();
        this._onClose(code);
    }

    kill() {
        this._killed = true;
        if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
        if (this._rlTimer) { clearInterval(this._rlTimer); this._rlTimer = null; }
        this._stopPing();
        this._onClose(1000);
    }

    on(ev, fn) {
        if (ev === 'message' || ev === 'recv') this.recv = fn;
        else if (ev === 'open' || ev === 'online') this.online = fn;
        else if (ev === 'close' || ev === 'offline') this.offline = fn;
        else if (ev === 'error') this.error = fn;
        else if (ev === 'ping') this.onPing = fn;
        else if (ev === 'flushed' || ev === 'drain') this.flushed = fn;
        else if (ev === 'reconnecting') this.reconnecting = fn;
        return this;
    }

    sendPing(payload, cb) { return this.ping(payload, cb); }
    sendPong(payload, cb) { return this.pong(payload, cb); }

    get socket() { return this._sock; }
    get bufferedAmount() { return this._sock ? this._sock.writableLength : 0; }
}

export default VoidSocket;
export { VoidSocket, VoidSocket as Void, VoidSocket as v0id, VoidSocket as vd, pack, packHeader, mask32, fastMask32, parseFrames, returnHeaderBuf };
