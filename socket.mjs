// xherz
import {
    createHash,
    randomBytes,
    randomFillSync,
    tlsConnect,
    netConnect,
    deflateRawSync,
    inflateRawSync,
    GUID,
    CRLF,
    EMPTY,
    CLOSE_FRAME,
    CLOSE_FRAME_UNMASKED,
    PONG_EMPTY,
    PONG_EMPTY_UNMASKED,
    PING_EMPTY,
    PING_EMPTY_UNMASKED,
    DEFAULT_CIPHERS,
    ERR_CODES,
    makeCloseFrame,
    fastMask32,
    nextMask,
    mask32,
    parseFrames,
    pack,
    packHeader,
    getHeaderBuf,
    returnHeaderBuf,
    getFragBuf,
    returnFragBuf,
} from './shared.mjs';

class RingQueue {
    constructor(initialCap = 256) {
        let cap = 1;
        while (cap < initialCap) cap <<= 1;
        this._buf = new Array(cap);
        this._head = 0;
        this._tail = 0;
        this._size = 0;
    }
    get length() { return this._size; }
    push(v) {
        if (this._size === this._buf.length) {
            const newBuf = new Array(this._buf.length * 2);
            for (let i = 0; i < this._size; i++) newBuf[i] = this._buf[(this._head + i) & (this._buf.length - 1)];
            this._head = 0;
            this._tail = this._size;
            this._buf = newBuf;
        }
        this._buf[this._tail] = v;
        this._tail = (this._tail + 1) & (this._buf.length - 1);
        this._size++;
    }
    shift() {
        if (this._size === 0) return undefined;
        const v = this._buf[this._head];
        this._buf[this._head] = undefined;
        this._head = (this._head + 1) & (this._buf.length - 1);
        this._size--;
        return v;
    }
    clear() { this._head = 0; this._tail = 0; this._size = 0; this._buf.fill(undefined); }
}

class VoidSocket {
    static _idCounter = 0;

    constructor(url, opts = {}) {
        if (typeof url === 'object' && url !== null && !Array.isArray(url) && !(url instanceof URL) && !url.href) {
            opts = url;
            url = opts.url || null;
        }

        this._uid = ++VoidSocket._idCounter;
        this._name = opts.name || null;

        this.isServer = opts.isServer === true || !!opts.socket;
        this.opts = opts;
        this.fast = opts.fastMask ?? true;
        this.autoPong = opts.autoPong !== false;
        this.saturated = false;

        this.recv = null;
        this.online = null;
        this.offline = null;
        this.error = null;
        this.onPing = null;
        this.flushed = null;
        this.reconnecting = null;
        this.connecting = null;
        this.onStateChange = null;

        this.beforeSend = null;
        this.afterRecv = null;

        this._maxPayload = opts.maxPayload ?? (1024 * 1024 * 1024);
        this._maxFragments = opts.maxFragments ?? 0;
        this._batchSize = opts.batchSize ?? 65536;
        this._highWaterMark = opts.highWaterMark ?? (2 * 1024 * 1024 * 1024);
        this._yieldAfterFrames = opts.yieldAfterFrames ?? 0;

        this._rbInitSize = opts.bufferSize || (1024 * 1024);
        this._rb = Buffer.allocUnsafe(this._rbInitSize);
        this._rbLen = 0;
        this._rbOff = 0;
        this._flushing = false;
        this._rbOverflow = false;

        this._framesProcessed = 0;
        this._framesPerBatch = 0;
        this._parsePending = false;
        this._paused = false;

        this._frags = null;
        this._fragOp = 0;
        this._fragBytes = 0;
        this._fragCount = 0;

        this._ackWaiters = [];
        this._msgQueue = [];

        this._pingInterval = opts.pingInterval ?? 0;
        this._pingTimeout = opts.pingTimeout ?? 0;
        this._autoPing = opts.autoPing === true;
        this._pingTimer = null;
        this._pongTimer = null;
        this._awaitingPong = false;
        this._lastPongTime = 0;

        this._json = opts.json === true;

        this._statsEnabled = opts.stats === true;
        this._msgIn = 0;
        this._msgOut = 0;
        this._bytesIn = 0;
        this._bytesOut = 0;
        this._reconnectCount = 0;
        this._connectedAt = 0;
        this._lastLatency = 0;
        this._pingSentAt = 0;

        this._minLatency = Infinity;
        this._maxLatency = 0;
        this._totalLatency = 0;
        this._pingCount = 0;

        this._msgId = 0;
        this._sentAcks = new Map();

        this._meta = {};

        this._state = 'idle';
        this._healthTimer = null;
        this._healthCheck = opts.healthCheck === true;
        this._healthInterval = opts.healthInterval ?? 30000;
        this._healthTimeout = opts.healthTimeout ?? 10000;
        this._lastHealthCheck = 0;
        this._healthFailures = 0;
        this._maxHealthFailures = opts.maxHealthFailures ?? 5;

        this._adaptiveDelay = opts.adaptiveReconnect === true;
        this._qualityScore = 100;

        this._compressEnabled = opts.compress === true;
        this._compressThreshold = opts.compressThreshold ?? 1024;
        this._compressLevel = opts.compressLevel ?? 1;
        this._compressSent = 0;
        this._compressReceived = 0;

        this._autoCompact = opts.autoCompact !== false;
        this._compactThreshold = opts.compactThreshold ?? (this._rbInitSize << 4);

        this._autoTimeouts = new Map();

        this._corked = false;
        this._killed = false;

        const rl = opts.rateLimit;
        this._rlEnabled = !!rl;
        this._rlCount = rl ? rl.count : 0;
        this._rlWindow = rl ? rl.window : 0;
        this._rlTokens = rl ? rl.count : 0;
        this._rlTimer = null;
        this._rlQueue = new RingQueue(64);

        if (this._rlEnabled) {
            this._rlTimer = setInterval(() => {
                this._rlTokens = this._rlCount;
                this._drainRlQueue();
            }, this._rlWindow);
            if (this._rlTimer.unref) this._rlTimer.unref();
        }

        if (this.isServer) {
            this._reconnect = false;
            this.reconnectAttempts = 0;
            this.maxReconnectAttempts = 0;
            this.reconnectDelay = 0;
            this.maxReconnectDelay = 0;
            this._queueEnabled = false;
            this._maxQueueSize = 0;
            this._queue = new RingQueue();

            this.req = opts.req || null;
            this.ip = opts.req ? (opts.req.headers['x-forwarded-for']?.split(',')[0].trim() || opts.req.socket?.remoteAddress || '') : (opts.socket?.remoteAddress || '');
            this.url = typeof url === 'string' ? new URL(url) : (url || (opts.req ? new URL(opts.req.url, 'http://localhost') : null));

            this.isOpen = true;
            this.isClosed = false;
            this._connectedAt = Date.now();

            this._sock = opts.socket;
            this._sock.setNoDelay(this.opts.noDelay !== false);
            if (this.opts.keepAlive !== false) this._sock.setKeepAlive(true, this.opts.keepAliveInterval || 10000);
            const frag = this.opts.maxSendFragment ?? 65536;
            if (frag && this._sock.setMaxSendFragment) this._sock.setMaxSendFragment(frag);
            this._sock.setTimeout(0);

            this._sock.on('data', d => this._onData(d));
            this._sock.on('error', e => this._fire(this.error, e));
            this._sock.on('close', () => this._onClose());
            this._sock.on('drain', () => {
                this.saturated = false;
                this._fire(this.flushed);
            });

            this._startPing();

            if (opts.head && opts.head.length > 0) {
                process.nextTick(() => {
                    if (this.isOpen && this._sock && !this._sock.destroyed) {
                        this._onData(opts.head);
                    }
                });
            }

            process.nextTick(() => {
                if (this.isOpen) this._fire(this.online);
            });
        } else {
            if (Array.isArray(url)) {
                this._urls = url.map(u => typeof u === 'string' ? new URL(u) : u);
                this._urlIndex = 0;
                this.url = this._urls[0];
            } else {
                this._urls = null;
                this._urlIndex = 0;
                this.url = typeof url === 'string' ? new URL(url) : url;
            }

            this.isOpen = false;
            this.isClosed = false;

            this._reconnect = opts.reconnect !== false;
            this.reconnectAttempts = 0;
            this.maxReconnectAttempts = opts.maxReconnectAttempts ?? Infinity;
            this.reconnectDelay = opts.reconnectDelay ?? 500;
            this.maxReconnectDelay = opts.maxReconnectDelay ?? 10000;

            this._queueEnabled = opts.queue !== false;
            this._maxQueueSize = opts.maxQueueSize ?? Infinity;
            this._queue = new RingQueue();

            this._sock = null;
            this._key = randomBytes(16).toString('base64');
            this._expect = createHash('sha1').update(this._key + GUID).digest('base64');
            this._timer = null;
            this._reconnectTimer = null;

            this._init();
        }
    }

    get stats() {
        return {
            messagesIn: this._msgIn,
            messagesOut: this._msgOut,
            bytesIn: this._bytesIn,
            bytesOut: this._bytesOut,
            latency: this._lastLatency,
            minLatency: this._minLatency === Infinity ? 0 : this._minLatency,
            maxLatency: this._maxLatency,
            avgLatency: this._pingCount > 0 ? Math.round(this._totalLatency / this._pingCount) : 0,
            pingCount: this._pingCount,
            uptime: this._connectedAt > 0 ? Date.now() - this._connectedAt : 0,
            reconnects: this._reconnectCount,
            queueSize: this._queue.length,
            saturated: this.saturated,
            messageId: this._msgId,
            meta: this._meta,
            id: this._uid,
            name: this._name,
            state: this._state,
            qualityScore: this._qualityScore,
            healthFailures: this._healthFailures,
            lastHealthCheck: this._lastHealthCheck,
            compressionEnabled: this._compressEnabled,
            compressSent: this._compressSent,
            compressReceived: this._compressReceived
        };
    }

    _startPing() {
        this._stopPing();
        if (!this._autoPing || this._pingInterval <= 0) return;
        this._pingTimer = setInterval(() => {
            if (!this.isOpen || !this._sock || this._sock.destroyed) return;
            this._awaitingPong = true;
            this._pingSentAt = Date.now();
            this._sock.write(this.isServer ? PING_EMPTY_UNMASKED : PING_EMPTY);
            this._pongTimer = setTimeout(() => {
                if (this._awaitingPong) {
                    this._healthFailures++;
                    this._fire(this.error, new Error('ping timeout'));
                    this._onClose(1006);
                }
            }, this._pingTimeout);
            if (this._pongTimer.unref) this._pongTimer.unref();
        }, this._pingInterval);
        if (this._pingTimer.unref) this._pingTimer.unref();
        this._startHealthCheck();
    }

    _stopPing() {
        if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
        if (this._pongTimer) { clearTimeout(this._pongTimer); this._pongTimer = null; }
        this._awaitingPong = false;
        this._stopHealthCheck();
    }

    _onPong() {
        if (this._awaitingPong) {
            this._awaitingPong = false;
            this._lastPongTime = Date.now();
            this._lastLatency = this._lastPongTime - this._pingSentAt;
            this._pingCount++;
            this._totalLatency += this._lastLatency;
            if (this._lastLatency < this._minLatency) this._minLatency = this._lastLatency;
            if (this._lastLatency > this._maxLatency) this._maxLatency = this._lastLatency;
            this._healthFailures = Math.max(0, this._healthFailures - 1);
            this._updateQuality();
            if (this._pongTimer) { clearTimeout(this._pongTimer); this._pongTimer = null; }
        }
    }

    _enqueue(data) {
        this._queue.push(Buffer.isBuffer(data) ? Buffer.from(data) : data);
        if (this._maxQueueSize !== Infinity && this._queue.length > this._maxQueueSize) {
            this._fire(this.error, new Error(`queue exceeded limit: ${this._queue.length}/${this._maxQueueSize} messages pending`));
        }
    }

    _flushQueue() {
        if (this._queue.length === 0 || this._flushing) return;
        if (!this.isOpen || !this._sock || this._sock.destroyed) return;
        this._flushing = true;
        const sock = this._sock;
        sock.cork();
        while (this._queue.length > 0) {
            if (!this.isOpen || !this._sock || this._sock.destroyed) break;
            this._sendDirect(this._queue.shift());
            if (this.saturated) break;
        }
        sock.uncork();
        if (this._queue.length > 0 && this.isOpen && this._sock && !this._sock.destroyed) {
            if (this.saturated) {
                this._sock.once('drain', () => {
                    this.saturated = false;
                    this._flushing = false;
                    this._flushQueue();
                });
                return;
            }
        }
        this._flushing = false;
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
        if (!this._sock || this._sock.destroyed) { this._throwError(ERR_CODES.ERR_NO_SOCKET, 'Cannot cork: no active socket'); return; }
        this._corked = true;
        this._sock.cork();
    }

    uncork() {
        if (!this._sock || this._sock.destroyed) { this._throwError(ERR_CODES.ERR_NO_SOCKET, 'Cannot uncork: no active socket'); return; }
        if (!this._corked) { this._throwError(ERR_CODES.ERR_INVALID_STATE, 'Cannot uncork: socket is not corked'); return; }
        this._corked = false;
        this._sock.uncork();
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

        this._fire(this.connecting);
        this._setState('connecting');

        this._sock.on('data', d => this._onData(d));
        this._sock.on('error', e => this._fire(this.error, e));
        this._sock.on('close', () => this._onClose());
        this._sock.on('drain', () => {
            this.saturated = false;
            this._fire(this.flushed);
        });

        const to = this.opts.handshakeTimeout ?? this.opts.connectTimeout ?? 5000;
        if (to > 0) {
            this._timer = setTimeout(() => {
                if (!this.isOpen) {
                    this._fire(this.error, new Error('timeout'));
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
                const last = len - 1;
                if (i !== last) this._ackWaiters[i] = this._ackWaiters[last];
                this._ackWaiters.length = last;
                clearTimeout(w.timer);
                w.resolve(payload);
                break;
            }
        }
    }

    _emitRecv(payload, op) {
        if (this._compressEnabled && payload.length > 0 && payload[0] === 1) { try { payload = inflateRawSync(payload.subarray(1)); } catch { } }
        if (this._statsEnabled) { this._msgIn++; this._bytesIn += payload.length; }
        if (this.afterRecv) { payload = this.afterRecv(payload, op); if (payload == null) return; }
        this._checkAck(payload, op);
        if (this.recv) {
            const h = this.recv;
            if (typeof h === 'function') { if (this._json && op === 1) { try { h(JSON.parse(payload), op); } catch { h(payload, op); } } else h(payload, op); }
            else for (let i = 0; i < h.length; i++) { if (this._json && op === 1) { try { h[i](JSON.parse(payload), op); } catch { h[i](payload, op); } } else h[i](payload, op); }
        }
    }

    _pushRb(chunk) {
        const cl = chunk.length;
        const needed = this._rbOff + this._rbLen + cl;
        if (needed > this._rb.length) {
            if (this._rb.length < this._highWaterMark) {
                const newSize = Math.max(this._rb.length << 1, this._rbLen + cl);
                const nb = Buffer.allocUnsafe(newSize);
                if (this._rbLen > 0) {
                    this._rb.copy(nb, 0, this._rbOff, this._rbOff + this._rbLen);
                }
                this._rb = nb;
                this._rbOff = 0;
            } else {
                this._rbOverflow = true;
                return;
            }
        }
        chunk.copy(this._rb, this._rbOff + this._rbLen);
        this._rbLen += cl;
    }

    _onData(c) {
        if (!this.isOpen) return this._parseHandshake(c);
        if (this._rbLen > 0) this._pushRb(c);
        const buf = this._rbLen > 0 ? this._rb.subarray(this._rbOff, this._rbOff + this._rbLen) : c;
        const L = buf.length;
        let o = 0;
        const isSrv = this.isServer;
        const maskReq = isSrv && this.opts.maskRequired !== false;
        const sock = this._sock;
        const maxP = this._maxPayload;
        const autoPong = this.autoPong;
        const sockAlive = sock && !sock.destroyed;
        const _fragErr = this._maxFragments > 0 ? 'too many fragments' : 'fragmented message exceeded maxPayload';
        const _maxFrags = this._maxFragments;
        let frames = 0, msgIn = 0, bytesIn = 0;
        while (L - o >= 2) {
            const b0 = buf[o], b1 = buf[o + 1];
            const op = b0 & 15;
            if (b1 < 126) {
                if (maskReq) { this._fire(this.error, new Error('unmasked client frame')); this._onClose(1002); return; }
                const t = 2 + b1; if (L - o < t) break;
                o += t;
                if (op === 1 || op === 2) {
                    if (b0 & 128) { this._emitRecv(b1 ? buf.subarray(o - b1, o) : EMPTY, op); msgIn++; bytesIn += b1; }
                    else { this._fragOp = op; this._frags = [b1 ? buf.subarray(o - b1, o) : EMPTY]; this._fragBytes = b1; this._fragCount = 1; }
                } else if (op === 0) {
                    if (this._frags) {
                        this._fragCount++; this._fragBytes += b1;
                        if ((_maxFrags > 0 && this._fragCount > _maxFrags) || (this._maxPayload > 0 && this._fragBytes > this._maxPayload)) { this._frags = null; this._fire(this.error, new Error(_fragErr)); this._onClose(1009); return; }
                        this._frags.push(b1 ? buf.subarray(o - b1, o) : EMPTY);
                        if (b0 & 128) { const f = this._frags[0]; this._frags = null; this._emitRecv(this._fragCount === 1 ? f : Buffer.concat(this._frags), this._fragOp); msgIn++; bytesIn += this._fragBytes; }
                    }
                } else if (op === 9) {
                    const p = b1 ? buf.subarray(o - b1, o) : EMPTY;
                    if (autoPong && sockAlive) { if (isSrv) { b1 ? sock.write(pack(10, p, false)) : sock.write(PONG_EMPTY_UNMASKED); } else { b1 ? sock.write(pack(10, p, true, this.fast)) : sock.write(PONG_EMPTY); } }
                    if (this.onPing) this._fire(this.onPing, p); else if (this.ping !== VoidSocket.prototype.ping) this.ping(p);
                } else if (op === 10) { this._onPong(); }
                else if (op === 8) { this._onClose(b1 >= 2 ? (buf[o - b1] << 8) | buf[o - b1 + 1] : 1005); return; }
                frames++; continue;
            }
            const masked = b1 & 128;
            if (isSrv && !masked && maskReq) { this._fire(this.error, new Error('unmasked client frame')); this._onClose(1002); return; }
            if (!isSrv && masked && maskReq) { this._fire(this.error, new Error('masked server frame')); this._onClose(1002); return; }
            let plen = b1 & 127, hs = 2;
            if (plen === 126) { if (L - o < 4) break; plen = (buf[o + 2] << 8) | buf[o + 3]; hs = 4; }
            else if (plen === 127) { if (L - o < 10) break; plen = buf.readUInt32BE(o + 6); hs = 10; }
            if (this._maxPayload > 0 && plen > this._maxPayload) { this._fire(this.error, new Error('payload exceeded maxPayload')); this._onClose(1009); return; }
            if (masked) hs += 4;
            const tot = hs + plen; if (L - o < tot) break;
            const p = plen ? buf.subarray(o + hs, o + tot) : EMPTY;
            if (masked) mask32(p, 0, plen, buf.readInt32LE(o + hs - 4));
            o += tot;
            if (op === 1 || op === 2) {
                if (b0 & 128) { this._emitRecv(p, op); msgIn++; bytesIn += plen; }
                else { this._fragOp = op; this._frags = [p]; this._fragBytes = plen; this._fragCount = 1; }
            } else if (op === 0) {
                if (this._frags) {
                    this._fragCount++; this._fragBytes += plen;
                    if ((_maxFrags > 0 && this._fragCount > _maxFrags) || (this._maxPayload > 0 && this._fragBytes > this._maxPayload)) { this._frags = null; this._fire(this.error, new Error(_fragErr)); this._onClose(1009); return; }
                    this._frags.push(p);
                    if (b0 & 128) { const f = this._frags[0]; this._frags = null; this._emitRecv(this._fragCount === 1 ? f : Buffer.concat(this._frags), this._fragOp); msgIn++; bytesIn += this._fragBytes; }
                }
            } else if (op === 9) {
                if (autoPong && sockAlive) { if (isSrv) { plen ? sock.write(pack(10, p, false)) : sock.write(PONG_EMPTY_UNMASKED); } else { plen ? sock.write(pack(10, p, true, this.fast)) : sock.write(PONG_EMPTY); } }
                if (this.onPing) this._fire(this.onPing, p); else if (this.ping !== VoidSocket.prototype.ping) this.ping(p);
            } else if (op === 10) { this._onPong(); }
            else if (op === 8) { this._onClose(plen >= 2 ? (p[0] << 8) | p[1] : 1005); return; }
            frames++;
        }
        if (this._rbLen > 0) {
            if (o < L) {
                this._rbOff += o;
                this._rbLen = L - o;
                if (this._rbOff > (this._rb.length >> 1)) {
                    this._rb.copy(this._rb, 0, this._rbOff, this._rbOff + this._rbLen);
                    this._rbOff = 0;
                }
            } else {
                this._rbLen = 0;
                this._rbOff = 0;
                if (this._rb.length > (this._rbInitSize << 2)) {
                    this._rb = Buffer.allocUnsafe(this._rbInitSize);
                }
            }
        } else if (o < L) {
            const r = L - o;
            if (r > this._rb.length) {
                this._rb = Buffer.allocUnsafe(Math.max(this._rb.length << 1, r));
                c.copy(this._rb, 0, o, L);
            } else if (c.buffer === this._rb.buffer) {
                if (o !== 0) this._rb.copyWithin(0, o, L);
            } else {
                c.copy(this._rb, 0, o, L);
            }
            this._rbLen = r;
            this._rbOff = 0;
        }
        if (this._statsEnabled) { this._msgIn += msgIn; this._bytesIn += bytesIn; }
    }

    _parseHandshake(chunk) {
        this._pushRb(chunk);
        const idx = this._rb.indexOf(CRLF, 0);
        if (idx === -1 || idx >= this._rbLen) return;

        const hdr = this._rb.toString("latin1", 0, idx);
        const tailStart = idx + 4;
        const tailLen = this._rbLen - tailStart;

        if (!hdr.includes("101")) {
            this._fire(this.error, new Error("upgrade failed"));
            this._onClose(1006);
            return;
        }

        if (this.opts.skipAcceptCheck !== true) {
            const lines = hdr.split("\r\n");
            let accept = "";
            for (let i = 1; i < lines.length; i++) {
                const l = lines[i];
                const colon = l.indexOf(":");
                if (colon !== -1 && l.substring(0, colon).trim().toLowerCase() === "sec-websocket-accept") {
                    accept = l.substring(colon + 1).trim();
                    break;
                }
            }

            if (accept !== this._expect) {
                this._fire(this.error, new Error("bad accept"));
                this._onClose(1006);
                return;
            }
        }

        this.isOpen = true;
        this.isClosed = false;
        this.reconnectAttempts = 0;
        this._connectedAt = Date.now();
        this._setState("connected");
        this._healthFailures = 0;
        this._qualityScore = 100;
        if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
        if (this._timer) { clearTimeout(this._timer); this._timer = null; }

        this._startPing();

        if (this._queueEnabled) this._flushQueue();

        this._fire(this.online);

        const tailCopy = tailLen > 0 ? Buffer.from(this._rb.subarray(tailStart, tailStart + tailLen)) : null;
        this._rbLen = 0;
        this._rbOff = 0;
        if (tailCopy) this._onData(tailCopy);
    }

    _onClose(code = 1006) {
        if (!this.isClosed) {
            this.isClosed = true;
            this.isOpen = false;
            this.saturated = false;
            this._setState('disconnected');
            this._stopPing();
            this.clearAllTimeouts();
            if (this._timer) { clearTimeout(this._timer); this._timer = null; }
            this._frags = null;
            this._fragOp = 0;
            this._fragBytes = 0;
            this._fragCount = 0;
            this._rbLen = 0;
            this._flushing = false;

            if (this._rlQueue.length > 0) {
                if (this._reconnect && !this._killed) {
                    const nq = new RingQueue(this._rlQueue.length + this._queue.length + 1);
                    while (this._rlQueue.length > 0) nq.push(this._rlQueue.shift().data);
                    while (this._queue.length > 0) nq.push(this._queue.shift());
                    this._queue = nq;
                }
                this._rlQueue.clear();
            }

            if (this._sock) {
                if (!this._sock.destroyed) {
                    try { this._sock.write(makeCloseFrame(code, !this.isServer)); } catch (_) { }
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

            this._fire(this.offline, code);

            if (this._reconnect && !this._killed && this.reconnectAttempts < this.maxReconnectAttempts) {
                this._reconnectCount++;

                if (this._urls && this.reconnectAttempts > 0 && this.reconnectAttempts % this._urls.length === 0) {
                    this._nextUrl();
                }

                const baseDelay = this._adaptiveDelay
                    ? this.reconnectDelay * (2 - this._qualityScore / 100)
                    : this.reconnectDelay;
                const delay = Math.min(baseDelay * Math.pow(1.5, this.reconnectAttempts), this.maxReconnectDelay) + ((Math.random() * 200) | 0);
                this.reconnectAttempts++;
                this._setState('reconnecting');
                this._fire(this.reconnecting, this.reconnectAttempts, delay);
                this._reconnectTimer = setTimeout(() => {
                    this._reconnectTimer = null;
                    if (!this._killed) {
                        this._key = randomBytes(16).toString('base64');
                        this._expect = createHash('sha1').update(this._key + GUID).digest('base64');
                        this.isOpen = false;
                        this.isClosed = false;
                        this._minLatency = Infinity;
                        this._maxLatency = 0;
                        this._totalLatency = 0;
                        this._pingCount = 0;
                        this._qualityScore = 100;
                        this._healthFailures = 0;
                        this._init();
                    }
                }, delay);
            }
        }
    }

    _sendDirect(data, cb) {
        if (!this.isOpen || !this._sock || this._sock.destroyed) { if (cb) cb(new Error('not open')); return false; }
        if (this.beforeSend) { const r = this.beforeSend(data); if (r === false) return false; if (r !== void 0 && r !== true) data = r; }
        let payload, op;
        if (typeof data === 'string') {
            payload = Buffer.from(data);
            op = 1;
        } else if (Buffer.isBuffer(data)) {
            payload = data;
            op = 2;
        } else if (data !== null && typeof data === 'object' && !ArrayBuffer.isView(data)) {
            payload = Buffer.from(JSON.stringify(data));
            op = 1;
        } else {
            payload = Buffer.from(data.buffer || data, data.byteOffset || 0, data.byteLength || 0);
            op = 2;
        }
        if (this._compressEnabled) { const c = this._compressPayload(payload); if (c !== payload) payload = c; }
        const mask = !this.isServer;
        const frame = pack(op, payload, mask, this.fast);
        const ok = this._sock.write(frame, cb);
        if (!ok) this.saturated = true;
        if (this._statsEnabled) { this._msgOut++; this._bytesOut += frame.length; }
        return ok;
    }

    send(data, cb) {
        if (!this.isOpen || !this._sock || this._sock.destroyed) return this._killed ? (cb?.(this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Cannot send on a killed socket')), false) : this._queueEnabled ? (this._enqueue(data), false) : (cb?.(this._throwError(ERR_CODES.ERR_NOT_OPEN, 'Socket is not open')), false);
        if (this._rlEnabled && !this._rlCheck(data, cb)) return false;
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
        if (this._killed) { if (cb) cb(this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Cannot use raw on a killed socket')); return false; }
        if (cb) cb(this._throwError(ERR_CODES.ERR_NOT_OPEN, 'Socket is not open'));
        return false;
    }

    json(obj, cb) {
        if (!this.isOpen || !this._sock || this._sock.destroyed) {
            if (this._killed) { if (cb) cb(this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Cannot use json on a killed socket')); return false; }
            if (this._queueEnabled) { this._enqueue(obj); return false; }
            if (cb) cb(this._throwError(ERR_CODES.ERR_NOT_OPEN, 'Socket is not open'));
            return false;
        }

        if (this.beforeSend) {
            const result = this.beforeSend(obj);
            if (result === false) return false;
            if (result !== undefined && result !== true) obj = result;
        }

        const mask = !this.isServer;
        const frame = pack(1, JSON.stringify(obj), mask, this.fast);
        const ok = this._sock.write(frame, cb);
        if (!ok) this.saturated = true;

        if (this._statsEnabled) {
            this._msgOut++;
            this._bytesOut += frame.length;
        }

        return ok;
    }

    sendAck(data, matcher, opts = {}) {
        if (this._killed) { return Promise.reject(this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Cannot use sendAck on a killed socket')); }
        if (!this.isOpen || !this._sock || this._sock.destroyed) {
            return Promise.reject(this._throwError(ERR_CODES.ERR_NOT_OPEN, 'Cannot use sendAck: socket is not open'));
        }
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
                    return reject(this._throwError(ERR_CODES.ERR_NOT_OPEN, 'Connection lost during sendAck'));
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
        if (this._killed) { return Promise.reject(this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Cannot use expectAck on a killed socket')); }
        if (!this.isOpen || !this._sock || this._sock.destroyed) {
            return Promise.reject(this._throwError(ERR_CODES.ERR_NOT_OPEN, 'Cannot use expectAck: socket is not open'));
        }
        const matchFn = typeof matcher === 'function' ? matcher :
            typeof matcher === 'string' ? buf => buf.includes(matcher) :
                typeof matcher === 'number' ? (_, op) => op === matcher :
                    matcher instanceof RegExp ? buf => matcher.test(buf.toString()) :
                        () => true;

        return new Promise((resolve, reject) => {
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
            const mask = !this.isServer;
            if (!mask && (!payload || payload.length === 0)) {
                return this._sock.write(PING_EMPTY_UNMASKED, cb);
            }
            return this._sock.write(pack(9, payload, mask, this.fast), cb);
        }
        if (this._killed) { if (cb) cb(this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Cannot ping on a killed socket')); return false; }
        if (cb) cb(this._throwError(ERR_CODES.ERR_NOT_OPEN, 'Cannot ping: socket is not open'));
        return false;
    }

    pong(payload = EMPTY, cb) {
        if (this.isOpen && this._sock && !this._sock.destroyed) {
            const mask = !this.isServer;
            if (!mask && (!payload || payload.length === 0)) {
                return this._sock.write(PONG_EMPTY_UNMASKED, cb);
            }
            return this._sock.write(pack(10, payload, mask, this.fast), cb);
        }
        if (this._killed) { if (cb) cb(this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Cannot pong on a killed socket')); return false; }
        if (cb) cb(this._throwError(ERR_CODES.ERR_NOT_OPEN, 'Cannot pong: socket is not open'));
        return false;
    }

    reconnect() {
        if (this._killed) { this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Cannot reconnect on a killed socket'); return; }
        if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
        this._killed = false;
        this._onClose(1000);
        this.isClosed = false;
        this._key = randomBytes(16).toString('base64');
        this._expect = createHash('sha1').update(this._key + GUID).digest('base64');
        this._init();
    }

    close(code = 1000) {
        if (this._killed) { this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Socket already killed'); return; }
        if (this.isClosed && !this.isOpen) { this._throwError(ERR_CODES.ERR_ALREADY_CLOSED, 'Socket already closed'); return; }
        this._killed = true;
        if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
        this._stopPing();
        this.clearAllTimeouts();
        this._onClose(code);
    }

    kill() {
        if (this._killed) { this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Socket already killed'); return; }
        this._killed = true;
        this._setState('killed');
        if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
        if (this._rlTimer) { clearInterval(this._rlTimer); this._rlTimer = null; }
        this.clearAllTimeouts();
        this._stopPing();
        this._stopHealthCheck();
        this._onClose(1000);
    }

    _throwError(code, message) { const e = new Error(`[VOIDSOCKET:${code}] ${message}`); e.code = code; this._fire(this.error, e); return e; }

    _fire(h, a, b) { if (h == null) return; if (typeof h === 'function') return h(a, b); for (let i = 0; i < h.length; i++) h[i](a, b); }

    _evKey(ev) {
        switch (ev) {
            case 'message': case 'recv': return 'recv';
            case 'open': case 'online': return 'online';
            case 'close': case 'offline': return 'offline';
            case 'error': return 'error';
            case 'ping': return 'onPing';
            case 'flushed': case 'drain': return 'flushed';
            case 'reconnecting': return 'reconnecting';
            case 'connecting': return 'connecting';
            case 'statechange': case 'state': return 'onStateChange';
            default: return '';
        }
    }

    on(ev, fn) {
        const key = this._evKey(ev);
        if (!key) return this;
        const cur = this[key];
        if (cur == null) {
            this[key] = fn;
        } else if (typeof cur === 'function') {
            this[key] = [cur, fn];
        } else {
            cur.push(fn);
        }
        return this;
    }

    off(ev, fn) {
        const key = this._evKey(ev);
        if (!key) return this;
        const cur = this[key];
        if (cur === fn) {
            this[key] = null;
        } else if (Array.isArray(cur)) {
            const i = cur.indexOf(fn);
            if (i !== -1) {
                if (cur.length === 2) {
                    this[key] = cur[1 - i];
                } else {
                    cur.splice(i, 1);
                }
            }
        }
        return this;
    }

    once(ev, fn) {
        const wrapped = (a, b) => {
            this.off(ev, wrapped);
            fn(a, b);
        };
        return this.on(ev, wrapped);
    }

    sendPing(payload, cb) { return this.ping(payload, cb); }
    sendPong(payload, cb) { return this.pong(payload, cb); }

    sendBatch(messages, cb) {
        if (!this.isOpen || !this._sock || this._sock.destroyed) {
            if (this._killed) { if (cb) cb(this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Cannot use sendBatch on a killed socket')); return false; }
            if (this._queueEnabled) { for (const msg of messages) this._enqueue(msg); return false; }
            if (cb) cb(this._throwError(ERR_CODES.ERR_NOT_OPEN, 'Socket is not open'));
            return false;
        }

        if (this._sock.cork) this._sock.cork();
        let ok = true;
        for (const msg of messages) {
            if (!this._sendDirect(msg)) ok = false;
        }
        if (this._sock.uncork) this._sock.uncork();
        if (cb) cb(ok ? null : new Error('backpressure'));
        return ok;
    }

    broadcastTo(filter, data) {
        if (!this.isServer) { this._throwError(ERR_CODES.ERR_NOT_SERVER, 'broadcastTo can only be called on a server socket'); return false; }
        if (!this.isOpen || !this._sock || this._sock.destroyed) { if (this._killed) { this._throwError(ERR_CODES.ERR_ALREADY_KILLED, 'Cannot broadcast on a killed socket'); return false; } return false; }
        if (filter && !filter(this)) return false;
        return this._sendDirect(data);
    }

    setMeta(key, value) {
        if (typeof key === 'object') {
            Object.assign(this._meta, key);
        } else {
            this._meta[key] = value;
        }
        return this;
    }

    getMeta(key) {
        return key ? this._meta[key] : this._meta;
    }

    hasMeta(key, value) {
        if (value !== undefined) return this._meta[key] === value;
        return key in this._meta;
    }

    removeMeta(key) {
        delete this._meta[key];
        return this;
    }

    _setState(newState) {
        if (this._state === newState) return;
        const old = this._state;
        this._state = newState;
        this._fire(this.onStateChange, newState, old);
    }

    setName(name) {
        this._name = name;
        return this;
    }

    enableCompression(threshold = this._compressThreshold, level = this._compressLevel) {
        this._compressEnabled = true;
        this._compressThreshold = threshold;
        this._compressLevel = level;
        return this;
    }

    disableCompression() {
        this._compressEnabled = false;
        return this;
    }

    isCompressionEnabled() { return this._compressEnabled; }

    getCompressionStats() {
        return {
            compressionEnabled: this._compressEnabled,
            compressThreshold: this._compressThreshold,
            compressLevel: this._compressLevel,
            compressSent: this._compressSent,
            compressReceived: this._compressReceived
        };
    }

    _compressPayload(data) { if (!this._compressEnabled || data.length < this._compressThreshold) return data; try { const c = deflateRawSync(data, { level: this._compressLevel }); if (c.length + 1 < data.length) { this._compressSent++; const b = Buffer.allocUnsafe(c.length + 1); b[0] = 1; c.copy(b, 1); return b; } } catch { } return data; }

    _startHealthCheck() {
        this._stopHealthCheck();
        if (!this._healthCheck || this._healthInterval <= 0) return;
        this._healthTimer = setInterval(() => {
            if (!this.isOpen || !this._sock || this._sock.destroyed) return;
            this._lastHealthCheck = Date.now();
            if (this._awaitingPong) {
                this._healthFailures++;
                if (this._healthFailures >= this._maxHealthFailures) {
                    this._fire(this.error, new Error('health check failed'));
                    this._onClose(1006);
                    return;
                }
            }
            this._updateQuality();
        }, this._healthInterval);
        if (this._healthTimer.unref) this._healthTimer.unref();
    }

    _stopHealthCheck() {
        if (this._healthTimer) { clearInterval(this._healthTimer); this._healthTimer = null; }
    }

    _updateQuality() {
        if (this._pingCount === 0) { this._qualityScore = 100; return; }
        const avg = this._totalLatency / this._pingCount;
        const loss = this._healthFailures / Math.max(this._pingCount, 1);
        let score = 100;
        if (avg > 100) score -= 30;
        else if (avg > 50) score -= 15;
        else if (avg > 20) score -= 5;
        score -= loss * 50;
        score -= this._reconnectCount * 5;
        this._qualityScore = Math.max(0, Math.min(100, Math.round(score)));
    }

    _compactBuffer() {}

    setTimeout(fn, delay) {
        const id = ++this._msgId;
        const timer = setTimeout(() => {
            this._autoTimeouts.delete(id);
            fn();
        }, delay);
        this._autoTimeouts.set(id, timer);
        return id;
    }

    clearTimeout(id) {
        const timer = this._autoTimeouts.get(id);
        if (timer) {
            clearTimeout(timer);
            this._autoTimeouts.delete(id);
        }
    }

    clearAllTimeouts() {
        for (const [, timer] of this._autoTimeouts) clearTimeout(timer);
        this._autoTimeouts.clear();
    }

    get isConnected() { return this.isOpen; }
    get latency() { return this._lastLatency; }
    get id() { return this._msgId; }
    get uid() { return this._uid; }
    get name() { return this._name; }
    get state() { return this._state; }
    get quality() { return this._qualityScore; }
    get meta() { return this._meta; }

    get socket() { return this._sock; }
    get bufferedAmount() { return this._sock ? this._sock.writableLength : 0; }
}

export default VoidSocket;
export { VoidSocket };