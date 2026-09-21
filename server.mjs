// xherz
import {
    createHash,
    createHttpServer,
    createHttpsServer,
    GUID,
    abortHandshake,
    pack,
} from './shared.mjs';
import VoidSocket from './socket.mjs';

class VoidServer {
    constructor(opts = {}, cb) {
        if (typeof opts === 'function') {
            cb = opts;
            opts = {};
        }

        this.opts = opts;
        this.clientOpts = opts.clientOpts ? { ...opts.clientOpts } : {};
        if (opts.maxPayload !== undefined) this.clientOpts.maxPayload = opts.maxPayload;
        if (opts.autoPong !== undefined) this.clientOpts.autoPong = opts.autoPong;
        if (opts.autoPing !== undefined) this.clientOpts.autoPing = opts.autoPing;
        if (opts.pingInterval !== undefined) this.clientOpts.pingInterval = opts.pingInterval;
        if (opts.pingTimeout !== undefined) this.clientOpts.pingTimeout = opts.pingTimeout;
        if (opts.bufferSize !== undefined) this.clientOpts.bufferSize = opts.bufferSize;
        if (opts.json !== undefined) this.clientOpts.json = opts.json;
        if (opts.compress !== undefined) this.clientOpts.compress = opts.compress;
        if (opts.compressThreshold !== undefined) this.clientOpts.compressThreshold = opts.compressThreshold;
        if (opts.compressLevel !== undefined) this.clientOpts.compressLevel = opts.compressLevel;

        this.path = opts.path ? (opts.path.startsWith('/') ? opts.path : '/' + opts.path) : null;
        this.verifyClient = opts.verifyClient || null;
        this.selectProtocol = opts.selectProtocol || null;
        this.maxConnections = opts.maxConnections ?? Infinity;
        this.clientTracking = opts.clientTracking !== false;
        this.clients = this.clientTracking ? new Set() : null;
        this.headers = opts.headers || null;

        this.connection = null;
        this.listening = null;
        this.onClose = null;
        this.error = null;
        this.onHeaders = null;

        if (cb) this.connection = cb;

        this.server = null;
        this.isListening = false;
        this._isSelfServer = false;

        if (opts.server) {
            this.server = opts.server;
            this._attachServer(this.server);
        } else if (opts.port != null) {
            this._isSelfServer = true;
            const isTls = !!(opts.tls || opts.https);
            const tlsOpts = opts.tls || opts.https;
            if (isTls) {
                this.server = createHttpsServer(tlsOpts);
            } else {
                this.server = createHttpServer();
            }

            this._attachServer(this.server);

            this.server.listen(opts.port, opts.host, opts.backlog, () => {
                this.isListening = true;
                this._fire(this.listening);
                this.emit('listening');
            });
        }
    }

    _attachServer(server) {
        server.on('upgrade', (req, socket, head) => {
            if (this.shouldHandle(req)) {
                this.handleUpgrade(req, socket, head, (client, req) => {
                });
            } else if (this._isSelfServer) {
                abortHandshake(socket, 404, 'Not Found');
            }
        });

        server.on('error', err => {
            this._fire(this.error, err);
            this.emit('error', err);
        });

        server.on('close', () => {
            this.isListening = false;
            this._fire(this.onClose);
            this.emit('close');
        });

        if (server.listening) {
            this.isListening = true;
        } else {
            server.once('listening', () => {
                this.isListening = true;
                this._fire(this.listening);
                this.emit('listening');
            });
        }
    }

    shouldHandle(req) {
        if (!this.path) return true;
        const url = req.url || '/';
        const q = url.indexOf('?');
        const pathname = q === -1 ? url : url.substring(0, q);
        return pathname === this.path;
    }

    handleUpgrade(req, socket, head, cb) {
        if (req.method !== 'GET') {
            abortHandshake(socket, 405, 'Method Not Allowed');
            return;
        }

        const headers = req.headers;
        const upgrade = headers.upgrade;
        if (!upgrade || upgrade.toLowerCase() !== 'websocket') {
            abortHandshake(socket, 400, 'Bad Request');
            return;
        }

        const connection = headers.connection;
        if (!connection || !connection.toLowerCase().split(/,\s*/).includes('upgrade')) {
            abortHandshake(socket, 400, 'Bad Request');
            return;
        }

        const version = headers['sec-websocket-version'];
        if (version !== '13') {
            abortHandshake(socket, 426, 'Upgrade Required', { 'Sec-WebSocket-Version': '13' });
            return;
        }

        const key = headers['sec-websocket-key'];
        if (!key) {
            abortHandshake(socket, 400, 'Bad Request');
            return;
        }

        if (this.clients && this.clients.size >= this.maxConnections) {
            abortHandshake(socket, 503, 'Service Unavailable');
            return;
        }

        if (this.verifyClient) {
            const info = {
                origin: headers.origin,
                secure: !!(req.socket.authorized || req.socket.encrypted),
                req
            };

            if (this.verifyClient.length === 2) {
                this.verifyClient(info, (verified, code, message, extraHeaders) => {
                    if (!verified) {
                        abortHandshake(socket, code || 401, message || 'Unauthorized', extraHeaders);
                        return;
                    }
                    this._completeUpgrade(key, req, socket, head, cb);
                });
                return;
            }

            const res = this.verifyClient(info);
            if (res && typeof res.then === 'function') {
                res.then(verified => {
                    if (!verified) {
                        abortHandshake(socket, 401, 'Unauthorized');
                        return;
                    }
                    this._completeUpgrade(key, req, socket, head, cb);
                }).catch(() => {
                    abortHandshake(socket, 500, 'Internal Server Error');
                });
                return;
            }

            if (!res) {
                abortHandshake(socket, 401, 'Unauthorized');
                return;
            }
        }

        this._completeUpgrade(key, req, socket, head, cb);
    }

    _completeUpgrade(key, req, socket, head, cb) {
        if (socket.destroyed) return;

        let selectedProtocol = null;
        const subprotocols = req.headers['sec-websocket-protocol'];
        if (subprotocols && this.selectProtocol) {
            const list = subprotocols.split(/,\s*/);
            selectedProtocol = this.selectProtocol(list, req);
        }

        const accept = createHash('sha1').update(key + GUID).digest('base64');
        const resHeaders = [
            'HTTP/1.1 101 Switching Protocols',
            'Upgrade: websocket',
            'Connection: Upgrade',
            `Sec-WebSocket-Accept: ${accept}`
        ];

        if (selectedProtocol) {
            resHeaders.push(`Sec-WebSocket-Protocol: ${selectedProtocol}`);
        }

        if (this.headers) {
            const extra = typeof this.headers === 'function' ? this.headers(req) : this.headers;
            if (extra) {
                for (const [k, v] of Object.entries(extra)) {
                    resHeaders.push(`${k}: ${v}`);
                }
            }
        }

        if (this.onHeaders) {
            this._fire(this.onHeaders, resHeaders, req);
        }

        socket.write(resHeaders.join('\r\n') + '\r\n\r\n');

        const client = new VoidSocket(null, {
            ...this.clientOpts,
            socket,
            isServer: true,
            req,
            head
        });

        if (this.clientTracking && this.clients) {
            this.clients.add(client);
            client.on('offline', () => {
                this.clients.delete(client);
            });
        }

        if (cb) cb(client, req);
        this.emit('connection', client, req);
    }

    broadcast(data, filter) {
        if (!this.clients || this.clients.size === 0) return 0;
        const isStr = typeof data === 'string';
        const isObj = !isStr && !Buffer.isBuffer(data) && typeof data === 'object' && data !== null && !ArrayBuffer.isView(data);
        const frame = isObj ? pack(1, JSON.stringify(data), false) : pack(isStr ? 1 : 2, data, false);
        let count = 0;
        for (const client of this.clients) {
            if (client.isOpen && (!filter || filter(client))) {
                client.raw(frame);
                count++;
            }
        }
        return count;
    }

    address() {
        return this.server ? this.server.address() : null;
    }

    close(cb) {
        if (this.clients) {
            for (const client of this.clients) {
                client.close(1001);
                if (client.socket && !client.socket.destroyed) {
                    client.socket.destroy();
                }
            }
            this.clients.clear();
        }

        if (this.server && this._isSelfServer) {
            if (typeof this.server.closeAllConnections === 'function') {
                this.server.closeAllConnections();
            }
            this.server.close(err => {
                this.isListening = false;
                this._fire(this.onClose);
                this.emit('close');
                if (cb) cb(err);
            });
        } else {
            this.isListening = false;
            this._fire(this.onClose);
            this.emit('close');
            if (cb) cb();
        }
    }

    _fire(h, a, b) {
        if (h == null) return;
        if (typeof h === 'function') return h(a, b);
        for (let i = 0; i < h.length; i++) h[i](a, b);
    }

    _evKey(ev) {
        switch (ev) {
            case 'connection': return 'connection';
            case 'listening': return 'listening';
            case 'close': case 'offline': return 'onClose';
            case 'error': return 'error';
            case 'headers': return 'onHeaders';
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

    emit(ev, a, b) {
        const key = this._evKey(ev);
        if (key) this._fire(this[key], a, b);
    }
}

export default VoidServer;
export { VoidServer };
