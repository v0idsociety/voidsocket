// xherz
import { createHash, randomBytes, randomFillSync } from 'node:crypto';
import { connect as tlsConnect } from 'node:tls';
import { connect as netConnect } from 'node:net';
import { createServer as createHttpServer } from 'node:http';
import { createServer as createHttpsServer } from 'node:https';
import { deflateRawSync, inflateRawSync } from 'node:zlib';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const CRLF = Buffer.from([0x0D, 0x0A, 0x0D, 0x0A]);
const EMPTY = Buffer.alloc(0);
const CLOSE_FRAME = Buffer.from([0x88, 0x82, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8]);
const CLOSE_FRAME_UNMASKED = Buffer.from([0x88, 0x02, 0x03, 0xE8]);
const PONG_EMPTY = Buffer.from([0x8A, 0x80, 0x00, 0x00, 0x00, 0x00]);
const PONG_EMPTY_UNMASKED = Buffer.from([0x8A, 0x00]);
const PING_EMPTY = Buffer.from([0x89, 0x80, 0x00, 0x00, 0x00, 0x00]);
const PING_EMPTY_UNMASKED = Buffer.from([0x89, 0x00]);
const DEFAULT_CIPHERS = 'TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256';
const ERR_CODES = {
    ERR_NOT_OPEN: 10001,
    ERR_ALREADY_CLOSED: 10002,
    ERR_ALREADY_KILLED: 10003,
    ERR_NO_SOCKET: 10004,
    ERR_NOT_SERVER: 10005,
    ERR_INVALID_STATE: 10007,
    ERR_JSON_MODE: 10008,
    ERR_RATE_LIMITED: 10009,
    ERR_QUEUE_FULL: 10010,
    ERR_ALREADY_RECONNECTING: 10011,
    ERR_INVALID_URL: 10012,
    ERR_INVALID_OPTIONS: 10013,
    ERR_HANDSHAKE_FAILED: 10014,
    ERR_PROTOCOL: 10015,
};

function makeCloseFrame(code = 1000, mask = false) {
    if (code <= 0 || code === 1005) {
        return mask ? Buffer.from([0x88, 0x80, 0x00, 0x00, 0x00, 0x00]) : Buffer.from([0x88, 0x00]);
    }
    if (code === 1000) {
        return mask ? CLOSE_FRAME : CLOSE_FRAME_UNMASKED;
    }
    const b = Buffer.allocUnsafe(mask ? 8 : 4);
    b[0] = 0x88;
    b[1] = (mask ? 0x80 : 0) | 2;
    if (mask) {
        b[2] = 0; b[3] = 0; b[4] = 0; b[5] = 0;
        b[6] = (code >> 8) & 0xFF;
        b[7] = code & 0xFF;
    } else {
        b[2] = (code >> 8) & 0xFF;
        b[3] = code & 0xFF;
    }
    return b;
}

function abortHandshake(socket, code, message, headers = {}) {
    if (socket.writable) {
        let res = `HTTP/1.1 ${code} ${message}\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: ${Buffer.byteLength(message)}\r\n`;
        for (const [k, v] of Object.entries(headers)) {
            res += `${k}: ${v}\r\n`;
        }
        res += `\r\n${message}`;
        socket.end(res, () => {
            socket.destroy();
        });
    } else {
        socket.destroy();
    }
}

const POOL_SIZE = 4096;
const pool = Buffer.allocUnsafe(POOL_SIZE);
let poolOff = POOL_SIZE;

let seed = (Date.now() ^ (Math.random() * 0x7FFFFFFF)) | 0;
function fastMask32() { seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5; return seed | 0; }
function nextMask(buf, off) { if (poolOff + 4 > POOL_SIZE) { randomFillSync(pool); poolOff = 0; } const m0 = pool[poolOff++], m1 = pool[poolOff++], m2 = pool[poolOff++], m3 = pool[poolOff++]; buf[off] = m0; buf[off+1] = m1; buf[off+2] = m2; buf[off+3] = m3; return (m0 | (m1<<8) | (m2<<16) | (m3<<24)); }

const FRAME_POOL_SIZE = 64;
const _framePool2 = [], _framePool4 = [], _framePool10 = [];
for (let i = 0; i < FRAME_POOL_SIZE; i++) { _framePool2.push(Buffer.allocUnsafe(6)); _framePool4.push(Buffer.allocUnsafe(8)); _framePool10.push(Buffer.allocUnsafe(14)); }
function getHeaderBuf(hs) { if (hs===6 && _framePool2.length) return _framePool2.pop(); if (hs===8 && _framePool4.length) return _framePool4.pop(); if (hs===14 && _framePool10.length) return _framePool10.pop(); return null; }
function returnHeaderBuf(buf) { const l = buf.length; if (l===6 && _framePool2.length<FRAME_POOL_SIZE) _framePool2.push(buf); else if (l===8 && _framePool4.length<FRAME_POOL_SIZE) _framePool4.push(buf); else if (l===14 && _framePool10.length<FRAME_POOL_SIZE) _framePool10.push(buf); }

const FRAG_POOL_SIZE = 128;
const _fragPool = [];
for (let i = 0; i < FRAG_POOL_SIZE; i++) _fragPool.push({buf: Buffer.allocUnsafe(16384), len: 0});
function getFragBuf() { return _fragPool.pop() || {buf: Buffer.allocUnsafe(16384), len: 0}; }
function returnFragBuf(f) { f.len = 0; if (_fragPool.length < FRAG_POOL_SIZE) _fragPool.push(f); }

function mask32(buf, off, len, m32) { const o = buf.byteOffset + off; if ((o & 3) === 0) { const u = new Uint32Array(buf.buffer, o, len >>> 2); const e4 = u.length & ~3; for (let i = 0; i < e4; i += 4) { u[i] ^= m32; u[i+1] ^= m32; u[i+2] ^= m32; u[i+3] ^= m32; } for (let i = e4; i < u.length; i++) u[i] ^= m32; const rem = len & 3; if (rem !== 0) { const b = off + (len & ~3); buf[b] ^= m32 & 255; if (rem > 1) buf[b + 1] ^= (m32 >>> 8) & 255; if (rem > 2) buf[b + 2] ^= (m32 >>> 16) & 255; } } else { const dv = new DataView(buf.buffer, o, len); const e4 = len & ~3; for (let i = 0; i < e4; i += 4) { dv.setUint32(i, dv.getUint32(i,1)^m32,1); } for (let i = e4; i < len; i++) buf[off+i] ^= (m32 >>> ((i & 3) << 3)) & 255; } }

function parseFrames(b, onFrame) { let o = 0, l = b.length; while (l - o >= 2) { const b1 = b[o+1]; if (b1 < 126) { const t = 2 + b1; if (l - o < t) break; const b0 = b[o]; o += t; onFrame(b1 ? b.subarray(o - b1, o) : EMPTY, b0 & 15, (b0 & 128) !== 0); continue; } let p = b1 & 127, hs = 2; if (p === 126) { if (l - o < 4) break; p = (b[o+2]<<8)|b[o+3]; hs = 4; } else if (p === 127) { if (l - o < 10) break; p = b.readUInt32BE(o+6); hs = 10; } if (b1 & 128) { if (l - o < hs + 4) break; hs += 4; } const t = hs + p; if (l - o < t) break; const b0 = b[o]; const pl = p ? b.subarray(o + hs, o + t) : EMPTY; if (b1 & 128) mask32(pl, 0, p, b.readInt32LE(o + hs - 4)); o += t; onFrame(pl, b0 & 15, (b0 & 128) !== 0); } return o; }

function pack(op, data, mask = true, fast = true) {
    const isStr = typeof data === 'string';
    const isBuf = !isStr && Buffer.isBuffer(data);
    const len = isStr ? Buffer.byteLength(data) : (isBuf ? data.length : (data ? data.byteLength || 0 : 0));
    const hs = (len <= 125 ? 2 : len <= 65535 ? 4 : 10) + (mask ? 4 : 0);
    const f = Buffer.allocUnsafe(hs + len);
    f[0] = 0x80 | op;
    if (len <= 125) f[1] = (mask ? 0x80 : 0) | len;
    else if (len <= 65535) { f[1] = (mask ? 0x80 : 0) | 126; f[2] = len >> 8; f[3] = len & 255; }
    else { f[1] = (mask ? 0x80 : 0) | 127; f[2]=f[3]=f[4]=f[5]=0; f[6]=(len>>>24)&255; f[7]=(len>>16)&255; f[8]=(len>>8)&255; f[9]=len&255; }
    if (mask) { const mo = hs - 4; f.writeInt32LE(fastMask32(), mo); if (len > 0) { if (isStr) f.write(data, hs); else if (isBuf) data.copy(f, hs); else if (data) Buffer.from(data.buffer || data, data.byteOffset || 0, len).copy(f, hs); mask32(f, hs, len, f.readInt32LE(mo)); } }
    else if (len > 0) { if (isStr) f.write(data, hs); else if (isBuf) data.copy(f, hs); else if (data) Buffer.from(data.buffer || data, data.byteOffset || 0, len).copy(f, hs); }
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

export {
    createHash,
    randomBytes,
    randomFillSync,
    tlsConnect,
    netConnect,
    createHttpServer,
    createHttpsServer,
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
    abortHandshake,
    POOL_SIZE,
    pool,
    poolOff,
    seed,
    fastMask32,
    nextMask,
    FRAME_POOL_SIZE,
    _framePool2,
    _framePool4,
    _framePool10,
    getHeaderBuf,
    returnHeaderBuf,
    FRAG_POOL_SIZE,
    _fragPool,
    getFragBuf,
    returnFragBuf,
    mask32,
    parseFrames,
    pack,
    packHeader,
};
