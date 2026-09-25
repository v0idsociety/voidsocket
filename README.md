# voidsocket

High-throughput, zero-dependency WebSocket client and server for Node.js, built directly on native TCP/TLS sockets. Designed for streaming pipelines, broadcast engines, and services that need explicit control over framing, backpressure, and reconnection — without a heavyweight API.

- **Zero dependencies.** Only Node.js core modules (`node:net`, `node:tls`, `node:crypto`, `node:http`, `node:https`, `node:zlib`).
- **Zero-copy receive path.** Incoming payloads are delivered as `Buffer.subarray` views. No per-message allocation on receive.
- **Pooled send path.** Frame buffers larger than 4 KB come from size-classed pools and are recycled on socket flush (`VOIDSKT_POOL_MIN` tunes the threshold, `VOIDSKT_NO_POOL=1` disables pooling).
- **Small-payload fast paths.** Word-size XOR masking with a byte-loop fast path for frames under 64 bytes.

---

## Table of Contents

- [What Is Voidsocket](#what-is-voidsocket)
- [Start to Finish](#start-to-finish)
- [What VoidSocket Handles Automatically](#what-voidsocket-handles-automatically)
- [Installation](#installation)
- [Imports](#imports)
- [Client Quick Start](#client-quick-start)
- [Server Quick Start](#server-quick-start-voidserver)
- [Usage Guide](#usage-guide)
  - [1. Zero-Copy Receive Semantics](#1-zero-copy-receive-semantics)
  - [2. Multi-Endpoint Failover](#2-multi-endpoint-failover)
  - [3. Offline Egress Queue](#3-offline-egress-queue)
  - [4. Backpressure and Flow Control](#4-backpressure-and-flow-control)
  - [5. Request–Response (ACK Engine)](#5-requestresponse-ack-engine)
  - [6. Cork / Uncork Batching](#6-cork--uncork-batching)
  - [7. Client-Side Rate Limiting](#7-client-side-rate-limiting)
  - [8. Broadcast](#8-broadcast)
  - [9. Telemetry](#9-telemetry)
  - [10. Hooks (beforeSend / afterRecv)](#10-hooks-beforesend--afterrecv)
  - [11. Events (on / off / once)](#11-events-on--off--once)
  - [12. Ping / Pong](#12-ping--pong)
  - [13. Compression](#13-compression)
  - [14. HTTP Server Integration and Authentication](#14-http-server-integration-and-authentication)
- [Benchmarks](#benchmarks)
- [Client Configuration](#client-configuration)
- [Server Configuration](#server-configuration)
- [API Reference](#api-reference)
- [Error Codes](#error-codes)
- [Architecture](#architecture)
- [License](#license)

---

## What Is Voidsocket

Voidsocket is a WebSocket **client and server in one package** for Node.js,
with no dependencies. It owns raw TCP/TLS sockets directly and turns the
RFC 6455 wire protocol into two things: a `recv(buf, op)` callback for
everything that arrives, and a `send(data)` call for everything you emit.
It also ships a production server (`VoidServer`) with upgrade handling,
broadcast, and per-client lifecycle.

Use it when a Node.js service must move many small messages with predictable
latency and explicit control over reconnection, backpressure, and framing.
Do not use it in browsers (use the native `WebSocket`), and do not expect
`permessage-deflate` interop — the optional compression here only talks to
itself.

## Start to Finish

### Client in four steps

**1. Install and import.**

```bash
npm install voidsocket
```

```javascript
import VoidSocket from 'voidsocket';
```

**2. Connect and set the four slots.** A socket that works needs `recv`
(data in), `online` (ready), `offline` (dropped), and `error` (diagnostics).
Without `error`, failures are silent. Without `offline`, disconnects go
unnoticed.

```javascript
const vd = new VoidSocket('wss://stream.example.com/live');
vd.recv = (buf, op) => { /* handle incoming */ };
vd.online = () => vd.send(JSON.stringify({ action: 'subscribe' }));
vd.offline = (code) => console.log('down:', code);
vd.error = (err) => console.error(err.message);
```

**3. Send at any time.** `send()` works before `online` too — messages wait
in a bounded offline queue (default 512, oldest dropped first) and flush in
order on connect. Strings go out as text, Buffers as binary, objects as
JSON text.

**4. Follow the buffer rule.** `recv` hands you a view into internal memory.
Use it inside the callback for free; `Buffer.from(buf)` anything you keep
for later. This is the single most important rule in the library.

When you are done, call `close()` (graceful) or `kill()` (immediate). Both
are terminal: to talk again, create a new instance — unless you keep the
default auto-reconnect, which redials dropped connections with backoff
and jitter on its own.

### Server in three steps

**1. Listen and handle connections.**

```javascript
import { VoidServer } from 'voidsocket';
const server = new VoidServer({ port: 8080 });
server.connection = (vd, req) => { /* per-client setup */ };
```

**2. Talk to each client.** Inside `connection`, set `recv`/`offline`/`error`
like on a client, tag sockets with `setMeta()` for grouping, and push to
many at once with `server.broadcast(data, filter?)`.

**3. Shut down cleanly.** `server.close()` sends `1001` to every client and
stops the HTTP server. Protect the endpoint with `verifyClient` (auth),
`path` (routing), and `maxConnections`/`maxPayload` caps.

### Checklist before production

- Heartbeats are on (`autoPing`, 30 s / 10 s) — turn them off only if your
  peer forbids protocol pings.
- Big bursts: `if (vd.saturated) await vd.flush()` before sending.
- API limits: `rateLimit: { count, window }` instead of getting banned.
- RPC style: `sendAck`/`expectAck` instead of hand-rolled correlation.
- Watch `vd.stats` (counters, latency, queue depth) and alert on it.

---

## What VoidSocket Handles Automatically

| Instead of writing | VoidSocket does |
| :--- | :--- |
| Frame parser | RFC 6455 parsing; `recv` gets a `Buffer` plus opcode (`1` = text, `2` = binary) |
| Masking / unmasking | Client frames are masked, server frames are verified and unmasked |
| HTTP upgrade handshake | Sends the request, validates `Sec-WebSocket-Accept` (bypass with `skipAcceptCheck`), fires `online` |
| Reconnect loops | Exponential backoff (`1.5x`, capped) plus 0–200 ms jitter |
| Dead-connection detection | Protocol ping/pong heartbeats; stale sockets are recycled |
| Offline buffering | `send()` before `online` is queued and flushed in order on connect |
| Backpressure handling | `saturated` flag, `flushed` callback, `await flush()` |
| Write batching | `cork()` / `uncork()` merge frames into fewer syscalls |
| Request–response matching | `sendAck()` / `expectAck()` with matcher, timeout, and retries |
| Backup endpoints | URL arrays with automatic rotation |
| Rate limiting | Fixed-window `{ count, window }` limiter with deferred queue |
| JSON plumbing | `json: true` parses inbound text; outbound objects are always JSON-encoded |
| State tracking | `isOpen`, `isClosed`, `saturated` reflect live state |
| Metrics | `stats` exposes counters, latency, uptime, queue depth |
| Interceptors | `beforeSend` / `afterRecv` hooks at the protocol boundary |
| Close handshake | `close(code)` sends a proper close frame; `kill()` destroys immediately |
| TCP tuning | `TCP_NODELAY`, keep-alive, and TLS fragment size preconfigured |

---

## Installation

```bash
npm install voidsocket
```

Requires Node.js 16 or higher.

---

## Imports

```javascript
// Client (all three names are the same class)
import VoidSocket, { Void, v0id, vd } from 'voidsocket';

// Server
import { VoidServer, VoidSocketServer, v0idServer, vdServer, Server } from 'voidsocket';
// Also available as VoidSocket.Server

// Low-level protocol tools (all caller-owned Buffers unless noted)
import {
    pack,            // build a complete frame (allocates)
    packInto,        // write a frame into a caller-provided buffer, returns byte length
    frameLen,        // byte length a frame will occupy (header + payload)
    packHeader,      // build only the header (pooled when possible)
    parseFrames,     // streaming zero-copy frame parser
    mask32,          // in-place XOR masking
    fastMask32,      // xorshift mask-key generator
    getFrameBuf,     // acquire a pooled frame buffer (>= requested size)
    releaseFrameBuf, // return a pooled frame buffer
    returnHeaderBuf, // return a header buffer to the pool
    ERR_CODES        // numeric error codes
} from 'voidsocket';
```

---

## Client Quick Start

```javascript
import VoidSocket from 'voidsocket';

const vd = new VoidSocket('wss://stream.example.com/live');

vd.online = () => {
    console.log('connected');
    vd.send(JSON.stringify({ action: 'subscribe', topic: 'trades' }));
};

vd.recv = (buf, op) => {
    // buf is a zero-copy view. Copy it if you keep it past this call.
    if (op === 1 && buf[0] === 0x7B) { // '{'
        console.log('trade:', JSON.parse(buf));
    }
};

vd.reconnecting = (attempt, delayMs) => {
    console.log(`reconnect #${attempt} in ${delayMs}ms`);
};

vd.offline = (code) => console.log('disconnected:', code);
vd.error = (err) => console.error('socket error:', err.message);
```

---

## Server Quick Start (VoidServer)

```javascript
import { VoidServer } from 'voidsocket';

const server = new VoidServer({ port: 8080 });

server.listening = () => console.log('listening on 8080');

server.connection = (vd, req) => {
    console.log(`client ${vd.ip} -> ${req.url}`);
    vd.recv = (buf, op) => vd.send(op === 1 ? buf.toString() : buf);
    vd.offline = (code) => console.log(`client left (${code})`);
    vd.error = (err) => console.error('client error:', err.message);
};

setInterval(() => {
    const n = server.broadcast(JSON.stringify({ time: Date.now() }));
    console.log(`tick -> ${n} clients`);
}, 1000);
```

---

## Usage Guide

### 1. Zero-Copy Receive Semantics

`recv` receives a `Buffer.subarray` view into the internal ring buffer. Process it synchronously for zero allocation. If the data must outlive the callback (queues, timers, async handlers), copy it:

```javascript
vd.recv = (buf, op) => {
    fastPath(buf);                    // zero-copy
    queue.push(Buffer.from(buf));     // explicit copy for later use
};
```

### 2. Multi-Endpoint Failover

Pass an array. On repeated reconnect failures the client rotates through the list:

```javascript
const vd = new VoidSocket([
    'wss://primary.example.com/feed',
    'wss://backup.example.com/feed'
], { reconnectDelay: 500, maxReconnectDelay: 10000 });
```

Delay grows as `reconnectDelay * 1.5^attempts` (capped) plus 0–200 ms jitter. With `adaptiveReconnect: true` the base delay also scales with connection quality.

### 3. Offline Egress Queue

Enabled by default. Messages sent while disconnected are queued and flushed in order after the handshake:

```javascript
const vd = new VoidSocket(url, { queue: true, maxQueueSize: 512 });

vd.send('a'); // works even before online
vd.send('b');
```

The queue is bounded (default 512). On overflow the **oldest** message is dropped and an `error` is emitted. Disable with `queue: false` (sends then fail via callback).

### 4. Backpressure and Flow Control

```javascript
if (vd.saturated) await vd.flush();
vd.send(bigChunk);

vd.flushed = () => console.log('write buffer drained');
```

`flush()` resolves immediately when the socket is not saturated.

### 5. Request–Response (ACK Engine)

```javascript
const reply = await vd.sendAck(
    JSON.stringify({ id: 4021, cmd: 'get_balance' }),
    '"id":4021',                 // string | RegExp | opcode number | (buf, op) => bool
    { timeout: 3000, retries: 3 } // retries = total send attempts
);
console.log(reply.toString());

// Watchdog style: recycle the connection if the app goes quiet
vd.expectAck('"status":"ok"', 15000).catch(() => vd.reconnect());
```

Pending waiters are rejected if the connection drops.

### 6. Cork / Uncork Batching

```javascript
vd.cork();
vd.send('a'); vd.send('b'); vd.send('c');
vd.uncork(); // one syscall for the batch
```

`uncork()` without `cork()` reports an error via the `error` slot; it never throws. `sendBatch([...])` corks internally.

### 7. Client-Side Rate Limiting

```javascript
const vd = new VoidSocket(url, { rateLimit: { count: 120, window: 60000 } });
```

At most 120 frames per 60 s window. Excess sends are deferred to the next window (their callbacks wait). Tokens refill on a fixed window timer.

### 8. Broadcast

```javascript
const n = server.broadcast(payload);                    // everyone
server.broadcast(payload, (c) => c.getMeta('tier') === 'vip'); // filtered
```

The payload is framed once and the same bytes go to every matching socket. Accepts strings, Buffers, typed arrays, and plain objects (JSON-encoded). Returns the recipient count. Per-socket variant: `clientSocket.broadcastTo((c) => bool, data)` (filter first).

### 9. Telemetry

```javascript
console.log(vd.stats);
// { messagesIn, messagesOut, bytesIn, bytesOut,
//   latency, minLatency, maxLatency, avgLatency, pingCount,
//   uptime, reconnects, queueSize, saturated,
//   messageId, meta, id, name, state,
//   qualityScore, healthFailures, lastHealthCheck,
//   compressionEnabled, compressSent, compressReceived }
```

Enabled by default; pass `stats: false` to skip the counters.

### 10. Hooks (beforeSend / afterRecv)

```javascript
vd.beforeSend = (data) => {
    if (data === null) return false; // cancel the send
    return data;                     // optionally transform
};

vd.afterRecv = (buf, op) => {
    if (buf.length === 0) return false; // drop the frame
    return buf;                         // optionally transform
};
```

Returning `false` from either hook cancels/drops. `afterRecv` runs before ACK matching and `recv`.

### 11. Events (on / off / once)

Single-handler slots (`vd.recv = fn`) are the fastest path. For multiple listeners:

```javascript
vd.on('message', h1);
vd.on('message', h2);
vd.once('open', init);
vd.off('message', h1);
```

Event names: `message`/`recv`, `open`/`online`, `close`/`offline`, `error`, `ping`, `flushed`/`drain`, `reconnecting`, `connecting`, `statechange`/`state`. Direct slot assignment is ~14x faster than `EventEmitter` for a single listener.

### 12. Ping / Pong

```javascript
vd.onPing = (payload) => console.log('ping,', payload.length, 'bytes');
vd.ping(Buffer.from('hi'), (err) => { /* sent */ });
vd.pong(); // manual pong (auto-pong is separate via autoPong: true)

// You can also override as a receive slot (legacy style):
vd.ping = (payload) => console.log('ping received');
```

Protocol heartbeats (`autoPing`, defaults 30 s / 10 s timeout) are independent of app-level `ping()`. Round-trip latency is visible as `vd.stats.latency`.

### 13. Compression

Opt-in, voidsocket-to-voidsocket only (a flag byte prefixes deflated payloads, which plain RFC peers do not understand):

```javascript
const vd = new VoidSocket(url, { compress: true, compressThreshold: 1024, compressLevel: 1 });
// or per socket: vd.enableCompression(); vd.disableCompression();
```

Payloads below the threshold go out untouched. See `getCompressionStats()` for counters.

### 14. HTTP Server Integration and Authentication

```javascript
import http from 'node:http';
import { VoidServer } from 'voidsocket';

const httpServer = http.createServer((req, res) => res.end('api'));
const server = new VoidServer({
    server: httpServer,
    path: '/ws', // only upgrade this pathname (query strings ignored)
    verifyClient: (info) => info.req.headers['authorization'] === 'Bearer secret'
    // verifyClient also accepts (info, cb) => void and async/promise forms.
    // selectProtocol: (list, req) => 'chat'  // subprotocol negotiation
});
httpServer.listen(8080);
```

Servers created with `port` answer plain (non-upgrade) HTTP requests with `426 Upgrade Required`. With `noServer: true` nothing is bound — call `server.handleUpgrade(req, socket, head, cb)` yourself. `maxConnections` rejects excess clients with `503`. Client IPs are available as `vd.ip` (respects `x-forwarded-for`).

---

## Benchmarks

Measured with the loopback scripts on Node.js v24 / Windows 11 / AMD Ryzen 5 5600 (best of repeated runs):

| Operation | Throughput |
| :--- | :--- |
| `pack()` 1-byte text frame | ~7.3M frames/sec |
| `pack()` 100-byte text frame | ~2.8M frames/sec |
| `parseFrames()` 7-byte frame | ~19M frames/sec |
| `parseFrames()` 500-byte frame | ~7.8M frames/sec |
| `mask32()` 64 bytes | ~1.3 GB/sec |
| `mask32()` 64 KB | ~9.5 GB/sec |
| Echo, single client, 32 B pipelined | ~280K msgs/sec |
| Broadcast, 50 clients × 2000 | ~2.2K broadcasts/sec |
| 1 MB round-trip | OK |
| 100 concurrent echo clients | OK |
| 200 rapid connect/disconnect cycles | OK, no leaks |

Same-machine comparisons: ~1.2–1.4x the `ws` package on sustained throughput; ~2.5–3x an equivalently optimized `gobwas/ws` echo setup on this box. Single-shot ping latency is comparable across all three (~60–100 µs).

---

## Client Configuration

`new VoidSocket(url | url[] | opts, opts?)` — an object may replace the URL (`{ url, ...opts }`).

| Option | Default | Description |
| :--- | :--- | :--- |
| `reconnect` | `true` | Auto-reconnect with backoff + jitter. Note: `close()`/`kill()` are terminal; make a new instance afterwards. |
| `reconnectDelay` | `500` | Base delay in ms. |
| `maxReconnectDelay` | `10000` | Backoff cap in ms. |
| `maxReconnectAttempts` | `Infinity` | Give up after N attempts. |
| `adaptiveReconnect` | `false` | Scale base delay by connection quality. |
| `autoPing` | `true` | Protocol heartbeat to detect dead sockets. |
| `pingInterval` | `30000` | Heartbeat interval in ms. |
| `pingTimeout` | `10000` | recycle the socket if no pong arrives in time. |
| `queue` | `true` | Buffer sends while offline. |
| `maxQueueSize` | `512` | Bound; oldest dropped first (with an `error`). |
| `json` | `false` | Parse inbound text frames as JSON before `recv`. (Outbound objects are always JSON-encoded.) |
| `rateLimit` | `null` | `{ count, window }` fixed-window limiter; excess deferred. |
| `handshakeTimeout` | `5000` | Upgrade timeout in ms (`connectTimeout` is an alias). |
| `skipAcceptCheck` | `false` | Skip `Sec-WebSocket-Accept` validation (testing/proxies). |
| `noDelay` | `true` | `TCP_NODELAY`. |
| `keepAlive` | `true` | OS TCP keep-alive. |
| `keepAliveInterval` | `10000` | Keep-alive probe interval in ms. |
| `maxSendFragment` | `2048` | TLS fragment size. |
| `fastMask` | `true` | xorshift masking (disable only for testing). |
| `autoPong` | `true` | Auto-reply to pings. |
| `bufferSize` | `65536` | Initial ring-buffer size; grows as needed. |
| `highWaterMark` | `2 GB` | Ring-buffer growth cap. |
| `maxPayload` | `1 GB` | Inbound message cap; exceeded closes with `1009`. |
| `maxFragments` | `0` | Fragment-count cap (`0` = unlimited; payload cap still applies). |
| `stats` | `true` | Message/byte counters. |
| `compress` | `false` | Flag-byte compression for voidsocket peers. |
| `compressThreshold` | `1024` | Minimum bytes before compressing. |
| `compressLevel` | `1` | zlib level. |
| `healthCheck` | `false` | Extra liveness supervision. |
| `healthInterval` | `30000` | Supervision tick. |
| `maxHealthFailures` | `5` | Failures before recycling. |
| `headers` | `null` | Extra handshake headers. |
| `protocols` | `null` | `Sec-WebSocket-Protocol` value(s). |
| `servername` | hostname | TLS SNI. |
| `host` / `port` | URL's | Dial overrides. |
| `createSocket` | `null` | Custom socket factory `(url) => socket`. |
| TLS opts | — | `rejectUnauthorized` (default `true`), `minVersion`, `maxVersion`, `ciphers`, `ALPNProtocols`, `secureContext`, `session`, `tls` |

Constructor-time URL problems (`Invalid URL`, bad protocol, missing URL) are delivered asynchronously through the `error` slot so handlers attached right after `new` still observe them.

Environment tuning: `VOIDSKT_NO_POOL=1` disables send-buffer pooling; `VOIDSKT_POOL_MIN=<bytes>` (default `4096`) sets the frame size above which pooled buffers are used.

---

## Server Configuration

`new VoidServer(opts, connectionListener?)`

| Option | Default | Description |
| :--- | :--- | :--- |
| `port` | `null` | Listen port; spins up an internal HTTP(S) server. Invalid numbers throw. |
| `host` | all interfaces | Bind address. |
| `backlog` | Node default | Listen backlog. |
| `server` | `null` | Attach to an existing HTTP(S) server. |
| `tls` / `https` | `null` | `{ key, cert, ... }` for a native WSS server. |
| `noServer` | `false` | Do not bind; upgrade manually via `handleUpgrade()`. |
| `path` | `null` | Accept upgrades only for this pathname (else `404`). |
| `verifyClient` | `null` | `(info) => bool` \| `(info, cb) => void` \| async. Rejects with `401` (`500` on error). |
| `selectProtocol` | `null` | `(list, req) => protocol`. |
| `maxConnections` | `Infinity` | Excess clients get `503`. |
| `clientTracking` | `true` | Keep `server.clients`; `false` sets it to `null` (broadcasts send to nobody). |
| `headers` | `null` | Extra `101` response headers (object or `(req) => object`). |
| `clientOpts` | `{}` | Base options merged into every accepted socket, plus: |
| `clientOpts.maxPayload` … | — | `maxPayload`, `autoPong`, `autoPing`, `pingInterval`, `pingTimeout`, `bufferSize`, `json`, `compress`, `compressThreshold`, `compressLevel` may also be set top-level and are forwarded. |

Slots: `connection(client, req)`, `listening()`, `onClose()`, `error(err)`, `onHeaders(resHeaders, req)` — plus `on`/`off`/`once`/`emit` (`connection`, `listening`, `close`/`offline`, `error`, `headers`).

---

## API Reference

### Connection slots

| Slot | Signature | Notes |
| :--- | :--- | :--- |
| `recv` | `(buf, op) => void` | Zero-copy view. `op`: `1` text, `2` binary. |
| `online` | `() => void` | Handshake complete. |
| `offline` | `(code) => void` | Disconnected. Codes follow RFC 6455 (`1006` abnormal). |
| `error` | `(err) => void` | Network/protocol/config errors, including `ERR_*`-coded operational errors. |
| `reconnecting` | `(attempt, delayMs) => void` | Client only. |
| `connecting` | `() => void` | Dial started. |
| `onStateChange` | `(next, prev) => void` | `idle` → `connecting` → `connected` → `disconnected` / `reconnecting` / `killed`. |
| `flushed` | `() => void` | Write buffer drained. |
| `onPing` | `(payload) => void` | Ping received (auto-pong already sent). |
| `beforeSend` | `(data) => data \| false` | `false` cancels the send. |
| `afterRecv` | `(buf, op) => buf \| false` | `false` drops the frame. |
| `req` / `ip` / `url` | — | Server-side upgrade request, remote IP, parsed URL. |

### Connection methods

| Method | Notes |
| :--- | :--- |
| `send(data, cb?)` | String → text; Buffer → binary; object → JSON text; typed array → binary. Queues offline, defers under rate limit. Returns `false` when not sent. |
| `raw(buffer, cb?)` | Writes bytes as-is. No framing — only pass complete frames. |
| `json(obj, cb?)` | JSON text frame. |
| `sendAck(data, matcher, { timeout?, retries? }?)` | Resolves with the first matching inbound payload. `retries` counts total attempts. |
| `expectAck(matcher, timeout?)` | Resolves on the next matching inbound payload. |
| `ping(payload?, cb?)` / `pong(payload?, cb?)` (`sendPing`/`sendPong` aliases) | Control frames; client side masked. |
| `cork()` / `uncork()` / `sendBatch(msgs, cb?)` | Batching. |
| `flush()` | Resolves on drain (immediately if not saturated). |
| `reconnect()` | Drop and redial the current URL (no-op once killed). |
| `close(code?)` | Graceful close frame, then teardown. Terminal. |
| `kill()` | Immediate destroy, no close frame. Terminal. |
| `broadcastTo(filter, data)` | Server-side sockets: send if `filter(this)` passes. |
| `setMeta(k, v?)` / `getMeta(k?)` / `hasMeta(k, v?)` / `removeMeta(k)` / `setName(n)` | Per-socket metadata (handy for broadcast filters). |
| `enableCompression(t?, l?)` / `disableCompression()` / `isCompressionEnabled()` / `getCompressionStats()` | Opt-in compression. |
| `setAutoTimeout(fn, ms)` / `clearAutoTimeout(id)` / `clearAllTimeouts()` | Managed timers (cleared on close). `setTimeout`/`clearTimeout` are aliases. |
| `on/off/once` | Multi-listener events (see list above). |

Failures never throw for I/O state (closed socket, killed socket, uncorked `uncork`): the method returns `false` and reports through `error` with an `ERR_*` code.

### Connection properties

`socket`, `bufferedAmount`, `isServer`, `isOpen`, `isConnected` (alias), `isClosed`, `saturated`, `stats`, `latency`, `id`, `uid`, `name`, `state`, `quality`, `meta`.

### Server members

`server.clients: Set`, `server.broadcast(data, filter?) → count`, `server.address()`, `server.close(cb?)` (sends `1001` to clients), `server.handleUpgrade(req, socket, head, cb)`, `server.shouldHandle(req)`, `server.isListening`.

---

## Error Codes

`ERR_NOT_OPEN (10001)`, `ERR_ALREADY_CLOSED (10002)`, `ERR_ALREADY_KILLED (10003)`, `ERR_NO_SOCKET (10004)`, `ERR_NOT_SERVER (10005)`, `ERR_INVALID_STATE (10007)`, `ERR_JSON_MODE (10008)`, `ERR_RATE_LIMITED (10009)`, `ERR_QUEUE_FULL (10010)`, `ERR_ALREADY_RECONNECTING (10011)`, `ERR_INVALID_URL (10012)`, `ERR_INVALID_OPTIONS (10013)`, `ERR_HANDSHAKE_FAILED (10014)`, `ERR_PROTOCOL (10015)`.

---

## Architecture

```
index.mjs          public surface (client/server aliases, tools)
server.mjs         VoidServer (upgrade, broadcast, lifecycle)
socket.mjs         re-export shim
shared.mjs         protocol primitives re-exported from lib/
lib/constants      GUID, opcodes, close/ping frames, error codes
lib/masking        xorshift keys, 32-bit and small-size XOR paths
lib/frame          pack / packInto / frameLen / packHeader / parseFrames
lib/buffer-pool    header pool, fragment pool, size-classed frame pool
lib/handshake      close frames, handshake aborts, accept keys
lib/socket/        VoidSocket split by concern:
  void-socket      constructor, handshake init, stats, getters
  receive          streaming parser, ring buffer, handshake check
  send             send/raw/json/batch/queue/cork/flush, pooled writes
  lifecycle        close paths, reconnect engine, kill
  ack              matchers, sendAck/expectAck
  ping             heartbeats, latency, health, quality
  compression · rate-limit · events · meta · timers · ring-queue
```

Hot-path rules the code follows: no per-message heap allocation on receive (views only), pooled buffers for large sends with release-on-flush, small frames via slab allocation, single syscall per frame, direct slot calls instead of an event emitter.

---

## License

MIT © v0idsociety
