# voidsocket

Ultra-high-throughput, zero-dependency WebSocket client and server for Node.js built directly on native TLS/TCP sockets. Engineered for mission-critical streaming pipelines, financial market ingestion, automated connection resilience, kernel backpressure management, zero-copy broadcasting, and zero-allocation frame parsing.

---

## Table of Contents

- [What VoidSocket Handles Automatically](#what-voidsocket-handles-automatically)
- [Key Features](#key-features)
- [Installation](#installation)
- [Three Ways to Use (VoidSocket, v0id, vd)](#three-ways-to-use-voidsocket-v0id-vd)
- [Client Quick Start](#client-quick-start)
- [Server Quick Start (VoidServer)](#server-quick-start-voidserver)
- [Deep Dive & Production Recipes](#deep-dive--production-recipes)
  - [1. Zero-Copy Ingestion & Memory Semantics](#1-zero-copy-ingestion--memory-semantics)
  - [2. Multi-Endpoint Failover](#2-multi-endpoint-failover)
  - [3. Offline Egress Queue](#3-offline-egress-queue)
  - [4. Kernel Backpressure & Flow Control](#4-kernel-backpressure--flow-control)
  - [5. Declarative Request-Response (ACK Engine)](#5-declarative-request-response-ack-engine)
  - [6. Cork / Uncork Frame Batching](#6-cork--uncork-frame-batching)
  - [7. Client-Side Rate Limiting](#7-client-side-rate-limiting)
  - [8. Zero-Copy Multi-Client Broadcast](#8-zero-copy-multi-client-broadcast)
  - [9. Connection Telemetry & Health Metrics](#9-connection-telemetry--health-metrics)
  - [10. Low-Level Protocol Tools](#10-low-level-protocol-tools)
  - [11. Hook System (beforeSend / afterRecv)](#11-hook-system-beforesend--afterrecv)
  - [12. Event Binding (on / off / once)](#12-event-binding-on--off--once)
  - [13. Socket Properties](#13-socket-properties)
  - [14. Object Constructor](#14-object-constructor)
  - [15. Ping / Pong Events](#15-ping--pong-events)
  - [16. Express / HTTP Server Integration & Authentication](#16-express--http-server-integration--authentication)
  - [17. Performance Benchmarks](#17-performance-benchmarks)
- [Configuration Reference](#configuration-reference)
- [Server Configuration Reference](#server-configuration-reference)
- [API & Event Reference](#api--event-reference)
- [Türkçe Dokümantasyon](#türkçe-dokümantasyon)
  - [VoidSocket'in Otomatik Hallettiği Şeyler](#voidsocketin-otomatik-hallettiği-şeyler)
  - [İstemci ve Sunucu Kullanım Modelleri](#i̇stemci-ve-sunucu-kullanım-modelleri)
  - [Kurulum & İçe Aktarma](#kurulum--i̇çe-aktarma)
  - [Detaylı Kullanım Rehberi](#detaylı-kullanım-rehberi)
  - [Sunucu Kullanım Rehberi (VoidServer)](#sunucu-kullanım-rehberi-voidserver)
  - [Yapılandırma Seçenekleri](#yapılandırma-seçenekleri)
  - [Olay Yuvaları ve Metod Tablosu](#olay-yuvaları-ve-metod-tablosu)
- [License](#license)

---

## What VoidSocket Handles Automatically

The core purpose of `voidsocket` is to eliminate boilerplate. Here's everything it manages so you don't have to:

| What You'd Do Manually | What VoidSocket Does Instead |
| :--- | :--- |
| Write WebSocket frame parsing logic | **Zero-copy frame parser** — parses RFC 6455 frames automatically, `recv` gives you raw `Buffer` |
| Implement XOR masking/unmasking | **Automatic mask negotiation** — client frames are masked, server frames are unmasked, you never touch a byte |
| Handle HTTP 101 upgrade handshake | **Full handshake lifecycle** — sends the request, validates `Sec-WebSocket-Accept`, fires `online` when ready |
| Write reconnection loops with backoff | **Self-healing reconnection** — exponential backoff + jitter, configurable delays and max attempts |
| Detect zombie/dead TCP connections | **Ping/Pong heartbeat** — sends RFC 6455 pings at `pingInterval`, kills stale connections after `pingTimeout` |
| Buffer messages during disconnects | **Offline egress queue** — `send()` before `online`? Messages are queued and auto-flushed on connect |
| Monitor kernel write buffer saturation | **Backpressure detection** — `saturated` flag, `flushed` callback, `await vd.flush()` |
| Frame multiple sends into one syscall | **Cork/Uncork batching** — `vd.cork()` + `vd.uncork()` = one kernel write for N frames |
| Implement request-response correlation | **ACK engine** — `sendAck()` with matcher, timeout, and auto-retry built in |
| Rotate through backup servers | **Multi-URL failover** — pass an array of URLs, it cycles through them automatically |
| Enforce API rate limits | **Client-side rate limiter** — `{ count: 120, window: 60000 }` queues excess messages |
| Parse incoming JSON manually | **Auto JSON mode** — `{ json: true }` parses inbound and stringifies outbound automatically |
| Calculate payload sizes for frames | **Auto framing** — `send(string)` → text frame, `send(buffer)` → binary frame, `send(object)` + json → JSON text frame |
| Manage connection state flags | **State management** — `isOpen`, `isClosed`, `saturated` always reflect real-time status |
| Track message/byte statistics | **Built-in telemetry** — `vd.stats` gives `messagesIn/Out`, `bytesIn/Out`, `latency`, `uptime`, `reconnects` |
| Transform data before send/after recv | **Hook system** — `beforeSend` and `afterRecv` interceptors at the protocol boundary |
| Handle close handshakes gracefully | **RFC 6455 close** — `close(code)` sends proper close frames, `kill()` force-destroys |
| Set up TCP keepalive + nagle | **Pre-configured** — `TCP_NODELAY`, `keepAlive: true`, `keepAliveInterval: 10s` all on by default |
| Parse binary protocol headers | **Raw tools** — `pack()`, `parseFrames()`, `mask32()`, `fastMask32()` for custom protocol work |

---

## Key Features

- **Zero External Dependencies:** Built exclusively on Node.js core modules (`node:tls`, `node:net`, `node:crypto`). No `node-gyp`, no Python, no C++ compilation steps.
- **Zero-Copy Stream Parser:** Parses RFC 6455 frames with single-cycle header checks (`b1 < 126`) and exposes payload buffers via `Buffer.subarray` slices without intermediate heap allocations.
- **SIMD-Style 16-Byte Masking:** Employs an unrolled 16-byte masking loop powered by a 32-bit xorshift pseudo-random number generator (PRNG) for outbound client frames.
- **Self-Healing Reconnection:** Automated reconnect engine equipped with exponential backoff and randomized jitter to eliminate thundering herd synchronization.
- **Silent Zombie Socket Recycling:** Periodic RFC 6455 Ping/Pong heartbeats proactively detect half-open TCP connections and trigger rapid recovery.
- **Granular Flow Control (Backpressure):** Synchronous saturation flags (`saturated`), drain callbacks (`flushed`), and awaitable drain promises (`flush()`) linked to a self-contracting sliding receive buffer.
- **Offline Egress Queue:** Bounded FIFO queue retains messages emitted during transient disconnects and automatically flushes them in order upon handshake completion.
- **Declarative Request-Response Engine:** High-level `sendAck` and `expectAck` primitives coordinate message correlation, timeouts, and automated retry policies.
- **Multi-Endpoint URL Failover:** Seamlessly rotates through a pool of backup WebSocket endpoints when primary hosts are unreachable.
- **Cork/Uncork Frame Batching:** Bundles multiple sequential frame writes into a single socket system call to minimize kernel transition overhead.

---

## Installation

```bash
npm install voidsocket
```

*Requirements: Node.js 16.0.0 or higher. Compatible with Node.js 18, 20, 22, and 24+.*

---

## Three Ways to Use (VoidSocket, v0id, vd)

To reflect our identity and provide developer convenience, `voidsocket` exports three interchangeable forms:

```javascript
// 1. Standard OOP Form
import VoidSocket from 'voidsocket';
const vd = new VoidSocket('wss://stream.example.com');

// 2. Brand Identity Form (v0idsociety)
import { v0id } from 'voidsocket';
const vd = new v0id('wss://stream.example.com');

// 3. Ultra-Fast Shorthand Form
import { vd as Socket } from 'voidsocket';
const vd = new Socket('wss://stream.example.com');
```

In all code examples and production setups, the instance is referenced as `vd` (e.g. `vd.uncork()`, `vd.send()`).

---

## Import Options

`voidsocket` provides clean ES module imports for client and server instantiation alongside low-level protocol tooling:

```javascript
// Client imports
import VoidSocket, { Void, v0id, vd } from 'voidsocket';

// Server imports
import { VoidServer, VoidSocketServer, v0idServer, vdServer, Server } from 'voidsocket';
// Or as a static property:
// const { Server } = VoidSocket;

// Low-level protocol tools
import {
    pack,            // High-speed frame creator
    packHeader,      // Frame header serializer
    fastMask32,      // 32-bit xorshift key generator
    mask32,          // 16-byte unrolled XOR masking
    parseFrames,     // Zero-copy streaming frame parser
    returnHeaderBuf  // Buffer pool recycler
} from 'voidsocket';
```

---

## Client Quick Start

```javascript
import VoidSocket from 'voidsocket';

// Initialize instance as 'vd'
const vd = new VoidSocket('wss://stream.example.com/live');

vd.online = () => {
    console.log('Connected to stream');
    vd.send(JSON.stringify({ action: 'subscribe', topic: 'trades' }));
};

vd.recv = (buf, op) => {
    // buf: Zero-copy Buffer slice referencing incoming socket data
    // op: 1 = UTF-8 Text, 2 = Binary
    if (buf[0] === 0x7B) { // ASCII '{'
        const data = JSON.parse(buf);
        console.log('Received trade:', data);
    }
};

vd.reconnecting = (attempt, delayMs) => {
    console.log(`Reconnecting (attempt ${attempt}) in ${delayMs}ms`);
};

vd.offline = (code) => {
    console.log(`Disconnected, code: ${code}`);
};

vd.error = (err) => {
    console.error('Socket error:', err.message);
};
```

---

## Server Quick Start (VoidServer)

```javascript
import { VoidServer } from 'voidsocket';

// 1. Standalone High-Throughput Server
const server = new VoidServer({ port: 8080 });

server.listening = () => {
    console.log('WebSocket server listening on port 8080');
};

server.connection = (vd, req) => {
    console.log(`New client connected from ${vd.ip} (${req.url})`);

    // Inbound zero-copy frames
    vd.recv = (buf, op) => {
        // Echo message back to client (unmasked server frame)
        vd.send(`Echo: ${buf}`);
    };

    vd.offline = (code) => {
        console.log(`Client ${vd.ip} disconnected (code: ${code})`);
    };

    vd.error = (err) => {
        console.error(`Socket error from ${vd.ip}:`, err.message);
    };
};

// 2. High-Speed Broadcast to all connected clients (Framed ONCE)
setInterval(() => {
    const tick = JSON.stringify({ time: Date.now(), price: 68420.50 });
    const count = server.broadcast(tick);
    console.log(`Broadcasted market tick to ${count} active clients`);
}, 1000);
```

---

## Deep Dive & Production Recipes

### 1. Zero-Copy Ingestion & Memory Semantics

The `vd.recv` callback provides a direct `Buffer.subarray` view into the client's internal sliding receive buffer. This delivers raw throughput without allocating intermediate objects.

```javascript
vd.recv = (buf, op) => {
    // Synchronous execution: Zero heap allocations occur
    processPayloadInPlace(buf);

    // If storing across asynchronous queues or ticks:
    // Clone explicitly to prevent overwritten memory as the ring buffer advances
    const persistentCopy = Buffer.from(buf);
    asyncWorkerQueue.push(persistentCopy);
};
```

### 2. Multi-Endpoint Failover

Pass an array of endpoint URLs. If the primary host goes offline or fails TLS negotiation, `voidsocket` cascades to the next endpoint in the list:

```javascript
const vd = new VoidSocket([
    'wss://primary.region1.example.com/feed',
    'wss://secondary.region2.example.com/feed',
    'wss://backup.region3.example.com/feed'
], {
    reconnectDelay: 1000,
    maxReconnectDelay: 15000
});
```

### 3. Offline Egress Queue

Messages passed to `vd.send()` while disconnected or during the HTTP 101 upgrade phase are safely spooled in a bounded FIFO queue and delivered immediately upon connection:

```javascript
const vd = new VoidSocket('wss://stream.example.com', {
    queue: true,        // Enabled by default
    maxQueueSize: 1024  // Bounded buffer to avoid memory leaks
});

// Immediately dispatchable before online event:
vd.send(JSON.stringify({ type: 'init_sequence', step: 1 }));
vd.send(JSON.stringify({ type: 'init_sequence', step: 2 }));
```

### 4. Kernel Backpressure & Flow Control

When transmitting heavy data bursts (e.g. initial orderbook snapshots), check whether the underlying socket kernel buffer is saturated:

```javascript
// Synchronous check before transmission
if (vd.saturated) {
    // Await kernel drain before pushing additional frames
    await vd.flush();
}

vd.send(heavyOrderbookData);

// Or listen passively via property slot:
vd.flushed = () => {
    console.log('Kernel write buffer has drained completely');
};
```

### 5. Declarative Request-Response (ACK Engine)

Coordinate request-response workflows with automated matcher criteria, timeouts, and automatic retry policies:

```javascript
try {
    // Transmits query, awaits response containing '"id":4021', retries up to 3 times
    const reply = await vd.sendAck(
        JSON.stringify({ id: 4021, cmd: 'get_balance' }),
        '"id":4021',
        { timeout: 3000, retries: 3 }
    );
    console.log('Confirmed response:', reply.toString());
} catch (err) {
    console.error('Failed to receive response after retries:', err.message);
}

// Passively monitor heartbeats to identify application-level deadlocks:
vd.expectAck('"status":"ok"', 15000).catch(() => {
    console.warn('Expected application ACK missing. Recycling connection...');
    vd.reconnect();
});
```

### 6. Cork / Uncork Frame Batching

Consolidate multiple small frames into a single system write call (`socket.write`) using `vd.cork()` and `vd.uncork()` to eliminate operating system context-switching costs:

```javascript
vd.cork();
vd.send('tick_1');
vd.send('tick_2');
vd.send('tick_3');
vd.uncork(); // Emitted to the socket in a single write operation
```

### 7. Client-Side Rate Limiting

Ensure compliance with upstream API message limits without third-party token bucket libraries:

```javascript
const vd = new VoidSocket('wss://api.exchange.com/v1', {
    rateLimit: {
        count: 120,    // Maximum frames allowed
        window: 60000  // Per 60,000ms (1 minute window)
    }
});
```

### 8. Zero-Copy Multi-Client Broadcast

In high-concurrency streaming servers, re-encoding and re-masking data for hundreds or thousands of connected sockets exhausts CPU cycles. Since RFC 6455 mandates that server-to-client frames remain unmasked, `VoidServer.broadcast` serializes and frames the payload **once**, writing the identical buffer slice to all matching client sockets:

```javascript
// Broadcasts to all active clients
const totalSent = server.broadcast(JSON.stringify({ event: 'price_update', price: 95400 }));

// Or apply a selective filter:
server.broadcast(vipPayload, (client) => client.req.headers['x-tier'] === 'vip');
```

### 9. Connection Telemetry & Health Metrics

Inspect live runtime statistics and round-trip ping-pong latency at any time:

```javascript
console.log(vd.stats);
/*
{
  messagesIn: 12040,
  messagesOut: 450,
  bytesIn: 2490100,
  bytesOut: 45200,
  latency: 3,         // Round-trip ping/pong latency in ms
  uptime: 1800000,    // Connection duration in ms
  reconnects: 1,      // Total reconnect cycles
  queueSize: 0,       // Current offline queue count
  saturated: false    // Socket backpressure status
}
*/
```

### 10. Low-Level Protocol Tools

`voidsocket` exposes raw WebSocket frame primitives for custom protocol stacks, proxy servers, and testing:

```javascript
import { pack, packHeader, mask32, fastMask32, parseFrames, returnHeaderBuf } from 'voidsocket';

// pack(op, data, mask, fast) - Create a complete WebSocket frame
const textFrame = pack(1, 'Hello', true);       // Masked text frame
const binaryFrame = pack(2, buffer, true);       // Masked binary frame
const pongFrame = pack(10, Buffer.alloc(0), false); // Unmasked pong

// parseFrames(buffer, onFrame) - Zero-copy streaming frame parser
const payload = Buffer.alloc(1024);
parseFrames(payload, (frame, opcode, fin) => {
    // frame: Buffer.subarray view (zero-copy)
    // opcode: 1=text, 2=binary, 8=close, 9=ping, 10=pong
    // fin: boolean - is this the final fragment?
});

// mask32(buf, offset, length, maskKey) - XOR mask a payload in-place
const maskKey = 0x12345678;
mask32(payload, 0, payload.length, maskKey);

// fastMask32() - Generate a 32-bit xorshift pseudo-random mask key
const key = fastMask32(); // Returns signed 32-bit integer

// packHeader(op, length, mask, fast) - Serialize only the frame header
const { hdr, m32, hs } = packHeader(1, 100, true);
// hdr: Buffer containing the frame header
// m32: The mask key used
// hs: Total header size in bytes
returnHeaderBuf(hdr); // Return buffer to pool for reuse
```

### 11. Hook System (beforeSend / afterRecv)

Intercept and transform data at the protocol boundary:

```javascript
// beforeSend: Called before every outbound frame. Return false to cancel.
vd.beforeSend = (data) => {
    if (typeof data === 'string') return data.toUpperCase(); // Transform
    if (data === null) return false;                         // Block send
    return data;                                             // Pass through
};

// afterRecv: Called on every inbound frame before recv. Return false to drop.
vd.afterRecv = (buf, opcode) => {
    const decoded = decompress(buf);
    return decoded; // Return transformed buffer
    // return false; // Drop the frame entirely
};
```

### 12. Event Binding (on / off / once)

For multiple listeners on the same event, use the EventEmitter-style API:

```javascript
const handler1 = (buf) => console.log('Handler 1');
const handler2 = (buf) => console.log('Handler 2');

vd.on('message', handler1);
vd.on('message', handler2); // Both fire on each message

vd.once('message', (buf) => {
    console.log('Fires only once');
});

vd.off('message', handler1); // Remove handler1

// Supported events: message, open/online, close/offline, error, ping, flushed/drain, reconnecting
```

### 13. Socket Properties

Runtime access to internal state:

```javascript
vd.socket          // Underlying Node.js net/tls socket instance
vd.bufferedAmount  // Bytes waiting in kernel write buffer
vd.isServer        // true if this is a server-side connection
vd.isOpen          // true if WebSocket handshake is complete
vd.isClosed        // true if connection is closed
vd.saturated       // true if write buffer exceeds highWaterMark
vd.stats           // Live telemetry object (see section 9)
```

### 14. Object Constructor

Pass options without a URL (useful for dynamic reconnection):

```javascript
const vd = new VoidSocket({
    url: 'wss://feed.example.com',
    reconnect: true,
    json: true,
    queue: true
});
```

### 15. Ping / Pong Events

Monitor heartbeat timing at the application level:

```javascript
vd.onPing = (payload) => {
    console.log('Received ping, payload length:', payload.length);
    // Auto-pong is sent before this fires (if autoPong: true)
};

vd.ping(Buffer.from('app-data'), (err) => {
    if (!err) console.log('Ping sent');
});

vd.pong(Buffer.from('app-data')); // Manual pong response

// Measure latency via stats
console.log('Last RTT:', vd.stats.latency, 'ms');
```

### 16. Express / HTTP Server Integration & Authentication

Attach `VoidServer` directly to an existing `http.Server` or use `handleUpgrade` with authentication:

```javascript
import http from 'node:http';
import { VoidServer } from 'voidsocket';

const httpServer = http.createServer((req, res) => {
    res.writeHead(200);
    res.end('HTTP API Online');
});

const server = new VoidServer({
    server: httpServer,
    path: '/ws',
    verifyClient: (info) => {
        // Authenticate via token, cookie, or origin
        const token = info.req.headers['authorization'];
        return token === 'Bearer secret-token';
    }
});

server.connection = (vd, req) => {
    console.log('Authenticated client connected:', vd.ip);
    vd.recv = (buf) => vd.send(`Echo: ${buf}`);
};

httpServer.listen(8080);
```

### 17. Performance Benchmarks

Measured on Node.js v24 / Windows 11 / Intel Core i7:

| Operation | Throughput |
| :--- | :--- |
| `pack()` (1 byte text frame) | ~4.6M frames/sec |
| `pack()` (100 byte text frame) | ~2.5M frames/sec |
| `parseFrames()` (7 bytes) | ~18M frames/sec |
| `parseFrames()` (504 bytes) | ~24M frames/sec |
| `mask32()` (64 bytes) | ~1.6 GB/sec XOR |
| `mask32()` (65KB) | ~7.0 GB/sec XOR |
| Echo round-trip (single client) | ~93K msgs/sec |
| Broadcast (50 clients, 2000 msgs) | ~2K broadcasts/sec |
| 1MB payload round-trip | Success |
| 100 concurrent connections | All connected |
| 500 rapid connect/disconnect | No memory leak |

Zero-copy architecture eliminates per-message heap allocation. The `recv` callback delivers `Buffer.subarray` views directly into the internal ring buffer — no intermediate copies.

---

## Configuration Reference

### Client Configuration (VoidSocket)

```javascript
new VoidSocket(url | url[], options)
```

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `reconnect` | `boolean` | `true` | Enables automatic reconnection with exponential backoff and randomized jitter. |
| `reconnectDelay` | `number` | `500` | Initial reconnect delay in milliseconds. |
| `maxReconnectDelay` | `number` | `10000` | Maximum reconnect delay cap in milliseconds. |
| `maxReconnectAttempts` | `number` | `Infinity` | Maximum reconnect retry attempts before giving up. |
| `autoPing` | `boolean` | `true` | Automated RFC 6455 Ping heartbeats to identify half-open connections. |
| `pingInterval` | `number` | `30000` | Ping transmission interval in milliseconds. |
| `pingTimeout` | `number` | `10000` | Maximum time to await a pong response before recycling the connection. |
| `queue` | `boolean` | `true` | Enables offline message buffering during transient network disconnects. |
| `maxQueueSize` | `number` | `512` | Maximum offline queue capacity (FIFO drop on overflow). |
| `json` | `boolean` | `false` | Enables automatic JSON parsing in `recv` and auto-stringification in `send`. |
| `rateLimit` | `object` | `null` | Rate limiter configuration: `{ count: number, window: number }`. |
| `handshakeTimeout` | `number` | `5000` | HTTP 101 upgrade handshake timeout in milliseconds. |
| `noDelay` | `boolean` | `true` | Disables Nagle's algorithm (`TCP_NODELAY`). |
| `keepAlive` | `boolean` | `true` | Enables operating system TCP keep-alive probes. |
| `keepAliveInterval` | `number` | `10000` | TCP keep-alive probe interval in milliseconds. |
| `maxSendFragment` | `number` | `2048` | TLS maximum send fragment size in bytes. |
| `fastMask` | `boolean` | `true` | Uses 32-bit xorshift PRNG for client frame masking. |
| `autoPong` | `boolean` | `true` | Automatically responds to incoming server ping frames. |
| `bufferSize` | `number` | `65536` | Baseline sliding ring buffer allocation size in bytes. |
| `headers` | `object` | `null` | Custom HTTP headers to include in the upgrade handshake. |
| `servername` | `string` | `url.hostname` | TLS server name for SNI. |

---

## Server Configuration Reference

### Server Configuration (VoidServer)

```javascript
new VoidServer(options, [connectionListener])
```

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `port` | `number` | `null` | Port to listen on (automatically creates native HTTP or HTTPS server). |
| `host` | `string` | `'0.0.0.0'` | Hostname or IP interface to bind server. |
| `server` | `http.Server` | `null` | Attach to an existing Node.js HTTP or HTTPS server. |
| `noServer` | `boolean` | `false` | Prevents server binding. Upgrades are handled manually via `server.handleUpgrade()`. |
| `path` | `string` | `null` | Filters incoming upgrade requests by pathname (e.g. `'/ws'`). Rejects others. |
| `tls` / `https` | `object` | `null` | TLS options (`{ cert, key }`) to spin up secure WSS server directly. |
| `verifyClient` | `function` | `null` | Authentication hook: `(info) => boolean`, `(info, cb) => void`, or Promise. |
| `selectProtocol` | `function` | `null` | Custom subprotocol negotiation: `(protocols, req) => string`. |
| `maxConnections` | `number` | `Infinity` | Maximum concurrent connected WebSocket clients. Returns 503 if exceeded. |
| `clientTracking` | `boolean` | `true` | Tracks all active connections in `server.clients` Set. |
| `autoPing` | `boolean` | `true` | Automated per-client Ping heartbeats to recycle dead/frozen client connections. |
| `pingInterval` | `number` | `30000` | Interval between server pings in milliseconds. |
| `pingTimeout` | `number` | `10000` | Maximum duration to await pong before terminating client connection. |
| `maxPayload` | `number` | `104857600` | Maximum inbound message size in bytes (100MB default). Closes with 1009 if exceeded. |
| `maskRequired` | `boolean` | `true` | Strictly enforces RFC 6455 client masking. Rejects unmasked frames with code 1002. |
| `headers` | `object \| fn` | `null` | Additional HTTP headers to include in the `101 Switching Protocols` handshake. |

---

## API & Event Reference

### Client & Server Connection Event Slots

Every connected socket (whether on the client or returned via `server.connection`) implements the signature monomorphic event slots:

| Slot | Signature | Description |
| :--- | :--- | :--- |
| `vd.recv` | `(buf: Buffer, op: number) => void` | Invoked on incoming frame with zero-copy buffer slice. |
| `vd.online` | `() => void` | Invoked when the WebSocket handshake successfully completes. |
| `vd.offline` | `(code: number) => void` | Invoked when the socket disconnects. |
| `vd.error` | `(err: Error) => void` | Invoked on network, protocol, or handshake errors. |
| `vd.reconnecting` | `(attempt: number, delayMs: number) => void` | Invoked before initiating an auto-reconnect attempt (client-only). |
| `vd.flushed` | `() => void` | Invoked when kernel socket backpressure drains completely. |
| `vd.onPing` | `(payload: Buffer) => void` | Invoked when a ping frame is received (auto-pong fires first). |
| `vd.beforeSend` | `(data: any) => any \| false` | Outbound data interceptor. Return `false` to cancel send. |
| `vd.afterRecv` | `(buf: Buffer, op: number) => Buffer \| false` | Inbound frame interceptor. Return `false` to drop. |
| `vd.req` | `http.IncomingMessage` | HTTP Upgrade request for server-side connections. |
| `vd.ip` | `string` | Remote client IP address (`x-forwarded-for` or TCP socket remote). |

### Socket Methods

| Method | Description |
| :--- | :--- |
| `vd.send(data)` | Frame and send text, Buffer, or object data (masked for client, unmasked for server). |
| `vd.json(obj)` | Serialize object to JSON and transmit as text frame. |
| `vd.raw(buffer)` | Direct-write an already framed buffer straight to the socket. |
| `vd.sendAck(data, matcher, opts)` | Send message and resolve on matching response with retry/timeout. |
| `vd.expectAck(matcher, timeout)` | Wait for an incoming frame matching criteria (e.g. heartbeat detection). |
| `vd.ping([payload], [cb])` | Send a WebSocket ping frame. Client frames are masked; server frames are unmasked. |
| `vd.pong([payload], [cb])` | Send a WebSocket pong frame manually (auto-pong is separate). |
| `vd.flush()` | Returns a Promise that resolves when write buffer drains. |
| `vd.cork()` | Temporarily buffers outgoing writes. |
| `vd.uncork()` | Flushes all corked frames in a single socket write. |
| `vd.reconnect()` | Manually recycles the current socket and triggers reconnection (client-only). |
| `vd.close([code])` | Gracefully closes the connection with an RFC 6455 close frame (Opcode 8). |
| `vd.kill()` | Instantly destroys socket, timers, and disables auto-reconnection. |
| `vd.on(event, fn)` | Add event listener. Supported: `message`, `open`/`online`, `close`/`offline`, `error`, `ping`, `flushed`/`drain`, `reconnecting`. |
| `vd.off(event, fn)` | Remove event listener. |
| `vd.once(event, fn)` | Add one-time event listener. |

### Socket Properties

| Property | Type | Description |
| :--- | :--- | :--- |
| `vd.socket` | `net.Socket \| tls.TLSSocket` | Underlying Node.js socket instance. |
| `vd.bufferedAmount` | `number` | Bytes pending in the kernel write buffer (`socket.writableLength`). |
| `vd.isServer` | `boolean` | `true` if this is a server-side connection. |
| `vd.isOpen` | `boolean` | `true` if WebSocket handshake is complete and socket is active. |
| `vd.isClosed` | `boolean` | `true` if the connection has been closed. |
| `vd.saturated` | `boolean` | `true` if write buffer exceeds highWaterMark (backpressure active). |
| `vd.stats` | `object` | Live telemetry: `{ messagesIn, messagesOut, bytesIn, bytesOut, latency, uptime, reconnects, queueSize, saturated }`. |

### Server Event Slots & Methods (VoidServer)

| Member | Signature / Type | Description |
| :--- | :--- | :--- |
| `server.connection` | `(client: VoidSocket, req: IncomingMessage) => void` | Invoked when a new client completes the WebSocket upgrade handshake. |
| `server.listening` | `() => void` | Invoked when the underlying HTTP/HTTPS server starts listening. |
| `server.onClose` | `() => void` | Invoked when the server stops accepting connections and closes. |
| `server.error` | `(err: Error) => void` | Invoked on server network or binding errors. |
| `server.clients` | `Set<VoidSocket>` | Set of all currently active connected client instances. |
| `server.broadcast(data[, filter])` | `(data: any, filter?: fn) => number` | Zero-copy broadcasts payload to all (or filtered) clients. Frames ONCE. Returns count. |
| `server.address()` | `() => object \| string \| null` | Returns bound IP/port information from underlying server. |
| `server.close([callback])` | `(cb?: fn) => void` | Gracefully closes all client connections with 1001 and terminates HTTP server. |
| `server.handleUpgrade(req, sock, head, cb)` | `(req, sock, head, cb) => void` | Manually handles HTTP 101 upgrade on custom routers. |

### İstemci ve Sunucu Kullanım Modelleri

`voidsocket`, hem okunabilirlik hem de geliştirici alışkanlıklarına uyum sağlamak için zengin isim desteği sunar:

```javascript
// İstemci Modelleri
import VoidSocket, { v0id, vd } from 'voidsocket';
const client1 = new VoidSocket('wss://feed.example.com');
const client2 = new v0id('wss://feed.example.com');
const client3 = new vd('wss://feed.example.com');

// Sunucu Modelleri
import { VoidServer, v0idServer, vdServer, Server } from 'voidsocket';
const server1 = new VoidServer({ port: 8080 });
const server2 = new v0idServer({ port: 8080 });
const server3 = new vdServer({ port: 8080 });
// Veya VoidSocket.Server üzerinden:
// const server = new VoidSocket.Server({ port: 8080 });
```

Dokümantasyondaki tüm örneklerde nesne değişkeni olarak `vd` kullanılır (`vd.send()`, `vd.uncork()`).

---

### Kurulum & İçe Aktarma

```bash
npm install voidsocket
```

#### İçe Aktarma Seçenekleri (ESM)

```javascript
// İstemci sınıfları
import VoidSocket, { Void, v0id, vd } from 'voidsocket';

// Sunucu sınıfları
import { VoidServer, VoidSocketServer, v0idServer, vdServer, Server } from 'voidsocket';

// Düşük seviyeli protokol araçları
import {
    pack,            // Hızlı çerçeve oluşturucu
    packHeader,      // Başlık serileştirici
    fastMask32,      // 32-bit xorshift maske anahtarı üretici
    mask32,          // 16-bayt açılmış XOR maskeleme
    parseFrames,     // Sıfır kopyalama çerçeve ayrıştırıcı
    returnHeaderBuf  // Başlık tampon havuzu iadesi
} from 'voidsocket';
```

---

### Detaylı Kullanım Rehberi

#### 1. Temel Bağlantı ve Sıfır Kopyalama Mantığı

Tüm performans ayarları (`TCP_NODELAY`, `KeepAlive: 10s`, `2KB TLS Fragment`) ve yeniden bağlanma mantığı varsayılan olarak devrededir:

```javascript
import VoidSocket from 'voidsocket';

const vd = new VoidSocket('wss://feed.example.com/stream');

vd.online = () => {
    console.log('El sıkışma tamamlandı, bağlantı aktif');
    vd.send(JSON.stringify({ op: 'subscribe', channel: 'ticker' }));
};

vd.recv = (buf, op) => {
    // buf: Gelen ağ verisine işaret eden sıfır kopyalama Buffer dilimidir.
    // op: 1 = Metin (Text), 2 = İkili (Binary).
    if (buf[0] === 0x7B) { // ASCII '{'
        const veri = JSON.parse(buf);
        console.log('Alınan veri:', veri);
    }
};

vd.reconnecting = (deneme, gecikmeMs) => {
    console.log(`Yeniden bağlanılıyor (${deneme}. deneme), gecikme: ${gecikmeMs}ms`);
};

vd.offline = (kod) => {
    console.log(`Bağlantı kapandı, kod: ${kod}`);
};

vd.error = (hata) => {
    console.error('Soket hatası:', hata.message);
};
```

> **Sıfır Kopyalama Hakkında Önemli Not:** `vd.recv` içine gelen `buf`, kütüphanenin dahili halka tamponuna referans verir. Veriyi `recv` fonksiyonu içinde eşzamanlı (synchronous) olarak işliyorsanız hiçbir bellek ayırma maliyeti oluşmaz. Ancak veriyi bir dizide veya asenkron bir kuyrukta saklayacaksanız `Buffer.from(buf)` ile bir kopyasını almanız önerilir.

---

#### 2. Çoklu Uç Nokta ve Yedekli Geçiş (URL Failover)

Birincil sunucuya ulaşılamadığında veya bağlantı koptuğunda otomatik olarak listedeki diğer sunuculara sırayla geçer:

```javascript
const vd = new VoidSocket([
    'wss://birincil.sunucu.com/feed',
    'wss://ikincil.sunucu.com/feed',
    'wss://yedek.sunucu.com/feed'
], {
    reconnectDelay: 1000,
    maxReconnectDelay: 15000
});
```

---

#### 3. Çevrimdışı Mesaj Kuyruğu (Veri Kaybı Önleme)

Bağlantı henüz açılmamışken veya geçici olarak koptuğunda gönderilen mesajlar hafızadaki bounded FIFO kuyruğunda tutulur; bağlantı sağlandığı an sırayla sunucuya gönderilir:

```javascript
const vd = new VoidSocket('wss://feed.example.com', {
    queue: true,        // Varsayılan: açık
    maxQueueSize: 512   // Kuyruk sınırı (taşmada en eski mesaj çıkarılır)
});

// Bağlantı kurulmadan önce dahi güvenle çağrılabilir:
vd.send('siradaki_veri_1');
vd.send('siradaki_veri_2');
```

---

#### 4. Backpressure ve Bellek Şişmesini Önleme

Ağ çıkış kapasitesinden daha hızlı veri üretildiğinde bellek şişmesini ve Node.js OOM çökmesini engellemek için:

```javascript
// Soket yazma tamponu doymuşsa bekle
if (vd.saturated) {
    await vd.flush();
}

vd.send(buyukVeri);

// Ya da callback yuvası ile dinleyin:
vd.flushed = () => {
    console.log('Soket yazma tamponu boşaldı, yeni veriler gönderilebilir');
};
```

---

#### 5. Dahili İstek-Cevap Motoru (ACK Engine)

Sunucuya bir istek gönderip belirli bir yanıtı beklemek ve zaman aşımında otomatik tekrar denemek için:

```javascript
try {
    // Mesajı gönderir, içinde "istek_1042" geçen yanıtı 2.5 saniye bekler, gelmezse 3 kez tekrar dener
    const yanit = await vd.sendAck(
        JSON.stringify({ id: 1042, cmd: 'bakiye' }),
        '"id":1042',
        { timeout: 2500, retries: 3 }
    );
    console.log('Sunucu yanıtı:', yanit.toString());
} catch (hata) {
    console.error('Tüm denemeler sonucunda yanıt alınamadı:', hata.message);
}

// Uygulama seviyesinde kilitlenmeleri yakalamak için:
vd.expectAck('"kalp_atisi_ack"', 15000).catch(() => {
    // Beklenen onay gelmedi, bağlantı donmuş; hemen yeniden bağlan
    vd.reconnect();
});
```

---

#### 6. Toplu Yazma (Cork / Uncork ile Syscall Tasarrufu)

Arka arkaya giden küçük çerçeveleri `vd.cork()` ve `vd.uncork()` ile tek bir soket yazma çağrısında (`socket.write`) birleştirerek işletim sistemi çağrısı maliyetini düşürür:

```javascript
vd.cork();
vd.send('parca_1');
vd.send('parca_2');
vd.send('parca_3');
vd.uncork(); // Biriken çerçeveleri tek socket.write çağrısında gönderir
```

---

#### 7. Hız Sınırlayıcı (Rate Limiter)

Sunucudan `429 Too Many Requests` yanıtı almamak için çıkış trafiğini sınırlar:

```javascript
const vd = new VoidSocket('wss://api.example.com', {
    rateLimit: {
        count: 100,     // En fazla 100 mesaj
        window: 60000   // 60 saniyelik zaman penceresi
    }
});
```

---

#### 8. Canlı İstatistikler (`stats`)

```javascript
console.log(vd.stats);
/*
{
  messagesIn: 12040,
  messagesOut: 450,
  bytesIn: 2490100,
  bytesOut: 45200,
  latency: 3,          // Milisaniye cinsinden son ping-pong gecikmesi
  uptime: 1800000,     // Bağlantı süresi (ms)
  reconnects: 1,       // Toplam yeniden bağlanma sayısı
  queueSize: 0,        // Kuyrukta bekleyen çevrimdışı mesaj sayısı
  saturated: false     // Backpressure durumu
}
*/
```

---

### Sunucu Kullanım Rehberi (VoidServer)

#### 1. Temel Echo Sunucusu ve İstemci Takibi

```javascript
import { VoidServer } from 'voidsocket';

const server = new VoidServer({ port: 8080 });

server.listening = () => {
    console.log('VoidServer 8080 portunda dinlemede');
};

server.connection = (vd, req) => {
    console.log(`Yeni istemci bağlandı: ${vd.ip} (${req.url})`);

    vd.recv = (buf, op) => {
        // İstemciye maskesiz sunucu çerçevesi olarak yankıla
        vd.send(`Echo: ${buf}`);
    };

    vd.offline = (kod) => {
        console.log(`İstemci ayrıldı: ${vd.ip}, kod: ${kod}`);
    };
};
```

#### 2. Sıfır Kopyalama Çoklu Yayın (`broadcast`)

Binlerce istemciye aynı piyasa fiyatını veya bildirimi gönderirken her soket için tekrar serialize ve maskeleme maliyeti oluşmaz. Mesaj **bir kez** paketlenir ve tüm bağlı istemcilere doğrudan yazılır:

```javascript
// Tüm bağlı istemcilere anlık yayın
const gonderilen = server.broadcast(JSON.stringify({ event: 'fiyat', btc: 95400 }));
console.log(`${gonderilen} istemciye piyasa verisi iletildi`);

// Ya da özel bir filtre uygulayarak gönderin:
server.broadcast(vipVeri, (client) => client.req.headers['x-tier'] === 'vip');
```

#### 3. Express / HTTP Sunucusu Entegrasyonu ve Yetkilendirme (`verifyClient`)

```javascript
import http from 'node:http';
import { VoidServer } from 'voidsocket';

const httpServer = http.createServer((req, res) => {
    res.writeHead(200);
    res.end('Web API Calisiyor');
});

const server = new VoidServer({
    server: httpServer,
    path: '/ws',
    verifyClient: (info) => {
        // Token veya cookie kontrolü
        return info.req.headers['authorization'] === 'Bearer gizli-anahtar';
    }
});

server.connection = (vd, req) => {
    console.log('Yetkili istemci baglandi:', vd.ip);
};

httpServer.listen(8080);
```

---

### İstemci Yapılandırma Seçenekleri (VoidSocket)

| Seçenek | Tür | Varsayılan | Açıklama |
| :--- | :--- | :--- | :--- |
| `reconnect` | `boolean` | `true` | Otomatik yeniden bağlanma özelliğini açar/kapatır. |
| `reconnectDelay` | `number` | `500` | İlk yeniden bağlanma gecikmesi (milisaniye). |
| `maxReconnectDelay` | `number` | `10000` | Maksimum yeniden bağlanma bekleme süresi sınırı. |
| `maxReconnectAttempts` | `number` | `Infinity` | Vazgeçmeden önceki maksimum deneme sayısı. |
| `autoPing` | `boolean` | `true` | Donmuş TCP bağlantılarını tespit eden otomatik ping döngüsü. |
| `pingInterval` | `number` | `30000` | Ping gönderme sıklığı (milisaniye). |
| `pingTimeout` | `number` | `10000` | Pong yanıtı gelmediğinde soketin kapatılacağı süre sınırı. |
| `queue` | `boolean` | `true` | Bağlantı kopukken mesajların kuyrukta tutulmasını sağlar. |
| `maxQueueSize` | `number` | `512` | Kuyruk kapasitesi (dolduğunda en eski mesaj silinir). |
| `json` | `boolean` | `false` | `recv` fonksiyonunda otomatik JSON parse, `send`'de otomatik serialize. |
| `rateLimit` | `object` | `null` | Hız sınırlayıcı: `{ count: number, window: number }`. |
| `handshakeTimeout` | `number` | `5000` | HTTP 101 WebSocket el sıkışması zaman aşımı (ms). |
| `noDelay` | `boolean` | `true` | Nagle algoritmasını devre dışı bırakır (`TCP_NODELAY`). |
| `keepAlive` | `boolean` | `true` | İşletim sistemi seviyesinde TCP keep-alive paketlerini açar. |
| `keepAliveInterval` | `number` | `10000` | TCP keep-alive prob aralığı (milisaniye). |
| `maxSendFragment` | `number` | `2048` | TLS maksimum gönderim fragmanı boyutu (bayt). |
| `fastMask` | `boolean` | `true` | İstemci çerçeve maskelemesi için 32-bit xorshift PRNG kullanır. |
| `autoPong` | `boolean` | `true` | Gelen ping çerçevelerine otomatik pong yanıtı gönderir. |
| `bufferSize` | `number` | `65536` | Dahili kayan halka tamponun temel başlangıç boyutu (bayt). |
| `headers` | `object` | `null` | El sıkışma isteğine eklenecek özel HTTP başlıkları. |
| `servername` | `string` | `url.hostname` | TLS SNI için sunucu adı. |

---

### Sunucu Yapılandırma Seçenekleri (VoidServer)

| Seçenek | Tür | Varsayılan | Açıklama |
| :--- | :--- | :--- | :--- |
| `port` | `number` | `null` | Dinlenecek port (otomatik yerel HTTP/HTTPS sunucusu açar). |
| `host` | `string` | `'0.0.0.0'` | Bağlanılacak ağ arayüzü veya IP adresi. |
| `server` | `http.Server` | `null` | Mevcut bir Node.js HTTP veya HTTPS sunucusuna bağlanır. |
| `noServer` | `boolean` | `false` | Port dinlemez; `server.handleUpgrade()` ile manuel el sıkışma yapılır. |
| `path` | `string` | `null` | Sadece belirtilen yola (örn: `'/ws'`) gelen yükseltme isteklerini kabul eder. |
| `tls` / `https` | `object` | `null` | Güvenli WSS sunucusu başlatmak için sertifika ayarları (`{ cert, key }`). |
| `verifyClient` | `function` | `null` | Bağlantı doğrulama kancası: `(info) => boolean`, callback veya Promise. |
| `selectProtocol` | `function` | `null` | İstemci subprotocol seçim fonksiyonu: `(protocols, req) => string`. |
| `maxConnections` | `number` | `Infinity` | Eşzamanlı izin verilen maksimum bağlantı sayısı. Aşılırsa 503 döner. |
| `clientTracking` | `boolean` | `true` | Bağlı tüm istemcileri `server.clients` Set koleksiyonunda izler. |
| `autoPing` | `boolean` | `true` | Bağlı istemcilere düzenli ping atarak zombi bağlantıları kapatır. |
| `pingInterval` | `number` | `30000` | Sunucu ping gönderme aralığı (milisaniye). |
| `pingTimeout` | `number` | `10000` | Pong yanıtı gelmediğinde istemcinin kapatılacağı süre sınırı. |
| `maxPayload` | `number` | `104857600` | Maksimum gelen paket boyutu bayt (varsayılan 100MB). Aşılırsa 1009 ile kapatılır. |
| `maskRequired` | `boolean` | `true` | RFC 6455 istemci maskeleme kuralını zorunlu tutar. Maskesiz veriler 1002 ile reddedilir. |
| `headers` | `object \| fn` | `null` | `101 Switching Protocols` el sıkışmasına eklenecek ek HTTP başlıkları. |

---

### Performans Ölçümleri

Node.js v24 / Windows 11 ortamında ölçülen değerler:

| İşlem | Hız |
| :--- | :--- |
| `pack()` (1 bayt metin çerçevesi) | ~4.6M çerçeve/sn |
| `pack()` (100 bayt metin çerçevesi) | ~2.5M çerçeve/sn |
| `parseFrames()` (7 bayt) | ~18M çerçeve/sn |
| `parseFrames()` (504 bayt) | ~24M çerçeve/sn |
| `mask32()` (64 bayt) | ~1.6 GB/sn XOR |
| `mask32()` (65KB) | ~7.0 GB/sn XOR |
| Echo round-trip (tek istemci) | ~93K mesaj/sn |
| Broadcast (50 istemci, 2000 mesaj) | ~2K yayın/sn |
| 1MB payload round-trip | Başarılı |
| 100 eşzamanlı bağlantı | Tümü bağlandı |
| 500 hızlı bağlan/ayrıl | Bellek sızıntısı yok |

Sıfır kopyalama mimarisi sayesinde her mesaj için bellek ayırma maliyeti oluşmaz. `recv` callback'i dahili halka tamponu üzerindeki `Buffer.subarray` dilimlerini doğrudan iletir — ara kopyalama yoktur.

---

### Olay Yuvaları ve Metod Tablosu

#### Soket Seviyesi (İstemci ve Sunucu Bağlantıları) — Olay Yuvaları

| Olay / Yuva | Açıklama |
| :--- | :--- |
| `vd.recv = (buf, op) => {}` | Gelen mesajları sıfır kopyalama Buffer dilimi olarak teslim alır. |
| `vd.online = () => {}` | HTTP 101 WebSocket el sıkışması tamamlandığında tetiklenir. |
| `vd.reconnecting = (deneme, gecikme) => {}` | Yeniden bağlanma girişiminden önce tetiklenir (yalnızca istemci). |
| `vd.flushed = () => {}` | Soket yazma tamponu boşaldığında tetiklenir. |
| `vd.offline = (kod) => {}` | Bağlantı kapandığında tetiklenir. |
| `vd.error = (hata) => {}` | Ağ veya protokol hatalarında tetiklenir. |
| `vd.onPing = (buf) => {}` | Ping çerçevesi alındığında tetiklenir (otomatik pong zaten gönderilir). |
| `vd.beforeSend = (veri) => veri` | Gönderim öncesi veri dönüştürücüsü. `false` dönerse gönderim iptal. |
| `vd.afterRecv = (buf, op) => buf` | Alım sonrası çerçeve dönüştürücüsü. `false` dönerse çerçeve düşürülür. |
| `vd.req` | Sunucu tarafındaki bağlantılarda gelen HTTP Upgrade isteği (`IncomingMessage`). |
| `vd.ip` | İstemcinin uzak IP adresi (`x-forwarded-for` veya TCP soket IP). |

#### Soket Seviyesi — Metotlar

| Metot | Açıklama |
| :--- | :--- |
| `vd.send(veri)` | Metin, Buffer veya nesneyi çerçeveleyip gönderir (istemcide maskeli, sunucuda maskesiz). |
| `vd.json(nesne)` | JavaScript nesnesini JSON metin çerçevesi olarak gönderir. |
| `vd.raw(tampon)` | Önceden hazırlanmış çerçeveyi doğrudan sokete yazar. |
| `vd.sendAck(veri, matcher[, opts])` | Veriyi gönderir, cevabı bekler; zaman aşımında otomatik tekrar dener. |
| `vd.expectAck(matcher[, timeout])` | Belirtilen kalıpta bir gelen paketi bekler. |
| `vd.ping([payload][, cb])` | Ping çerçevesi gönderir. İstemci çerçeveleri maskelidir, sunucu maskesiz. |
| `vd.pong([payload][, cb])` | Manuel pong çerçevesi gönderir. |
| `vd.flush()` | Soket yazma tamponu tamamen boşalana kadar bekleyen Promise döner. |
| `vd.cork()` | Soket yazımlarını geçici olarak biriktirir. |
| `vd.uncork()` | Biriktirilen tüm çerçeveleri tek soket çağrısında gönderir. |
| `vd.reconnect()` | Mevcut bağlantıyı temizleyip sıfırdan yeniden bağlanma başlatır (istemci). |
| `vd.close([kod])` | Standart WebSocket kapanış çerçevesiyle (Opcode 8) bağlantıyı kapatır. |
| `vd.kill()` | Soketi, zamanlayıcıları ve otomatik yeniden bağlanmayı tamamen kapatır. |
| `vd.on(olay, fn)` | Olay dinleyicisi ekler. Desteklenen: `message`, `open`/`online`, `close`/`offline`, `error`, `ping`, `flushed`/`drain`, `reconnecting`. |
| `vd.off(olay, fn)` | Olay dinleyicisini kaldırır. |
| `vd.once(olay, fn)` | Tek seferlik olay dinleyicisi ekler. |

#### Soket Seviyesi — Özellikler

| Özellik | Tür | Açıklama |
| :--- | :--- | :--- |
| `vd.socket` | `net.Socket \| tls.TLSSocket` | Altta yatan Node.js soket örneği. |
| `vd.bufferedAmount` | `number` | Çekirdek yazma tamponundaki beklemedeki bayt sayısı. |
| `vd.isServer` | `boolean` | Sunucu tarafı bağlantısıysa `true`. |
| `vd.isOpen` | `boolean` | WebSocket el sıkışması tamamlandıysa `true`. |
| `vd.isClosed` | `boolean` | Bağlantı kapatıldıysa `true`. |
| `vd.saturated` | `boolean` | Yazma tamponu highWaterMark'ı aştıysa `true` (backpressure aktif). |
| `vd.stats` | `object` | Canlı telemetri: `{ messagesIn, messagesOut, bytesIn, bytesOut, latency, uptime, reconnects, queueSize, saturated }`. |

#### Sunucu Seviyesi (VoidServer)

| Olay / Metod | Açıklama |
| :--- | :--- |
| `server.connection = (client, req) => {}` | Yeni bir istemci el sıkışmayı tamamladığında tetiklenir. |
| `server.listening = () => {}` | Altta yatan HTTP/HTTPS sunucusu portu dinlemeye başladığında tetiklenir. |
| `server.onClose = () => {}` | Sunucu kapandığında tetiklenir. |
| `server.error = (hata) => {}` | Sunucu ağ veya port bağlama hatalarında tetiklenir. |
| `server.clients` | Aktif bağlı tüm istemcilerin `Set<VoidSocket>` koleksiyonu. |
| `server.broadcast(veri[, filtre])` | Veriyi yalnızca bir kez çerçeveleyip bağlı tüm istemcilere sıfır kopyalama ile yayınlar. |
| `server.address()` | Sunucunun bağlı olduğu IP ve port bilgisini döner. |
| `server.close([cb])` | Tüm istemcileri 1001 koduyla kapatır ve sunucuyu durdurur. |
| `server.handleUpgrade(req, sock, head, cb)` | Özel HTTP yönlendiricilerinde el sıkışmayı manuel tamamlar. |

---

## License

MIT © v0idsociety
