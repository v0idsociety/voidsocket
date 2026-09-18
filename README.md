# voidsocket

Ultra-high-throughput, zero-dependency WebSocket client for Node.js built directly on native TLS/TCP sockets. Engineered for mission-critical streaming pipelines, financial market ingestion, automated connection resilience, kernel backpressure management, and zero-allocation frame parsing.

---

## Table of Contents

- [What's New in v1.0.1 (Release Notes)](#whats-new-in-v101-release-notes)
- [Architectural Rationale](#architectural-rationale)
- [Key Features](#key-features)
- [Installation](#installation)
- [Three Ways to Use (VoidSocket, v0id, vd)](#three-ways-to-use-voidsocket-v0id-vd)
- [Quick Start](#quick-start)
- [Deep Dive & Production Recipes](#deep-dive--production-recipes)
  - [1. Zero-Copy Ingestion & Memory Semantics](#1-zero-copy-ingestion--memory-semantics)
  - [2. Multi-Endpoint Failover](#2-multi-endpoint-failover)
  - [3. Offline Egress Queue](#3-offline-egress-queue)
  - [4. Kernel Backpressure & Flow Control](#4-kernel-backpressure--flow-control)
  - [5. Declarative Request-Response (ACK Engine)](#5-declarative-request-response-ack-engine)
  - [6. Cork / Uncork Frame Batching](#6-cork--uncork-frame-batching)
  - [7. Client-Side Rate Limiting](#7-client-side-rate-limiting)
  - [8. Connection Telemetry & Health Metrics](#8-connection-telemetry--health-metrics)
- [Configuration Reference](#configuration-reference)
- [API & Event Reference](#api--event-reference)
- [Türkçe Dokümantasyon](#türkçe-dokümantasyon)
  - [v1.0.1 Sürümünde Neler Değişti?](#v101-sürümünde-neler-değişti)
  - [Mimari Tercihler ve Neden voidsocket?](#mimari-tercihler-ve-neden-voidsocket)
  - [3 Farklı Kullanım Modeli (VoidSocket, v0id, vd)](#3-farklı-kullanım-modeli-voidsocket-v0id-vd)
  - [Kurulum & İçe Aktarma](#kurulum--i̇çe-aktarma)
  - [Detaylı Kullanım Rehberi](#detaylı-kullanım-rehberi)
  - [Yapılandırma Seçenekleri](#yapılandırma-seçenekleri)
  - [Olay Yuvaları ve Metod Tablosu](#olay-yuvaları-ve-metod-tablosu)
- [License](#license)

---

## What's New in v1.0.1 (Release Notes)

* **Signature `vd` Instance & Brand Shorthand:** Replaced generic `ws` variable references with our signature `vd` identifier (e.g., `vd.uncork()`, `vd.send()`, `vd.online`). This establishes a distinct identity unique to `voidsocket` and `v0idsociety`.
* **Triple Identifier Support (`VoidSocket`, `v0id`, `vd`):** Users can import and instantiate using whichever alias best suits their codebase:
  1. `VoidSocket` – Standard, explicit class name.
  2. `v0id` – Brand identity representation of `v0idsociety`.
  3. `vd` – High-speed, compact shorthand for minimal keystrokes.
* **Streamlined Protocol Exports:** Legacy aliases (`Blitz`, `Xherz`, `BlitzSocket`) have been purged. Clean exports are provided for `VoidSocket`, `Void`, `v0id`, `vd`, alongside raw protocol utilities (`pack`, `packHeader`, `fastMask32`, `mask32`, `parseFrames`, `returnHeaderBuf`).

---

## Architectural Rationale

In 24/7 real-time streaming architectures (such as cryptocurrency orderbooks, market tickers, IoT telemetries, and event-driven trading bots), standard WebSocket abstractions often encounter severe operational limitations:

1. **Garbage Collection (GC) Thrashing:** Traditional libraries allocate event wrapper objects, EventEmitter closures, and new `Buffer` instances for every single incoming message. Under ingestion rates of 50,000+ msgs/sec, the V8 garbage collector triggers frequent stop-the-world pauses (10ms–50ms), causing unacceptable latency spikes. `voidsocket` replaces event wrappers with direct monomorphic function slots (`recv`, `online`, `offline`) and yields direct zero-copy `Buffer.subarray` slices pointing into internal sliding ring buffers.
2. **Buffer Bloat & Fatal OOM:** When upstream publishers broadcast faster than the consumer's downstream socket can transmit, in-flight buffers accumulate in process heap memory, inevitably crashing the Node.js process with Out-Of-Memory (OOM). `voidsocket` monitors OS kernel write buffer saturation and provides synchronous status indicators (`vd.saturated`) alongside asynchronous drain promises (`await vd.flush()`).
3. **Silent Half-Open TCP Freezes:** Intermediate network hardware (NAT gateways, firewalls, cloud load balancers) routinely terminate idle TCP sessions without sending TCP `FIN` or `RST` packets. Conventional sockets stay hung forever without notifying application logic. `voidsocket` features automated RFC 6455 Ping/Pong heartbeats that verify socket responsiveness and autonomously cycle dead connections.
4. **Application-Level Plumbing Overhead:** Reconnection loops, exponential backoff, jitter calculation, offline message spooling, and request-response ACK pairing typically demand hundreds of lines of boilerplate. `voidsocket` encapsulates these primitives natively with zero third-party dependencies.

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

`voidsocket` provides clean ES module imports for client instantiation and low-level protocol tooling:

```javascript
// Primary client imports
import VoidSocket, { Void, v0id, vd } from 'voidsocket';

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

## Quick Start

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

### 8. Connection Telemetry & Health Metrics

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

---

## Configuration Reference

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

---

## API & Event Reference

### Event Slots

| Slot | Signature | Description |
| :--- | :--- | :--- |
| `vd.recv` | `(buf: Buffer, op: number) => void` | Invoked on incoming frame with zero-copy buffer slice. |
| `vd.online` | `() => void` | Invoked when the WebSocket handshake successfully completes. |
| `vd.offline` | `(code: number) => void` | Invoked when the socket disconnects. |
| `vd.error` | `(err: Error) => void` | Invoked on network, protocol, or handshake errors. |
| `vd.reconnecting` | `(attempt: number, delayMs: number) => void` | Invoked before initiating an auto-reconnect attempt. |
| `vd.flushed` | `() => void` | Invoked when kernel socket backpressure drains completely. |

### Methods

| Method | Description |
| :--- | :--- |
| `vd.send(data)` | Frame, mask, and send text, Buffer, or object data. |
| `vd.json(obj)` | Serialize object to JSON and transmit as text frame. |
| `vd.raw(buffer)` | Direct-write an already framed buffer straight to the socket. |
| `vd.sendAck(data, matcher, opts)` | Send message and resolve on matching response with retry/timeout. |
| `vd.expectAck(matcher, timeout)` | Wait for an incoming frame matching criteria (e.g. heartbeat detection). |
| `vd.flush()` | Returns a Promise that resolves when write buffer drains. |
| `vd.cork()` | Temporarily buffers outgoing writes. |
| `vd.uncork()` | Flushes all corked frames in a single socket write. |
| `vd.reconnect()` | Manually recycles the current socket and triggers reconnection. |
| `vd.close(code)` | Gracefully closes the connection with an RFC 6455 close frame (Opcode 8). |
| `vd.kill()` | Instantly destroys socket, timers, and disables auto-reconnection. |

---

## Türkçe Dokümantasyon

### v1.0.1 Sürümünde Neler Değişti?

* **Bize Özgü `vd` Nesne İsmi:** Standart ve jenerik `ws` değişkeni yerine, `v0idsociety` ve `voidsocket` kimliğimizi temsil eden **`vd`** kullanımına geçildi (`vd.uncork()`, `vd.send()`, `vd.online`).
* **3 Farklı İçe Aktarma ve Kullanım Modeli (`VoidSocket`, `v0id`, `vd`):** Kullanıcıların projelerinde istedikleri ismi kullanabilmesi için kütüphaneye 3 farklı alias eklendi.
* **Protokol Export Temizliği:** Eski `Blitz`, `Xherz`, `BlitzSocket` kalıntıları tamamen temizlendi; `VoidSocket`, `v0id`, `vd` ve düşük seviyeli araçlar (`pack`, `fastMask32`, `mask32` vb.) doğrudan export edildi.

---

### Mimari Tercihler ve Neden voidsocket?

`voidsocket`, Node.js üzerinde 7/24 kesintisiz çalışması gereken, yüksek hacimli veri akışlarını tüketen sistemler için tasarlanmış, sıfır bağımlılıklı bir WebSocket istemcisidir. Kripto para ve borsa emir defteri (orderbook) beslemeleri, gerçek zamanlı telemetri sistemleri, IoT veri toplama ağ geçitleri ve olay tabanlı bot servislerinde karşılaşılan operasyonel darboğazları çözmek üzere geliştirilmiştir.

#### Çözülen Temel Problemler:
1. **Çöp Toplayıcı (GC) Baskısı ve Gecikme Dalgalanmaları:** Standart kütüphaneler her gelen mesaj için yeni olay nesneleri, EventEmitter fonksiyon sarmalayıcıları ve ara tampon kopyaları oluşturur. Saniyede on binlerce mesaj akan bir sistemde bu durum V8 çöp toplayıcısını yorar ve 10ms–50ms arası ani gecikme sıçramalarına yol açar. `voidsocket`, doğrudan tekil özellik yuvaları (`vd.recv`, `vd.online`, `vd.offline`) üzerinden çağrı yapar ve gelen veriyi doğrudan dahili tampon üzerinden `Buffer.subarray` dilimiyle sıfır bellek kopyalaması ile iletir.
2. **Kontrolsüz Bellek Büyümesi (Buffer Bloat / OOM):** Ağ çıkış hızından daha yüksek hızda veri yazılmaya çalışıldığında soket belleğinde kuyruklar birikir ve süreç bellek tükenmesiyle (`Out of Memory`) çöker. `voidsocket`, `vd.saturated`, `vd.flushed` ve `await vd.flush()` mekanizmalarıyla çekirdek tampon doluluğunu anlık izlemenize imkan tanır.
3. **Donmuş (Zombi) Bağlantılar:** Güvenlik duvarları, NAT tabloları veya bulut yük dengeleyiciler bazen `FIN/RST` paketi göndermeden TCP bağlantısını kesebilir (half-open TCP). Standart soketler bu durumu fark edemez ve saatlerce asılı kalır. `voidsocket`, arka planda düzenli WebSocket Ping/Pong çerçeveleri gönderir; `pingTimeout` süresinde yanıt gelmezse soketi otomatik geri dönüştürerek yeniden bağlanır.
4. **Kod Hamallığı:** Yeniden bağlanma (exponential backoff ve jitter), istek-cevap eşleştirme (ACK/timeout/retry), yedek sunucu geçişi ve bağlantı kopukken mesajların kaybolmaması gibi işlevler kütüphane içinde yerleşik olarak sunulur.

---

### 3 Farklı Kullanım Modeli (VoidSocket, v0id, vd)

`voidsocket`, hem okunabilirlik hem de geliştirici alışkanlıklarına uyum sağlamak için 3 farklı isim desteği sunar:

```javascript
// 1. Standart OOP Modeli
import VoidSocket from 'voidsocket';
const vd = new VoidSocket('wss://feed.example.com');

// 2. v0idsociety Ekip Kimliği Modeli
import { v0id } from 'voidsocket';
const vd = new v0id('wss://feed.example.com');

// 3. Kısa ve Hızlı Kodlama Modeli
import { vd as Socket } from 'voidsocket';
const vd = new Socket('wss://feed.example.com');
```

Dokümantasyondaki tüm örneklerde nesne değişkeni olarak `vd` kullanılır (`vd.send()`, `vd.uncork()`).

---

### Kurulum & İçe Aktarma

```bash
npm install voidsocket
```

#### İçe Aktarma Seçenekleri (ESM)

```javascript
// Ana istemci sınıfları
import VoidSocket, { Void, v0id, vd } from 'voidsocket';

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

### Yapılandırma Seçenekleri

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
| `bufferSize` | `number` | `65536` | Dahili kayan halka tamponun temel başlangıç boyutu (bayt). |

---

### Olay Yuvaları ve Metod Tablosu

| Olay / Metod | Açıklama |
| :--- | :--- |
| `vd.recv = (buf, op) => {}` | Gelen mesajları sıfır kopyalama Buffer dilimi olarak teslim alır. |
| `vd.online = () => {}` | HTTP 101 WebSocket el sıkışması tamamlandığında tetiklenir. |
| `vd.reconnecting = (deneme, gecikme) => {}` | Yeniden bağlanma girişiminden önce tetiklenir. |
| `vd.flushed = () => {}` | Soket yazma tamponu boşaldığında tetiklenir. |
| `vd.offline = (kod) => {}` | Bağlantı kapandığında tetiklenir. |
| `vd.error = (hata) => {}` | Ağ veya protokol hatalarında tetiklenir. |
| `vd.send(veri)` | Metin, Buffer veya nesneyi çerçeveleyip maskeleyerek gönderir. |
| `vd.json(nesne)` | JavaScript nesnesini JSON metin çerçevesi olarak gönderir. |
| `vd.raw(tampon)` | Önceden hazırlanmış çerçeveyi doğrudan sokete yazar. |
| `vd.sendAck(veri, matcher[, opts])` | Veriyi gönderir, cevabı bekler; zaman aşımında otomatik tekrar dener. |
| `vd.expectAck(matcher[, timeout])` | Belirtilen kalıpta bir gelen paketi bekler. |
| `vd.flush()` | Soket yazma tamponu tamamen boşalana kadar bekleyen Promise döner. |
| `vd.cork()` | Soket yazımlarını geçici olarak biriktirir. |
| `vd.uncork()` | Biriktirilen tüm çerçeveleri tek soket çağrısında gönderir. |
| `vd.reconnect()` | Mevcut bağlantıyı temizleyip sıfırdan yeniden bağlanma başlatır. |
| `vd.close([kod])` | Standart WebSocket kapanış çerçevesiyle (Opcode 8) bağlantıyı kapatır. |
| `vd.kill()` | Soketi, zamanlayıcıları ve otomatik yeniden bağlanmayı tamamen kapatır. |

---

## License

MIT © v0idsociety
