import VoidServer from '../server.mjs';
import VoidSocket from '../socket.mjs';
import { mask32 } from '../shared.mjs';

console.log('voidsocket speed test');

console.log('Masking Tests Non-multiples of 4 & Unaligned Offsets');

const testLengths = [0, 1, 2, 3, 5, 7, 8, 9, 13, 15, 63, 64, 65, 66, 67, 125, 126, 127, 255, 1000, 15363, 102401];
const testOffsets = [0, 1, 2, 3, 4, 6, 7, 8, 14, 15];
const testKeys = [0x12345678, -1, 0x5a3c9e1f, -12345678, 0x7fffffff, -2147483648];

for (const m32 of testKeys) {
  for (const off of testOffsets) {
    for (const len of testLengths) {
      const parent = Buffer.alloc(off + len + 32);
      const b = parent.subarray(off, off + len);
      const original = Buffer.alloc(len);

      for (let j = 0; j < len; j++) {
        const val = (j * 17 + 31) & 0xff;
        b[j] = val;
        original[j] = val;
      }

      mask32(b, 0, len, m32);

      for (let j = 0; j < len; j++) {
        const expected = original[j] ^ ((m32 >>> ((j & 3) << 3)) & 0xff);
        if (b[j] !== expected) {
          console.error(`Mismatch at len=${len}, off=${off}, byte=${j}`);
          process.exit(1);
        }
      }

      mask32(b, 0, len, m32);
      if (!b.equals(original)) {
        console.error(`Unmask mismatch at len=${len}, off=${off}`);
        process.exit(1);
      }
    }
  }
}

console.log('masking pass');

console.log('XOR speed testing');

const iters = 10000;
const benchKey = 0x5a3c9e1f;
const benchCases = [
  { name: '15 KB (aligned, off 8)', len: 15 * 1024, off: 8 },
  { name: '15 KB (unaligned, off 6)', len: 15 * 1024, off: 6 },
  { name: '15 KB non-mult-4 (15,363 B)', len: 15 * 1024 + 3, off: 6 },
  { name: '100 KB (unaligned WS hdr, off 14)', len: 100 * 1024, off: 14 },
  { name: '100 KB (aligned, off 8)', len: 100 * 1024, off: 8 },
  { name: '100 KB non-mult-4 (102,401 B)', len: 100 * 1024 + 1, off: 14 }
];

for (const suite of benchCases) {
  const parent = Buffer.alloc(suite.off + suite.len + 64);
  const b = parent.subarray(suite.off, suite.off + suite.len);

  for (let i = 0; i < 500; i++) mask32(b, 0, suite.len, benchKey);

  const t0 = performance.now();
  for (let i = 0; i < iters; i++) {
    mask32(b, 0, suite.len, benchKey);
  }
  const elapsed = performance.now() - t0;
  const gbSec = ((suite.len * iters) / (1024 * 1024 * 1024)) / (elapsed / 1000);

  console.log(`  ${suite.name.padEnd(36)} -> ${elapsed.toFixed(1).padStart(6)} ms (${gbSec.toFixed(2)} GB/s)`);
}
console.log();

console.log('client server integration testing');

const PORT = 29876;
const srv = new VoidServer({ port: PORT });

srv.on('connection', (client) => {
  client.on('message', (msg) => {
    client.send(msg);
  });
});

srv.on('listening', () => {
  const ws = new VoidSocket(`ws://127.0.0.1:${PORT}`);

  ws.on('open', async () => {
    const testCases = [
      'Hello',
      'A',
      '123',
      'Four',
      'Seven!!',
      Buffer.alloc(13, 0x41),
      Buffer.alloc(63, 0x42),
      Buffer.alloc(65, 0x43),
      Buffer.alloc(125, 0x44),
      Buffer.alloc(127, 0x45),
      Buffer.alloc(15363, 0x46),
      Buffer.alloc(102401, 0x47)
    ];

    let passedCount = 0;

    for (const testData of testCases) {
      const isStr = typeof testData === 'string';
      const expectedBuf = isStr ? Buffer.from(testData) : testData;

      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error(`Timeout on len ${expectedBuf.length}`)), 3000);

        const handler = (recv) => {
          clearTimeout(timeout);
          ws.off('message', handler);
          const recvBuf = Buffer.isBuffer(recv) ? recv : Buffer.from(recv);
          if (recvBuf.equals(expectedBuf)) {
            passedCount++;
            resolve();
          } else {
            reject(new Error(`Mismatch on len ${expectedBuf.length}`));
          }
        };

        ws.on('message', handler);
        ws.send(testData);
      });
    }

    console.log(` pass: ${passedCount}/${testCases.length} end-to-end payloads verified.`);
    console.log('all tests passed');

    ws.close();
    srv.close(() => {
      process.exit(0);
    });
  });

  ws.on('error', (err) => {
    console.error('Client error:', err);
    process.exit(1);
  });
});
