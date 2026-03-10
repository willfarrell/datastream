---
title: digest
---

Compute cryptographic hash digests while streaming data.

## Install

```bash
npm install @datastream/digest
```

## `digestStream` <span class="badge">PassThrough</span> <span class="badge">async (browser)</span>

Computes a hash digest of all data passing through. The stream is async in the browser (returns a Promise) because it uses `hash-wasm`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `algorithm` | `string` | — | Hash algorithm (see table below) |
| `resultKey` | `string` | `"digest"` | Key in pipeline result |

### Supported algorithms

| Algorithm | Node.js | Browser |
|-----------|---------|---------|
| `SHA2-256` | `node:crypto` | `hash-wasm` |
| `SHA2-384` | `node:crypto` | `hash-wasm` |
| `SHA2-512` | `node:crypto` | `hash-wasm` |
| `SHA3-256` | — | `hash-wasm` |
| `SHA3-384` | — | `hash-wasm` |
| `SHA3-512` | — | `hash-wasm` |

### Result

```javascript
'SHA2-256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
```

### Example

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream } from '@datastream/file'
import { digestStream } from '@datastream/digest'

// Node.js — synchronous
const digest = digestStream({ algorithm: 'SHA2-256' })

const result = await pipeline([
  fileReadStream({ path: './data.csv' }),
  digest,
])

console.log(result)
// { digest: 'SHA2-256:e3b0c4429...' }
```

```javascript
// Browser — async, must await
const digest = await digestStream({ algorithm: 'SHA2-256' })
```
