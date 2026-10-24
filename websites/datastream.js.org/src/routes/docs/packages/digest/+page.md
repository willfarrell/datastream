---
title: digest
description: Compute cryptographic hash digests while streaming data.
---

Compute cryptographic hash digests while streaming data.

## Install

```bash
npm install @datastream/digest
# browser build only
npm install hash-wasm
```

## `digestStream` <span class="badge">PassThrough</span>

Computes a hash digest of all data passing through. Both builds return the stream synchronously: the browser build starts `hash-wasm` initialization when the stream is created and waits for it inside the stream. Call `.result()` only after the stream has finished (it throws otherwise); `pipeline()` handles this for you.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `algorithm` | `string` | — | Hash algorithm (see table below). The aliases `SHA256`, `SHA384` and `SHA512` are accepted and reported as `SHA2-*` |
| `resultKey` | `string` | `"digest"` | Key in pipeline result |

### Supported algorithms

| Algorithm | Node.js | Browser |
|-----------|---------|---------|
| `SHA2-256` | `node:crypto` | `hash-wasm` |
| `SHA2-384` | `node:crypto` | `hash-wasm` |
| `SHA2-512` | `node:crypto` | `hash-wasm` |
| `SHA3-256` | `node:crypto` | `hash-wasm` |
| `SHA3-384` | `node:crypto` | `hash-wasm` |
| `SHA3-512` | `node:crypto` | `hash-wasm` |

Any other value throws `Unsupported algorithm`.

### Result

```javascript
'SHA2-256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
```

### Example

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream } from '@datastream/file'
import { digestStream } from '@datastream/digest'

const digest = digestStream({ algorithm: 'SHA2-256' })

const result = await pipeline([
  await fileReadStream({ path: './data.csv' }),
  digest,
])

console.log(result)
// { digest: 'SHA2-256:e3b0c4429...' }
```
