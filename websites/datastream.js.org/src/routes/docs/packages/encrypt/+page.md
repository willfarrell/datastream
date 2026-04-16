---
title: encrypt
description: Symmetric encryption and decryption streams for AES-GCM, AES-CTR, and ChaCha20-Poly1305.
---

Symmetric encryption and decryption streams. Defaults to AES-256-GCM (authenticated encryption).

## Install

```bash
npm install @datastream/encrypt
```

For ChaCha20-Poly1305 in browser environments:

```bash
npm install libsodium-wrappers
```

## `encryptStream` <span class="badge">Transform</span> <span class="badge">async (browser)</span>

Encrypts data passing through. On Node.js, wraps `node:crypto` for true streaming. In the browser, uses Web Crypto API (AES-GCM buffers; AES-CTR streams).

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `algorithm` | `string` | `"AES-256-GCM"` | Encryption algorithm |
| `key` | `Uint8Array\|Buffer` | — | 32-byte encryption key |
| `iv` | `Uint8Array\|Buffer` | auto-generated | Initialization vector |
| `aad` | `Uint8Array\|Buffer` | — | Additional Authenticated Data (GCM/ChaCha only) |
| `maxInputSize` | `number` | `67108864` | Max input bytes for web AES-GCM (64MB) |

### Supported algorithms

| Algorithm | Auth | Node.js | Browser | IV size |
|-----------|------|---------|---------|---------|
| `AES-256-GCM` | authTag | `node:crypto` (streaming) | `crypto.subtle` (buffered) | 12 bytes |
| `AES-256-CTR` | none | `node:crypto` (streaming) | `crypto.subtle` (streaming) | 16 bytes |
| `CHACHA20-POLY1305` | authTag | `node:crypto` (streaming) | `libsodium-wrappers` (buffered) | 12 bytes |

### Result

```javascript
{
  algorithm: 'AES-256-GCM',
  iv: Uint8Array(12),
  authTag: Uint8Array(16)  // only for GCM and ChaCha20
}
```

### Example

```javascript
import { createReadableStream, pipeline } from '@datastream/core'
import { encryptStream, generateEncryptionKey } from '@datastream/encrypt'

const key = generateEncryptionKey()

// Node.js — synchronous
const enc = encryptStream({ key })

const result = await pipeline([
  createReadableStream(data),
  enc,
])

const { iv, authTag } = result.encrypt
```

```javascript
// Browser — async, must await
const enc = await encryptStream({ key })
```

## `decryptStream` <span class="badge">Transform</span> <span class="badge">async (browser)</span>

Decrypts data encrypted by `encryptStream`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `algorithm` | `string` | `"AES-256-GCM"` | Must match encryption algorithm |
| `key` | `Uint8Array\|Buffer` | — | Same key used for encryption |
| `iv` | `Uint8Array\|Buffer` | — | IV from `encryptStream` result |
| `authTag` | `Uint8Array\|Buffer` | — | Auth tag from result (GCM/ChaCha) |
| `aad` | `Uint8Array\|Buffer` | — | Must match encryption AAD |
| `maxOutputSize` | `number` | — | Max decrypted output bytes |

### Example

```javascript
import { createReadableStream, pipeline, streamToString } from '@datastream/core'
import { decryptStream } from '@datastream/encrypt'

const dec = decryptStream({ key, iv, authTag })

const plaintext = await streamToString(
  createReadableStream(ciphertext).pipe(dec)
)
```

## `generateEncryptionKey`

Generate a cryptographically random encryption key.

```javascript
import { generateEncryptionKey } from '@datastream/encrypt'

const key = generateEncryptionKey()              // 32 bytes (AES-256)
const key = generateEncryptionKey({ bits: 128 }) // 16 bytes (AES-128)
```

## Patterns

### Encrypt with plaintext and ciphertext digests

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream, fileWriteStream } from '@datastream/file'
import { encryptStream } from '@datastream/encrypt'
import { digestStream } from '@datastream/digest'

const result = await pipeline([
  fileReadStream({ path: './data.csv' }),
  digestStream({ algorithm: 'SHA2-256', resultKey: 'plaintextDigest' }),
  encryptStream({ key }),
  digestStream({ algorithm: 'SHA2-256', resultKey: 'ciphertextDigest' }),
  fileWriteStream({ path: './data.csv.enc' }),
])

// result.plaintextDigest  — verify correct decryption
// result.ciphertextDigest — verify file integrity on disk
// result.encrypt          — { algorithm, iv, authTag }
```

### Large file streaming with AES-CTR

AES-256-CTR supports true streaming on both Node.js and browser. Use it for large files where AES-GCM's buffering would cause memory issues. Pair with `digestStream` for integrity verification.

```javascript
import { encryptStream } from '@datastream/encrypt'

const enc = await encryptStream({ key, algorithm: 'AES-256-CTR' })
```

### With Additional Authenticated Data (AAD)

Bind encryption to metadata so ciphertext can't be reused in a different context.

```javascript
const aad = new TextEncoder().encode(JSON.stringify({ userId: '123' }))
const enc = encryptStream({ key, aad })
// Decryption must provide the same aad
const dec = decryptStream({ key, iv, authTag, aad })
```
