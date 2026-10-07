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

## `encryptStream` <span class="badge">Transform</span> <span class="badge">async</span>

Encrypts data passing through. On Node.js, wraps `node:crypto` for true streaming. In the browser, uses Web Crypto API (AES-GCM buffers; AES-CTR streams). `encryptStream` returns a Promise in both builds, so `await` it. Invalid options reject the Promise.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `algorithm` | `string` | `"AES-256-GCM"` | Encryption algorithm |
| `key` | `Uint8Array\|Buffer` | — | Encryption key: 32 bytes for `AES-256-*` and ChaCha20, 16 bytes for `AES-128-*`. Strings and `ArrayBuffer`s are rejected |
| `iv` | `Uint8Array\|Buffer` | auto-generated | Initialization vector. Strings and `ArrayBuffer`s are rejected |
| `aad` | `Uint8Array\|Buffer\|null` | — | Additional Authenticated Data (GCM/ChaCha only; rejects for CTR) |
| `maxInputSize` | `number\|null` | `67108864` (64MB) for GCM/ChaCha20; no limit for CTR | Max plaintext bytes. `null` means no limit. Applies in both Node.js and the browser. The stream errors with a `RangeError` when exceeded |
| `resultKey` | `string` | `"encrypt"` | Key in pipeline result |

### Supported algorithms

| Algorithm | Auth | Node.js | Browser | IV size |
|-----------|------|---------|---------|---------|
| `AES-256-GCM` | authTag | `node:crypto` (streaming) | `crypto.subtle` (buffered) | 12 bytes |
| `AES-128-GCM` | authTag | `node:crypto` (streaming) | `crypto.subtle` (buffered) | 12 bytes |
| `AES-256-CTR` | none | `node:crypto` (streaming) | `crypto.subtle` (streaming) | 16 bytes |
| `AES-128-CTR` | none | `node:crypto` (streaming) | `crypto.subtle` (streaming) | 16 bytes |
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
import { createReadableStream, pipejoin, streamToArray } from '@datastream/core'
import { encryptStream, generateEncryptionKey } from '@datastream/encrypt'

const key = generateEncryptionKey()

const enc = await encryptStream({ key })

const ciphertext = await streamToArray(
  pipejoin([createReadableStream('some plaintext'), enc]),
)

const { iv, authTag } = enc.result().value
```

## `decryptStream` <span class="badge">Transform</span> <span class="badge">async</span>

Decrypts data encrypted by `encryptStream`. For the authenticated algorithms (GCM and ChaCha20-Poly1305) both builds buffer the whole ciphertext and only release plaintext after the auth tag has been verified, so `maxInputSize` bounds that buffer. AES-CTR decrypts as it streams.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `algorithm` | `string` | `"AES-256-GCM"` | Must match encryption algorithm |
| `key` | `Uint8Array\|Buffer` | — | Same key used for encryption |
| `iv` | `Uint8Array\|Buffer` | — | IV from `encryptStream` result |
| `authTag` | `Uint8Array\|Buffer` | — | Auth tag from result (GCM/ChaCha) |
| `aad` | `Uint8Array\|Buffer\|null` | — | Must match encryption AAD |
| `maxInputSize` | `number\|null` | `67108864` (64MB) | Max ciphertext bytes buffered for GCM/ChaCha20. `null` means no limit. Not used for CTR. Exceeding it errors with a `RangeError` |
| `maxOutputSize` | `number\|null` | none | Max decrypted output bytes. Exceeding it errors with a `RangeError` |

### Example

Continuing from the `encryptStream` example:

```javascript
import { createReadableStream, pipejoin, streamToString } from '@datastream/core'
import { decryptStream } from '@datastream/encrypt'

const dec = await decryptStream({ key, iv, authTag })

const plaintext = await streamToString(
  pipejoin([createReadableStream(ciphertext), dec]),
)
// 'some plaintext'
```

## `generateEncryptionKey`

Generate a cryptographically random encryption key.

```javascript
import { generateEncryptionKey } from '@datastream/encrypt'

const key256 = generateEncryptionKey()              // 32 bytes (AES-256, ChaCha20)
const key128 = generateEncryptionKey({ bits: 128 }) // 16 bytes (AES-128)
```

## Patterns

### Encrypt with plaintext and ciphertext digests

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream, fileWriteStream } from '@datastream/file'
import { encryptStream, generateEncryptionKey } from '@datastream/encrypt'
import { digestStream } from '@datastream/digest'

const key = generateEncryptionKey()

// Node.js. The default AES-256-GCM rejects input over 64MB (maxInputSize);
// for larger files raise maxInputSize or encrypt them as separate smaller messages.
const result = await pipeline([
  await fileReadStream({ path: './data.csv' }),
  digestStream({ algorithm: 'SHA2-256', resultKey: 'plaintextDigest' }),
  await encryptStream({ key }),
  digestStream({ algorithm: 'SHA2-256', resultKey: 'ciphertextDigest' }),
  await fileWriteStream({ path: './data.csv.enc' }),
])

// result.plaintextDigest  — verify correct decryption
// result.ciphertextDigest — verify file integrity on disk
// result.encrypt          — { algorithm, iv, authTag }
```

### Large file streaming with AES-CTR

AES-256-CTR supports true streaming on both Node.js and browser, and has no default `maxInputSize`. Use it for large files: AES-GCM and ChaCha20 cap input at 64MB by default, and their decryption (and browser encryption) buffers the whole payload. CTR is not authenticated: anyone who can modify the ciphertext can flip plaintext bits undetected. Prefer an authenticated mode where you can, and if you use CTR, verify integrity separately (for example a keyed MAC; a plain `digestStream` hash only catches accidental corruption).

```javascript
import { encryptStream } from '@datastream/encrypt'

const enc = await encryptStream({ key, algorithm: 'AES-256-CTR' })
```

### With Additional Authenticated Data (AAD)

Bind encryption to metadata so ciphertext can't be reused in a different context.

```javascript
const aad = new TextEncoder().encode(JSON.stringify({ userId: '123' }))
const enc = await encryptStream({ key, aad })
// ...after the stream finishes, take iv and authTag from enc.result().value
// Decryption must provide the same aad
const dec = await decryptStream({ key, iv, authTag, aad })
```
