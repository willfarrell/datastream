---
title: base64
description: Base64 encoding and decoding streams.
---

Base64 encoding and decoding streams.

## Install

```bash
npm install @datastream/base64
```

## `base64EncodeStream` <span class="badge">Transform</span>

Encodes data to base64. Handles chunk boundaries correctly by buffering partial 3-byte groups.

Accepts string or byte (`Uint8Array`/`Buffer`) chunks. String chunks are encoded as UTF-8 before base64 encoding, so any Unicode text works. To base64 a different encoding, convert the text to bytes first with `charsetEncodeStream`.

### Example

```javascript
import { pipejoin, createReadableStream, streamToString } from '@datastream/core'
import { base64EncodeStream } from '@datastream/base64'

const output = await streamToString(
  pipejoin([createReadableStream('Hello, World!'), base64EncodeStream()]),
)
// 'SGVsbG8sIFdvcmxkIQ=='
```

## `base64DecodeStream` <span class="badge">Transform</span>

Decodes base64 data back to its original bytes. Handles chunk boundaries by buffering partial 4-character groups. Accepts string or byte chunks.

Decoding is strict, in both builds. The stream errors with `Invalid base64 string` when the input:

- contains a character outside the base64 alphabet (`A–Z`, `a–z`, `0–9`, `+`, `/`, `=` padding), including whitespace and newlines
- has more data after `=` padding (`=` ends a base64 stream, even when the next data arrives in a later chunk)
- ends with an incomplete 4-character group

### Example

```javascript
import { pipejoin, createReadableStream, streamToString } from '@datastream/core'
import { base64DecodeStream } from '@datastream/base64'

const output = await streamToString(
  pipejoin([createReadableStream('SGVsbG8sIFdvcmxkIQ=='), base64DecodeStream()]),
)
// 'Hello, World!'
```
