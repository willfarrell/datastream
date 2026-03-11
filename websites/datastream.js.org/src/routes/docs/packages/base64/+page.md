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

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { base64EncodeStream } from '@datastream/base64'

await pipeline([
  createReadableStream('Hello, World!'),
  base64EncodeStream(),
])
```

## `base64DecodeStream` <span class="badge">Transform</span>

Decodes base64 data back to its original form. Handles chunk boundaries by buffering partial 4-character groups.

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { base64DecodeStream } from '@datastream/base64'

await pipeline([
  createReadableStream('SGVsbG8sIFdvcmxkIQ=='),
  base64DecodeStream(),
])
```
