---
title: protobuf
description: Protobuf encode/decode and length-prefix framing streams.
---

Protobuf encode/decode transform streams and length-prefix framing for delimited Protobuf records. Works with a [protobufjs](https://protobufjs.github.io/protobuf.js/)-style `Type` (anything exposing `encode`, `decode`, and `create`).

## Install

```bash
npm install @datastream/protobuf
```

Bring your own Protobuf runtime (for example `protobufjs`) to obtain message `Type` objects.

## Type input

Each encode/decode stream takes a `Type`. It may be:

- a static `Type` value (cached on the hot path), or
- a function `(chunk) => Type` (sync or async), re-invoked per chunk so you can select a different message type per record (for example from a Schema Registry envelope).

## `protobufEncodeStream` <span class="badge">Transform</span>

Encodes each incoming message object into Protobuf wire bytes (`Uint8Array`).

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Type` | `Type \| (chunk) => Type` | — | Protobuf message type, or a per-chunk resolver (required) |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { protobufEncodeStream } from '@datastream/protobuf'

await pipeline([
  createReadableStream([{ id: 1, name: 'Alice' }]),
  protobufEncodeStream({ Type: Person }),
])
```

## `protobufDecodeStream` <span class="badge">Transform</span>

Decodes Protobuf wire bytes into message objects.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Type` | `Type \| (chunk) => Type` | — | Protobuf message type, or a per-chunk resolver (required) |
| `payload` | `(chunk) => Uint8Array` | identity | Extract the protobuf bytes from each chunk (e.g. from a registry envelope) |
| `maxOutputSize` | `number` | — | Maximum total input bytes; decoding aborts with an error when exceeded |

#### Output size protection

Decoding untrusted Protobuf can amplify a small input into large objects. Set `maxOutputSize` to bound total decoded volume by input bytes and abort before memory is exhausted.

### Example

```javascript
import { protobufDecodeStream } from '@datastream/protobuf'

protobufDecodeStream({ Type: Person, maxOutputSize: 10 * 1024 * 1024 })
```

## `protobufLengthPrefixFrameStream` <span class="badge">Transform</span>

Prefixes each chunk with a base-128 varint length, producing a self-delimiting stream of framed records (the same framing Kafka and gRPC-style transports use).

## `protobufLengthPrefixUnframeStream` <span class="badge">Transform</span>

Reassembles a varint-length-prefixed byte stream back into individual record frames. Handles records split across chunk boundaries.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxMessageSize` | `number` | — | Reject any frame whose declared length exceeds this, aborting with an error |

Throws on flush if trailing bytes do not form a complete frame, and on a varint that overflows.

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import {
  protobufEncodeStream,
  protobufLengthPrefixFrameStream,
  protobufLengthPrefixUnframeStream,
  protobufDecodeStream,
} from '@datastream/protobuf'

// Encode + frame on the way out
await pipeline([
  createReadableStream(messages),
  protobufEncodeStream({ Type: Person }),
  protobufLengthPrefixFrameStream(),
  // ... write framed bytes
])

// Unframe + decode on the way in
await pipeline([
  framedByteStream,
  protobufLengthPrefixUnframeStream({ maxMessageSize: 1 * 1024 * 1024 }),
  protobufDecodeStream({ Type: Person }),
])
```
