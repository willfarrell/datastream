---
title: schema-registry
description: Confluent and AWS Glue Schema Registry framing streams.
---

Framing and unframing transform streams for the Confluent and AWS Glue Schema Registry wire formats. Each input chunk is treated as one envelope (one Kafka message = one chunk = one framed record).

## Install

```bash
npm install @datastream/schema-registry
```

## Confluent format

5-byte header: a `0x00` magic byte followed by a big-endian unsigned 32-bit schema id, then the payload bytes.

### `confluentFrameStream` <span class="badge">Transform</span>

Prepends the Confluent header to each payload chunk.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `schemaId` | `number` | — | Unsigned 32-bit schema id (required) |
| `resultKey` | `string` | `"confluentSchemaId"` | Key in pipeline result |

### `confluentUnframeStream` <span class="badge">Transform</span>

Validates the magic byte, reads the schema id, and emits a `{ schemaId, payload }` envelope.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `resultKey` | `string` | `"confluentSchemaId"` | Key in pipeline result |

### Emitted envelope

```javascript
{ schemaId: number, payload: Uint8Array }
```

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { confluentUnframeStream } from '@datastream/schema-registry'
import { protobufDecodeStream } from '@datastream/protobuf'

await pipeline([
  framedByteStream,
  confluentUnframeStream(),
  protobufDecodeStream({
    // pick the Type per message from the envelope schemaId
    Type: (envelope) => registry.get(envelope.schemaId),
    payload: (envelope) => envelope.payload,
  }),
])
```

## Glue format

18-byte header: a `0x03` magic byte, a 1-byte compression flag (`none` or `zlib`), and a 16-byte schema version UUID, then the payload bytes.

### `glueFrameStream` <span class="badge">Transform</span>

Prepends the Glue header to each payload chunk, optionally deflating the payload.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `schemaVersionId` | `string` | — | Schema version UUID (required) |
| `compression` | `"none" \| "zlib"` | `"none"` | Payload compression |
| `resultKey` | `string` | `"glueSchemaVersionId"` | Key in pipeline result |

### `glueUnframeStream` <span class="badge">Transform</span>

Validates the magic byte, reads the schema version UUID and compression flag, inflates `zlib` payloads, and emits a `{ schemaVersionId, compression, payload }` envelope.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxDecompressedBytes` | `number` | `10485760` (10MB) | Maximum decompressed payload size; aborts with an error when exceeded |
| `resultKey` | `string` | `"glueSchemaVersionId"` | Key in pipeline result |

#### Decompression protection

A malicious zlib payload can expand to far more than its framed size. `maxDecompressedBytes` caps the inflated output and aborts before memory is exhausted. Always keep this bounded for untrusted input.

### Emitted envelope

```javascript
{ schemaVersionId: string, compression: 'none' | 'zlib', payload: Uint8Array }
```

## Per-chunk envelopes vs `result()`

Unframe streams emit `{ schemaId | schemaVersionId, payload }` envelopes downstream so a decoder can select the right schema per chunk. The `.result()` accessor is exposed for parity with the other detect streams, but it reflects only the most recently seen envelope and is racy under backpressure — prefer the per-chunk envelope when wiring a decoder.

## Platform support

Works on both Node.js and the browser; compression uses the platform `CompressionStream` / `DecompressionStream`.
