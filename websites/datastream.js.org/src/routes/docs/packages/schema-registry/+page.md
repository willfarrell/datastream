---
title: schema-registry
description: Confluent and AWS Glue Schema Registry framing streams.
---

Framing and unframing transform streams for the Confluent and AWS Glue Schema Registry wire formats. Each input chunk is treated as one whole envelope (one Kafka message = one chunk = one framed record), and each chunk's envelope is emitted as soon as it is decoded.

The wire formats carry no length field, so a frame split across several chunks is not reassembled, and the split cannot be detected reliably. A slice that does not start with a full header is rejected with an error, but a continuation slice that happens to start with the magic byte is decoded as a frame of its own with a bogus id. Confluent's magic byte is `0x00`, which is common inside binary payloads: a 40,005-byte frame fed as 16KB slices yields three envelopes and no error. If your source concatenates or slices framed records (for example `createReadableStream(buffer)`, which cuts buffers into 16KB chunks), re-frame them upstream so each chunk holds exactly one record (pass `[buffer]` to `createReadableStream` for a single record).

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
| `resultKey` | `string` | `"confluentFrameSchemaId"` | Key in pipeline result. The value is `{ schemaId }` |

### `confluentUnframeStream` <span class="badge">Transform</span>

Validates the magic byte, reads the schema id, and emits a `{ schemaId, payload }` envelope.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxSchemaIds` | `number \| null` | `1000` | Maximum distinct ids recorded in `.result()`; later new ids are counted in `untrackedSchemaIds` instead. `null` disables the limit |
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
| `maxOutputSize` | `number \| null` | `268435456` (256MiB) | Maximum compressed payload size per frame; aborts with a `RangeError` when exceeded. `null` disables the limit |
| `resultKey` | `string` | `"glueFrameSchemaVersionId"` | Key in pipeline result. The value is `{ schemaVersionId }` |

### `glueUnframeStream` <span class="badge">Transform</span>

Validates the magic byte, reads the schema version UUID and compression flag, inflates `zlib` payloads, and emits a `{ schemaVersionId, compression, payload }` envelope.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxOutputSize` | `number \| null` | `268435456` (256MiB) | Maximum decompressed payload size per frame; aborts with a `RangeError` when exceeded. `null` disables the limit |
| `maxSchemaIds` | `number \| null` | `1000` | Maximum distinct ids recorded in `.result()`; later new ids are counted in `untrackedSchemaVersionIds` instead. `null` disables the limit |
| `resultKey` | `string` | `"glueSchemaVersionId"` | Key in pipeline result |

#### Decompression protection

A malicious zlib payload can expand to far more than its framed size. `maxOutputSize` caps the inflated output and aborts before memory is exhausted. Always keep this bounded for untrusted input.

### Emitted envelope

```javascript
{ schemaVersionId: string, compression: 'none' | 'zlib', payload: Uint8Array }
```

## Per-chunk envelopes vs `result()`

Unframe streams emit `{ schemaId | schemaVersionId, payload }` envelopes downstream so a decoder can select the right schema per chunk. The `.result()` accessor is exposed for parity with the other detect streams. It lists the distinct ids the stream saw, in first-seen order, alongside the most recently seen id:

```javascript
// confluentUnframeStream
{ key: 'confluentSchemaId', value: { schemaId: 2, schemaIds: [1, 2], untrackedSchemaIds: 0 } }
// glueUnframeStream
{ key: 'glueSchemaVersionId', value: { schemaVersionId, compression, schemaVersionIds: [...], untrackedSchemaVersionIds: 0 } }
```

The id list holds at most `maxSchemaIds` entries (default 1000) so a long-lived consumer does not grow it forever. Once it is full, frames carrying an id that is not already listed are still decoded and emitted, and are counted in `untrackedSchemaIds` / `untrackedSchemaVersionIds`.

A stream that carries several schema ids no longer makes `.result()` (and so `pipeline()`) throw. The most recently seen `schemaId` / `schemaVersionId` / `compression` fields are racy under backpressure, so use the per-chunk envelope when wiring a decoder.

## Platform support

Works on both Node.js and the browser; compression uses the platform `CompressionStream` / `DecompressionStream`.
