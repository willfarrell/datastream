---
title: schema-registry
description: Confluent and AWS Glue Schema Registry framing streams.
---

Framing and unframing transform streams for the Confluent and AWS Glue Schema Registry wire formats. Each input chunk is treated as one whole envelope (one Kafka message = one chunk = one framed record), and each chunk's envelope is emitted as soon as it is decoded.

A chunk can also be a message object that holds the frame in its `value` field, such as the messages [`kafkaConsumeStream`](/docs/packages/kafka) emits. The unframe streams then copy the message's other fields onto the envelope they emit, so `topic`, `partition` and `offset` are still there for `kafkaCommitStream` further down the pipeline. A message whose `value` is `null` (a Kafka tombstone) errors the stream with a `TypeError`.

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

Validates the magic byte, reads the schema id, and emits a `{ schemaId, payload }` envelope. For a message object, the frame is read from `value` and the envelope also carries the message's other fields.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxSchemaIds` | `number \| null` | `1000` | Maximum distinct ids recorded in `.result()`; later new ids are counted in `untrackedSchemaIds` instead. `null` disables the limit |
| `resultKey` | `string` | `"confluentSchemaId"` | Key in pipeline result |

### Emitted envelope

```javascript
{ schemaId: number, payload: Uint8Array }
// from a message object, e.g. a kafkaConsumeStream message:
{ topic, partition, offset, key, value, headers, timestamp, schemaId: number, payload: Uint8Array }
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

### `confluentSchemaRegistryResolver`

Returns an async `resolve(schemaId)` function that fetches a schema from a Confluent Schema Registry (`GET {url}/schemas/ids/{id}`) and caches it. It caches, evicts and dedupes like [`awsGlueSchemaRegistryResolver`](/docs/packages/aws), and both results carry `schemaDefinition` and `dataFormat`, so either can feed a decoder such as `avroDecodeStream({ resolveSchema })`. The id field differs: `schemaId` here, `schemaVersionId` for Glue.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | `string` | — | Registry base URL (required). A trailing `/` on its path is dropped; a query string is kept, so `https://registry.example.com/?tenant=a` requests `/schemas/ids/7?tenant=a` |
| `headers` | `object` | — | Request headers, such as `Authorization` |
| `cacheExpiry` | `number` | `-1` | Cache TTL in ms. `-1` caches forever |
| `maxCacheSize` | `number \| null` | `1000` | Maximum cached schemas; the oldest is evicted first. `0` disables caching, `null` means no limit |
| `timeout` | `number` | `30000` | Time limit in ms for each lookup, including reading the response body. A lookup that runs over rejects with a `TimeoutError` |
| `signal` | `AbortSignal` | — | Aborts in-flight lookups; they reject with the signal's reason |
| `maxBodySize` | `number \| null` | `1048576` (1MiB) | Maximum response body size; a larger body rejects with a `RangeError`. `null` disables the limit |
| `fetch` | `function` | `globalThis.fetch` | `fetch` implementation; it must return a `Response` |

`resolve(schemaId)` returns:

```javascript
{
  schemaId: number,
  schemaDefinition: string,
  dataFormat: 'AVRO' | 'PROTOBUF' | 'JSON',
  references: [{ name: string, subject: string, version: number }],
}
```

The registry leaves `schemaType` out for Avro schemas, so a missing `schemaType` maps to `'AVRO'`. `references` lists the schemas this one imports, exactly as the registry returns them (an empty array when there are none); resolving them is up to the caller. Concurrent lookups for the same id share one request, and failed, aborted or timed-out lookups are not cached, so the next call retries. `schemaId` must be an unsigned 32-bit integer, otherwise `resolve` rejects with a `TypeError`. A non-2xx response rejects with an `Error` whose `cause.status` holds the HTTP status.

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { kafkaConsumeStream, kafkaCommitStream } from '@datastream/kafka'
import {
  confluentSchemaRegistryResolver,
  confluentUnframeStream,
} from '@datastream/schema-registry'
import { avroDecodeStream } from '@datastream/avro'

const resolveSchema = confluentSchemaRegistryResolver({
  url: 'https://registry.example.com',
  headers: { Authorization: `Basic ${btoa(`${key}:${secret}`)}` },
})

await pipeline([
  await kafkaConsumeStream({ consumer, topics: ['orders'] }),
  confluentUnframeStream(), // { topic, partition, offset, ..., schemaId, payload }
  avroDecodeStream({ resolveSchema }), // { topic, partition, offset, ..., schemaId, value }
  await kafkaCommitStream({ consumer }),
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

Validates the magic byte, reads the schema version UUID and compression flag, inflates `zlib` payloads, and emits a `{ schemaVersionId, compression, payload }` envelope. For a message object, the frame is read from `value` and the envelope also carries the message's other fields.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxOutputSize` | `number \| null` | `268435456` (256MiB) | Maximum decompressed payload size per frame; aborts with a `RangeError` when exceeded. `null` disables the limit |
| `maxSchemaIds` | `number \| null` | `1000` | Maximum distinct ids recorded in `.result()`; later new ids are counted in `untrackedSchemaVersionIds` instead. `null` disables the limit |
| `resultKey` | `string` | `"glueSchemaVersionId"` | Key in pipeline result |

#### Decompression protection

A malicious zlib payload can expand to far more than its framed size. `maxOutputSize` caps the inflated output and aborts before memory is exhausted. The payload is fed to the decompressor in 16 KiB slices, with the limit checked between slices, so a small frame cannot inflate far past the limit first. Always keep this bounded for untrusted input.

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

Works on both Node.js and the browser; compression uses the platform `CompressionStream` / `DecompressionStream`, and `confluentSchemaRegistryResolver` uses the global `fetch`.
