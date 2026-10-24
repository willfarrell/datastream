---
title: avro
description: Apache Avro encode/decode transform streams built on avsc.
---

Apache Avro encode/decode transform streams built on [avsc](https://github.com/mtth/avsc). Each chunk is one Avro record in binary encoding: no object container file and no framing. Pair them with [`@datastream/schema-registry`](/docs/packages/schema-registry) for Kafka messages in the Confluent wire format.

## Install

```bash
npm install @datastream/avro avsc
```

`avsc` is an optional peer dependency.

## Schema input

Both streams take a `schema`. It may be:

- an Avro schema object, such as `{ type: 'record', name: 'User', fields: [...] }`,
- the schema's JSON text, such as a registry's `schemaDefinition`,
- a compiled avsc `Type`, or
- a zero-argument function returning any of the above, or a Promise of one.

A concrete schema is compiled when the stream is created, so an invalid one throws right away. A function is called once, when the first chunk arrives, so an earlier stage of the pipeline can produce the schema first. The result is reused for every later chunk, so one stream decodes with one schema. To pick the schema per message, use `avroDecodeStream`'s `resolveSchema` option instead.

## `avroEncodeStream` <span class="badge">Transform</span>

Encodes each incoming object into Avro binary (`Uint8Array`).

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `schema` | `object \| string \| Type \| () => …` | — | Avro schema (required) |

A record that does not match the schema errors the stream with a `TypeError` that lists the path of each bad field, for example `avroEncodeStream: record does not match schema at $.id, $.name`. Field values are not included in the message.

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { avroEncodeStream } from '@datastream/avro'

const schema = {
  type: 'record',
  name: 'User',
  fields: [
    { name: 'id', type: 'int' },
    { name: 'name', type: 'string' },
  ],
}

await pipeline([
  createReadableStream([{ id: 1, name: 'Alice' }]),
  avroEncodeStream({ schema }),
])
```

## `avroDecodeStream` <span class="badge">Transform</span>

Decodes each chunk (one whole Avro record) into an object. A chunk is a `Uint8Array` (or any `ArrayBuffer` view) or an `ArrayBuffer`; anything else, strings included, errors the stream with a `TypeError`. Truncated input or trailing bytes error the stream, for example `avroDecodeStream: truncated buffer`. The original avsc error is kept as `cause`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `schema` | `object \| string \| Type \| () => …` | — | Writer schema. Required unless `resolveSchema` is set |
| `resolveSchema` | `(id) => schema \| Promise<schema>` | — | Looks the writer schema up per message. Each chunk is then an unframe stream's envelope (see below). Cannot be combined with `schema` |
| `maxMessageSize` | `number \| null` | `67108864` (64MiB) | Maximum bytes per message; a larger one errors the stream with a `RangeError`. `null` disables the limit |

### Per-message schemas: `resolveSchema`

With `resolveSchema`, each chunk is an envelope from [`confluentUnframeStream`](/docs/packages/schema-registry) or `glueUnframeStream`: `{ schemaId, payload }` or `{ schemaVersionId, compression, payload }`, plus any fields of the message it was unframed from. The stream calls `resolveSchema(envelope.schemaId ?? envelope.schemaVersionId)`, decodes `payload` with that schema, and emits the envelope with `payload` replaced by `value`:

```javascript
// in:  { topic, partition, offset, ..., schemaId: 2, payload: Uint8Array }
// out: { topic, partition, offset, ..., schemaId: 2, value: { id: 1, name: 'Alice' } }
```

`resolveSchema` may return a schema in any form listed under [Schema input](#schema-input), or a schema registry resolver's result (`{ schemaDefinition, ... }`), so `confluentSchemaRegistryResolver(...)` and `awsGlueSchemaRegistryResolver(...)` can be passed as is. The compiled schema is cached per id (up to 1000 ids, oldest evicted first), so the resolver runs once per id.

### Example: Kafka with a Confluent Schema Registry

Unframe each Kafka message, decode it with the writer schema its id points to, then commit it. The topic, partition and offset survive both steps, so `kafkaCommitStream` can commit the decoded messages:

```javascript
import { pipeline, createTransformStream } from '@datastream/core'
import { kafkaConsumeStream, kafkaCommitStream } from '@datastream/kafka'
import {
  confluentSchemaRegistryResolver,
  confluentUnframeStream,
} from '@datastream/schema-registry'
import { avroDecodeStream } from '@datastream/avro'

await pipeline([
  await kafkaConsumeStream({ consumer, topics: ['orders'] }),
  confluentUnframeStream(),
  avroDecodeStream({
    resolveSchema: confluentSchemaRegistryResolver({ url: 'https://registry.example.com' }),
  }),
  createTransformStream(async (message, enqueue) => {
    await handleOrder(message.value)
    enqueue(message)
  }),
  await kafkaCommitStream({ consumer }),
])
```

Messages may carry different schema ids; each id is looked up once.

## Limits

A small hostile message can claim far more data than it holds. Two limits apply when decoding:

- `maxMessageSize` (default 64MiB) caps each message's bytes before decoding starts.
- An array or map block whose item count is larger than the bytes left in the message is rejected, for example `avroDecodeStream: block count 50000000 exceeds the 1 bytes left`. Without this check, a 5-byte message claiming 50 million items makes avsc allocate about 750MB. Every map entry and almost every array item takes at least one byte, so valid data is not affected. The exception is arrays of items that encode to zero bytes (`null`, a record with no fields, `fixed` of size 0): such an array can be rejected once it has more items than the bytes that follow its count.

The block-count check applies to schemas the stream compiles: schema objects, JSON text, and anything `resolveSchema` returns except a compiled avsc `Type`. A compiled avsc `Type` you pass in is used as it is, without the check, so pass the schema instead when decoding untrusted input.

## Platform support

Node.js only. avsc 5 reads and writes through `Buffer`, so the package has no `browser` export condition. A browser build would need avsc 6 or a `Buffer` polyfill.
