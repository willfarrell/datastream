---
title: kafka
description: Kafka producer and consumer streams built on kafkajs.
---

Kafka producer and consumer streams built on [kafkajs](https://kafka.js.org). Produce messages from a writable stream and consume them from a readable stream with end-to-end backpressure.

## Install

```bash
npm install @datastream/kafka kafkajs
```

`kafkajs` is an optional peer dependency.

## `kafkaConnect`

Creates a Kafka client and connects a producer and/or consumer. Returns a connection object. A consumer is created only when a `groupId` is provided; the producer is created unless `producer` is set to `false`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `brokers` | `string[]` | — | Broker addresses |
| `clientId` | `string` | — | Client identifier |
| `ssl` | `object` | — | TLS configuration |
| `sasl` | `object` | — | SASL authentication (see `@datastream/aws` MSK IAM) |
| `groupId` | `string` | — | Consumer group id; required to create a consumer |
| `producer` | `object \| false` | `{}` | Producer options, or `false` to skip the producer |
| `consumer` | `object` | `{}` | Additional consumer options |

### Result

```javascript
{ kafka, producer, consumer, disconnect }
```

Call `await connection.disconnect()` to disconnect the producer and consumer.

### Example

```javascript
import { kafkaConnect } from '@datastream/kafka'

const { producer, consumer, disconnect } = await kafkaConnect({
  brokers: ['localhost:9092'],
  clientId: 'my-app',
  groupId: 'my-group',
})
```

## `kafkaProduceStream` <span class="badge">Writable</span>

Batches messages and sends them to a topic. Returns a Promise resolving to the writable stream. Chunks may be a `Uint8Array`, a `string`, or a `{ value, key?, headers?, partition? }` message object.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `producer` | `Producer` | — | Producer from `kafkaConnect` (required) |
| `topic` | `string` | — | Destination topic (required) |
| `batchSize` | `number` | `100` | Messages buffered before a `send` |
| `acks` | `-1 \| 0 \| 1` | `-1` | Acknowledgements (`-1` = all in-sync replicas) |
| `compression` | `0 \| 1 \| 2 \| 3 \| 4` | — | None, GZIP, Snappy, LZ4, ZSTD |
| `timeout` | `number` | — | Per-request send timeout in ms |

On a failed send the thrown error carries the un-sent batch on `err.failedMessages` so callers can re-queue instead of losing data.

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { kafkaConnect, kafkaProduceStream } from '@datastream/kafka'

const { producer } = await kafkaConnect({ brokers: ['localhost:9092'] })

await pipeline([
  createReadableStream([
    { value: 'event-1' },
    { value: 'event-2' },
  ]),
  await kafkaProduceStream({ producer, topic: 'events' }),
])
```

## `kafkaConsumeStream` <span class="badge">Readable</span>

Subscribes to one or more topics and emits each message as a chunk. Returns a Promise resolving to a readable stream with a `stop()` method. Backpressure propagates from the downstream consumer through kafkajs to the broker.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `consumer` | `Consumer` | — | Consumer from `kafkaConnect` (required) |
| `topics` | `string \| string[]` | — | Topic(s) to subscribe to (required) |
| `fromBeginning` | `boolean` | `false` | Start from the earliest offset |
| `autoCommit` | `boolean` | `true` | Auto-commit offsets |
| `partitionsConsumedConcurrently` | `number` | `1` | Concurrent partition processing |
| `signal` | `AbortSignal` | — | Aborting stops the consumer |

### Emitted chunk

```javascript
{ topic, partition, offset, key, value, headers, timestamp }
```

### Example

```javascript
import { kafkaConnect, kafkaConsumeStream } from '@datastream/kafka'

const { consumer } = await kafkaConnect({
  brokers: ['localhost:9092'],
  groupId: 'my-group',
})

const stream = await kafkaConsumeStream({ consumer, topics: 'events' })

for await (const message of stream) {
  console.log(message.value)
}

await stream.stop()
```

## Platform support

Available on Node.js (via `kafkajs`). Not available in the browser.
