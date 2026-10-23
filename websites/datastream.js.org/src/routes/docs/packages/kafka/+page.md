---
title: kafka
description: Kafka producer, consumer, and offset-commit streams built on kafkajs.
---

Kafka producer, consumer, and offset-commit streams built on [kafkajs](https://kafka.js.org). Produce messages from a writable stream, consume them from a readable stream with end-to-end backpressure, and commit offsets once your pipeline has processed them.

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
| `autoCommit` | `boolean` | `false` | Let kafkajs auto-commit offsets. See [Committing offsets](#committing-offsets) |
| `partitionsConsumedConcurrently` | `number` | `1` | Concurrent partition processing |

### Stream options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `signal` | `AbortSignal` | — | Aborting stops the consumer, then ends the stream. Pending offsets are not committed. See [Shutdown](#shutdown). Passed in the second argument, like every datastream factory |
| `highWaterMark` | `number` | `100` | Messages buffered before backpressure pauses kafkajs |

### Emitted chunk

```javascript
{ topic, partition, offset, key, value, headers, timestamp }
```

### Committing offsets

**Breaking change:** `autoCommit` now defaults to `false` (it used to be `true`).

kafkajs auto-commits a message's offset as soon as its `eachMessage` handler returns. Here the handler returns once the message is buffered in the readable stream (up to `highWaterMark` messages), not once your pipeline has processed it. With `autoCommit: true`, a crash or `destroy()` loses every message that was committed but still sitting in the buffer.

With the default `autoCommit: false`, commit after processing by ending the pipeline with [`kafkaCommitStream`](#kafkacommitstream):

```javascript
import { pipeline, createTransformStream } from '@datastream/core'
import {
  kafkaCommitStream,
  kafkaConnect,
  kafkaConsumeStream,
} from '@datastream/kafka'

const { consumer, disconnect } = await kafkaConnect({
  brokers: ['localhost:9092'],
  groupId: 'my-group',
})

const source = await kafkaConsumeStream({ consumer, topics: 'events' })
// stop() drains the pipeline and commits the tail before leaving the group
process.once('SIGTERM', () => source.stop())

await pipeline([
  source,
  createTransformStream(async (message, enqueue) => {
    console.log(message.value)
    // Pass the message on: kafkaCommitStream needs its topic, partition and offset
    enqueue(message)
  }),
  await kafkaCommitStream({ consumer, batchSize: 10 }),
])

await disconnect()
```

To get the old behaviour back (at-most-once delivery: buffered messages can be lost on a crash), pass `autoCommit: true`:

```javascript
const stream = await kafkaConsumeStream({ consumer, topics: 'events', autoCommit: true })
```

### Shutdown

**Breaking change:** `stop()` now drains before it stops the consumer. It used to call `consumer.stop()` first, which left the group before `kafkaCommitStream` could commit the tail.

- `stop()` (graceful): ends the readable, waits until every open `kafkaCommitStream` on the same consumer has closed, then calls `consumer.stop()`. The pipeline finishes its in-flight messages and the final commit runs while the group is still joined. If a `kafkaCommitStream` for this consumer is never ended or destroyed, `stop()` waits for it.
- Aborting the `signal`: calls `consumer.stop()` first, then ends the readable. kafkajs refuses the final commit, so nothing pending is committed and those messages are redelivered (at-least-once).
- `destroy()`: stops the consumer immediately without waiting for commit streams.

A message that kafkajs delivers after `stop()`, `destroy()`, or an abort is not pushed. Its handler waits until `consumer.stop()` is called, then throws, so kafkajs neither commits it nor crashes and restarts the consumer while the pipeline drains. It is redelivered on the next run.

## `kafkaCommitStream` <span class="badge">Writable</span>

Commits the offsets of processed messages. Place it at the end of a pipeline that starts with `kafkaConsumeStream`. It takes the emitted chunks (it only reads `topic`, `partition` and `offset`) and calls `consumer.commitOffsets` with `offset + 1`, the offset of the next message Kafka should deliver, for each topic-partition. Returns a Promise resolving to the writable stream.

- Each commit sends the highest offset seen per topic-partition since the last commit.
- A commit never moves an offset backwards: redelivered or out-of-order messages with a lower or equal offset are skipped.
- Offsets are computed with `BigInt`, so int64 offsets past `Number.MAX_SAFE_INTEGER` stay exact.
- It commits every `batchSize` messages and once more when the stream ends.
- It does not commit on `destroy()` or abort. Those messages may not have been processed, so they are left uncommitted and redelivered.
- While kafkajs has no consumer group (after `consumer.stop()`, or between a crash and kafkajs restarting the consumer), `commitOffsets` throws `KafkaJSNonRetriableError: Consumer group was not initialized`. That error is ignored and the offsets stay pending for the next commit. Any other `commitOffsets` error errors the stream.
- When the consumer (re)joins the group (the kafkajs `GROUP_JOIN` event, after a rebalance or a crash-restart), the per-partition high-water marks and pending offsets are reset. kafkajs redelivers from the committed offsets, which may have been reset backwards, and a partition may now belong to another member. The dropped messages are redelivered (at-least-once).

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `consumer` | `Consumer` | — | The same consumer passed to `kafkaConsumeStream` (required) |
| `batchSize` | `number` | `100` | Messages between commits |

### Example

See [Committing offsets](#committing-offsets).

## Platform support

Node.js only (via `kafkajs`, which needs raw TCP sockets). Like `@datastream/aws`, the package has no `browser` export condition.
