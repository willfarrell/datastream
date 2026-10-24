---
title: gcp
description: Google Cloud streams for Cloud Storage, Pub/Sub, and BigQuery. Bring your own SDK client.
---

Google Cloud streams for Cloud Storage, Pub/Sub, and BigQuery.

<span class="badge">Node.js only</span> The package has no browser build.

## Install

```bash
npm install @datastream/gcp
```

The package has no dependency on the Google Cloud SDKs. You create the SDK objects (a `Bucket`, `Topic`, `Subscription`, `BigQuery` or `Table`) and pass them in, so you control credentials, the project, retries, and the endpoint. Install only the SDKs for the services you use.

### Import from subpaths

| Subpath | SDK you pass objects from |
|---------|---------------------------|
| `@datastream/gcp/storage` | `@google-cloud/storage` |
| `@datastream/gcp/pubsub` | `@google-cloud/pubsub` |
| `@datastream/gcp/bigquery` | `@google-cloud/bigquery` |

```bash
# for example, Cloud Storage only
npm install @datastream/gcp @google-cloud/storage
```

```javascript
import { Storage } from '@google-cloud/storage'
import { gcpStorageReadStream } from '@datastream/gcp/storage'

const bucket = new Storage().bucket('my-bucket')
const stream = gcpStorageReadStream({ bucket, name: 'data.csv' })
```

The package root (`@datastream/gcp`) re-exports every subpath.

## Cloud Storage

Import from `@datastream/gcp/storage`. Each stream takes a `bucket` option, a `Bucket` from `@google-cloud/storage` (`new Storage().bucket('my-bucket')`). The package never creates one, so you control credentials, retries, and the endpoint.

### `gcpStorageReadStream` <span class="badge">Readable</span>

Reads an object with `bucket.file(name).createReadStream()` and returns the SDK's byte stream. The download request starts on the first read. If the signal aborts, the stream is destroyed, which tears the download down. The abort listener is removed once the stream ends.

#### Options

Accepts `File.createReadStream()` options plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bucket` | `Bucket` | none | Required. The bucket holding the object |
| `name` | `string` | none | Required. The object name |
| `start` | `number` | `0` | First byte to read |
| `end` | `number` | to the end | Last byte to read (inclusive) |
| `decompress` | `boolean` | `true` | Gunzip objects stored with `Content-Encoding: gzip` |
| `validation` | `'crc32c'` \| `'md5'` \| `false` | `'crc32c'` | Checksum to verify the download against. The SDK does not validate range (`start`/`end`) reads |

Throws `TypeError: gcpStorageReadStream: bucket required` if `bucket` is missing, and `TypeError: gcpStorageReadStream: name required` if `name` is not a non-empty string. A missing object or a checksum mismatch errors the stream with the SDK error.

#### Example

```javascript
import { Storage } from '@google-cloud/storage'
import { pipeline } from '@datastream/core'
import { gcpStorageReadStream } from '@datastream/gcp/storage'
import { csvParseStream } from '@datastream/csv'

const bucket = new Storage().bucket('my-bucket')

await pipeline([
  gcpStorageReadStream({ bucket, name: 'exports/data.csv' }),
  csvParseStream(),
])
```

### `gcpStorageWriteStream` <span class="badge">Writable</span>

Uploads the bytes written to it with `bucket.file(name).createWriteStream()`. `pipeline()` resolves only after the object is committed: the SDK holds `finish` until the service returns the object's metadata and the checksum matches. An upload or validation error rejects the pipeline with the SDK's own error. On a checksum mismatch the SDK deletes the object and the error has `code: 'FILE_NO_UPLOAD'`.

If the stream is destroyed or the signal aborts before the end, the SDK write stream is destroyed. The SDK never sends the final chunk, so no object is committed. An open resumable session expires on the service side.

The stream adds no key to the `pipeline()` result.

#### Options

Accepts `File.createWriteStream()` options plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bucket` | `Bucket` | none | Required. The bucket to write to |
| `name` | `string` | none | Required. The object name |
| `resumable` | `boolean` | `true` | Use a resumable upload. Set `false` for a single request, which is faster for small objects |
| `contentType` | `string` | from the name | The object's `Content-Type` |
| `metadata` | `object` | none | Object resource fields, such as `cacheControl` or custom `metadata` |
| `validation` | `'crc32c'` \| `'md5'` \| `false` | `'crc32c'` | Checksum to verify the upload against |

Throws `TypeError: gcpStorageWriteStream: bucket required` if `bucket` is missing, and `TypeError: gcpStorageWriteStream: name required` if `name` is not a non-empty string.

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { csvFormatStream } from '@datastream/csv'
import { gcpStorageWriteStream } from '@datastream/gcp/storage'

await pipeline([
  source,
  csvFormatStream(),
  gcpStorageWriteStream({
    bucket,
    name: 'exports/data.csv',
    contentType: 'text/csv',
  }),
])
```

### `gcpStorageListStream` <span class="badge">Readable</span>

Lists objects with `bucket.getFiles({ autoPaginate: false })`, one page per request, and yields each object's metadata: the plain object resource (`name`, `size`, `contentType`, `updated`, and so on) that the SDK keeps on `file.metadata`. Plain objects serialize to JSON or CSV directly. To read an object, pass its `name` to `gcpStorageReadStream`.

The next page is requested only when the consumer reads past the current one. Destroying the stream stops paging. `getFiles()` can't be cancelled, so a page request already in flight is abandoned, not aborted, and the stream closes without waiting for it.

#### Options

Accepts `getFiles()` options (such as `delimiter`, `matchGlob`, `startOffset`, and `versions`) plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bucket` | `Bucket` | none | Required. The bucket to list |
| `prefix` | `string` | none | Only list objects whose names start with this |
| `maxResults` | `number` | `1000` | Objects per page |
| `fields` | `string` | none | Partial response, such as `'items(name,size),nextPageToken'`. The SDK then returns raw items, which are yielded as they are. Include `nextPageToken`, or only the first page is listed |

`autoPaginate` is always `false`. Throws `TypeError: gcpStorageListStream: bucket required` if `bucket` is missing.

#### Example

```javascript
import { createTransformStream, pipejoin, streamToArray } from '@datastream/core'
import { gcpStorageListStream } from '@datastream/gcp/storage'

const names = await streamToArray(
  pipejoin([
    gcpStorageListStream({ bucket, prefix: 'exports/' }),
    createTransformStream((object, enqueue) => enqueue(object.name)),
  ]),
)
```

## Pub/Sub

Import from `@datastream/gcp/pubsub`. Pass in a `Topic` or `Subscription` from `@google-cloud/pubsub`.

### `gcpPubSubPublishStream` <span class="badge">Writable</span>

Publishes each chunk with `topic.publishMessage()`. Each chunk is a message: `{ data, attributes, orderingKey }`, or `{ json }` to have the SDK serialize an object. `data` can be a `Buffer`, a `Uint8Array`, or a string. Strings are sent as UTF-8; the SDK itself only accepts bytes. Attribute values must be strings.

The SDK batches publishes on its own (by default up to 100 messages or 10 ms per batch). The stream does not wait for each publish before taking the next chunk, because that would send one message per batch. It keeps up to `maxInFlight` publishes outstanding. When that many are unconfirmed, it waits for the oldest one. At the end it calls `topic.flush()` and waits for every publish to be confirmed.

If any publish fails, the stream errors right away with that error. Every message not yet confirmed, including the failed one, is on `err.failedMessages`, in write order. The same applies when the stream is destroyed or aborted (see [Unsent messages on destroy or abort](#unsent-messages-on-destroy-or-abort)). A publish already handed to the SDK cannot be cancelled, so a message on `err.failedMessages` may still be delivered. If you retry those messages, subscribers can receive duplicates.

With an `orderingKey`, after one publish fails, the SDK fails every later publish with that key until you call `topic.resumePublishing(orderingKey)`.

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topic` | `Topic` | none | Required. The topic to publish to. Set batching with `topic.setPublishOptions()` |
| `maxInFlight` | `number` | `100` | Publishes kept outstanding at once. Must be at least 1 |

#### Example

```javascript
import { PubSub } from '@google-cloud/pubsub'
import { createReadableStream, pipeline } from '@datastream/core'
import { gcpPubSubPublishStream } from '@datastream/gcp/pubsub'

const topic = new PubSub().topic('orders')

await pipeline([
  createReadableStream([
    { data: JSON.stringify({ id: 1 }), attributes: { type: 'created' } },
    { json: { id: 2 }, orderingKey: 'customer-7' },
  ]),
  gcpPubSubPublishStream({ topic }),
])
```

### `gcpPubSubSubscribeStream` <span class="badge">Readable</span>

Yields each `Message` the subscription receives (`id`, `data`, `attributes`, `orderingKey`, `publishTime`, `deliveryAttempt`, `ack()`, `nack()`). Adding the listener opens the subscription. An `error` from the subscription errors the stream.

Pub/Sub pushes messages, so the stream cannot slow the subscription down. The subscription's flow control is the only limit on how many messages are waiting. Two things follow:

- Ack or nack every message downstream, for example with `gcpPubSubAckStream`. Flow control counts a message until it is acked or nacked, so unacked messages stop delivery once `flowControl.maxMessages` is reached.
- Keep `flowControl.maxMessages` at or below the stream's `highWaterMark` (default 1024 messages), and set `flowControl.allowExcessMessages: false`. The SDK default is `true`, which lets a burst go past `maxMessages`. If the buffer overflows, the stream errors with `createReadableStream queue size (...) exceeds limit (...)` and closes the subscription.

#### Stopping

- `await stream.stop()` stops gracefully. The stream stops taking messages and nacks any that arrive after this point. It ends the readable so the pipeline can finish. It waits until every `gcpPubSubAckStream` and `gcpPubSubNackStream` on the same subscription has closed, then calls `subscription.close()`.
- Destroying the stream, aborting `signal`, or a subscription error stops it at once. The stream calls `subscription.close()` and removes its listeners. Nothing more is acked through ack streams that share the signal.

`subscription.close()` handles messages that are still unacked according to the subscription's `closeOptions`. In the current SDK code the default is to nack them, so Pub/Sub delivers them again soon. If the process exits without closing, Pub/Sub delivers them again once their ack deadline passes. If the subscription is closed somewhere else, the stream ends.

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `subscription` | `Subscription` | none | Required. Set `flowControl` (and `ackDeadline`) when you create it, or with `subscription.setOptions()` |

#### Example

```javascript
import { PubSub } from '@google-cloud/pubsub'
import { pipeline } from '@datastream/core'
import {
  gcpPubSubSubscribeStream,
  gcpPubSubAckStream,
} from '@datastream/gcp/pubsub'

const subscription = new PubSub().subscription('orders-worker', {
  flowControl: { maxMessages: 500, allowExcessMessages: false },
})
const stream = gcpPubSubSubscribeStream({ subscription })
process.once('SIGTERM', () => stream.stop())

await pipeline([
  stream,
  // process each message here
  gcpPubSubAckStream({ subscription }),
])
```

### `gcpPubSubAckStream` <span class="badge">Writable</span>

Calls `message.ack()` on each message. Each chunk is a message from `gcpPubSubSubscribeStream`. `stop()` on the subscribe stream waits for this stream to close, so pass the same subscription. After an abort, no more messages are acked.

`message.ack()` does not report failures. On a subscription with exactly-once delivery, set `withResponse: true`. The stream then awaits `message.ackWithResponse()` and errors if the ack fails. On other subscriptions `ackWithResponse()` succeeds at once.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `subscription` | `Subscription` | none | Required. The subscription the messages came from |
| `withResponse` | `boolean` | `false` | Await `ackWithResponse()` and error on failure |

### `gcpPubSubNackStream` <span class="badge">Writable</span>

Calls `message.nack()` on each message, so Pub/Sub delivers it again right away (or to the dead-letter topic once its delivery attempts run out). It takes the same options as `gcpPubSubAckStream`; `withResponse: true` awaits `message.nackWithResponse()`.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `subscription` | `Subscription` | none | Required. The subscription the messages came from |
| `withResponse` | `boolean` | `false` | Await `nackWithResponse()` and error on failure |

## BigQuery

Import from `@datastream/gcp/bigquery`. The query stream takes a `bigquery` option, a `BigQuery` from `@google-cloud/bigquery` (`new BigQuery()`). The load and insert streams take a `table` option, a `Table` (`new BigQuery().dataset('my_dataset').table('my_table')`). The package never creates these, so you control credentials, the project, and the location.

There are two ways to write. `gcpBigQueryLoadStream` runs one load job: all or nothing, and no streaming insert charges. `gcpBigQueryInsertStream` uses streaming inserts: rows show up within seconds, but each batch commits on its own, so a failure can leave some rows written.

### `gcpBigQueryQueryStream` <span class="badge">Readable</span> <span class="badge">async</span>

Runs a query with `bigquery.createQueryStream()` and yields one object per row, keyed by column name. The SDK runs the query job and fetches the result pages. No query is sent until the stream is first read.

If the stream is destroyed or the signal aborts, the SDK stream is ended so it requests no more pages. The SDK takes no signal, so a page request already in flight still completes; its rows are dropped.

#### Options

Accepts `createQueryStream()` query options (such as `location`, `maxResults`, `types`, `useLegacySql`) plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bigquery` | `BigQuery` | none | Required. The client to query with |
| `query` | `string` | none | Required. The SQL to run |
| `params` | `array` \| `object` | none | Positional (`?`) or named (`@name`) query parameters |
| `wrapIntegers` | `boolean` \| `object` | `false` | Return `INT64` as `BigQueryInt` instead of a `number`. An object `{ integerTypeCastFunction, fields }` casts them with your function |
| `parseJSON` | `boolean` | `false` | Parse `JSON` columns instead of returning strings |

Throws `TypeError: gcpBigQueryQueryStream: bigquery required` if `bigquery` is missing. A query error errors the stream with the SDK error.

#### Value types

The SDK converts each column by its type:

| Column type | Value |
|-------------|-------|
| `INT64` | `number`. Without `wrapIntegers`, values above `Number.MAX_SAFE_INTEGER` lose precision silently. With `wrapIntegers: true`, a `BigQueryInt` (`.value` is the exact string) |
| `FLOAT64` | `number` |
| `NUMERIC`, `BIGNUMERIC` | `Big` (big.js) |
| `BOOL` | `boolean` |
| `BYTES` | `Buffer` |
| `TIMESTAMP` | `BigQueryTimestamp` (`.value` is an ISO 8601 string, with microseconds when the value has them) |
| `DATE`, `DATETIME`, `TIME` | `BigQueryDate`, `BigQueryDatetime`, `BigQueryTime` (`.value` is the string) |
| `JSON` | `string`, or the parsed value with `parseJSON` |
| `RECORD` / `STRUCT` | object |

The wrapper classes serialize with `JSON.stringify` as `{"value":"..."}` and `Big` as a string. Map them to plain values in a transform if the next stream needs them, for example `row.created.value`.

#### Example

```javascript
import { BigQuery } from '@google-cloud/bigquery'
import { pipeline } from '@datastream/core'
import { gcpBigQueryQueryStream } from '@datastream/gcp/bigquery'
import { csvFormatStream } from '@datastream/csv'

const bigquery = new BigQuery()

await pipeline([
  await gcpBigQueryQueryStream({
    bigquery,
    query: 'SELECT id, name FROM `my_dataset.users` WHERE created > @since',
    params: { since: '2026-01-01' },
    wrapIntegers: true,
  }),
  csvFormatStream(),
  destination,
])
```

### `gcpBigQueryLoadStream` <span class="badge">Writable</span>

Loads the bytes written to it into a table as one load job, with `table.createWriteStream()`. Write formatted bytes, for example from `ndjsonFormatStream` or `csvFormatStream`. The bytes are uploaded as the job's data, and BigQuery creates the job once the upload ends.

`pipeline()` resolves only after the job has finished. A failed job (its `status.errorResult`) or a failed upload rejects the pipeline with the SDK error. A load job either loads every row or none, unless you allow bad rows with `maxBadRecords`.

If the stream is destroyed or the signal aborts:

- **Before the end:** the upload is torn down and never completes, so BigQuery creates no job.
- **After the end:** the job is cancelled with `job.cancel()`, and the SDK stops polling it. Cancelling is a request: a job that has already committed stays committed. If the upload response arrives after the abort, that job is cancelled as soon as it arrives.

The SDK has no way to abort the upload request itself.

#### Options

Accepts `JobConfigurationLoad` fields (such as `schema`, `writeDisposition`, `createDisposition`, `skipLeadingRows`, `autodetect`, `maxBadRecords`) and the SDK's `jobId` / `jobPrefix`, plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `table` | `Table` | none | Required. The table to load into |
| `sourceFormat` | `string` | `'NEWLINE_DELIMITED_JSON'` | The format of the bytes: `'NEWLINE_DELIMITED_JSON'`, `'CSV'`, `'AVRO'`, `'PARQUET'`, or `'ORC'` |
| `resultKey` | `string` | `'bigqueryLoad'` | The key in the `pipeline()` result |

Throws `TypeError: gcpBigQueryLoadStream: table required` if `table` is missing.

#### Result

| Field | Type | Description |
|-------|------|-------------|
| `jobId` | `string` | The load job's ID |
| `outputRows` | `number` | Rows loaded (`statistics.load.outputRows`) |
| `statistics` | `object` | The job's statistics. `statistics.load` has `inputFileBytes`, `outputBytes`, and `badRecords` |

#### Example

```javascript
import { BigQuery } from '@google-cloud/bigquery'
import { pipeline } from '@datastream/core'
import { gcpBigQueryLoadStream } from '@datastream/gcp/bigquery'
import { ndjsonFormatStream } from '@datastream/json'

const table = new BigQuery().dataset('my_dataset').table('events')

const { bigqueryLoad } = await pipeline([
  source,
  ndjsonFormatStream(),
  gcpBigQueryLoadStream({ table, writeDisposition: 'WRITE_APPEND' }),
])
console.log(bigqueryLoad.jobId, bigqueryLoad.outputRows)
```

### `gcpBigQueryInsertStream` <span class="badge">Writable</span>

Writes row objects with streaming inserts (`table.insert()`, the `insertAll` API). Rows are sent in batches of up to `batchSize` rows and `maxBatchBytes` bytes. Each row is counted as its `JSON.stringify` size plus 64 bytes for the `insertId` wrapper the SDK adds. A `Buffer` value counts as its JSON form, which is larger than the base64 the SDK sends, so such batches come out smaller than needed.

Streaming inserts are not atomic. Each batch commits on its own, and rows written before a failure stay written. Use `gcpBigQueryLoadStream` when you need all or nothing.

A row larger than `maxBatchBytes` errors the stream with `RangeError: gcpBigQueryInsertStream row exceeds maxBatchBytes` before anything is sent. The error's `cause` is `{ bytes, limit }`.

When some rows of a batch fail, the SDK first retries them (`partialRetries` times). If rows still fail, the stream errors with the SDK's `PartialFailureError`. Its `errors` property lists each failed row as `{ row, errors: [{ reason, message }] }`. The other rows of that batch were inserted.

#### Rows not inserted

When the stream errors, is destroyed, or the signal aborts, `err.failedMessages` holds every row that was not inserted:

- the rows that failed in a `PartialFailureError`,
- the whole batch, when the request itself failed (for example a 404 for a missing table),
- the rows still waiting for the next batch.

A batch whose request is in flight when the stream is torn down is not included: the SDK cannot cancel it, and its result arrives after the stream has closed. A destroy without an error gets one with the message `gcpBigQueryInsertStream destroyed with buffered rows`.

By default the SDK gives each row a new random `insertId` on every attempt, so BigQuery cannot drop duplicates when you retry `failedMessages`. To deduplicate (best effort), set `raw: true` and write `{ insertId, json }` rows with your own IDs.

#### Options

Accepts `table.insert()` options plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `table` | `Table` | none | Required. The table to insert into |
| `batchSize` | `number` | `500` | Rows per request. BigQuery recommends 500 |
| `maxBatchBytes` | `number` | `9437184` (9 MiB) | Bytes per request. The `insertAll` limit is 10 MB per request |
| `skipInvalidRows` | `boolean` | `false` | Insert the valid rows of a batch even if some are invalid. Otherwise one invalid row fails the whole batch |
| `ignoreUnknownValues` | `boolean` | `false` | Drop values for columns the table does not have, instead of failing the row |
| `raw` | `boolean` | `false` | Rows are already in `insertAll` form: `{ insertId, json }` |
| `createInsertId` | `boolean` | `true` | Give each row a random `insertId` |
| `partialRetries` | `number` | `3` | How many times the SDK retries the failed rows of a batch |
| `schema` | `string` \| `object` | none | If the table does not exist, the SDK creates it with this schema and retries after 60 seconds |
| `templateSuffix` | `string` | none | Insert into (and create) the table `<table><suffix>` |

Throws `TypeError: gcpBigQueryInsertStream: table required` if `table` is missing.

#### Example

```javascript
import { BigQuery } from '@google-cloud/bigquery'
import { pipeline } from '@datastream/core'
import { gcpBigQueryInsertStream } from '@datastream/gcp/bigquery'

const table = new BigQuery().dataset('my_dataset').table('events')

try {
  await pipeline([source, gcpBigQueryInsertStream({ table, skipInvalidRows: true })])
} catch (e) {
  if (e.name === 'PartialFailureError') {
    for (const { row, errors } of e.errors) console.error(row, errors)
  }
  if (e.failedMessages) await retryLater(e.failedMessages)
}
```
