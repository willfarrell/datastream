---
title: aws
description: AWS service streams for CloudWatch Logs, DynamoDB, DynamoDB Streams, Kinesis, Lambda, S3, SNS, and SQS, plus MSK IAM auth and Glue Schema Registry helpers.
---

AWS service streams for CloudWatch Logs, DynamoDB, DynamoDB Streams, Kinesis, Lambda, S3, SNS, and SQS, plus helpers for Amazon MSK IAM authentication and the AWS Glue Schema Registry.

<span class="badge">Node.js only</span> The package has no browser build.

## Install

```bash
npm install @datastream/aws
```

The AWS SDK v3 clients are optional peer dependencies: install only the ones for the services you use.

### Import from subpaths

Import each service from its own subpath, and install that subpath's peer dependencies:

| Subpath | Peer dependencies |
|---------|-------------------|
| `@datastream/aws/cloudwatch-logs` | `@aws-sdk/client-cloudwatch-logs` |
| `@datastream/aws/dynamodb` | `@aws-sdk/client-dynamodb` |
| `@datastream/aws/dynamodb-streams` | `@aws-sdk/client-dynamodb-streams` |
| `@datastream/aws/kinesis` | `@aws-sdk/client-kinesis` |
| `@datastream/aws/lambda` | `@aws-sdk/client-lambda` |
| `@datastream/aws/s3` | `@aws-sdk/client-s3` `@aws-sdk/lib-storage` |
| `@datastream/aws/sns` | `@aws-sdk/client-sns` |
| `@datastream/aws/sqs` | `@aws-sdk/client-sqs` |
| `@datastream/aws/msk-iam` | `aws-msk-iam-sasl-signer-js` |
| `@datastream/aws/glue-schema-registry` | `@aws-sdk/client-glue` |

```bash
# for example, S3 only
npm install @datastream/aws @aws-sdk/client-s3 @aws-sdk/lib-storage
```

```javascript
import { awsS3GetObjectStream } from '@datastream/aws/s3'
```

Avoid importing from the package root (`@datastream/aws`). The root statically imports the first eight subpaths above, so it fails to load unless all of their SDK clients are installed, and it creates a default client for every service. `msk-iam` and `glue-schema-registry` are only available from their subpaths.

Each service creates a default client on import. On US and Canada regions (read from `AWS_REGION` when the client is created) FIPS endpoints are enabled. Use the `*SetClient` function, or the per-call `client` option, to supply your own.

## CloudWatch Logs

### `awsCloudWatchLogsSetClient`

Mutates module-level state — not safe for concurrent multi-tenant use.

```javascript
import { CloudWatchLogsClient } from '@aws-sdk/client-cloudwatch-logs'
import { awsCloudWatchLogsSetClient } from '@datastream/aws/cloudwatch-logs'

awsCloudWatchLogsSetClient(new CloudWatchLogsClient({ region: 'us-east-1' }))
```

### `awsCloudWatchLogsGetLogEventsStream` <span class="badge">Readable</span> <span class="badge">async</span>

Gets log events from a CloudWatch Logs log stream. Auto-paginates until no new events are returned.

#### Options

Accepts `GetLogEventsCommand` parameters plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `logGroupName` | `string` | — | Log group name |
| `logStreamName` | `string` | — | Log stream name |
| `pollingActive` | `boolean` | `false` | Keep polling for new events |
| `pollingDelay` | `number` | `1000` | Delay (ms) between polls when no new events |

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { awsCloudWatchLogsGetLogEventsStream } from '@datastream/aws/cloudwatch-logs'

await pipeline([
  createReadableStream(await awsCloudWatchLogsGetLogEventsStream({
    logGroupName: '/aws/lambda/my-function',
    logStreamName: 'stream-id',
  })),
])
```

### `awsCloudWatchLogsFilterLogEventsStream` <span class="badge">Readable</span> <span class="badge">async</span>

Filters log events across log streams in a log group. Auto-paginates through all matching results.

#### Options

Accepts `FilterLogEventsCommand` parameters:

| Option | Type | Description |
|--------|------|-------------|
| `logGroupName` | `string` | Log group name |
| `filterPattern` | `string` | CloudWatch Logs filter pattern |
| `startTime` | `number` | Start of time range (epoch ms) |
| `endTime` | `number` | End of time range (epoch ms) |

#### Example

```javascript
import { awsCloudWatchLogsFilterLogEventsStream } from '@datastream/aws/cloudwatch-logs'

const events = await awsCloudWatchLogsFilterLogEventsStream({
  logGroupName: '/aws/lambda/my-function',
  filterPattern: 'ERROR',
})
```

## S3

### `awsS3SetClient`

Set a custom S3 client. Mutates module-level state — not safe for concurrent multi-tenant use.

```javascript
import { S3Client } from '@aws-sdk/client-s3'
import { awsS3SetClient } from '@datastream/aws/s3'

awsS3SetClient(new S3Client({ region: 'eu-west-1' }))
```

### `awsS3GetObjectStream` <span class="badge">Readable</span> <span class="badge">async</span>

Downloads an object from S3 as a stream.

#### Options

Accepts all `GetObjectCommand` parameters plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `S3Client` | default client | Custom S3 client for this call |
| `Bucket` | `string` | — | S3 bucket name |
| `Key` | `string` | — | S3 object key |

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { awsS3GetObjectStream } from '@datastream/aws/s3'
import { csvParseStream } from '@datastream/csv'

await pipeline([
  await awsS3GetObjectStream({ Bucket: 'my-bucket', Key: 'data.csv' }),
  csvParseStream(),
])
```

### `awsS3PutObjectStream` <span class="badge">PassThrough</span>

Uploads data to S3 using `Upload` from `@aws-sdk/lib-storage`: a single `PutObject` for small bodies, a multipart upload otherwise. `pipeline()` waits for the upload to finish (it awaits the stream's `.result()`), and upload errors reject the pipeline.

#### Options

Accepts all S3 PutObject parameters plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `S3Client` | default client | Custom S3 client |
| `Bucket` | `string` | — | S3 bucket name |
| `Key` | `string` | — | S3 object key |
| `onProgress` | `(progress) => void` | — | Called with lib-storage's `httpUploadProgress` events: `{ loaded, total, part, Key, Bucket }` |
| `tags` | `{ Key: string, Value: string }[]` | — | S3 object tags |
| `partSize` | `number` | `5242880` (5 MiB) | Multipart part size in bytes (minimum 5 MiB). An upload can have at most 10,000 parts, so the default caps an object at about 50 GiB; raise it for larger objects |
| `queueSize` | `number` | `4` | Number of parts uploaded concurrently |

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { awsS3PutObjectStream } from '@datastream/aws/s3'
import { gzipCompressStream } from '@datastream/compress'

await pipeline([
  createReadableStream('id,name\r\n1,Alice\r\n'),
  gzipCompressStream(),
  awsS3PutObjectStream({
    Bucket: 'my-bucket',
    Key: 'output.csv.gz',
    partSize: 64 * 1024 * 1024, // 64 MiB parts: objects up to ~640 GiB
    queueSize: 4,
    onProgress: ({ loaded }) => console.log(`${loaded} bytes uploaded`),
  }),
])
```

### `awsS3ChecksumStream` <span class="badge">PassThrough</span>

Computes a multi-part S3 checksum while data passes through, for example to send checksums alongside an upload made with pre-signed URLs. Use the same `partSize` as the upload.

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ChecksumAlgorithm` | `string` | `"SHA256"` | `"SHA1"` or `"SHA256"` |
| `partSize` | `number` | `17179870` | Part size in bytes |
| `resultKey` | `string` | `"s3"` | Key in pipeline result |

#### Result

```javascript
{ checksum: 'base64hash-3', checksums: ['part1hash', 'part2hash', 'part3hash'], partSize: 17179870 }
```

## DynamoDB

### `awsDynamoDBSetClient`

Mutates module-level state — not safe for concurrent multi-tenant use.

```javascript
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { awsDynamoDBSetClient } from '@datastream/aws/dynamodb'

awsDynamoDBSetClient(new DynamoDBClient({ region: 'us-east-1' }))
```

### `awsDynamoDBQueryStream` <span class="badge">Readable</span> <span class="badge">async</span>

Queries a DynamoDB table and auto-paginates through all results.

#### Options

Accepts all `QueryCommand` parameters:

| Option | Type | Description |
|--------|------|-------------|
| `TableName` | `string` | DynamoDB table name |
| `KeyConditionExpression` | `string` | Query key condition |
| `ExpressionAttributeValues` | `object` | Expression values |

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { awsDynamoDBQueryStream } from '@datastream/aws/dynamodb'

await pipeline([
  createReadableStream(await awsDynamoDBQueryStream({
    TableName: 'Users',
    KeyConditionExpression: 'PK = :pk',
    ExpressionAttributeValues: { ':pk': { S: 'USER#123' } },
  })),
])
```

### `awsDynamoDBScanStream` <span class="badge">Readable</span> <span class="badge">async</span>

Scans an entire DynamoDB table with automatic pagination.

```javascript
import { awsDynamoDBScanStream } from '@datastream/aws/dynamodb'

const items = await awsDynamoDBScanStream({ TableName: 'Users' })
```

### `awsDynamoDBExecuteStatementStream` <span class="badge">Readable</span> <span class="badge">async</span>

Executes a PartiQL statement against DynamoDB with automatic pagination.

#### Options

Accepts `ExecuteStatementCommand` parameters:

| Option | Type | Description |
|--------|------|-------------|
| `Statement` | `string` | PartiQL statement |
| `Parameters` | `object[]` | Statement parameters |

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { awsDynamoDBExecuteStatementStream } from '@datastream/aws/dynamodb'

await pipeline([
  createReadableStream(await awsDynamoDBExecuteStatementStream({
    Statement: 'SELECT * FROM "Users" WHERE PK = ?',
    Parameters: [{ S: 'USER#123' }],
  })),
])
```

### `awsDynamoDBGetItemStream` <span class="badge">Readable</span> <span class="badge">async</span>

Batch gets items by keys. Automatically retries unprocessed keys with exponential backoff.

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `TableName` | `string` | — | DynamoDB table name |
| `Keys` | `object[]` | — | Array of key objects (at most 100, the `BatchGetItem` limit; more throws a `RangeError`) |
| `ConsistentRead` | `boolean` | — | Sent in the table's `KeysAndAttributes` |
| `ProjectionExpression` | `string` | — | Sent in the table's `KeysAndAttributes` |
| `ExpressionAttributeNames` | `object` | — | Sent in the table's `KeysAndAttributes` |
| `AttributesToGet` | `string[]` | — | Sent in the table's `KeysAndAttributes` |
| `ReturnConsumedCapacity` | `string` | — | Sent at the `BatchGetItem` request level |
| `retryMaxCount` | `number \| null` | `10` | Maximum retry attempts; `null` retries without limit |

When keys are still unprocessed after the last retry, the error's `cause` is `{ TableName, UnprocessedKeysCount }`. Key values are left out because they may contain personal data.

### `awsDynamoDBPutItemStream` <span class="badge">Writable</span>

Writes items to DynamoDB using `BatchWriteItem`. Automatically batches 25 items per request and retries unprocessed items with exponential backoff.

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `TableName` | `string` | DynamoDB table name |
| `retryMaxCount` | `number \| null` | Maximum retry attempts (default 10); `null` retries without limit |

#### Example

```javascript
import { pipeline, createReadableStream, createTransformStream } from '@datastream/core'
import { awsDynamoDBPutItemStream } from '@datastream/aws/dynamodb'

await pipeline([
  createReadableStream(items),
  createTransformStream((item, enqueue) => {
    enqueue({
      PK: { S: `USER#${item.id}` },
      SK: { S: 'PROFILE' },
      name: { S: item.name },
    })
  }),
  awsDynamoDBPutItemStream({ TableName: 'Users' }),
])
```

### `awsDynamoDBDeleteItemStream` <span class="badge">Writable</span>

Deletes items from DynamoDB using `BatchWriteItem`. Batches 25 items per request.

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `TableName` | `string` | DynamoDB table name |
| `retryMaxCount` | `number \| null` | Maximum retry attempts (default 10); `null` retries without limit |

#### Example

```javascript
import { awsDynamoDBDeleteItemStream } from '@datastream/aws/dynamodb'

awsDynamoDBDeleteItemStream({ TableName: 'Users' })
// Input chunks: { PK: { S: 'USER#1' }, SK: { S: 'PROFILE' } }
```

## DynamoDB Streams

### `awsDynamoDBStreamsSetClient`

Mutates module-level state — not safe for concurrent multi-tenant use.

```javascript
import { DynamoDBStreamsClient } from '@aws-sdk/client-dynamodb-streams'
import { awsDynamoDBStreamsSetClient } from '@datastream/aws/dynamodb-streams'

awsDynamoDBStreamsSetClient(new DynamoDBStreamsClient({ region: 'us-east-1' }))
```

### `awsDynamoDBStreamsGetRecordsStream` <span class="badge">Readable</span> <span class="badge">async</span>

Reads change records from a DynamoDB Streams shard with `GetRecords`, following `NextShardIterator`. Without polling it stops at the first empty response or when the shard is closed; with `pollingActive` it keeps waiting for new records until the shard closes.

DynamoDB Streams `GetRecords` has no `MillisBehindLatest` field (unlike Kinesis), so a non-polling read can't tell an empty page that is still behind the tip from one that is caught up. It may stop early on an open shard. To read a shard to its end, set `pollingActive`.

#### Options

Accepts `GetRecordsCommand` parameters plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `DynamoDBStreamsClient` | default client | Custom client for this call |
| `ShardIterator` | `string` | — | Shard iterator from `GetShardIterator` |
| `pollingActive` | `boolean` | `false` | Keep polling for new records |
| `pollingDelay` | `number` | `1000` | Delay (ms) between polls when no records |

`streamOptions.signal` aborts in-flight requests and the polling wait.

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { awsDynamoDBStreamsGetRecordsStream } from '@datastream/aws/dynamodb-streams'
import { objectCountStream } from '@datastream/object'

const count = objectCountStream()

const result = await pipeline([
  createReadableStream(await awsDynamoDBStreamsGetRecordsStream({
    ShardIterator: 'arn:aws:dynamodb:...',
  })),
  count,
])
// each chunk is a stream record: { eventName: 'INSERT', dynamodb: { Keys, NewImage, ... }, ... }
```

## Kinesis

### `awsKinesisSetClient`

Mutates module-level state — not safe for concurrent multi-tenant use.

```javascript
import { KinesisClient } from '@aws-sdk/client-kinesis'
import { awsKinesisSetClient } from '@datastream/aws/kinesis'

awsKinesisSetClient(new KinesisClient({ region: 'us-east-1' }))
```

### `awsKinesisGetRecordsStream` <span class="badge">Readable</span> <span class="badge">async</span>

Gets records from a Kinesis shard. Without polling it keeps reading until a page is empty and `MillisBehindLatest` is `0`, or the shard is closed.

After an empty page it waits `pollingDelay` before reading again. This applies both while polling and while catching up (`MillisBehindLatest > 0`), and keeps the reader under the Kinesis limit of 5 `GetRecords` calls per second per shard.

#### Options

Accepts `GetRecordsCommand` parameters plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ShardIterator` | `string` | — | Shard iterator |
| `pollingActive` | `boolean` | `false` | Keep polling for new records |
| `pollingDelay` | `number` | `1000` | Delay (ms) after an empty page, before the next `GetRecords` |

`streamOptions.signal` aborts in-flight requests and the wait.

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { awsKinesisGetRecordsStream } from '@datastream/aws/kinesis'

await pipeline([
  createReadableStream(await awsKinesisGetRecordsStream({
    ShardIterator: 'AAA...',
  })),
])
```

### `awsKinesisPutRecordsStream` <span class="badge">Writable</span>

Writes records to a Kinesis stream. Batches 500 records per `PutRecordsCommand`. A single record over 1 MiB throws a `RangeError`.

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `StreamName` | `string` | Kinesis stream name |
| `retryMaxCount` | `number \| null` | Maximum retry attempts for failed records (default 10); `null` retries without limit |

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { awsKinesisPutRecordsStream } from '@datastream/aws/kinesis'

await pipeline([
  createReadableStream(records),
  awsKinesisPutRecordsStream({ StreamName: 'my-stream' }),
])
```

## Lambda

### `awsLambdaSetClient`

Mutates module-level state — not safe for concurrent multi-tenant use.

```javascript
import { LambdaClient } from '@aws-sdk/client-lambda'
import { awsLambdaSetClient } from '@datastream/aws/lambda'

awsLambdaSetClient(new LambdaClient({ region: 'us-east-1' }))
```

### `awsLambdaReadableStream` <span class="badge">Readable</span>

Invokes a Lambda function with response streaming (`InvokeWithResponseStream`).

Also exported as `awsLambdaResponseStream`.

#### Options

Accepts `InvokeWithResponseStreamCommand` parameters. Pass an array to invoke multiple functions sequentially.

| Option | Type | Description |
|--------|------|-------------|
| `FunctionName` | `string` | Lambda function name or ARN |
| `Payload` | `string` | JSON payload |

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { awsLambdaReadableStream } from '@datastream/aws/lambda'
import { csvParseStream } from '@datastream/csv'

await pipeline([
  awsLambdaReadableStream({
    FunctionName: 'data-processor',
    Payload: JSON.stringify({ key: 'input.csv' }),
  }),
  csvParseStream(),
])
```

## SNS

### `awsSNSSetClient`

Mutates module-level state — not safe for concurrent multi-tenant use.

```javascript
import { SNSClient } from '@aws-sdk/client-sns'
import { awsSNSSetClient } from '@datastream/aws/sns'

awsSNSSetClient(new SNSClient({ region: 'us-east-1' }))
```

### `awsSNSPublishMessageStream` <span class="badge">Writable</span>

Publishes messages to an SNS topic. Batches 10 messages per `PublishBatchCommand`. A single entry over 256 KiB throws a `RangeError`.

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `TopicArn` | `string` | SNS topic ARN |
| `retryMaxCount` | `number \| null` | Maximum retry attempts for failed entries (default 10); `null` retries without limit |

#### Example

```javascript
import { pipeline, createReadableStream, createTransformStream } from '@datastream/core'
import { awsSNSPublishMessageStream } from '@datastream/aws/sns'

await pipeline([
  createReadableStream(events),
  createTransformStream((event, enqueue) => {
    enqueue({
      Id: event.id,
      Message: JSON.stringify(event),
    })
  }),
  awsSNSPublishMessageStream({ TopicArn: 'arn:aws:sns:us-east-1:123:my-topic' }),
])
```

## SQS

### `awsSQSSetClient`

Mutates module-level state — not safe for concurrent multi-tenant use.

```javascript
import { SQSClient } from '@aws-sdk/client-sqs'
import { awsSQSSetClient } from '@datastream/aws/sqs'

awsSQSSetClient(new SQSClient({ region: 'us-east-1' }))
```

### `awsSQSReceiveMessageStream` <span class="badge">Readable</span> <span class="badge">async</span>

Polls an SQS queue and yields messages until the queue is empty.

#### Options

Accepts `ReceiveMessageCommand` parameters plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `QueueUrl` | `string` | — | SQS queue URL |
| `MaxNumberOfMessages` | `number` | — | Max messages per poll (1-10) |
| `pollingActive` | `boolean` | `false` | Keep polling even when queue is empty |
| `pollingDelay` | `number` | `1000` | Delay (ms) between polls when queue is empty |

#### Example

```javascript
import { pipeline, createReadableStream, createTransformStream } from '@datastream/core'
import { awsSQSReceiveMessageStream, awsSQSDeleteMessageStream } from '@datastream/aws/sqs'

const QueueUrl = 'https://sqs.us-east-1.amazonaws.com/123/my-queue'

await pipeline([
  createReadableStream(await awsSQSReceiveMessageStream({ QueueUrl })),
  // process each message here, then map it to a delete batch entry
  createTransformStream((message, enqueue) => {
    enqueue({ Id: message.MessageId, ReceiptHandle: message.ReceiptHandle })
  }),
  awsSQSDeleteMessageStream({ QueueUrl }),
])
```

### `awsSQSSendMessageStream` <span class="badge">Writable</span>

Sends messages to an SQS queue. Batches up to 10 entries (and 256 KiB) per `SendMessageBatchCommand` and retries failed entries with exponential backoff. Each chunk is a batch entry: `{ Id, MessageBody, ... }`. A single entry over 256 KiB throws a `RangeError`.

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `QueueUrl` | `string` | SQS queue URL |
| `retryMaxCount` | `number \| null` | Maximum retry attempts for failed entries (default 10); `null` retries without limit |

### `awsSQSDeleteMessageStream` <span class="badge">Writable</span>

Deletes messages from an SQS queue. Batches up to 10 entries per `DeleteMessageBatchCommand` and retries failed entries with exponential backoff. Each chunk is a batch entry: `{ Id, ReceiptHandle }` (see the receive example above). A single entry over 256 KiB throws a `RangeError`.

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `QueueUrl` | `string` | SQS queue URL |
| `retryMaxCount` | `number \| null` | Maximum retry attempts for failed entries (default 10); `null` retries without limit |

## MSK IAM

### `awsMskIamMechanism`

Builds a kafkajs SASL `oauthbearer` configuration that signs Amazon MSK IAM auth tokens with [`aws-msk-iam-sasl-signer-js`](https://github.com/aws/aws-msk-iam-sasl-signer-js), using the default AWS credential provider chain. Pass it as `sasl` to [`kafkaConnect`](/docs/packages/kafka) (or to kafkajs directly).

```bash
npm install @datastream/aws aws-msk-iam-sasl-signer-js
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `region` | `string` | — | Required. AWS region of the MSK cluster; throws if missing |
| `ttl` | `number` | signer default | Token lifetime in ms |
| `awsDebugCreds` | `boolean` | `false` | Log credential details. Leaks credential material to your logs: local debugging only |

```javascript
import { kafkaConnect } from '@datastream/kafka'
import { awsMskIamMechanism } from '@datastream/aws/msk-iam'

const { producer, disconnect } = await kafkaConnect({
  brokers: ['b-1.my-cluster.kafka.us-east-1.amazonaws.com:9098'],
  ssl: true,
  sasl: awsMskIamMechanism({ region: 'us-east-1' }),
})
```

## Glue Schema Registry

### `awsGlueSchemaRegistrySetClient`

Sets the Glue client used by every resolver that isn't given its own `client`. Mutates module-level state — not safe for concurrent multi-tenant use.

```javascript
import { GlueClient } from '@aws-sdk/client-glue'
import { awsGlueSchemaRegistrySetClient } from '@datastream/aws/glue-schema-registry'

awsGlueSchemaRegistrySetClient(new GlueClient({ region: 'us-east-1' }))
```

### `awsGlueSchemaRegistryResolver`

Returns an async `resolve(schemaVersionId)` function that looks up a schema version with Glue `GetSchemaVersion` and caches the answer. Concurrent lookups for the same id share one request. Use it with `glueUnframeStream` from [`@datastream/schema-registry`](/docs/packages/schema-registry), which reports the `schemaVersionId` of each record.

```bash
npm install @datastream/aws @aws-sdk/client-glue
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `GlueClient` | set client, else a new one | Glue client for this resolver |
| `clientOptions` | `object` | — | Options for the `GlueClient` created when no client is given or set |
| `cacheExpiry` | `number` | `-1` | Cache lifetime in ms. `-1` caches forever (schema versions are immutable) |
| `maxCacheSize` | `number` | `1000` | Max cached versions; the oldest is evicted first. `0` disables caching, `null` never evicts |

```javascript
import { awsGlueSchemaRegistryResolver } from '@datastream/aws/glue-schema-registry'

const resolve = awsGlueSchemaRegistryResolver({ clientOptions: { region: 'us-east-1' } })

const { schemaVersionId, schemaDefinition, dataFormat } = await resolve(
  'b7b4a7f0-9c3d-4c1e-8a4b-2f6d7e8a9b0c',
)
// dataFormat: 'AVRO' | 'JSON' | 'PROTOBUF'
```
