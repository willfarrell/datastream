---
title: aws
---

AWS service streams for S3, DynamoDB, Lambda, SNS, and SQS. Node.js only.

## Install

```bash
npm install @datastream/aws
```

Requires the corresponding AWS SDK v3 client packages:

```bash
npm install @aws-sdk/client-s3 @aws-sdk/lib-storage
npm install @aws-sdk/client-dynamodb
npm install @aws-sdk/client-lambda
npm install @aws-sdk/client-sns
npm install @aws-sdk/client-sqs
```

## S3

### `awsS3SetClient`

Set a custom S3 client. By default, FIPS endpoints are enabled for US and CA regions.

```javascript
import { S3Client } from '@aws-sdk/client-s3'
import { awsS3SetClient } from '@datastream/aws'

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
import { awsS3GetObjectStream } from '@datastream/aws'
import { csvParseStream } from '@datastream/csv'

await pipeline([
  await awsS3GetObjectStream({ Bucket: 'my-bucket', Key: 'data.csv' }),
  csvParseStream(),
])
```

### `awsS3PutObjectStream` <span class="badge">PassThrough</span>

Uploads data to S3 using multipart upload.

#### Options

Accepts all S3 PutObject parameters plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `S3Client` | default client | Custom S3 client |
| `Bucket` | `string` | — | S3 bucket name |
| `Key` | `string` | — | S3 object key |
| `onProgress` | `function` | — | Upload progress callback |
| `tags` | `object` | — | S3 object tags |

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { awsS3PutObjectStream } from '@datastream/aws'
import { gzipCompressStream } from '@datastream/compress'

await pipeline([
  createReadableStream(data),
  gzipCompressStream(),
  awsS3PutObjectStream({ Bucket: 'my-bucket', Key: 'output.csv.gz' }),
])
```

### `awsS3ChecksumStream` <span class="badge">PassThrough</span>

Computes a multi-part S3 checksum while data passes through. Designed for pre-signed URL uploads in the browser.

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

```javascript
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { awsDynamoDBSetClient } from '@datastream/aws'

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
import { awsDynamoDBQueryStream } from '@datastream/aws'

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
import { awsDynamoDBScanStream } from '@datastream/aws'

const items = await awsDynamoDBScanStream({ TableName: 'Users' })
```

### `awsDynamoDBGetItemStream` <span class="badge">Readable</span> <span class="badge">async</span>

Batch gets items by keys. Automatically retries unprocessed keys with exponential backoff.

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `TableName` | `string` | — | DynamoDB table name |
| `Keys` | `object[]` | — | Array of key objects |
| `retryMaxCount` | `number` | `10` | Maximum retry attempts |

### `awsDynamoDBPutItemStream` <span class="badge">Writable</span>

Writes items to DynamoDB using `BatchWriteItem`. Automatically batches 25 items per request and retries unprocessed items with exponential backoff.

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `TableName` | `string` | DynamoDB table name |
| `retryMaxCount` | `number` | Maximum retry attempts (default 10) |

#### Example

```javascript
import { pipeline, createReadableStream, createTransformStream } from '@datastream/core'
import { awsDynamoDBPutItemStream } from '@datastream/aws'

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
| `retryMaxCount` | `number` | Maximum retry attempts (default 10) |

#### Example

```javascript
import { awsDynamoDBDeleteItemStream } from '@datastream/aws'

awsDynamoDBDeleteItemStream({ TableName: 'Users' })
// Input chunks: { PK: { S: 'USER#1' }, SK: { S: 'PROFILE' } }
```

## Lambda

### `awsLambdaSetClient`

```javascript
import { LambdaClient } from '@aws-sdk/client-lambda'
import { awsLambdaSetClient } from '@datastream/aws'

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
import { awsLambdaReadableStream } from '@datastream/aws'
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

```javascript
import { SNSClient } from '@aws-sdk/client-sns'
import { awsSNSSetClient } from '@datastream/aws'

awsSNSSetClient(new SNSClient({ region: 'us-east-1' }))
```

### `awsSNSPublishMessageStream` <span class="badge">Writable</span>

Publishes messages to an SNS topic. Batches 10 messages per `PublishBatchCommand`.

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `TopicArn` | `string` | SNS topic ARN |

#### Example

```javascript
import { pipeline, createReadableStream, createTransformStream } from '@datastream/core'
import { awsSNSPublishMessageStream } from '@datastream/aws'

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

```javascript
import { SQSClient } from '@aws-sdk/client-sqs'
import { awsSQSSetClient } from '@datastream/aws'

awsSQSSetClient(new SQSClient({ region: 'us-east-1' }))
```

### `awsSQSReceiveMessageStream` <span class="badge">Readable</span> <span class="badge">async</span>

Polls an SQS queue and yields messages until the queue is empty.

#### Options

Accepts `ReceiveMessageCommand` parameters:

| Option | Type | Description |
|--------|------|-------------|
| `QueueUrl` | `string` | SQS queue URL |
| `MaxNumberOfMessages` | `number` | Max messages per poll (1-10) |

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { awsSQSReceiveMessageStream, awsSQSDeleteMessageStream } from '@datastream/aws'

await pipeline([
  createReadableStream(await awsSQSReceiveMessageStream({
    QueueUrl: 'https://sqs.us-east-1.amazonaws.com/123/my-queue',
  })),
  awsSQSDeleteMessageStream({
    QueueUrl: 'https://sqs.us-east-1.amazonaws.com/123/my-queue',
  }),
])
```

### `awsSQSSendMessageStream` <span class="badge">Writable</span>

Sends messages to an SQS queue. Batches 10 messages per `SendMessageBatchCommand`.

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `QueueUrl` | `string` | SQS queue URL |

### `awsSQSDeleteMessageStream` <span class="badge">Writable</span>

Deletes messages from an SQS queue. Batches 10 messages per `DeleteMessageBatchCommand`.

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `QueueUrl` | `string` | SQS queue URL |
