---
title: azure
description: Azure service streams for Blob Storage, Cosmos DB, Event Hubs, Queue Storage, and Service Bus, plus Event Hubs Kafka auth.
---

Azure service streams for Blob Storage, Cosmos DB, Event Hubs, Queue Storage, and Service Bus. A helper also signs in to the Event Hubs Kafka endpoint with Microsoft Entra ID.

<span class="badge">Node.js only</span> The package has no browser build.

## Install

```bash
npm install @datastream/azure
```

The Azure SDK packages are optional peer dependencies. Install only the ones for the services you use.

### Import from subpaths

| Subpath | Peer dependencies |
|---------|-------------------|
| `@datastream/azure/blob` | `@azure/storage-blob` |
| `@datastream/azure/cosmos` | `@azure/cosmos` |
| `@datastream/azure/event-hubs` | `@azure/event-hubs` |
| `@datastream/azure/event-hubs-kafka` | `@azure/identity` (or any `TokenCredential`) |
| `@datastream/azure/queue` | `@azure/storage-queue` |
| `@datastream/azure/service-bus` | `@azure/service-bus` |

```bash
# for example, Blob Storage only
npm install @datastream/azure @azure/storage-blob
```

```javascript
import { azureBlobDownloadStream } from '@datastream/azure/blob'
```

The package root (`@datastream/azure`) also works. It does not load any Azure SDK, so it imports fine with only some peer dependencies installed.

### Clients

Every stream takes a `client` option: the Azure SDK client to use. The package never creates a client for you, so you control credentials, retries, and the endpoint. Each section below names the client type it expects.

Every request gets `streamOptions.signal` as its `abortSignal`, so aborting the pipeline cancels the call in flight.

## Blob Storage

### `azureBlobDownloadStream` <span class="badge">Readable</span> <span class="badge">async</span>

Downloads a blob as a stream. If the stream errors or the signal aborts, the HTTP connection is released.

#### Options

Accepts `BlobClient.download()` options plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `BlobClient` | none | Required. The blob to download |
| `offset` | `number` | `0` | Byte position to start from |
| `count` | `number` | to the end | Number of bytes to read |

Throws `Blob.download has no body` if the service returns no body.

#### Example

```javascript
import { BlobServiceClient } from '@azure/storage-blob'
import { DefaultAzureCredential } from '@azure/identity'
import { pipeline } from '@datastream/core'
import { azureBlobDownloadStream } from '@datastream/azure/blob'
import { csvParseStream } from '@datastream/csv'

const client = new BlobServiceClient(
  'https://myaccount.blob.core.windows.net',
  new DefaultAzureCredential(),
)
  .getContainerClient('my-container')
  .getBlobClient('data.csv')

await pipeline([
  await azureBlobDownloadStream({ client }),
  csvParseStream(),
])
```

### `azureBlobUploadStream` <span class="badge">PassThrough</span>

Uploads a stream to a block blob with `BlockBlobClient.uploadStream()`. `pipeline()` waits for the upload to finish, and an upload error rejects the pipeline.

#### Options

Accepts `uploadStream()` options (such as `blobHTTPHeaders` and `metadata`) plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `BlockBlobClient` | none | Required. The blob to write |
| `bufferSize` | `number` | SDK default | Size in bytes of each block buffer |
| `maxConcurrency` | `number` | SDK default | Number of blocks uploaded at the same time |

#### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { azureBlobUploadStream } from '@datastream/azure/blob'
import { gzipCompressStream } from '@datastream/compress'

await pipeline([
  createReadableStream('id,name\r\n1,Alice\r\n'),
  gzipCompressStream(),
  azureBlobUploadStream({
    client: containerClient.getBlockBlobClient('output.csv.gz'),
    blobHTTPHeaders: { blobContentEncoding: 'gzip' },
  }),
])
```

## Cosmos DB

### `azureCosmosQueryStream` <span class="badge">Readable</span> <span class="badge">async</span>

Runs a SQL query on a container and yields each item. It follows every page of results.

#### Options

Accepts `FeedOptions` (such as `partitionKey` and `maxItemCount`) plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `Container` | none | Required. The container to query |
| `query` | `string \| { query, parameters }` | none | Required. The query, with optional parameters |

#### Example

```javascript
import { CosmosClient } from '@azure/cosmos'
import { pipeline, createReadableStream } from '@datastream/core'
import { azureCosmosQueryStream } from '@datastream/azure/cosmos'

const client = new CosmosClient({ endpoint, aadCredentials })
  .database('my-db')
  .container('users')

await pipeline([
  createReadableStream(await azureCosmosQueryStream({
    client,
    query: {
      query: 'SELECT * FROM c WHERE c.status = @status',
      parameters: [{ name: '@status', value: 'active' }],
    },
  })),
])
```

### `azureCosmosUpsertItemStream` <span class="badge">Writable</span>

Upserts items with `executeBulkOperations`, up to 100 operations per request. Each chunk is an item. The SDK reads the partition key from the item.

Throttled operations (status 429) are retried with exponential backoff, from 50 ms up to about 59 s between tries. Any other failure throws `azureCosmosBulkOperations has failed operations`. The error's `cause.statusCodes` lists the failed status codes. It never includes item bodies, so personal data stays out of your logs.

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `Container` | none | Required. The container to write |
| `retryMaxCount` | `number \| null` | `10` | Retries for throttled operations. `null` retries without limit |

### `azureCosmosDeleteItemStream` <span class="badge">Writable</span>

Deletes items the same way as the upsert stream: batched, with the same retry rules and options. Each chunk is `{ id, partitionKey }`.

## Event Hubs

### `azureEventHubsReceiveEventsStream` <span class="badge">Readable</span> <span class="badge">async</span>

Subscribes to an event hub and yields each event. The SDK reads the next page only after the pipeline uses the last one. A slow pipeline does not pile events up in memory. The subscription closes when the stream ends, errors, or aborts. Errors the SDK marks as `retryable` do not end the stream, because the SDK retries them itself.

#### Options

Accepts `subscribe()` options (such as `startPosition` and `maxBatchSize`) plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `EventHubConsumerClient` | none | Required. The consumer client |
| `partitionId` | `string` | all partitions | Read one partition only |
| `pollingActive` | `boolean` | `false` | Keep waiting for new events instead of ending |
| `maxWaitTimeInSeconds` | `number` | SDK default | How long the SDK waits before it delivers an empty page |

Without `pollingActive`, the stream ends on the first empty page. When you read all partitions, an empty page from any one partition ends the stream, even if other partitions still have events. To drain a backlog, set `partitionId` and read each partition on its own.

#### Example

```javascript
import { EventHubConsumerClient, earliestEventPosition } from '@azure/event-hubs'
import { pipeline, createReadableStream } from '@datastream/core'
import { azureEventHubsReceiveEventsStream } from '@datastream/azure/event-hubs'

const client = new EventHubConsumerClient('$Default', connectionString, 'my-hub')

await pipeline([
  createReadableStream(await azureEventHubsReceiveEventsStream({
    client,
    partitionId: '0',
    startPosition: earliestEventPosition,
    maxWaitTimeInSeconds: 5,
  })),
])
```

### `azureEventHubsSendEventsStream` <span class="badge">Writable</span>

Sends events in batches. Each chunk is an `EventData` object, such as `{ body }`. The SDK batch enforces the size limit. When a batch is full it is sent and a new one starts. A single event too big for an empty batch throws a `RangeError`.

#### Options

Accepts `createBatch()` options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `EventHubProducerClient` | none | Required. The producer client |
| `partitionKey` | `string` | none | Sends every event to the partition this key hashes to |
| `partitionId` | `string` | none | Sends every event to this partition |
| `maxSizeInBytes` | `number` | hub limit | Upper size limit of each batch |

## Event Hubs for Kafka

### `azureEventHubsKafkaMechanism`

Builds a kafkajs SASL `oauthbearer` configuration for the Event Hubs Kafka endpoint (`<namespace>.servicebus.windows.net:9093`). It gets a Microsoft Entra ID token for `https://<namespace>.servicebus.windows.net/.default`. Pass it as `sasl` to [`kafkaConnect`](/docs/packages/kafka) (or to kafkajs directly).

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `credential` | `TokenCredential` | none | Required. Any `@azure/identity` credential, such as `DefaultAzureCredential` |
| `namespace` | `string` | none | Required. The Event Hubs namespace name |

Throws a `TypeError` if either option is missing. The token provider throws if the credential returns no token.

```bash
npm install @datastream/azure @datastream/kafka @azure/identity
```

```javascript
import { DefaultAzureCredential } from '@azure/identity'
import { kafkaConnect } from '@datastream/kafka'
import { azureEventHubsKafkaMechanism } from '@datastream/azure/event-hubs-kafka'

const { producer, disconnect } = await kafkaConnect({
  brokers: ['my-namespace.servicebus.windows.net:9093'],
  ssl: true,
  sasl: azureEventHubsKafkaMechanism({
    credential: new DefaultAzureCredential(),
    namespace: 'my-namespace',
  }),
})
```

## Queue Storage

### `azureQueueReceiveMessagesStream` <span class="badge">Readable</span> <span class="badge">async</span>

Polls a queue and yields each message until the queue is empty.

#### Options

Accepts `receiveMessages()` options plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `QueueClient` | none | Required. The queue to read |
| `numberOfMessages` | `number` | `1` | Messages per request (1 to 32) |
| `visibilityTimeout` | `number` | `30` | Seconds a message stays hidden from other readers |
| `pollingActive` | `boolean` | `false` | Keep polling when the queue is empty |
| `pollingDelay` | `number` | `1000` | Delay (ms) between polls when the queue is empty |

#### Example

```javascript
import { QueueClient } from '@azure/storage-queue'
import { pipeline, createReadableStream } from '@datastream/core'
import {
  azureQueueReceiveMessagesStream,
  azureQueueDeleteMessageStream,
} from '@datastream/azure/queue'

const client = new QueueClient(queueUrl, credential)

await pipeline([
  createReadableStream(await azureQueueReceiveMessagesStream({
    client,
    numberOfMessages: 32,
  })),
  // process each message here
  azureQueueDeleteMessageStream({ client }),
])
```

### `azureQueueSendMessageStream` <span class="badge">Writable</span>

Sends each chunk as one message. Queue Storage has no batch API, so this makes one request per message. Each chunk is the message text.

#### Options

Accepts `sendMessage()` options (such as `messageTimeToLive` and `visibilityTimeout`) plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `QueueClient` | none | Required. The queue to write |

### `azureQueueDeleteMessageStream` <span class="badge">Writable</span>

Deletes each message. Each chunk is `{ messageId, popReceipt }`, so a received message works as is (see the receive example above).

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `QueueClient` | none | Required. The queue to delete from |

## Service Bus

### `azureServiceBusReceiveMessagesStream` <span class="badge">Readable</span> <span class="badge">async</span>

Receives messages from a queue or subscription and yields each one until none are left.

#### Options

Accepts `receiveMessages()` options plus:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `ServiceBusReceiver` | none | Required. A receiver in `peekLock` or `receiveAndDelete` mode |
| `maxMessageCount` | `number` | `10` | Messages per request |
| `maxWaitTimeInMs` | `number` | SDK default | How long one request waits for messages |
| `pollingActive` | `boolean` | `false` | Keep polling when no messages arrive |
| `pollingDelay` | `number` | `1000` | Delay (ms) between polls when no messages arrive |

#### Example

```javascript
import { ServiceBusClient } from '@azure/service-bus'
import { pipeline, createReadableStream } from '@datastream/core'
import {
  azureServiceBusReceiveMessagesStream,
  azureServiceBusCompleteMessageStream,
} from '@datastream/azure/service-bus'

const client = new ServiceBusClient(connectionString).createReceiver('my-queue')

await pipeline([
  createReadableStream(await azureServiceBusReceiveMessagesStream({ client })),
  // process each message here
  azureServiceBusCompleteMessageStream({ client }),
])
```

### `azureServiceBusSendMessagesStream` <span class="badge">Writable</span>

Sends messages in batches. Each chunk is a `ServiceBusMessage`, such as `{ body }`. The SDK batch enforces the size limit. When a batch is full it is sent and a new one starts. A single message too big for an empty batch throws a `RangeError`.

#### Options

Accepts `createMessageBatch()` options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `ServiceBusSender` | none | Required. The sender |
| `maxSizeInBytes` | `number` | entity limit | Upper size limit of each batch |

### `azureServiceBusCompleteMessageStream` <span class="badge">Writable</span>

Completes each message so Service Bus removes it from the queue. Each chunk is a received message. Use the same `peekLock` receiver that received it (see the receive example above).

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `ServiceBusReceiver` | none | Required. The receiver that received the messages |

## Glossary

### SASL

Simple Authentication and Security Layer. The sign-in step Kafka clients use. Event Hubs uses its `OAUTHBEARER` mechanism with an Entra ID token.

### SDK

Software Development Kit. Here, the `@azure/*` client packages.
