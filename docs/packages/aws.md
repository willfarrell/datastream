# aws

<details open>
<summary>Table of Contents</summary>
<ul>
<li>DynamoDB</li>
<ul>
<li><a href="/packages/aws#awsDynamoDBQueryStream">awsDynamoDBQueryStream</a></li>
<li><a href="/packages/aws#awsDynamoDBScanStream">awsDynamoDBScanStream</a></li>
<li><a href="/packages/aws#awsDynamoDBGetItemStream">awsDynamoDBGetItemStream</a></li>
<li><a href="/packages/aws#awsDynamoDBPutItemStream">awsDynamoDBPutItemStream</a></li>
<li><a href="/packages/aws#awsDynamoDBDeleteItemStream">awsDynamoDBDeleteItemStream</a></li>
</ul>
<li>Lambda</li>
<ul>
<li><a href="/packages/aws#awsLambdaResponseStream">awsLambdaResponseStream</a></li>
</ul>
<li>S3</li>
<ul>
<li><a href="/packages/aws#awsS3GetObjectStream">awsS3GetObjectStream</a></li>
<li><a href="/packages/aws#awsS3PutObjectStream">awsS3PutObjectStream</a></li>
</ul>
<li>SNS</li>
<ul>
<li><a href="/packages/aws#awsSNSPublishMessageStream">awsSNSPublishMessageStream</a></li>
</ul>
<li>SQS</li>
<ul>
<li><a href="/packages/aws#awsSQSReceiveMessageStream">awsSQSReceiveMessageStream</a></li>
<li><a href="/packages/aws#awsSQSDeleteMessageStream">awsSQSDeleteMessageStream</a></li>
<li><a href="/packages/aws#awsSQSSendMessageStream">awsSQSSendMessageStream</a></li>
</ul>
</ul>
</details>


## DynamoDB


### awsDynamoDBQueryStream (Readable)
<a id="awsDynamoDBQueryStream"></a>

Readable stream containing the results of a DynamoDB Query.

#### Options
See AWS documentation [DynamoDB/Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html)

IAM: `dynamodb:Query`

#### Egress chunk

A map of attributes and their values.

```json
{ 
  "string" : { 
    "B": blob,
    "BOOL": boolean,
    "BS": [ blob ],
    "L": [ "AttributeValue" ],
    "M": { "string" : "AttributeValue" },
    "N": "string",
    "NS": [ "string" ],
    "NULL": boolean,
    "S": "string",
    "SS": [ "string" ]
  }
}
```

See AWS documentation [DynamoDB/Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) for `Responses` structure.

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { awsDynamoDBQueryStream } from '@datastream/aws/dynamodb'

const streams = [
  await awsDynamoDBQueryStream(options),
  ...
]

await pipeline(streams)
```


### awsDynamoDBScanStream (Readable)
<a id="awsDynamoDBScanStream"></a>

Readable stream containing the results of a DynamoDB Scan.

#### Options
See AWS documentation [DynamoDB/Scan](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html)

IAM: `dynamodb:Scan`

#### Egress chunk

A map of attributes and their values.

```json
{ 
  "string" : { 
    "B": blob,
    "BOOL": boolean,
    "BS": [ blob ],
    "L": [ "AttributeValue" ],
    "M": { "string" : "AttributeValue" },
    "N": "string",
    "NS": [ "string" ],
    "NULL": boolean,
    "S": "string",
    "SS": [ "string" ]
  }
}
```

See AWS documentation [DynamoDB/Scan](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html) for `Responses` structure.

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { awsDynamoDBScanStream } from '@datastream/aws/dynamodb'

const streams = [
  await awsDynamoDBScanStream(options),
  ...
]

await pipeline(streams)
```

### awsDynamoDBGetItemStream (Readable)
<a id="awsDynamoDBGetItemStream"></a>

Readable stream containing the results of a DynamoDB BatchGetItems.

#### Options

- `TableName` (string): Name of the table to get items from.
- `Keys` (object[]): See AWS documentation [DynamoDB/BatchGetItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html) for `Keys` structure.
- `retryCount` (int) [0]: Starting retry count used for back-off timer, `3 ^ retryCount`
- `retryMaxCount` (int) [10]: Max number of retries before stopping

IAM: `dynamodb:BatchGetItem`

#### Egress chunk

A map of attributes and their values.

```json
{ 
  "string" : { 
    "B": blob,
    "BOOL": boolean,
    "BS": [ blob ],
    "L": [ "AttributeValue" ],
    "M": { "string" : "AttributeValue" },
    "N": "string",
    "NS": [ "string" ],
    "NULL": boolean,
    "S": "string",
    "SS": [ "string" ]
  }
}
```

See AWS documentation [DynamoDB/BatchGetItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html) for `Responses` structure.

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { awsDynamoDBGetItemStream } from '@datastream/aws/dynamodb'

const streams = [
  await awsDynamoDBGetItemStream(options),
  ...
]

await pipeline(streams)
```

### awsDynamoDBPutItemStream (Writable)
<a id="awsDynamoDBPutItemStream"></a>

Writable stream that sends items to DynamoDB BatchWriteItems.

#### Options

- `TableName` (string): Name of the table to get items from.
- `retryCount` (int) [0]: Starting retry count used for back-off timer, `3 ^ retryCount`
- `retryMaxCount` (int) [10]: Max number of retries before stopping

IAM: `dynamodb:BatchWriteItem`

#### Ingress chunk
A map of attributes and their values. Each entry in this map consists of an attribute name and an attribute value. Attribute values must not be null; string and binary type attributes must have lengths greater than zero; and set type attributes must not be empty. Requests that contain empty values are rejected with a ValidationException exception.

If you specify any attributes that are part of an index key, then the data types for those attributes must match those of the schema in the table's attribute definition.

```json
{ 
  "string" : { 
    "B": blob,
    "BOOL": boolean,
    "BS": [ blob ],
    "L": [ "AttributeValue" ],
    "M": { "string" : "AttributeValue" },
    "N": "string",
    "NS": [ "string" ],
    "NULL": boolean,
    "S": "string",
    "SS": [ "string" ]
  }
}
```

See AWS documentation [DynamoDB/BatchWriteItem.PutRequest.Item](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { awsDynamoDBPutItemStream } from '@datastream/aws/dynamodb'

const streams = [
  ...
  await awsDynamoDBPutItemStream(options)
]

await pipeline(streams)
```

### awsDynamoDBDeleteItemStream (Writable)
<a id="awsDynamoDBDeleteItemStream"></a>

Writable stream that sends keys to DynamoDB BatchWriteItems.

#### Options

- `TableName` (string) Required: Name of the table to get items from.
- `retryCount` (int) Optional (0): Starting retry count used for back-off timer, `3 ^ retryCount`
- `retryMaxCount` (int) Optional (10): Max number of retries before stopping

IAM: `dynamodb:BatchWriteItem`

#### Ingress chunk
A map of primary key attribute values that uniquely identify the item. Each entry in this map consists of an attribute name and an attribute value. For each primary key, you must provide all of the key attributes. For example, with a simple primary key, you only need to provide a value for the partition key. For a composite primary key, you must provide values for both the partition key and the sort key.


```json
{ 
  "string" : { 
    "B": blob,
    "BOOL": boolean,
    "BS": [ blob ],
    "L": [ "AttributeValue" ],
    "M": { "string" : "AttributeValue" },
    "N": "string",
    "NS": [ "string" ],
    "NULL": boolean,
    "S": "string",
    "SS": [ "string" ]
  }
}
```

See AWS documentation [DynamoDB/BatchWriteItem.DeleteRequest.Key](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { awsDynamoDBDeleteItemStream } from '@datastream/aws/dynamodb'

const streams = [
  ...
  await awsDynamoDBDeleteItemStream(options)
]

await pipeline(streams)
```

## Lambda

### awsLambdaResponseStream (Readable)
<a id="awsLambdaResponseStream"></a>

Readable stream from the response of a Lambda invoke.

### Options

See AWS documentation [Lambda/InvokeWithResponseStream](https://docs.aws.amazon.com/lambda/latest/dg/API_InvokeWithResponseStream.html)

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { awsLambdaResponseStream } from '@datastream/aws/lambda'

const streams = [
  await awsLambdaResponseStream(options),
  ...
]

await pipeline(streams)
```


## S3

<a id="awsS3GetObjectStream"></a>

### awsS3GetObjectStream (Readable)

#### Support

| node:stream | node:stream/web | Chrome | Edge | Firefox | Safari | Comments |
| ----------- | --------------- | ------ | ---- | ------- | ------ | -------- |
| 16.x        | 18.0.0          | N/A    | N/A  | N/A     | N/A    |          |

#### Options

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { awsS3GetObjectStream } from '@datastream/aws/s3'

const streams = [
  await awsS3GetObjectStream(options),
  ...
]

await pipeline(streams)
```


### awsS3PutObjectStream (Writable)
<a id="awsS3PutObjectStream"></a>

#### Support

| node:stream | node:stream/web | Chrome | Edge | Firefox | Safari | Comments |
| ----------- | --------------- | ------ | ---- | ------- | ------ | -------- |
| 16.x        | NO              | N/A    | N/A  | N/A     | N/A    |          |

#### Options

#### Example

```javascript
import { pipeline } from '@datastream/core'
import { awsS3PutObjectStream } from '@datastream/aws/s3'

const streams = [
  ...
  await awsS3PutObjectStream(options)
]

await pipeline(streams)
```

## SNS


### awsSNSPublishMessageStream (Writable)
<a id="awsSNSPublishMessageStream"></a>

## SQS


### awsSQSReceiveMessageStream (Readable)
<a id="awsSQSReceiveMessageStream"></a>



### awsSQSDeleteMessageStream (Writable)
<a id="awsSQSDeleteMessageStream"></a>


### awsSQSSendMessageStream (Writable)
<a id="awsSQSSendMessageStream"></a>
