---
title: API pagination to DynamoDB
---

Fetch paginated API data and write to DynamoDB:

```javascript
import { pipeline } from '@datastream/core'
import { fetchReadableStream } from '@datastream/fetch'
import { objectCountStream } from '@datastream/object'
import { awsDynamoDBPutItemStream } from '@datastream/aws'
import { createTransformStream } from '@datastream/core'

const count = objectCountStream()

const result = await pipeline([
  fetchReadableStream({
    url: 'https://api.example.com/users',
    dataPath: 'data',
    nextPath: 'pagination.next_url',
  }),
  createTransformStream((item, enqueue) => {
    enqueue({
      PK: { S: `USER#${item.id}` },
      SK: { S: `PROFILE` },
      name: { S: item.name },
      email: { S: item.email },
    })
  }),
  count,
  awsDynamoDBPutItemStream({ TableName: 'Users' }),
])

console.log(result)
// { count: 450 }
```
