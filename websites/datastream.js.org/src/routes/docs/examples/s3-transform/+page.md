---
title: S3 to S3 — transform and re-upload
description: Read from S3, transform CSV data, and write back with datastream.
---

Read from S3, parse CSV, validate, re-format, and write back:

```javascript
import { pipeline } from '@datastream/core'
import { awsS3GetObjectStream, awsS3PutObjectStream } from '@datastream/aws'
import {
  csvDetectDelimitersStream,
  csvDetectHeaderStream,
  csvParseStream,
  csvRemoveMalformedRowsStream,
  csvFormatStream,
} from '@datastream/csv'
import { objectFromEntriesStream } from '@datastream/object'
import { validateStream } from '@datastream/validate'
import { gzipCompressStream, gzipDecompressStream } from '@datastream/compress'

const detectDelimiters = csvDetectDelimitersStream()
const detectHeader = csvDetectHeaderStream({
  delimiterChar: () => detectDelimiters.result().value.delimiterChar,
  newlineChar: () => detectDelimiters.result().value.newlineChar,
  quoteChar: () => detectDelimiters.result().value.quoteChar,
  escapeChar: () => detectDelimiters.result().value.escapeChar,
})

const result = await pipeline([
  await awsS3GetObjectStream({ Bucket: 'my-bucket', Key: 'input.csv.gz' }),
  gzipDecompressStream(),
  detectDelimiters,
  detectHeader,
  csvParseStream({
    delimiterChar: () => detectDelimiters.result().value.delimiterChar,
    newlineChar: () => detectDelimiters.result().value.newlineChar,
    quoteChar: () => detectDelimiters.result().value.quoteChar,
    escapeChar: () => detectDelimiters.result().value.escapeChar,
  }),
  csvRemoveMalformedRowsStream({
    headers: () => detectHeader.result().value.header,
  }),
  objectFromEntriesStream({
    keys: () => detectHeader.result().value.header,
  }),
  validateStream({ schema }),
  csvFormatStream({ header: true }),
  gzipCompressStream(),
  awsS3PutObjectStream({ Bucket: 'my-bucket', Key: 'output.csv.gz' }),
])
```
