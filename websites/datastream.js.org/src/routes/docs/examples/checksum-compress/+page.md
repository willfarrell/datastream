---
title: Checksum and compress
description: Calculate a digest while compressing and writing a file with datastream.
---

Calculate a digest while compressing and writing a file:

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream, fileWriteStream } from '@datastream/file'
import { digestStream } from '@datastream/digest'
import { gzipCompressStream } from '@datastream/compress'

const digest = digestStream({ algorithm: 'SHA2-256' })

const result = await pipeline([
  await fileReadStream({ path: './data.csv' }), // file streams are async: always await
  digest,
  gzipCompressStream(),
  await fileWriteStream({ path: './data.csv.gz' }),
])

console.log(result)
// { digest: 'SHA2-256:e3b0c44298fc1c14...' }
```
