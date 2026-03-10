---
title: Checksum and compress
---

Calculate a digest while compressing and writing a file:

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream, fileWriteStream } from '@datastream/file'
import { digestStream } from '@datastream/digest'
import { gzipCompressStream } from '@datastream/compress'

const digest = await digestStream({ algorithm: 'SHA2-256' })

const result = await pipeline([
  fileReadStream({ path: './data.csv' }),
  digest,
  gzipCompressStream(),
  fileWriteStream({ path: './data.csv.gz' }),
])

console.log(result)
// { digest: 'SHA2-256:e3b0c44298fc1c14...' }
```
