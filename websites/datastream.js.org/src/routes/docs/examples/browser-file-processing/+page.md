---
title: Browser file processing
---

Use the File System Access API to process files in the browser:

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream, fileWriteStream } from '@datastream/file'
import {
  csvDetectDelimitersStream,
  csvDetectHeaderStream,
  csvParseStream,
  csvFormatStream,
} from '@datastream/csv'
import { objectFromEntriesStream, objectCountStream } from '@datastream/object'

const types = [{ accept: { 'text/csv': ['.csv'] } }]

const detectDelimiters = csvDetectDelimitersStream()
const detectHeader = csvDetectHeaderStream({
  delimiterChar: () => detectDelimiters.result().value.delimiterChar,
  newlineChar: () => detectDelimiters.result().value.newlineChar,
  quoteChar: () => detectDelimiters.result().value.quoteChar,
  escapeChar: () => detectDelimiters.result().value.escapeChar,
})
const count = objectCountStream()

const result = await pipeline([
  await fileReadStream({ types }),
  detectDelimiters,
  detectHeader,
  csvParseStream({
    delimiterChar: () => detectDelimiters.result().value.delimiterChar,
    newlineChar: () => detectDelimiters.result().value.newlineChar,
    quoteChar: () => detectDelimiters.result().value.quoteChar,
    escapeChar: () => detectDelimiters.result().value.escapeChar,
  }),
  objectFromEntriesStream({
    keys: () => detectHeader.result().value.header,
  }),
  count,
  csvFormatStream({ header: true }),
  await fileWriteStream({ path: 'output.csv', types }),
])

console.log(result)
// { count: 500 }
```
