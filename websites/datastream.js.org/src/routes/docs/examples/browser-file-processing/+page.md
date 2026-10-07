---
title: Browser file processing
description: Process files in the browser using the File System Access API and datastream.
---

Use the File System Access API to process files in the browser:

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream, fileWriteStream } from '@datastream/file'
import { charsetDecodeStream } from '@datastream/charset'
import {
  csvDetectDelimitersStream,
  csvDetectHeaderStream,
  csvParseStream,
  csvObjectToArrayStream,
  csvInjectHeaderStream,
  csvFormatStream,
} from '@datastream/csv'
import { objectFromEntriesStream, objectCountStream } from '@datastream/object'

const types = [{ accept: { 'text/csv': ['.csv'] } }]
const headers = ['name', 'city'] // columns to write, in order

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
  charsetDecodeStream({ charset: 'UTF-8' }), // File.stream() yields bytes; the CSV streams expect text
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
  csvObjectToArrayStream({ headers }),
  csvInjectHeaderStream({ header: headers }),
  csvFormatStream(),
  await fileWriteStream({ path: 'output.csv', types }),
])

console.log(result)
// { csvDetectDelimiters: {...}, csvDetectHeader: {...}, csvErrors: {}, objectCount: 500 }
```
