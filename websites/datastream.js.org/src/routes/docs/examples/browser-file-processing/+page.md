---
title: Browser file processing
description: Process files in the browser using the File System Access API and datastream.
---

Use the File System Access API to process files in the browser. `File.stream()` yields bytes, and the CSV detect streams decode UTF-8 bytes themselves, so no decode step is needed. For a file in another encoding, add `charsetDecodeStream({ charset })` from [`@datastream/charset`](/docs/packages/charset) before `detectDelimiters`.

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream, fileWriteStream } from '@datastream/file'
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
