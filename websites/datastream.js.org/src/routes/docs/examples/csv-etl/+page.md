---
title: CSV ETL — validate and compress
description: Read, detect, parse, validate, re-format, compress, and write CSV files with datastream.
---

Read a CSV file, detect format, parse, validate, re-format, compress, and write:

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream, fileWriteStream } from '@datastream/file'
import {
  csvDetectDelimitersStream,
  csvDetectHeaderStream,
  csvParseStream,
  csvRemoveEmptyRowsStream,
  csvRemoveMalformedRowsStream,
  csvCoerceValuesStream,
  csvObjectToArrayStream,
  csvInjectHeaderStream,
  csvFormatStream,
} from '@datastream/csv'
import { objectFromEntriesStream } from '@datastream/object'
import { validateStream } from '@datastream/validate'
import { gzipCompressStream } from '@datastream/compress'

const headers = ['id', 'name', 'email']
const schema = {
  type: 'object',
  required: headers,
  properties: {
    id: { type: 'number' },
    name: { type: 'string', minLength: 1 },
    email: { type: 'string', format: 'email' },
  },
  additionalProperties: false,
}

const detectDelimiters = csvDetectDelimitersStream()
const detectHeader = csvDetectHeaderStream({
  delimiterChar: () => detectDelimiters.result().value.delimiterChar,
  newlineChar: () => detectDelimiters.result().value.newlineChar,
  quoteChar: () => detectDelimiters.result().value.quoteChar,
  escapeChar: () => detectDelimiters.result().value.escapeChar,
})

const result = await pipeline([
  await fileReadStream({ path: './input.csv' }), // file streams are async: always await
  detectDelimiters,
  detectHeader,
  csvParseStream({
    delimiterChar: () => detectDelimiters.result().value.delimiterChar,
    newlineChar: () => detectDelimiters.result().value.newlineChar,
    quoteChar: () => detectDelimiters.result().value.quoteChar,
    escapeChar: () => detectDelimiters.result().value.escapeChar,
  }),
  csvRemoveEmptyRowsStream(),
  csvRemoveMalformedRowsStream({
    headers: () => detectHeader.result().value.header,
  }),
  objectFromEntriesStream({
    keys: () => detectHeader.result().value.header,
  }),
  csvCoerceValuesStream(),
  validateStream({ schema }),
  // csvFormatStream formats array rows: convert objects back and re-add the header row
  csvObjectToArrayStream({ headers }),
  csvInjectHeaderStream({ header: headers }),
  csvFormatStream(),
  gzipCompressStream(),
  await fileWriteStream({ path: './output.csv.gz' }),
])

console.log(result)
// { csvDetectDelimiters: {...}, csvDetectHeader: {...}, csvErrors: {}, csvRemoveEmptyRows: {...}, csvRemoveMalformedRows: {...}, validate: {} }
// csvCoerceValuesStream adds no result entry
```
