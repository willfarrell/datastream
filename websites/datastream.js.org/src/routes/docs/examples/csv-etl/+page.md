---
title: CSV ETL — validate and compress
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
  csvFormatStream,
} from '@datastream/csv'
import { objectFromEntriesStream } from '@datastream/object'
import { validateStream } from '@datastream/validate'
import { gzipCompressStream } from '@datastream/compress'

const schema = {
  type: 'object',
  required: ['id', 'name', 'email'],
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
  fileReadStream({ path: './input.csv' }),
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
  csvFormatStream({ header: true }),
  gzipCompressStream(),
  fileWriteStream({ path: './output.csv.gz' }),
])

console.log(result)
// { csvDetectDelimiters: {...}, csvDetectHeader: {...}, csvErrors: {}, csvRemoveEmptyRows: {...}, csvRemoveMalformedRows: {...}, validate: {} }
```
