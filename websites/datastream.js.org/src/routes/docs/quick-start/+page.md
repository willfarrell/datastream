---
title: Quick Start
description: Install datastream and build your first stream pipeline in minutes.
---

## Install

```bash
npm install @datastream/core
```

Add packages as needed:

```bash
npm install @datastream/csv @datastream/validate @datastream/compress @datastream/file
```

## Your first pipeline

Read a CSV string, parse it, and collect the results:

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { csvDetectDelimitersStream, csvDetectHeaderStream, csvParseStream } from '@datastream/csv'
import { objectFromEntriesStream, objectCountStream } from '@datastream/object'

const csvData = 'name,age,city\r\nAlice,30,Toronto\r\nBob,25,Vancouver\r\nCharlie,35,Montreal'

const detectDelimiters = csvDetectDelimitersStream()
const detectHeader = csvDetectHeaderStream({
  delimiterChar: () => detectDelimiters.result().value.delimiterChar,
  newlineChar: () => detectDelimiters.result().value.newlineChar,
  quoteChar: () => detectDelimiters.result().value.quoteChar,
  escapeChar: () => detectDelimiters.result().value.escapeChar,
})
const count = objectCountStream()

const result = await pipeline([
  createReadableStream(csvData),
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
])

console.log(result)
// { csvDetectDelimiters: { delimiterChar: ',', ... }, csvDetectHeader: { header: ['name','age','city'] }, csvErrors: {}, objectCount: 3 }
```

## Add validation

Extend the pipeline with schema validation using JSON Schema:

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { csvDetectDelimitersStream, csvDetectHeaderStream, csvParseStream } from '@datastream/csv'
import { objectFromEntriesStream } from '@datastream/object'
import { validateStream } from '@datastream/validate'

const schema = {
  type: 'object',
  required: ['name', 'age', 'city'],
  properties: {
    name: { type: 'string' },
    age: { type: 'number' },
    city: { type: 'string' },
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
  createReadableStream(csvData),
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
  validateStream({ schema }),
])

console.log(result)
// { csvDetectDelimiters: {...}, csvDetectHeader: {...}, csvErrors: {}, validate: {} }
```

## Add file I/O

Read from and write to files (Node.js). `csvFormatStream` formats array rows, so convert the objects back to arrays with `csvObjectToArrayStream` and prepend the header row with `csvInjectHeaderStream`:

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
import { objectFromEntriesStream } from '@datastream/object'
import { validateStream } from '@datastream/validate'
import { gzipCompressStream } from '@datastream/compress'

const headers = ['name', 'age', 'city']
const schema = {
  type: 'object',
  required: headers,
  properties: {
    name: { type: 'string' },
    age: { type: 'number' },
    city: { type: 'string' },
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
  objectFromEntriesStream({
    keys: () => detectHeader.result().value.header,
  }),
  validateStream({ schema }),
  csvObjectToArrayStream({ headers }),
  csvInjectHeaderStream({ header: headers }),
  csvFormatStream(),
  gzipCompressStream(),
  await fileWriteStream({ path: './output.csv.gz' }),
])
```

## Next steps

- Learn about [Core Concepts](/docs/core-concepts) — stream types, pipeline patterns, and error handling
- Browse [Examples](/docs/examples/csv-etl) for complete real-world examples
- Explore the [core](/docs/packages/core) package API reference
