---
title: csv
description: Parse, format, detect, clean, and coerce CSV data streams.
---

Parse, format, detect, clean, and coerce CSV data.

## Install

```bash
npm install @datastream/csv
```

## `csvDetectDelimitersStream` <span class="badge">PassThrough</span>

Auto-detects the delimiter, newline, quote, and escape characters from the first chunk of data.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `chunkSize` | `number` | `1024` (1KB) | Minimum bytes to buffer before detecting |
| `resultKey` | `string` | `"csvDetectDelimiters"` | Key in pipeline result |

### Result

```javascript
{ delimiterChar: ',', newlineChar: '\r\n', quoteChar: '"', escapeChar: '"' }
```

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { csvDetectDelimitersStream } from '@datastream/csv'

const detect = csvDetectDelimitersStream()

const result = await pipeline([
  createReadableStream('name\tage\r\nAlice\t30'),
  detect,
])

console.log(result.csvDetectDelimiters)
// { delimiterChar: '\t', newlineChar: '\r\n', quoteChar: '"', escapeChar: '"' }
```

## `csvDetectHeaderStream` <span class="badge">Transform</span>

Detects and strips the header row. Outputs data rows only (without the header).

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `chunkSize` | `number` | `1024` (1KB) | Minimum bytes to buffer before detecting |
| `delimiterChar` | `string \| () => string` | `","` | Delimiter character or lazy function |
| `newlineChar` | `string \| () => string` | `"\r\n"` | Newline character or lazy function |
| `quoteChar` | `string \| () => string` | `'"'` | Quote character or lazy function |
| `escapeChar` | `string \| () => string` | quoteChar | Escape character or lazy function |
| `parser` | `function` | `csvQuotedParser` | Custom parser function |
| `resultKey` | `string` | `"csvDetectHeader"` | Key in pipeline result |

### Result

```javascript
{ header: ['name', 'age', 'city'] }
```

### Example

```javascript
import { csvDetectDelimitersStream, csvDetectHeaderStream } from '@datastream/csv'

const detectDelimiters = csvDetectDelimitersStream()
const detectHeader = csvDetectHeaderStream({
  delimiterChar: () => detectDelimiters.result().value.delimiterChar,
  newlineChar: () => detectDelimiters.result().value.newlineChar,
  quoteChar: () => detectDelimiters.result().value.quoteChar,
  escapeChar: () => detectDelimiters.result().value.escapeChar,
})
```

## `csvParseStream` <span class="badge">Transform</span>

Parses CSV text into arrays of field values (string arrays). Each output chunk is one row as `string[]`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `chunkSize` | `number` | `2097152` (2MB) | Input buffer size before first parse |
| `fieldMaxSize` | `number` | `16777216` (16MB) | Maximum size of a single field in bytes |
| `delimiterChar` | `string \| () => string` | `","` | Delimiter character or lazy function |
| `newlineChar` | `string \| () => string` | `"\r\n"` | Newline character or lazy function |
| `quoteChar` | `string \| () => string` | `'"'` | Quote character or lazy function |
| `escapeChar` | `string \| () => string` | quoteChar | Escape character or lazy function |
| `parser` | `function` | `csvQuotedParser` | Custom parser function |
| `resultKey` | `string` | `"csvErrors"` | Key in pipeline result |

#### Field size protection

A crafted CSV with an unterminated quoted field causes the parser to buffer the entire remaining input into a single field, consuming unbounded memory. An attacker can exploit this to exhaust process memory with a relatively small file. Setting `fieldMaxSize` caps per-field memory and aborts parsing with an error when exceeded. Always set this when parsing untrusted CSV input, and lower it from the default if your data has known field size bounds.

```javascript
// Parse with a 1MB field limit for untrusted input
csvParseStream({ fieldMaxSize: 1 * 1024 * 1024 })
```

### Result

Parse errors collected during processing:

```javascript
{
  UnterminatedQuote: { id: 'UnterminatedQuote', message: 'Unterminated quoted field', idx: [5] }
}
```

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { csvParseStream } from '@datastream/csv'

const result = await pipeline([
  createReadableStream('a,b,c\r\n1,2,3\r\n4,5,6'),
  csvParseStream(),
])

// Chunks emitted: ['a','b','c'], ['1','2','3'], ['4','5','6']
```

## `csvFormatStream` <span class="badge">Transform</span>

Formats objects or arrays back to CSV text.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `header` | `boolean \| string[]` | — | `true` to auto-detect from object keys, or provide array of column names |
| `delimiterChar` | `string` | `","` | Delimiter character |
| `newlineChar` | `string` | `"\r\n"` | Newline character |
| `quoteChar` | `string` | `'"'` | Quote character |
| `escapeChar` | `string` | quoteChar | Escape character |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { csvFormatStream } from '@datastream/csv'

await pipeline([
  createReadableStream([
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 },
  ]),
  csvFormatStream({ header: true }),
])

// Output: "name","age"\r\n"Alice","30"\r\n"Bob","25"\r\n
```

## `csvRemoveEmptyRowsStream` <span class="badge">Transform</span>

Removes rows where all fields are empty strings.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `onErrorEnqueue` | `boolean` | `false` | If `true`, empty rows are kept in stream (but still tracked) |
| `resultKey` | `string` | `"csvRemoveEmptyRows"` | Key in pipeline result |

### Result

```javascript
{ EmptyRow: { id: 'EmptyRow', message: 'Row is empty', idx: [3, 7] } }
```

## `csvRemoveMalformedRowsStream` <span class="badge">Transform</span>

Removes rows with an incorrect number of fields compared to the first row or the provided header.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `headers` | `string[] \| () => string[]` | — | Expected header array, or lazy function. If not provided, uses first row's field count |
| `onErrorEnqueue` | `boolean` | `false` | If `true`, malformed rows are kept in stream |
| `resultKey` | `string` | `"csvRemoveMalformedRows"` | Key in pipeline result |

### Result

```javascript
{ MalformedRow: { id: 'MalformedRow', message: 'Row has incorrect number of fields', idx: [2] } }
```

## `csvCoerceValuesStream` <span class="badge">Transform</span>

Converts string field values to typed JavaScript values. Works on objects (use after `objectFromEntriesStream`).

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `columns` | `object` | — | Map of column names to types. Without this, auto-coercion is used |
| `resultKey` | `string` | `"csvCoerceValues"` | Key in pipeline result |

### Auto-coercion rules

| Input | Output |
|-------|--------|
| `""` | `null` |
| `"true"` / `"false"` | `true` / `false` |
| Numeric strings | `Number` |
| ISO 8601 date strings | `Date` |
| JSON strings (`{...}`, `[...]`) | Parsed object/array |

### Column types

When specifying `columns`, valid types are: `"number"`, `"boolean"`, `"null"`, `"date"`, `"json"`.

```javascript
csvCoerceValuesStream({
  columns: { age: 'number', active: 'boolean', birthday: 'date' }
})
```

## `csvQuotedParser` / `csvUnquotedParser`

Standalone parser functions for use outside of streams. `csvUnquotedParser` is faster but does not handle quoted fields.

```javascript
import { csvQuotedParser } from '@datastream/csv'

const { rows, tail, numCols, idx, errors } = csvQuotedParser(
  'a,b,c\r\n1,2,3\r\n',
  { delimiterChar: ',', newlineChar: '\r\n', quoteChar: '"' },
  true
)
// rows: [['a','b','c'], ['1','2','3']]
```

## Full pipeline example

Detect, parse, clean, coerce, and validate:

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import {
  csvDetectDelimitersStream,
  csvDetectHeaderStream,
  csvParseStream,
  csvRemoveEmptyRowsStream,
  csvRemoveMalformedRowsStream,
  csvCoerceValuesStream,
} from '@datastream/csv'
import { objectFromEntriesStream } from '@datastream/object'
import { validateStream } from '@datastream/validate'

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
  csvRemoveEmptyRowsStream(),
  csvRemoveMalformedRowsStream({
    headers: () => detectHeader.result().value.header,
  }),
  objectFromEntriesStream({
    keys: () => detectHeader.result().value.header,
  }),
  csvCoerceValuesStream(),
  validateStream({ schema }),
])
```
