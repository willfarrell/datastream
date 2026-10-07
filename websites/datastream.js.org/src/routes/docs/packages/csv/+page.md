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

Auto-detects the delimiter, newline, quote, and escape characters from the first line of data. Input chunks can be strings or UTF-8 bytes (`Uint8Array`/`Buffer`); output is always decoded text, so multi-byte characters split across byte chunks stay intact.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxBufferSize` | `number \| null` | `16777216` (16MB) | Maximum characters to buffer while waiting for the first newline; throws a `RangeError` when exceeded. `null` means no limit. Detection runs as soon as the first newline is buffered |
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

Detects and strips the header row. Outputs data rows only (without the header). Input chunks can be strings or UTF-8 bytes; output is always decoded text. A leading UTF-8 BOM is removed.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxBufferSize` | `number \| null` | `16777216` (16MB) | Maximum characters to buffer while waiting for a complete header row; throws a `RangeError` when exceeded. `null` means no limit. The header row is looked for on the first chunk, then again each time the buffer doubles in size, so a long header row costs linear time |
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

Parses CSV text into arrays of field values (string arrays). Each output chunk is one row as `string[]`. Input chunks can be strings or UTF-8 bytes (`Uint8Array`/`Buffer`); multi-byte characters split across byte chunks are decoded correctly. A leading UTF-8 BOM is removed.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxFieldSize` | `number \| null` | `16777216` (16MB) | Maximum size of a single field; throws a `RangeError` when exceeded. `null` means no limit |
| `maxErrorRows` | `number \| null` | `1000` | Maximum row indexes kept in each error's `idx` array. `count` still holds the true total. `null` keeps every index |
| `delimiterChar` | `string \| () => string` | `","` | Delimiter character or lazy function |
| `newlineChar` | `string \| () => string` | `"\r\n"` | Newline character or lazy function |
| `quoteChar` | `string \| () => string` | `'"'` | Quote character or lazy function |
| `escapeChar` | `string \| () => string` | quoteChar | Escape character or lazy function |
| `parser` | `function` | `csvQuotedParser` | Custom parser function |
| `resultKey` | `string` | `"csvErrors"` | Key in pipeline result |

#### Field size protection

A crafted CSV with an unterminated quoted field causes the parser to buffer the entire remaining input into a single field, consuming unbounded memory. An attacker can exploit this to exhaust process memory with a relatively small file. Setting `maxFieldSize` caps per-field memory and aborts parsing with a `RangeError` when exceeded. Always set this when parsing untrusted CSV input, and lower it from the default if your data has known field size bounds.

```javascript
// Parse with a 1MB field limit for untrusted input
csvParseStream({ maxFieldSize: 1 * 1024 * 1024 })
```

### Result

Parse errors collected during processing. `idx` lists the affected row indexes (at most `maxErrorRows` of them) and `count` is the total number of affected rows.

```javascript
{
  UnterminatedQuote: { id: 'UnterminatedQuote', message: 'Unterminated quoted field', idx: [5], count: 1 },
  UnexpectedQuote: { id: 'UnexpectedQuote', message: 'Unexpected text after closing quote', idx: [2], count: 1 }
}
```

Text after a closing quote (for example `"a"b,c`) is kept in the same field (`["ab", "c"]`) so the row keeps its field count, and the row is reported as `UnexpectedQuote`.

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

Formats rows back to CSV text. Each input chunk is one row as an array of values (`null`/`undefined` become empty fields, `Date` values use ISO 8601). It does not write a header row or accept objects: convert objects to arrays with `csvObjectToArrayStream`, and prepend a header row with `csvInjectHeaderStream`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `delimiterChar` | `string` | `","` | Delimiter character |
| `newlineChar` | `string` | `"\r\n"` | Newline character |
| `quoteChar` | `string` | `'"'` | Quote character |
| `escapeChar` | `string` | quoteChar | Escape character |
| `escapeFormulae` | `boolean` | `true` | Prefix `'` to fields that start with `=`, `+`, `-`, `@`, TAB, or CR so spreadsheets treat them as text. Signed numbers such as `-1.5` are left as-is. Set to `false` to write fields unchanged |

#### Formula injection protection

Spreadsheet apps run a cell that starts with `=`, `+`, `-`, or `@` as a formula, even when the field is quoted. A crafted value such as `=HYPERLINK("http://evil.example","Click")` in exported data can then run when a user opens the file. With `escapeFormulae` on (the default), such fields get a leading `'`, so `=1+2` is written as `'=1+2`. The `'` becomes part of the value: a CSV parser reads it back as `'=1+2`. Pass `escapeFormulae: false` when the output is not meant for a spreadsheet and values must round-trip exactly.

### Example

```javascript
import { pipejoin, streamToString, createReadableStream } from '@datastream/core'
import {
  csvFormatStream,
  csvInjectHeaderStream,
  csvObjectToArrayStream,
} from '@datastream/csv'

const headers = ['name', 'age']
const output = await streamToString(pipejoin([
  createReadableStream([
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 },
  ]),
  csvObjectToArrayStream({ headers }),
  csvInjectHeaderStream({ header: headers }),
  csvFormatStream(),
]))

// output: "name,age\r\nAlice,30\r\nBob,25\r\n"
```

## `csvRemoveEmptyRowsStream` <span class="badge">Transform</span>

Removes rows where all fields are empty strings.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `onErrorEnqueue` | `boolean` | `false` | If `true`, empty rows are kept in stream (but still tracked) |
| `maxErrorRows` | `number \| null` | `1000` | Maximum row indexes kept in `idx`. `count` still holds the true total. `null` keeps every index |
| `resultKey` | `string` | `"csvRemoveEmptyRows"` | Key in pipeline result |

### Result

```javascript
{ EmptyRow: { id: 'EmptyRow', message: 'Row is empty', idx: [3, 7], count: 2 } }
```

## `csvRemoveMalformedRowsStream` <span class="badge">Transform</span>

Removes rows with an incorrect number of fields compared to the first row or the provided header.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `headers` | `string[] \| () => string[]` | — | Expected header array, or lazy function. If not provided, uses first row's field count |
| `onErrorEnqueue` | `boolean` | `false` | If `true`, malformed rows are kept in stream |
| `maxErrorRows` | `number \| null` | `1000` | Maximum row indexes kept in `idx`. `count` still holds the true total. `null` keeps every index |
| `resultKey` | `string` | `"csvRemoveMalformedRows"` | Key in pipeline result |

### Result

```javascript
{ MalformedRow: { id: 'MalformedRow', message: 'Row has incorrect number of fields', idx: [2], count: 1 } }
```

## `csvCoerceValuesStream` <span class="badge">Transform</span>

Converts string field values to typed JavaScript values. Works on objects (use after `csvArrayToObjectStream`). It has no pipeline result.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `columns` | `object` | — | Map of column names to types. Without this, auto-coercion is used |

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

## `csvArrayToObjectStream` / `csvObjectToArrayStream` <span class="badge">Transform</span>

`csvArrayToObjectStream` turns each row array into an object keyed by `headers`; `csvObjectToArrayStream` does the reverse. They wrap `objectFromEntriesStream` / `objectToEntriesStream` from `@datastream/object`. A `__proto__` header stays an own key and does not change the object's prototype.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `headers` | `string[] \| () => string[]` | — | Column names in row order, or lazy function |

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
  csvArrayToObjectStream,
  csvCoerceValuesStream,
} from '@datastream/csv'
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
  csvArrayToObjectStream({
    headers: () => detectHeader.result().value.header,
  }),
  csvCoerceValuesStream(),
  validateStream({ schema }),
])
```
