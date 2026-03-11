# csv

## csvParseStream (Transform)

Takes CSV formatted string chunks and parses into flat object or array chunks.

### Options

| Option | Default | Description |
|---|---|---|
| `delimiterChar` | `,` | Field delimiter |
| `newlineChar` | `\r\n` | Row delimiter |
| `quoteChar` | `"` | Quote character |
| `escapeChar` | `quoteChar` | Escape character |
| `parser` | `csvQuotedParser` | Custom parser function |
| `chunkSize` | `2097152` | Buffer size before parsing begins |

### Example

```javascript
import { pipeline } from '@datastream/core'
import { csvParseStream } from '@datastream/csv'

const streams = [
	...
	csvParseStream(options),
	...
]

await pipeline(streams)
```

## csvFormatStream (Transform)

Takes array chunks and outputs CSV formatted strings. Each array is formatted as one CSV row.

### Options

| Option | Default | Description |
|---|---|---|
| `delimiterChar` | `,` | Field delimiter |
| `newlineChar` | `\r\n` | Row delimiter |
| `quoteChar` | `"` | Quote character |
| `escapeChar` | `quoteChar` | Escape character |

Values are automatically quoted when they contain: delimiters, newlines, quote characters, BOM, leading/trailing spaces, or formula triggers (`=`, `+`, `-`, `@`).

### Example

```javascript
import { pipeline } from '@datastream/core'
import { csvFormatStream, csvInjectHeaderStream, csvObjectToArray } from '@datastream/csv'

const headers = ['a', 'b', 'c']
const streams = [
	...
	csvObjectToArray({ headers }),
	csvInjectHeaderStream({ header: headers }),
	csvFormatStream(),
	...
]

await pipeline(streams)
```

## csvInjectHeaderStream (Transform)

Pushes a header array into the stream before the first data chunk. All subsequent chunks pass through unchanged.

### Options

| Option | Description |
|---|---|
| `header` | Array of header values to inject |

### Example

```javascript
import { csvInjectHeaderStream, csvFormatStream } from '@datastream/csv'

const streams = [
	...
	csvInjectHeaderStream({ header: ['a', 'b', 'c'] }),
	csvFormatStream(),
	...
]
```

## csvArrayToObject (Transform)

Converts array chunks to objects using provided header keys. Wrapper around `objectFromEntriesStream`.

### Options

| Option | Description |
|---|---|
| `headers` | Array of key names |

## csvObjectToArray (Transform)

Converts object chunks to arrays using provided header keys. Wrapper around `objectToEntriesStream`.

### Options

| Option | Description |
|---|---|
| `headers` | Array of key names |
