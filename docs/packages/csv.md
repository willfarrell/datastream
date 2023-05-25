# csv

## csvParseStream (Transform)

Takes CSV formatted string chunks and parses into flat object or array chunks.

### Options

See csv-rex documentation [parse](https://csv-rex.js.org/#/docs/parse)

### Example

```javascript
import { pipeline } from '@datastream/core'
import { csvParseStream } from '@datastream/csv/parse'

const streams = [
	...
	csvParseStream(option),
	...
]

await pipeline(streams)
```

## csvFormatStream (Transform)

Takes a flat object or array chunks and outputs CSV formatted string.

### Options

See csv-rex documentation [format](https://csv-rex.js.org/#/docs/format)

### Example

```javascript
import { pipeline } from '@datastream/core'
import { csvFormatStream } from '@datastream/csv/format'

const streams = [
	...
	csvFormatStream(option),
	...
]

await pipeline(streams)
```