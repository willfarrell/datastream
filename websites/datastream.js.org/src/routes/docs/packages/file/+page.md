---
title: file
description: File read and write streams for Node.js and the browser.
---

File read and write streams for Node.js and the browser.

## Install

```bash
npm install @datastream/file
```

## `fileReadStream` <span class="badge">Readable</span>

Reads a file as a stream.

- **Node.js**: Uses `fs.createReadStream`
- **Browser**: Uses `window.showOpenFilePicker` (File System Access API)

### Options

| Option | Type | Description |
|--------|------|-------------|
| `path` | `string` | File path (Node.js) |
| `types` | `object[]` | File type filter for the file picker (see below) |

### Example — Node.js

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream } from '@datastream/file'

await pipeline([
  fileReadStream({ path: './data.csv' }),
])
```

### Example — Browser

```javascript
import { fileReadStream } from '@datastream/file'

const stream = await fileReadStream({
  types: [{ accept: { 'text/csv': ['.csv'] } }],
})
```

## `fileWriteStream` <span class="badge">Writable</span>

Writes a stream to a file.

- **Node.js**: Uses `fs.createWriteStream`
- **Browser**: Uses `window.showSaveFilePicker` (File System Access API)

### Options

| Option | Type | Description |
|--------|------|-------------|
| `path` | `string` | File path (Node.js), suggested file name (Browser) |
| `types` | `object[]` | File type filter |

### Example — Node.js

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { fileWriteStream } from '@datastream/file'

await pipeline([
  createReadableStream('hello world'),
  fileWriteStream({ path: './output.txt' }),
])
```

### Example — Browser

```javascript
import { fileWriteStream } from '@datastream/file'

const writable = await fileWriteStream({
  path: 'output.csv',
  types: [{ accept: { 'text/csv': ['.csv'] } }],
})
```

## File type filtering

The `types` option validates file extensions (Node.js) and configures the file picker dialog (Browser):

```javascript
const types = [
  {
    accept: {
      'text/csv': ['.csv'],
      'application/json': ['.json'],
    },
  },
]
```

On Node.js, if `types` is provided and the file extension doesn't match, an `"Invalid extension"` error is thrown.
