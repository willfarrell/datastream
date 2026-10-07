---
title: file
description: File read and write streams for Node.js and the browser.
---

File read and write streams for Node.js and the browser.

## Install

```bash
npm install @datastream/file
```

## `fileReadStream` <span class="badge">Readable</span> <span class="badge">async</span>

Reads a file as a stream. Returns a Promise in both Node.js and the browser, so `await` it. Invalid options (path traversal, extension) reject the Promise.

- **Node.js**: Uses `fs.createReadStream`
- **Browser**: Uses `window.showOpenFilePicker` (File System Access API)

### Options

| Option | Type | Description |
|--------|------|-------------|
| `path` | `string` | File path (Node.js) |
| `basePath` | `string` | When provided, enforces that `path` resolves within `basePath`. Prevents path traversal and rejects symbolic links |
| `types` | `object[]` | File type filter for the file picker (see below) |

### Example — Node.js

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream } from '@datastream/file'

await pipeline([
  await fileReadStream({ path: './data.csv' }),
])
```

### Example — Browser

```javascript
import { fileReadStream } from '@datastream/file'

const stream = await fileReadStream({
  types: [{ accept: { 'text/csv': ['.csv'] } }],
})
```

## `fileWriteStream` <span class="badge">Writable</span> <span class="badge">async</span>

Writes a stream to a file. Returns a Promise in both Node.js and the browser, so `await` it.

- **Node.js**: Uses `fs.createWriteStream`
- **Browser**: Uses `window.showSaveFilePicker` (File System Access API)

### Options

| Option | Type | Description |
|--------|------|-------------|
| `path` | `string` | File path (Node.js), suggested file name (Browser) |
| `basePath` | `string` | When provided, enforces that `path` resolves within `basePath`. Prevents path traversal and rejects symbolic links |
| `types` | `object[]` | File type filter |

### Example — Node.js

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { fileWriteStream } from '@datastream/file'

await pipeline([
  createReadableStream('hello world'),
  await fileWriteStream({ path: './output.txt' }),
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

## Stream options (Node.js)

The second argument is passed to `fs.createReadStream` / `fs.createWriteStream`, so fs options such as `flags`, `encoding`, `start`, `end` and `mode` work as documented by Node.js. The datastream-only options `objectMode`, `readableObjectMode`, `writableObjectMode` and `chunkSize` are removed first, because fs streams are byte streams.

```javascript
// Append instead of overwriting
await fileWriteStream({ path: './log.csv' }, { flags: 'a' })

// Read strings instead of Buffers
await fileReadStream({ path: './data.csv' }, { encoding: 'utf8' })
```

With `basePath`, the file is opened with `O_NOFOLLOW`, so `flags` and `mode` are applied at open time: a flag containing `a` appends (the file is not truncated) and a flag containing `x` fails with `EEXIST` if the file exists. Any other write truncates the file.

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

On Node.js, if `types` is provided and the file extension doesn't match, the Promise rejects with an `"Invalid extension"` error.

## Security

When accepting file paths from user input, always use an absolute `path` or set `basePath` to prevent path traversal attacks (e.g., `../../etc/passwd`). Relative paths without a `basePath` constraint can resolve outside the intended directory.

`basePath` is opt-in. When provided, `path` must resolve to a file inside `basePath` (names that only start with `..`, like `..cache`, are allowed), the real path of its parent directory must also be inside the real `basePath` (so a symlinked directory cannot escape), and a symbolic link as the file itself is rejected. A missing parent directory rejects with `"Path not found"`. When omitted, no path restriction is applied.

These checks run before the file is opened. A local attacker who can swap a parent directory for a symlink between the check and the open is out of scope; `O_NOFOLLOW` only protects the file itself.

```javascript
// Restrict reads to a specific directory
await fileReadStream({ path: userInput, basePath: '/data/uploads', types })

// Convenience helper for cwd-scoped reads
const safeFileRead = (path, types) =>
  fileReadStream({ path, basePath: process.cwd(), types })
```
