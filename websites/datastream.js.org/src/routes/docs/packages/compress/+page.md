---
title: compress
description: Compression and decompression streams for gzip, deflate, brotli, and zstd.
---

Compression and decompression streams for gzip, deflate, brotli, and zstd.

## Install

```bash
npm install @datastream/compress
```

Each algorithm is also published as a subpath: `@datastream/compress/gzip`, `/deflate`, `/brotli` and `/zstd`. Prefer the subpath in browser bundles: the package root re-exports every algorithm (zstd only on Node.js), so importing it pulls in the browser brotli implementation and its optional peer dependency `brotli-wasm` even when you only need gzip.

```javascript
import { gzipCompressStream } from '@datastream/compress/gzip'
```

## gzip

### `gzipCompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `quality` | `number` | `-1` | Compression level (-1 to 9). -1 = default, 0 = none, 9 = best. Node.js only: the browser's `CompressionStream` has no level setting |
| `maxOutputSize` | `number \| null` | none | Maximum compressed output in bytes. The stream errors when exceeded |

### `gzipDecompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxOutputSize` | `number \| null` | `268435456` (256 MiB) | Maximum decompressed output in bytes. The stream errors when exceeded. `null` disables the limit |

### Example

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream, fileWriteStream } from '@datastream/file'
import { gzipCompressStream, gzipDecompressStream } from '@datastream/compress/gzip'

// Compress
await pipeline([
  await fileReadStream({ path: './data.csv' }),
  gzipCompressStream({ quality: 9 }),
  await fileWriteStream({ path: './data.csv.gz' }),
])

// Decompress
await pipeline([
  await fileReadStream({ path: './data.csv.gz' }),
  gzipDecompressStream(),
  await fileWriteStream({ path: './data.csv' }),
])
```

## deflate

### `deflateCompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `quality` | `number` | `-1` | Compression level (-1 to 9). Node.js only, as for gzip |
| `maxOutputSize` | `number \| null` | none | Maximum compressed output in bytes. The stream errors when exceeded |

### `deflateDecompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxOutputSize` | `number \| null` | `268435456` (256 MiB) | Maximum decompressed output in bytes. The stream errors when exceeded. `null` disables the limit |

## brotli

### `brotliCompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `quality` | `number` | `11` | Compression level (0 to 11) |
| `maxOutputSize` | `number \| null` | none | Maximum compressed output in bytes. The stream errors when exceeded |

### `brotliDecompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxOutputSize` | `number \| null` | `268435456` (256 MiB) | Maximum decompressed output in bytes. The stream errors when exceeded. `null` disables the limit |

## zstd <span class="badge">Node.js only</span>

Requires Node.js with zstd support (`node:zlib`). The `@datastream/compress/zstd` subpath has no `browser` export condition, and the browser build of the package root does not export the zstd streams.

### `zstdCompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `quality` | `number` | `3` | Compression level |
| `maxOutputSize` | `number \| null` | none | Maximum compressed output in bytes. The stream errors when exceeded |

### `zstdDecompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxOutputSize` | `number \| null` | `268435456` (256 MiB) | Maximum decompressed output in bytes. The stream errors when exceeded. `null` disables the limit |

## Output size protection

### Decompression bombs

A malicious compressed payload known as a "decompression bomb" can be as small as a few kilobytes but expand to gigabytes when decompressed, exhausting memory and crashing the process. Decompression streams stop at 256 MiB of output by default, so decompression is aborted before memory is exhausted. Lower the limit to what you expect for untrusted input, and only pass `null` for input you trust. The limit behaves the same in the Node.js and browser builds; when it is exceeded the stream errors with a `RangeError`: `Decompression output exceeds maxOutputSize (… bytes)`. The compression limit errors the same way (`Compression output exceeds maxOutputSize (… bytes)`).

```javascript
import { gzipDecompressStream } from '@datastream/compress/gzip'

// Limit decompressed output to 100MB
gzipDecompressStream({ maxOutputSize: 100 * 1024 * 1024 })

// Trusted input only: no limit
gzipDecompressStream({ maxOutputSize: null })
```

### Compression output limits

Compression streams also support `maxOutputSize` (in both builds) to cap compressed output size. This can be useful to enforce storage limits. There is no default limit.

```javascript
import { gzipCompressStream } from '@datastream/compress/gzip'

// Limit compressed output to 50MB
gzipCompressStream({ maxOutputSize: 50 * 1024 * 1024 })
```

## Platform support

| Algorithm | Node.js | Browser |
|-----------|---------|---------|
| gzip | `node:zlib` | `CompressionStream` |
| deflate | `node:zlib` | `CompressionStream` |
| brotli | `node:zlib` | [`brotli-wasm`](https://github.com/httptoolkit/brotli-wasm) (optional peer dependency: `npm install brotli-wasm`) |
| zstd | `node:zlib` | Not available (no browser export) |
