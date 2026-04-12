---
title: compress
description: Compression and decompression streams for gzip, deflate, brotli, and zstd.
---

Compression and decompression streams for gzip, deflate, brotli, and zstd.

## Install

```bash
npm install @datastream/compress
```

## gzip

### `gzipCompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `quality` | `number` | `-1` | Compression level (-1 to 9). -1 = default, 0 = none, 9 = best |

### `gzipDecompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxOutputSize` | `number` | — | Maximum decompressed output in bytes. Destroys the stream with an error when exceeded |

### Example

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream, fileWriteStream } from '@datastream/file'
import { gzipCompressStream, gzipDecompressStream } from '@datastream/compress'

// Compress
await pipeline([
  fileReadStream({ path: './data.csv' }),
  gzipCompressStream({ quality: 9 }),
  fileWriteStream({ path: './data.csv.gz' }),
])

// Decompress
await pipeline([
  fileReadStream({ path: './data.csv.gz' }),
  gzipDecompressStream(),
  fileWriteStream({ path: './data.csv' }),
])
```

## deflate

### `deflateCompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `quality` | `number` | `-1` | Compression level (-1 to 9) |

### `deflateDecompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxOutputSize` | `number` | — | Maximum decompressed output in bytes. Destroys the stream with an error when exceeded |

## brotli

### `brotliCompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `quality` | `number` | `11` | Compression level (0 to 11) |

### `brotliDecompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxOutputSize` | `number` | — | Maximum decompressed output in bytes. Destroys the stream with an error when exceeded |

## zstd <span class="badge">Node.js only</span>

Requires Node.js with zstd support.

### `zstdCompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `quality` | `number` | `3` | Compression level |

### `zstdDecompressStream` <span class="badge">Transform</span>

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxOutputSize` | `number` | — | Maximum decompressed output in bytes. Destroys the stream with an error when exceeded |

## Decompression bomb protection

A malicious compressed payload known as a "decompression bomb" can be as small as a few kilobytes but expand to gigabytes when decompressed, exhausting memory and crashing the process. Setting `maxOutputSize` ensures decompression is aborted before memory is exhausted. Always set this when decompressing untrusted input.

```javascript
import { gzipDecompressStream } from '@datastream/compress'

// Limit decompressed output to 100MB
gzipDecompressStream({ maxOutputSize: 100 * 1024 * 1024 })
```

## Platform support

| Algorithm | Node.js | Browser |
|-----------|---------|---------|
| gzip | `node:zlib` | `CompressionStream` |
| deflate | `node:zlib` | `CompressionStream` |
| brotli | `node:zlib` | `CompressionStream` |
| zstd | `node:zlib` | Not supported |
