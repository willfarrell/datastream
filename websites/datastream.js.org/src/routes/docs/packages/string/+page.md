---
title: string
description: String manipulation streams for splitting, replacing, counting, and measuring text.
---

String manipulation streams — split, replace, count, and measure text data.

## Install

```bash
npm install @datastream/string
```

To stream a string, use `createReadableStream` from `@datastream/core`.

## `stringLengthStream` <span class="badge">PassThrough</span>

Counts the total character length of all chunks.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `resultKey` | `string` | `"length"` | Key in pipeline result |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { stringLengthStream } from '@datastream/string'

const length = stringLengthStream()
const result = await pipeline([
  createReadableStream('hello world'),
  length,
])

console.log(result)
// { length: 11 }
```

## `stringCountStream` <span class="badge">PassThrough</span>

Counts occurrences of a substring across all chunks.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `substr` | `string` | — | Substring to count |
| `resultKey` | `string` | `"stringCount"` | Key in pipeline result |

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { stringCountStream } from '@datastream/string'

const count = stringCountStream({ substr: '\n' })
const result = await pipeline([
  createReadableStream('line1\nline2\nline3'),
  count,
])

console.log(result)
// { stringCount: 2 }
```

## `stringSplitStream` <span class="badge">Transform</span>

Splits streaming text by a separator, emitting one chunk per segment. Handles splits that cross chunk boundaries.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `separator` | `string` | — | String to split on |
| `maxBufferSize` | `number \| null` | `16777216` | Throws a `RangeError` if the text after the last separator grows past this many characters. `null` = unlimited |

### Example

```javascript
import { pipeline, createReadableStream, streamToArray, pipejoin } from '@datastream/core'
import { stringSplitStream } from '@datastream/string'

const river = pipejoin([
  createReadableStream('alice,bob,charlie'),
  stringSplitStream({ separator: ',' }),
])

const output = await streamToArray(river)
// ['alice', 'bob', 'charlie']
```

## `stringReplaceStream` <span class="badge">Transform</span>

Replaces pattern matches in streaming text, including matches that span chunk boundaries. The output is the same as `input.replace(pattern, replacement)` on the whole text (`replaceAll` for a string pattern), as long as every match fits in the held-back window:

- A string pattern holds back `pattern.length - 1` characters, so its matches always fit.
- A RegExp holds back the latest chunk, or `maxMatchLength - 1` characters when `maxMatchLength` is set. Lookahead counts toward the match length. A match that ends at the end of the buffer is held until more data arrives, so greedy matches aren't split.
- Lookbehind, `^` and `\b` can see up to `lookbehind` characters that were already emitted.
- A sticky (`y`) RegExp stops at its first failed match, like `String.prototype.replace`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pattern` | `string \| RegExp` | — | Pattern to search for. A RegExp needs the `g` or `y` flag |
| `replacement` | `string \| function` | — | Replacement template (`$$`, `$&`, `$1`–`$99`, `$<name>`, `` $` ``, `$'`) or function |
| `maxMatchLength` | `number` | — | RegExp only. Longest possible match. Without it, a match longer than the latest chunk can be missed |
| `lookbehind` | `number` | `16` | Number of already-emitted characters kept as context for lookbehind, `^` and `\b` |
| `maxBufferSize` | `number \| null` | `16777216` | Throws a `RangeError` if the held-back text grows past this many characters. `null` = unlimited |

A template that uses `` $` `` or `$'` needs the whole stream, so the stream is buffered and replaced at flush (still limited by `maxBufferSize`).

A function replacement is called like a `String.prototype.replace` callback, `(match, p1, …, offset, string, groups)`. `offset` counts from the start of the stream. `string` is only the currently buffered text (with up to `lookbehind` characters before it), not the whole stream. `groups` is always passed, and is `undefined` when the pattern has no named groups.

### Example

```javascript
import { stringReplaceStream } from '@datastream/string'

stringReplaceStream({ pattern: /\t/g, replacement: ',' })
```

## `stringMinimumFirstChunkSizeStream` <span class="badge">Transform</span>

Buffers data until the first chunk meets a minimum size, then passes all subsequent chunks through unchanged.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `chunkSize` | `number` | `1024` (1KB) | Minimum first chunk size in characters |

## `stringMinimumChunkSizeStream` <span class="badge">Transform</span>

Buffers every chunk to meet a minimum size before emitting. Unlike `stringMinimumFirstChunkSizeStream` which only buffers the first chunk then passes through, this continues buffering all subsequent chunks that are smaller than `chunkSize`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `chunkSize` | `number` | `1024` (1KB) | Minimum chunk size in characters |

## `stringSkipConsecutiveDuplicatesStream` <span class="badge">Transform</span>

Skips consecutive duplicate string chunks.

```javascript
import { stringSkipConsecutiveDuplicatesStream } from '@datastream/string'

// Input chunks: 'a', 'a', 'b', 'a' → Output: 'a', 'b', 'a'
```
