---
title: charset
description: Character set detection, decoding, and encoding streams.
---

Character set detection, decoding, and encoding streams.

## Install

```bash
npm install @datastream/charset
```

Each stream is also available as a subpath: `@datastream/charset/detect`, `@datastream/charset/decode` and `@datastream/charset/encode`.

## `charsetDetectStream` <span class="badge">PassThrough</span>

Detects the character encoding of the data passing through by analyzing byte patterns.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `resultKey` | `string` | `"charset"` | Key in pipeline result |

### Result

Returns the most likely charset with confidence score:

```javascript
{ charset: 'UTF-8', confidence: 80 }
```

### Supported charsets

UTF-8, UTF-16BE, UTF-16LE, UTF-32BE, UTF-32LE, Shift_JIS, ISO-2022-JP, ISO-2022-CN, ISO-2022-KR, GB18030, EUC-JP, EUC-KR, Big5, ISO-8859-1, ISO-8859-2, ISO-8859-5, ISO-8859-6, ISO-8859-7, ISO-8859-8, ISO-8859-9, windows-1250, windows-1251, windows-1252, windows-1254, windows-1256, KOI8-R

### Example

```javascript
import { pipeline } from '@datastream/core'
import { fileReadStream } from '@datastream/file'
import { charsetDetectStream, charsetDecodeStream } from '@datastream/charset'

const detect = charsetDetectStream()

const result = await pipeline([
  await fileReadStream({ path: './data.csv' }),
  detect,
])

console.log(result.charset)
// { charset: 'UTF-8', confidence: 80 }
```

## `charsetDecodeStream` <span class="badge">Transform</span>

Decodes binary data to text using the specified character encoding. Node.js uses [`iconv-lite`](https://github.com/pillarjs/iconv-lite); the browser uses the native `TextDecoderStream`, so the available encodings are those of the WHATWG Encoding standard.

An unknown `charset` throws when the stream is created, in both builds (`Unsupported encoding "…"` on Node.js, `Unsupported web encoding "…"` in the browser). There is no silent UTF-8 fallback.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `charset` | `string` | `"UTF-8"` | Character encoding name (e.g. `"UTF-8"`, `"ISO-8859-1"`) |

### Example

```javascript
import { charsetDecodeStream } from '@datastream/charset'

charsetDecodeStream({ charset: 'ISO-8859-1' })
```

## `charsetEncodeStream` <span class="badge">Transform</span>

Encodes text to binary using the specified character encoding. Node.js uses `iconv-lite` and supports any encoding it knows. The browser uses the native `TextEncoderStream`, which only produces UTF-8: the labels `utf8` / `utf-8` are accepted in any letter case, and any other charset throws `Web only supports UTF-8 encoding`.

An unknown `charset` throws when the stream is created, in both builds.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `charset` | `string` | `"UTF-8"` | Character encoding name. UTF-8 only in the browser |

### Example

```javascript
import { charsetEncodeStream } from '@datastream/charset'

charsetEncodeStream({ charset: 'UTF-8' })
```
