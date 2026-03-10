---
title: charset
---

Character set detection, decoding, and encoding streams.

## Install

```bash
npm install @datastream/charset
```

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
  fileReadStream({ path: './data.csv' }),
  detect,
])

console.log(result.charset)
// { charset: 'UTF-8', confidence: 80 }
```

## `charsetDecodeStream` <span class="badge">Transform</span>

Decodes binary data to text using the specified character encoding. Uses `TextDecoderStream` internally.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `charset` | `string` | — | Character encoding name (e.g. `"UTF-8"`, `"ISO-8859-1"`) |

### Example

```javascript
import { charsetDecodeStream } from '@datastream/charset'

charsetDecodeStream({ charset: 'ISO-8859-1' })
```

## `charsetEncodeStream` <span class="badge">Transform</span>

Encodes text to binary using the specified character encoding. Uses `TextEncoderStream` internally.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `charset` | `string` | — | Character encoding name |

### Example

```javascript
import { charsetEncodeStream } from '@datastream/charset'

charsetEncodeStream({ charset: 'UTF-8' })
```
