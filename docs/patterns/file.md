# file

## fileReadStream (Readable)

```javascript
import { createReadStream } from 'node:fs'

const fileReadableStream = (fileName, streamOptions) => {
  return createReadStream(fileName)
}

export default {
  readableStream: fileReadableStream
}
```

## fileWriteStream (Writable)

```javascript
import { createWriteStream } from 'node:fs'


export const fileWritableStream = (fileName, streamOptions) => {
  return createWriteStream(fileName)
}

export default {
  writableStream: fileWritableStream
}
```
