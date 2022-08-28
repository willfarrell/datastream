import { Transform } from 'node:stream'
import { makeOptions } from '@datastream/core'
import iconv from 'iconv-lite'

export const charsetEncodeStream = (charset, streamOptions) => {
  charset = getSupportedEncoding(charset)

  const conv = iconv.getEncoder(charset)

  return new Transform({
    ...makeOptions(streamOptions),
    transform (chunk, encoding, callback) {
      const res = conv.write(chunk)
      if (res?.length) {
        this.push(res)
      }
      callback()
    },
    flush (callback) {
      const res = conv.end()
      if (res?.length) {
        this.push(res)
      }
      callback()
    }
  })
}

const getSupportedEncoding = (charset) => {
  if (charset === 'ISO-8859-8-I') charset = 'ISO-8859-8'
  if (!iconv.encodingExists(charset)) charset = 'UTF-8'
  return charset
}
export default charsetEncodeStream
