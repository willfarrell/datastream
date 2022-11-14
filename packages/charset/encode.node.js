import { createTransformStream } from '@datastream/core'
import iconv from 'iconv-lite'

export const charsetEncodeStream = ({ charset } = {}, streamOptions = {}) => {
  charset = getSupportedEncoding(charset)

  const conv = iconv.getEncoder(charset)
  const transform = (chunk, enqueue) => {
    const res = conv.write(chunk)
    if (res?.length) {
      enqueue(res)
    }
  }
  streamOptions.flush = (enqueue) => {
    const res = conv.end()
    if (res?.length) {
      enqueue(res)
    }
  }
  return createTransformStream(transform, streamOptions)
}

const getSupportedEncoding = (charset) => {
  if (charset === 'ISO-8859-8-I') charset = 'ISO-8859-8'
  if (!iconv.encodingExists(charset)) charset = 'UTF-8'
  return charset
}
export default charsetEncodeStream
