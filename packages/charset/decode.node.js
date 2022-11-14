import { createTransformStream } from '@datastream/core'
import iconv from 'iconv-lite'

export const charsetDecodeStream = ({ charset } = {}, streamOptions = {}) => {
  charset = getSupportedEncoding(charset)

  const conv = iconv.getDecoder(charset)

  const transform = (chunk, enqueue) => {
    const res = conv.write(chunk)
    if (res?.length) {
      enqueue(res, 'utf8')
    }
  }
  streamOptions.flush = (enqueue) => {
    const res = conv.end()
    if (res?.length) {
      enqueue(res, 'utf8')
    }
  }
  return createTransformStream(transform, streamOptions)
}
const getSupportedEncoding = (charset) => {
  if (charset === 'ISO-8859-8-I') charset = 'ISO-8859-8'
  if (!iconv.encodingExists(charset)) charset = 'UTF-8'
  return charset
}
export default charsetDecodeStream
