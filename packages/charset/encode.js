import { createTransformStream } from '@datastream/core'
import iconv from 'iconv-lite' // doesn't support esm

export const charsetEncodeStream = (charset) => {
  charset = getSupportedEncoding(charset)
  if (charset === 'UTF-8') return createTransformStream()
  return iconv.encodeStream(charset)
}
const getSupportedEncoding = (charset) => {
  if (charset === 'ISO-8859-8-I') charset = 'ISO-8859-8'
  if (!iconv.encodingExists(charset)) charset = 'UTF-8'
  return charset
}
export default charsetEncodeStream
