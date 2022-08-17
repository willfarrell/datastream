import detectStream from '@datastream/charset/detect'
import decodeStream from '@datastream/charset/decode'
import encodeStream from '@datastream/charset/encode'

export const charsetDetectStream = detectStream
export const charsetDecodeStream = decodeStream
export const charsetEncodeStream = encodeStream

export default {
  detectStream,
  decodeStream,
  encodeStream
}
