import { createPassThroughStream } from '@datastream/core'
import {
  createSHA256,
  createSHA384,
  createSHA512,
  createSHA3
} from 'hash-wasm'

const algorithms = {
  'SHA2-256': createSHA256(),
  'SHA2-384': createSHA384(),
  'SHA2-512': createSHA512(),
  'SHA3-256': createSHA3(256),
  'SHA3-384': createSHA3(384),
  'SHA3-512': createSHA3(512)
}

export const digestStream = async ({ algorithm, resultKey }, streamOptions) => {
  const hash = await algorithms[algorithm]
  const passThrough = (chunk) => {
    hash.update(chunk)
  }
  const stream = createPassThroughStream(passThrough, streamOptions)
  let checksum
  stream.result = () => {
    checksum ??= hash.digest()
    return {
      key: resultKey ?? 'digest',
      value: `${algorithm}:${checksum}`
    }
  }
  return stream
}

export default digestStream
