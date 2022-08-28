import { createPassThroughStream } from '@datastream/core'
import { createHash } from 'node:crypto'

const algorithmMap = {
  'SHA2-256': 'SHA256',
  'SHA2-384': 'SHA384',
  'SHA2-512': 'SHA512'
}

export const digestStream = async ({ algorithm, resultKey }, streamOptions) => {
  const hash = createHash(algorithmMap[algorithm] ?? algorithm)
  const transform = (chunk) => {
    hash.update(chunk)
  }
  const stream = createPassThroughStream(transform, streamOptions)
  let checksum
  stream.result = () => {
    checksum ??= hash.digest('hex')
    return {
      key: resultKey ?? 'digest',
      value: `${algorithm}:${checksum}`
    }
  }
  return stream
}

export default digestStream
