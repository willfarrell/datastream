import { createTransformStream } from '@datastream/core'
import { createHash } from 'node:crypto'

const algorithmMap = {
  'SHA2-256': 'SHA256',
  'SHA2-384': 'SHA384',
  'SHA2-512': 'SHA512',
}

export const digestStream = async (algorithm, options) => {
  const hash = algorithmMap[algorithm]
    ? createHash(algorithmMap[algorithm])
    : createHash(algorithm)
  const transform = (chunk) => {
    hash.update(chunk)
  }
  const stream = createTransformStream(transform, {...options, objectMode: false})
  let checksum
  stream.result = () => {
    checksum ??= hash.digest('hex')
    return {key: options?.key ?? 'digest', value: `${algorithm}:${checksum}`}
  }
  return stream
}

export default digestStream
