import { extname } from 'node:path'
import { createReadStream, createWriteStream } from 'node:fs'
import { makeOptions } from '@datastream/core'

export const fileReadStream = async ({ path, types }, streamOptions) => {
  enforceType(path, types)
  return createReadStream(path, makeOptions(streamOptions))
}

export const fileWriteStream = async ({ path, types }, streamOptions) => {
  enforceType(path, types)
  return createReadStream(path, makeOptions(streamOptions))
}

const enforceType = (path, types = []) => {
  const pathExt = extname(path)
  for(const type of types) {
    for(const mime in type.accept) {
      for(const ext of type.accept[mime]){
        if (pathExt === ext) {
          return
        }
      }
    }
  }
  if (types.length) {
    throw new Error('fileReadStream: invalid extension')
  }
}

export default {
  readStream: fileReadStream,
  writeStream: fileWriteStream
}
