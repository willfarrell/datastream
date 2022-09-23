
export const indexedDBReadStream = async (option, streamOptions) => {
  throw new Error('indexedDBReadStream: Not supported')
}

export const indexedDBWriteStream = async ({ path, types }, streamOptions) => {
  throw new Error('indexedDBWriteStream: Not supported')
}

export default {
  readStream: indexedDBReadStream,
  writeStream: indexedDBWriteStream
}
