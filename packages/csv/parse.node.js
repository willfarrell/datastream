import { Transform } from 'node:stream'
import { makeOptions } from '@datastream/core'
import { parse } from 'csv-rex/parse'

export const csvParseStream = (options, streamOptions) => {
  const { chunkParse, previousChunk } = parse(options)

  const value = {}
  const handlerError = ({ idx, err }) => {
    const { code: id, message } = err
    if (!value[id]) {
      value[id] = { id, message, idx: [] }
    }
    value[id].idx.push(idx)
  }
  const stream = new Transform({
    ...makeOptions(streamOptions),
    decodeStrings: false,
    transform (chunk, encoding, callback) {
      const enqueue = (row) => {
        if (row.err) {
          handlerError(row)
        } else {
          this.push(row.data)
        }
      }

      chunk = previousChunk() + chunk
      chunkParse(chunk, { enqueue })
      callback()
    },
    flush (callback) {
      const enqueue = (row) => {
        if (row.err) {
          handlerError(row)
        } else {
          this.push(row.data)
        }
      }
      const chunk = previousChunk()
      chunkParse(chunk, { enqueue }, true)
      callback()
    }
  })
  stream.result = () => ({ key: options?.resultKey ?? 'csvErrors', value })
  return stream
}
export default csvParseStream
