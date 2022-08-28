import { Transform } from 'node:stream'
import { makeOptions } from '@datastream/core'
import { defaultOptions, formatArray, formatObject } from 'csv-rex/format'

export const csvFormatStream = (options, streamOptions) => {
  const csvOptions = { ...defaultOptions, ...options }
  csvOptions.escapeChar ??= csvOptions.quoteChar
  let format
  return new Transform({
    ...makeOptions(streamOptions),
    transform (chunk, encoding, callback) {
      if (csvOptions.header === true) {
        csvOptions.header = Object.keys(chunk)
      }
      if (typeof format === 'undefined' && Array.isArray(csvOptions.header)) {
        this.push(formatArray(csvOptions.header, csvOptions))
      }
      format ??= Array.isArray(chunk) ? formatArray : formatObject
      this.push(format(chunk, csvOptions))
      callback()
    }
  })
}
export default csvFormatStream
