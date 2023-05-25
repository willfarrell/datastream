import { createTransformStream } from '@datastream/core'
import { defaultOptions, formatArray, formatObject } from 'csv-rex/format'

export const csvFormatStream = (options, streamOptions) => {
  const csvOptions = { ...defaultOptions, ...options }
  csvOptions.escapeChar ??= csvOptions.quoteChar
  let columns, format
  if (Array.isArray(csvOptions.header)) {
    columns = csvOptions.header
  }
  const transform = (chunk, enqueue) => {
    if (typeof format === 'undefined' && csvOptions.header !== false) {
      if (csvOptions.header === true && !Array.isArray(chunk)) {
        csvOptions.header = Object.keys(chunk)
      }
      enqueue(formatArray(csvOptions.header, csvOptions))
    }
    format ??= Array.isArray(chunk) ? formatArray : formatObject
    enqueue(format(chunk, csvOptions))
  }
  return createTransformStream(transform, streamOptions)
}
export default csvFormatStream
