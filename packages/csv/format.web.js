/* global TransformStream */
import { makeOptions } from '@datastream/core'
import { defaultOptions, formatArray, formatObject } from 'csv-rex/format'

export const csvFormatStream = (options = {}, streamOptions) => {
  const csvOptions = { ...defaultOptions, ...options }
  csvOptions.escapeChar ??= csvOptions.quoteChar
  let format
  return new TransformStream(
    {
      start () {},
      transform (chunk, controller) {
        if (csvOptions.header === true) {
          csvOptions.header = Object.keys(chunk)
        }
        if (typeof format === 'undefined' && Array.isArray(csvOptions.header)) {
          controller.enqueue(formatArray(csvOptions.header, csvOptions))
        }
        format ??= Array.isArray(chunk) ? formatArray : formatObject
        controller.enqueue(format(chunk, csvOptions))
      }
    },
    makeOptions(streamOptions)
  )
}
export default csvFormatStream
