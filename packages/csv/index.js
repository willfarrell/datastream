import formatStream from '@datastream/csv/format'
import parseStream from '@datastream/csv/parse'

export const csvParseStream = parseStream
export const csvFormatStream = formatStream

export default {
  parseStream,
  formatStream
}
