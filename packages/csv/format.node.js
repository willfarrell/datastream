import { Transform } from 'node:stream'
import { makeOptions } from '@datastream/core'
import { defaultOptions, formatArray, formatObject } from 'csv-rex/format'

export const csvFormatStream = (options) => {
	const csvOptions = {...defaultOptions, ...options}
	csvOptions.escapeChar ??= csvOptions.quoteChar
	console.log({csvOptions})
	let format
	return new Transform({
		...makeOptions(options),
		writableObjectMode: true,
		readableObjectMode: false,
		transform (chunk, encoding, callback) {
			if (csvOptions.header === true) {
				csvOptions.header = Object.keys(chunk)
			}
			if (typeof format === 'undefined' && Array.isArray(csvOptions.header)) {
				this.push(formatArray(csvOptions.header, csvOptions))
			}
			format ??= Array.isArray(chunk) ? formatArray : formatObject
			console.log({chunk}, format(chunk, csvOptions))
			this.push(format(chunk, csvOptions))
			callback()
		}
	})
}
export default csvFormatStream
