import { makeOptions } from '@datastream/core'
import { parse } from 'csv-rex/parse'

export const csvParseStream = (options) => {
	const { chunkParse, previousChunk } = parse(options)

	let value = {}
	const handlerError = ({idx, err}) => {
		const {code: id, message} = err
		if (!value[id]) {
			value[id] = { id, message, idx: [] }
		}
		value[id].idx.push(idx)
	}
	const stream = new TransformStream({
		transform (chunk, controller) {
			const enqueue = (row) => {
				if (row.err) {
					handlerError(row)
				} else {
					controller.enqueue(row.data)
				}
			}

			chunk = previousChunk() + chunk
			chunkParse(chunk, {enqueue})
		},
		flush(controller) {
			const enqueue = (row) => {
				if (row.err) {
					handlerError(row)
				} else {
					controller.enqueue(row.data)
				}
			}
			const chunk = previousChunk()
			chunkParse(chunk, {enqueue}, true)
			controller.terminate()
		}
	}, makeOptions(options))
	stream.result = () => ({key: options?.key ?? 'csvErrors', value})
	return stream
}
export default csvParseStream