import { createTransformStream } from "@datastream/core";
import { parse } from "csv-rex/parse";

export const csvParseStream = (options, streamOptions = {}) => {
	const { chunkParse, previousChunk } = parse(options);

	const value = {};
	const handlerError = ({ idx, err }) => {
		const { code: id, message } = err;
		if (!value[id]) {
			value[id] = { id, message, idx: [] };
		}
		value[id].idx.push(idx);
	};
	const transform = (chunk, enqueue) => {
		const enqueueRow = (row) => {
			if (row.err) {
				handlerError(row);
			} else {
				enqueue(row.data);
			}
		};

		chunk = previousChunk() + chunk;
		chunkParse(chunk, { enqueue: enqueueRow });
	};
	const flush = (enqueue) => {
		const enqueueRow = (row) => {
			if (row.err) {
				handlerError(row);
			} else {
				enqueue(row.data);
			}
		};
		const chunk = previousChunk();
		chunkParse(chunk, { enqueue: enqueueRow }, true);
	};
	streamOptions.decodeStrings = false;
	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({ key: options?.resultKey ?? "csvErrors", value });
	return stream;
};
export default csvParseStream;
