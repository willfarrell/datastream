// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createPassThroughStream,
	createReadableStream,
	createTransformStream,
} from "@datastream/core";

export const stringReadableStream = (input, streamOptions = {}) => {
	return createReadableStream(input, streamOptions);
};

export const stringLengthStream = ({ resultKey } = {}, streamOptions = {}) => {
	let value = 0;
	const passThrough = (chunk) => {
		value += chunk.length;
	};
	const stream = createPassThroughStream(passThrough, streamOptions);
	stream.result = () => ({ key: resultKey ?? "length", value });
	return stream;
};
export const stringCountStream = (
	{ substr, resultKey } = {},
	streamOptions = {},
) => {
	let value = 0;
	const passThrough = (chunk) => {
		let cursor = -1;
		while (cursor < chunk.length) {
			cursor = chunk.indexOf(substr, cursor + 1);
			if (cursor === -1) {
				break;
			}
			value += 1;
		}
	};
	const stream = createPassThroughStream(passThrough, streamOptions);
	stream.result = () => ({ key: resultKey ?? "count", value });
	return stream;
};

export const stringSkipConsecutiveDuplicates = (
	_options,
	streamOptions = {},
) => {
	let previousChunk;
	const transform = (chunk, enqueue) => {
		if (chunk !== previousChunk) {
			enqueue(chunk);
			previousChunk = chunk;
		}
	};
	return createTransformStream(transform, streamOptions);
};

export const stringReplaceStream = (options, streamOptions = {}) => {
	const { pattern, replacement } = options;
	let previousChunk = "";
	const transform = (chunk, enqueue) => {
		const newChunk = (previousChunk + chunk).replace(pattern, replacement);
		enqueue(newChunk.substring(0, previousChunk.length));
		previousChunk = newChunk.substring(previousChunk.length);
	};
	const flush = (enqueue) => {
		enqueue(previousChunk);
	};
	return createTransformStream(transform, flush, streamOptions);
};

export const stringSplitStream = (options, streamOptions = {}) => {
	const { separator } = options;
	let previousChunk = "";
	const transform = (chunk, enqueue) => {
		chunk = previousChunk + chunk;
		let pos = 0;
		while (true) {
			const idx = chunk.indexOf(separator, pos);
			if (idx > -1) {
				enqueue(chunk.substring(pos, idx));
				pos = idx + separator.length;
			} else {
				previousChunk = chunk.substring(pos);
				break;
			}
		}
	};
	const flush = (enqueue) => {
		enqueue(previousChunk);
	};
	return createTransformStream(transform, flush, streamOptions);
};

export default {
	readableStream: stringReadableStream,
	lengthStream: stringLengthStream,
	skipConsecutiveDuplicates: stringSkipConsecutiveDuplicates,
	replaceStream: stringReplaceStream,
};
