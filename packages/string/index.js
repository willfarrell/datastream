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

export const stringMinimumFirstChunkSize = (
	options = {},
	streamOptions = {},
) => {
	const { chunkSize = 1024 } = options;
	let buffer = "";
	let done = false;
	const transform = (chunk, enqueue) => {
		if (done) {
			enqueue(chunk);
			return;
		}
		buffer += chunk;
		if (buffer.length >= chunkSize) {
			enqueue(buffer);
			buffer = "";
			done = true;
		}
	};
	const flush = (enqueue) => {
		if (!done && buffer.length > 0) {
			enqueue(buffer);
		}
	};
	const stream = createTransformStream(transform, flush, streamOptions);
	return stream;
};

export const stringMinimumChunkSize = (options = {}, streamOptions = {}) => {
	const { chunkSize = 1024 } = options;
	let buffer = "";
	const transform = (chunk, enqueue) => {
		buffer += chunk;
		if (buffer.length >= chunkSize) {
			enqueue(buffer);
			buffer = "";
		}
	};
	const flush = (enqueue) => {
		if (buffer.length > 0) {
			enqueue(buffer);
		}
	};
	const stream = createTransformStream(transform, flush, streamOptions);
	return stream;
};

export const stringSkipConsecutiveDuplicates = (
	_options = {},
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
	const {
		pattern,
		replacement,
		maxBufferSize = 16_777_216, // 16MB
	} = options;
	if (
		pattern instanceof RegExp &&
		!pattern.flags.includes("g") &&
		!pattern.flags.includes("y")
	) {
		throw new Error(
			"RegExp pattern must include the global (g) or sticky (y) flag",
		);
	}
	let previousChunk = "";
	const useReplaceAll = typeof pattern === "string";
	const transform = (chunk, enqueue) => {
		const combined = previousChunk + chunk;
		const newChunk = useReplaceAll
			? combined.replaceAll(pattern, replacement)
			: combined.replace(pattern, replacement);
		enqueue(newChunk.substring(0, previousChunk.length));
		previousChunk = newChunk.substring(previousChunk.length);
		if (previousChunk.length > maxBufferSize) {
			throw new Error(
				`stringReplaceStream buffer (${previousChunk.length}) exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
	};
	const flush = (enqueue) => {
		enqueue(previousChunk);
	};
	return createTransformStream(transform, flush, streamOptions);
};

export const stringSplitStream = (options, streamOptions = {}) => {
	const {
		separator,
		maxBufferSize = 16_777_216, // 16MB
	} = options;
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
		if (previousChunk.length > maxBufferSize) {
			throw new Error(
				`stringSplitStream buffer (${previousChunk.length}) exceeds maxBufferSize (${maxBufferSize}), separator not found`,
			);
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
	countStream: stringCountStream,
	skipConsecutiveDuplicates: stringSkipConsecutiveDuplicates,
	replaceStream: stringReplaceStream,
	splitStream: stringSplitStream,
};
