// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createPassThroughStream,
	createReadableStream,
	createTransformStream,
} from "@datastream/core";

export const objectReadableStream = (input = [], streamOptions = {}) => {
	return createReadableStream(input, streamOptions);
};

export const objectCountStream = ({ resultKey } = {}, streamOptions = {}) => {
	let value = 0;
	const passThrough = () => {
		value += 1;
	};
	const stream = createPassThroughStream(passThrough, streamOptions);
	stream.result = () => ({ key: resultKey ?? "count", value });
	return stream;
};

export const objectBatchStream = ({ keys }, streamOptions = {}) => {
	let previousId;
	let batch;
	const transform = (chunk, enqueue) => {
		const id = keys.map((key) => chunk[key]).join(" ");
		if (previousId !== id) {
			if (batch) {
				enqueue(batch);
			}
			previousId = id;
			batch = [];
		}
		batch.push(chunk);
	};
	const flush = (enqueue) => {
		if (batch) {
			enqueue(batch);
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};

export const objectPivotLongToWideStream = (
	{ keys, valueParam, delimiter },
	streamOptions = {},
) => {
	delimiter ??= " ";

	const transform = (chunks, enqueue) => {
		if (!Array.isArray(chunks)) {
			throw new Error("Expected chunk to be array, use with objectBatchStream");
		}
		const row = chunks[0];

		for (const chunk of chunks) {
			const keyParam = keys.map((key) => chunk[key]).join(delimiter);
			row[keyParam] = chunk[valueParam];
		}

		for (const key of keys) {
			delete row[key];
		}
		delete row[valueParam];

		enqueue(row);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectPivotWideToLongStream = (
	{ keys, keyParam, valueParam },
	streamOptions = {},
) => {
	keyParam ??= "keyParam";
	valueParam ??= "valueParam";

	const transform = (chunk, enqueue) => {
		const value = structuredClone(chunk);
		for (const key of keys) {
			delete value[key];
		}
		for (const key of keys) {
			// skip if pivot key doesn't exist
			if (Object.hasOwn(chunk, key)) {
				enqueue({ ...value, [keyParam]: key, [valueParam]: chunk[key] });
			}
		}
	};
	return createTransformStream(transform, streamOptions);
};

export const objectKeyValueStream = ({ key, value }, streamOptions = {}) => {
	const transform = (chunk, enqueue) => {
		chunk = { [chunk[key]]: chunk[value] };
		enqueue(chunk);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectKeyValuesStream = ({ key, values }, streamOptions = {}) => {
	const transform = (chunk, enqueue) => {
		const value =
			typeof values === "undefined"
				? chunk
				: values.reduce((value, key) => {
						value[key] = chunk[key];
						return value;
					}, {});
		chunk = {
			[chunk[key]]: value,
		};
		enqueue(chunk);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectKeyJoinStream = (
	{ keys, separator },
	streamOptions = {},
) => {
	const transform = (chunk, enqueue) => {
		const value = structuredClone(chunk);
		for (const newKey of Object.keys(keys)) {
			// perf opportunity
			value[newKey] = keys[newKey]
				.map((oldKey) => {
					delete value[oldKey];
					return chunk[oldKey];
				})
				.join(separator);
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectKeyMapStream = ({ keys }, streamOptions = {}) => {
	const transform = (chunk, enqueue) => {
		const value = {};
		for (const key of Object.keys(chunk)) {
			const newKey = keys[key] ?? key;
			value[newKey] = chunk[key];
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectValueMapStream = ({ key, values }, streamOptions = {}) => {
	const transform = (chunk, enqueue) => {
		chunk[key] = values[chunk[key]];
		enqueue(chunk);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectPickStream = ({ keys }, streamOptions = {}) => {
	const keySet = Object.fromEntries(keys.map((k) => [k, true]));
	const transform = (chunk, enqueue) => {
		const value = {};
		for (const key of Object.keys(chunk)) {
			if (keySet[key]) {
				value[key] = chunk[key];
			}
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectOmitStream = ({ keys }, streamOptions = {}) => {
	const keySet = Object.fromEntries(keys.map((k) => [k, true]));
	const transform = (chunk, enqueue) => {
		const value = {};
		for (const key of Object.keys(chunk)) {
			if (!keySet[key]) {
				value[key] = chunk[key];
			}
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};
// objectKeySplit = ({keys: { oldKey: /^(?<newKey>.*)$/ }) => { }

export const objectFromEntriesStream = ({ keys }, streamOptions = {}) => {
	let resolvedKeys;
	const transform = (chunk, enqueue) => {
		resolvedKeys ??= typeof keys === "function" ? keys() : keys;
		const value = {};
		for (let i = 0; i < resolvedKeys.length; i++) {
			value[resolvedKeys[i]] = chunk[i];
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectSkipConsecutiveDuplicatesStream = (
	_options = {},
	streamOptions = {},
) => {
	let previousChunk;
	const transform = (chunk, enqueue) => {
		const chunkStringified = JSON.stringify(chunk);
		if (chunkStringified !== previousChunk) {
			enqueue(chunk);
			previousChunk = chunkStringified;
		}
	};
	return createTransformStream(transform, streamOptions);
};

export default {
	readableStream: objectReadableStream,
	countStream: objectCountStream,
	pickStream: objectPickStream,
	omitStream: objectOmitStream,
	batchStream: objectBatchStream,
	pivotLongToWideStream: objectPivotLongToWideStream,
	pivotWideToLongStream: objectPivotWideToLongStream,
	keyValueStream: objectKeyValueStream,
	keyValuesStream: objectKeyValuesStream,
	keyJoinStream: objectKeyJoinStream,
	keyMapStream: objectKeyMapStream,
	valueMapStream: objectValueMapStream,
	fromEntriesStream: objectFromEntriesStream,
	skipConsecutiveDuplicatesStream: objectSkipConsecutiveDuplicatesStream,
};
