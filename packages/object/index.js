// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createPassThroughStream,
	createTransformStream,
} from "@datastream/core";
import { deepEqual } from "#deepEqual";
import { deepClone, shallowClone, shallowEqual } from "./helpers.js";

// defineProperty so reserved keys such as "__proto__" become own enumerable
// data properties instead of mutating the object's prototype (which a plain
// `object[key] = ...` would do, silently dropping the key). For ordinary keys
// this is equivalent to a normal assignment.
const defineValue = (object, key, value) => {
	Object.defineProperty(object, key, {
		value,
		writable: true,
		enumerable: true,
		configurable: true,
	});
};

export const objectCountStream = ({ resultKey } = {}, streamOptions = {}) => {
	let value = 0;
	const passThrough = () => {
		value += 1;
	};
	const stream = createPassThroughStream(passThrough, streamOptions);
	stream.result = () => ({ key: resultKey ?? "objectCount", value });
	return stream;
};

export const objectBatchStream = (
	{ keys, maxBatchSize },
	streamOptions = {},
) => {
	// Unlimited by default (undefined and null alike): a finite default would
	// split one key group across batches, and objectPivotLongToWideStream turns
	// each batch into its own row, silently emitting partial rows.
	const limit = maxBatchSize ?? Number.POSITIVE_INFINITY;
	let previousId;
	let batch = [];
	const transform = (chunk, enqueue) => {
		const id = JSON.stringify(keys.map((key) => chunk[key]));
		if (previousId !== id) {
			// batch is empty when a maxBatchSize flush just happened
			if (batch.length) {
				enqueue(batch);
			}
			previousId = id;
			batch = [];
		}
		batch.push(chunk);
		if (batch.length >= limit) {
			enqueue(batch);
			batch = [];
		}
	};
	const flush = (enqueue) => {
		if (batch.length) {
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
		const row = { ...chunks[0] };

		for (const chunk of chunks) {
			const keyParam = keys.map((key) => chunk[key]).join(delimiter);
			defineValue(row, keyParam, chunk[valueParam]);
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
	{ keys, keyParam, valueParam, isNestedObject },
	streamOptions = {},
) => {
	keyParam ??= "keyParam";
	valueParam ??= "valueParam";

	const clone = isNestedObject ? deepClone : shallowClone;

	const transform = (chunk, enqueue) => {
		const value = clone(chunk);
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
						// defineValue: a "__proto__" value key must stay an own property
						defineValue(value, key, chunk[key]);
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
	{ keys, separator, isNestedObject },
	streamOptions = {},
) => {
	const clone = isNestedObject ? deepClone : shallowClone;
	const transform = (chunk, enqueue) => {
		const value = clone(chunk);
		for (const newKey of Object.keys(keys)) {
			// defineValue: a "__proto__" new key must stay an own property
			defineValue(
				value,
				newKey,
				keys[newKey]
					.map((oldKey) => {
						delete value[oldKey];
						return chunk[oldKey];
					})
					.join(separator),
			);
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectKeyMapStream = ({ keys }, streamOptions = {}) => {
	const transform = (chunk, enqueue) => {
		const value = {};
		for (const key of Object.keys(chunk)) {
			// hasOwn: inherited names (constructor, toString, __proto__) are not mappings
			const newKey = Object.hasOwn(keys, key) ? keys[key] : key;
			defineValue(value, newKey, chunk[key]);
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectValueMapStream = ({ key, values }, streamOptions = {}) => {
	const transform = (chunk, enqueue) => {
		if (Object.hasOwn(values, chunk[key])) {
			chunk[key] = values[chunk[key]];
		}
		enqueue(chunk);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectPickStream = ({ keys }, streamOptions = {}) => {
	// Set, not a plain-object lookup: inherited names (constructor, toString,
	// __proto__) must not match
	const keySet = new Set(keys);
	const transform = (chunk, enqueue) => {
		const value = {};
		for (const key of Object.keys(chunk)) {
			if (keySet.has(key)) {
				defineValue(value, key, chunk[key]);
			}
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectOmitStream = ({ keys }, streamOptions = {}) => {
	// Set, not a plain-object lookup: inherited names (toString, valueOf)
	// must not be treated as omitted
	const keySet = new Set(keys);
	const transform = (chunk, enqueue) => {
		const value = {};
		for (const key of Object.keys(chunk)) {
			if (!keySet.has(key)) {
				defineValue(value, key, chunk[key]);
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
			defineValue(value, resolvedKeys[i], chunk[i]);
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectToEntriesStream = ({ keys }, streamOptions = {}) => {
	let resolvedKeys;
	const transform = (chunk, enqueue) => {
		resolvedKeys ??= typeof keys === "function" ? keys() : keys;
		const value = [];
		for (let i = 0; i < resolvedKeys.length; i++) {
			value[i] = chunk[resolvedKeys[i]];
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};

export const objectSkipConsecutiveDuplicatesStream = (
	options = {},
	streamOptions = {},
) => {
	const { isNestedObject } = options;
	const equal = isNestedObject ? deepEqual : shallowEqual;
	let previousChunk;
	const transform = (chunk, enqueue) => {
		if (!equal(chunk, previousChunk)) {
			enqueue(chunk);
			previousChunk = isNestedObject ? deepClone(chunk) : chunk;
		}
	};
	return createTransformStream(transform, streamOptions);
};
