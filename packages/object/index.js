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

// Fresh object whose own data properties are `keys` (a "__proto__" key
// included, which defineProperty keeps an own key). Spreading it copies them as
// own data properties and a plain assignment to an existing own data property
// never reaches Object.prototype's "__proto__" accessor, so per-row objects
// built from a fixed key list need no defineProperty call.
const createTemplate = (keys) => {
	const template = {};
	for (const key of keys) {
		defineValue(template, key, undefined);
	}
	return template;
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
		// Drop the long-format columns BEFORE adding pivoted ones, so a pivoted
		// column named like a key or valueParam is not deleted afterwards.
		for (const key of keys) {
			delete row[key];
		}
		delete row[valueParam];

		for (const chunk of chunks) {
			const keyParam = keys.map((key) => chunk[key]).join(delimiter);
			defineValue(row, keyParam, chunk[valueParam]);
		}

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
	// No template when values is undefined: the whole chunk is the value.
	const template = values && createTemplate(values);
	const transform = (chunk, enqueue) => {
		let value = chunk;
		if (typeof values !== "undefined") {
			value = { ...template };
			for (const name of values) {
				value[name] = chunk[name];
			}
		}
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
		const entries = [];
		for (const key of Object.keys(chunk)) {
			// hasOwn: inherited names (constructor, toString, __proto__) are not mappings
			const newKey = Object.hasOwn(keys, key) ? keys[key] : key;
			entries.push([newKey, chunk[key]]);
		}
		// fromEntries defines own data properties, so a "__proto__" key stays own
		enqueue(Object.fromEntries(entries));
	};
	return createTransformStream(transform, streamOptions);
};

export const objectValueMapStream = ({ key, values }, streamOptions = {}) => {
	const transform = (chunk, enqueue) => {
		if (Object.hasOwn(values, chunk[key])) {
			// Copy, never mutate the incoming chunk. Spread keeps a "__proto__"
			// own key as data; a computed key defines one too when it is the
			// mapped key (only the literal `__proto__:` form sets the prototype).
			// An existing key keeps its position, as defineProperty would.
			chunk = { ...chunk, [key]: values[chunk[key]] };
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
		const entries = [];
		for (const key of Object.keys(chunk)) {
			if (keySet.has(key)) {
				entries.push([key, chunk[key]]);
			}
		}
		// fromEntries defines own data properties, so a "__proto__" key stays own
		enqueue(Object.fromEntries(entries));
	};
	return createTransformStream(transform, streamOptions);
};

export const objectOmitStream = ({ keys }, streamOptions = {}) => {
	// Set, not a plain-object lookup: inherited names (toString, valueOf)
	// must not be treated as omitted
	const keySet = new Set(keys);
	const transform = (chunk, enqueue) => {
		const entries = [];
		for (const key of Object.keys(chunk)) {
			if (!keySet.has(key)) {
				entries.push([key, chunk[key]]);
			}
		}
		// fromEntries defines own data properties, so a "__proto__" key stays own
		enqueue(Object.fromEntries(entries));
	};
	return createTransformStream(transform, streamOptions);
};
// objectKeySplit = ({keys: { oldKey: /^(?<newKey>.*)$/ }) => { }

export const objectFromEntriesStream = ({ keys }, streamOptions = {}) => {
	let resolvedKeys;
	let template;
	const transform = (chunk, enqueue) => {
		resolvedKeys ??= typeof keys === "function" ? keys() : keys;
		template ??= createTemplate(resolvedKeys);
		const value = { ...template };
		for (let i = 0; i < resolvedKeys.length; i++) {
			value[resolvedKeys[i]] = chunk[i];
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
