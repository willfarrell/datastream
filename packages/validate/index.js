// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream } from "@datastream/core";
import { compile } from "ajv-cmd";

const ajvDefaults = {
	strict: true,
	coerceTypes: true,
	allErrors: true,
	useDefaults: "empty",
	messages: true, // needs to be true to allow multi-locale errorMessage to work
};

// This is pulled out due to it's performance cost (50-100ms on cold start)
// Precompile your schema during a build step is recommended.
export const transpileSchema = (schema, ajvOptions) => {
	const options = { ...ajvDefaults, ...ajvOptions };
	return compile(schema, options);
};

export const validateStream = (
	{
		schema,
		idxStart,
		onErrorEnqueue,
		allowCoerceTypes,
		resultKey,
		maxErrorRows = Infinity,
		maxErrorKeys = 1000,
	} = {},
	streamOptions = {},
) => {
	if (!schema) {
		throw new Error(
			"validateStream requires a schema (JSON Schema object or compiled AJV function)",
		);
	}

	idxStart ??= 0;

	if (typeof schema !== "function") {
		schema = transpileSchema(schema);
	}

	const value = {}; // aka errors
	let valueCount = 0; // track distinct id count without Object.keys() per error
	let idx = idxStart - 1;
	const transform = (chunk, enqueue) => {
		idx += 1;

		let emitChunk;
		let validateTarget;

		if (allowCoerceTypes === false) {
			// Validate on a clone so AJV coercion does not mutate the original.
			// Emit the original (uncoerced) chunk.
			try {
				validateTarget = structuredClone(chunk);
			} catch {
				// Non-cloneable: fall back to validating the original (best-effort)
				validateTarget = chunk;
			}
			emitChunk = chunk;
		} else {
			// Clone before validation so AJV coercion does not mutate caller's object.
			// Emit the coerced clone.
			try {
				validateTarget = structuredClone(chunk);
			} catch {
				// Non-cloneable: validate the original (caller's object may be mutated)
				validateTarget = chunk;
			}
			emitChunk = validateTarget;
		}

		const chunkValid = schema(validateTarget);
		if (!chunkValid) {
			for (const error of schema.errors) {
				const { id, keys, message } = processError(error);

				if (!value[id]) {
					// Stop creating new entries once maxErrorKeys cap is reached
					if (valueCount >= maxErrorKeys) {
						continue;
					}
					value[id] = { id, keys, message, idx: [] };
					valueCount += 1;
				}
				if (value[id].idx.length < maxErrorRows) {
					value[id].idx.push(idx);
				}
			}
		}
		if (chunkValid || onErrorEnqueue) {
			enqueue(emitChunk);
		}
	};
	const stream = createTransformStream(transform, streamOptions);
	stream.result = () => ({ key: resultKey ?? "validate", value });
	return stream;
};

const processError = (error) => {
	const message = error.message || "";

	let id = error.schemaPath;

	let keys = [];
	if (error.keyword === "errorMessage") {
		error.params.errors.forEach((error) => {
			const value = makeKeys(error);
			if (value) keys.push(value);
		});
		keys = [...new Set(keys.sort())];
	} else {
		const key = makeKeys(error);
		if (key) keys.push(key);
	}
	if (!error.instancePath && keys.length) {
		id += `/${keys.join("|")}`;
	}
	return { id, keys, message };
};

const makeKeys = (error) => {
	// deps groups columns that are related in anyOf/oneOf.
	/* error.params.deps ?? */
	return (
		error.params.missingProperty ||
		error.params.additionalProperty ||
		error.instancePath.replace("/", "")
	);
};

export default validateStream;
