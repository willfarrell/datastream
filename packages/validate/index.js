// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream } from "@datastream/core";
// Subpath: the ajv-cmd root also loads its CLI (esbuild, node:path), breaking browser bundles
import { compile } from "ajv-cmd/compile";

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
		maxErrorRows = 1_000,
		maxErrorKeys = 1_000,
	} = {},
	streamOptions = {},
) => {
	if (!schema) {
		throw new Error(
			"validateStream requires a schema (JSON Schema object or compiled AJV function)",
		);
	}

	idxStart ??= 0;
	// undefined -> the 1000 defaults above; null -> unlimited.
	const rowLimit = maxErrorRows ?? Number.POSITIVE_INFINITY;
	const keyLimit = maxErrorKeys ?? Number.POSITIVE_INFINITY;

	if (typeof schema !== "function") {
		schema = transpileSchema(schema);
	}

	const value = {}; // aka errors
	let valueCount = 0; // track distinct id count without Object.keys() per error
	let idx = idxStart - 1;
	const transform = (chunk, enqueue) => {
		idx += 1;

		// Validate a clone so AJV coercion does not mutate the caller's object.
		// Non-cloneable: fall back to validating the original (best-effort; it
		// may then be coerced in place).
		let validateTarget;
		try {
			validateTarget = clone(chunk);
		} catch {
			validateTarget = chunk;
		}
		// allowCoerceTypes: false emits the original (uncoerced) chunk;
		// otherwise the coerced clone.
		const emitChunk = allowCoerceTypes === false ? chunk : validateTarget;

		const chunkValid = schema(validateTarget);
		if (!chunkValid) {
			for (const error of schema.errors) {
				const { id, keys, message } = processError(error);

				if (!value[id]) {
					// Stop creating new entries once maxErrorKeys cap is reached
					if (valueCount >= keyLimit) {
						continue;
					}
					value[id] = { id, keys, message, idx: [] };
					valueCount += 1;
				}
				if (value[id].idx.length < rowLimit) {
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

// structuredClone runs every value through V8's serializer: ~1µs per small
// row, over half of this stream's time. Rows are nearly always plain objects,
// arrays and primitives, which a direct copy clones several times faster. Any
// other object (Date, Map, typed array, class instance, ...) still goes through
// structuredClone, so the same values are non-cloneable. A function or symbol
// value is copied by reference, where structuredClone rejected the whole row.
// Shared and cyclic references are preserved, as structuredClone does.
const clone = (root) => {
	const seen = new Map();
	const copy = (value) => {
		if (typeof value !== "object" || value === null) return value;
		const isArray = Array.isArray(value);
		if (!isArray) {
			const proto = Object.getPrototypeOf(value);
			if (proto !== Object.prototype && proto !== null) {
				return structuredClone(value);
			}
		}
		// Plain assignment of an own "__proto__" key would replace the prototype;
		// structuredClone keeps it as a data property (and still rejects a function
		// value inside, which is how a mutant forcing this path is observed).
		if (Object.hasOwn(value, "__proto__")) return structuredClone(value);
		const found = seen.get(value);
		if (found) return found;
		const out = isArray ? [] : {};
		seen.set(value, out);
		for (const key of Object.keys(value)) out[key] = copy(value[key]);
		return out;
	};
	return copy(root);
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
