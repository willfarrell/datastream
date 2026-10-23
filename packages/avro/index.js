// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// ponytail: node-only. avsc 5 reads and writes through Buffer (fromBuffer
// decodes strings via buf.utf8Slice), so a browser build needs avsc 6 or a
// Buffer polyfill.
import { createTransformStream } from "@datastream/core";
import avro from "avsc";

const DEFAULT_MAX_MESSAGE_SIZE = 64 * 1024 * 1024;
const MAX_CACHED_TYPES = 1000;

const compile = (name, value, options) => {
	if (!value) throw new TypeError(`${name}: schema required`);
	// Type.forSchema treats a bare string as a type *name*, so JSON text
	// (e.g. a registry's schemaDefinition) has to be parsed first.
	return avro.Type.forSchema(
		typeof value === "string" ? JSON.parse(value) : value,
		options,
	);
};

const makeTransform = (name, schema, compileType, handle) => {
	if (typeof schema !== "function") {
		const type = compileType(name, schema);
		return (chunk, enqueue) => handle(type, chunk, enqueue);
	}
	let type;
	return async (chunk, enqueue) => {
		type ??= compileType(name, await schema());
		handle(type, chunk, enqueue);
	};
};

// avsc 5 trusts an array/map block count: a 5-byte message claiming 50M null
// items allocates ~750MB, and maxMessageSize cannot catch it. Every map entry
// (its key) and every array item takes at least one byte, except zero-width
// items (null, a record with no fields, fixed of size 0), so a block claiming
// more items than bytes left is rejected. avsc types are frozen and have no
// read hook, so the decoder compiles arrays and maps (via forSchema's
// typeHook) as subclasses whose _read is avsc 5's own plus the check. A
// precompiled avsc Type passed in by the caller is used as-is, unguarded.
const readBlockCount = (tap) => {
	let n = tap.readLong();
	// A negative count is followed by the block's size in bytes.
	if (n < 0) {
		n = -n;
		tap.skipLong();
	}
	const left = tap.buf.length - tap.pos;
	// n is 0 once reading has run past the end (left < 0): leave that to
	// avsc's own "truncated buffer" check.
	if (n > 0 && n > left) {
		throw new RangeError(`block count ${n} exceeds the ${left} bytes left`);
	}
	return n;
};
class GuardedArrayType extends avro.types.ArrayType {
	_read(tap) {
		const value = [];
		for (let n = readBlockCount(tap); n > 0; n = readBlockCount(tap)) {
			for (let i = 0; i < n; i++) value.push(this.itemsType._read(tap));
		}
		return value;
	}
}
class GuardedMapType extends avro.types.MapType {
	_read(tap) {
		const value = {};
		for (let n = readBlockCount(tap); n > 0; n = readBlockCount(tap)) {
			for (let i = 0; i < n; i++) {
				const key = tap.readString();
				value[key] = this.valuesType._read(tap);
			}
		}
		return value;
	}
}
// Called by forSchema for every schema in the tree; returning undefined lets
// avsc build the type itself.
const typeHook = (schema, options) => {
	if (schema.type === "array") return new GuardedArrayType(schema, options);
	if (schema.type === "map") return new GuardedMapType(schema, options);
};
const compileDecoder = (name, value) => compile(name, value, { typeHook });

export const avroEncodeStream = ({ schema } = {}, streamOptions = {}) => {
	const name = "avroEncodeStream";
	// toBuffer's own error names only the leaf type ('invalid "int": "x"');
	// isValid's errorHook gives the path of every offending field instead.
	const paths = [];
	const errorHook = (path) => paths.push(["$", ...path].join("."));
	const checkOptions = { errorHook };
	const encode = (type, chunk, enqueue) => {
		if (!type.isValid(chunk, checkOptions)) {
			throw new TypeError(
				`${name}: record does not match schema at ${paths.join(", ")}`,
			);
		}
		enqueue(type.toBuffer(chunk));
	};
	const transform = makeTransform(name, schema, compile, encode);
	return createTransformStream(transform, streamOptions);
};

export const avroDecodeStream = (
	{ schema, resolveSchema, maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE } = {},
	streamOptions = {},
) => {
	const name = "avroDecodeStream";
	if (schema && resolveSchema) {
		throw new TypeError(`${name}: pass schema or resolveSchema, not both`);
	}
	const messageLimit = maxMessageSize ?? Number.POSITIVE_INFINITY;
	// Zero-copy Buffer view: avsc 5 decodes strings with Buffer methods that
	// plain Uint8Array / ArrayBuffer chunks lack.
	const toBuffer = (bytes, what) => {
		let buffer;
		if (ArrayBuffer.isView(bytes)) {
			buffer = Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
		} else if (bytes instanceof ArrayBuffer) {
			buffer = Buffer.from(bytes);
		} else {
			throw new TypeError(
				`${name}: ${what} must be a Uint8Array, ArrayBuffer view, or ArrayBuffer`,
			);
		}
		if (buffer.byteLength > messageLimit) {
			throw new RangeError(
				`${name}: message exceeds maxMessageSize (${maxMessageSize} bytes)`,
			);
		}
		return buffer;
	};
	const decode = (type, buffer) => {
		try {
			return type.fromBuffer(buffer);
		} catch (cause) {
			throw new Error(`${name}: ${cause.message}`, { cause });
		}
	};

	if (resolveSchema) {
		const types = new Map();
		const loadType = async (id) => {
			const resolved = await resolveSchema(id);
			const type = compileDecoder(name, resolved?.schemaDefinition ?? resolved);
			if (types.size >= MAX_CACHED_TYPES) {
				types.delete(types.keys().next().value);
			}
			types.set(id, type);
			return type;
		};
		const transform = ({ payload, ...envelope }, enqueue) => {
			const buffer = toBuffer(payload, "payload");
			const id = envelope.schemaId ?? envelope.schemaVersionId;
			const emit = (type) =>
				enqueue({ ...envelope, value: decode(type, buffer) });
			const type = types.get(id);
			if (type !== undefined) {
				emit(type);
				return;
			}
			return loadType(id).then(emit);
		};
		return createTransformStream(transform, streamOptions);
	}

	const decodeBuffer = makeTransform(
		name,
		schema,
		compileDecoder,
		(type, buffer, enqueue) => enqueue(decode(type, buffer)),
	);
	const transform = (chunk, enqueue) =>
		decodeBuffer(toBuffer(chunk, "chunk"), enqueue);
	return createTransformStream(transform, streamOptions);
};
