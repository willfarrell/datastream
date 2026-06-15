// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream } from "@datastream/core";

// Resolve a ProtobufTypeInput once: a static Type is returned as-is (the hot
// path skips recomputation), while a function is invoked per chunk and may
// return either a Type or a Promise<Type>.
const makeResolveType = (Type) => {
	if (typeof Type === "function") {
		return (chunk) => Type(chunk);
	}
	return () => Type;
};

export const protobufEncodeStream = ({ Type } = {}, streamOptions = {}) => {
	const resolveType = makeResolveType(Type);
	const transform = async (chunk, enqueue) => {
		const type = await resolveType(chunk);
		enqueue(type.encode(type.create(chunk)).finish());
	};
	return createTransformStream(transform, streamOptions);
};

export const protobufDecodeStream = (
	{ Type, payload, maxOutputSize = Number.POSITIVE_INFINITY } = {},
	streamOptions = {},
) => {
	const resolveType = makeResolveType(Type);
	const getPayload = payload ?? ((chunk) => chunk);
	let inputSize = 0;
	const transform = async (chunk, enqueue) => {
		const bytes = getPayload(chunk);
		// Default ceiling is Infinity, so the guard is the sole gate and the
		// comparison stays load-bearing (no redundant null check to mutate away).
		inputSize += bytes.length;
		if (inputSize > maxOutputSize) {
			throw new Error(
				`Protobuf decode input exceeds maxOutputSize (${maxOutputSize} bytes)`,
			);
		}
		const type = await resolveType(chunk);
		enqueue(type.decode(bytes));
	};
	return createTransformStream(transform, streamOptions);
};

// Base-128 varint, matching the protobuf length-delimited wire format. Uses
// arithmetic (not bitwise) so lengths beyond 2^31 encode/decode correctly.
const encodeVarint = (value) => {
	const bytes = [];
	let v = value;
	while (v > 0x7f) {
		bytes.push((v % 0x80) | 0x80);
		v = Math.floor(v / 0x80);
	}
	bytes.push(v);
	return Uint8Array.from(bytes);
};

export const protobufLengthPrefixFrameStream = (
	_options = {},
	streamOptions = {},
) => {
	const transform = (chunk, enqueue) => {
		const prefix = encodeVarint(chunk.length);
		const framed = new Uint8Array(prefix.length + chunk.length);
		framed.set(prefix, 0);
		framed.set(chunk, prefix.length);
		enqueue(framed);
	};
	return createTransformStream(transform, streamOptions);
};

export const protobufLengthPrefixUnframeStream = (
	{ maxMessageSize = Number.POSITIVE_INFINITY } = {},
	streamOptions = {},
) => {
	// Growable ring-free byte buffer: `buffer` is the backing store (capacity),
	// `start` is the first unconsumed byte and `end` is one past the last buffered
	// byte. readMessage advances `start` instead of re-slicing per message, and
	// append() writes into spare tail capacity — compacting/doubling only when the
	// chunk doesn't fit. The old `concat([buffer, chunk])` per chunk was O(n²) both
	// across many small messages (per-message re-slice) and across one large
	// message spanning many chunks (full re-copy each append); this is amortized
	// O(n) for both.
	let buffer = new Uint8Array(0);
	let start = 0;
	let end = 0;

	const append = (chunk) => {
		if (buffer.length - end >= chunk.length) {
			// Fits in the existing tail capacity — no copy of prior bytes.
			buffer.set(chunk, end);
			end += chunk.length;
			return;
		}
		// Doesn't fit at the tail. Reclaim the consumed prefix [0, start); grow
		// (doubling) only when even the reclaimed space is insufficient.
		const used = end - start;
		const capacity = Math.max(used + chunk.length, buffer.length * 2);
		if (capacity > buffer.length) {
			const next = new Uint8Array(capacity);
			next.set(buffer.subarray(start, end), 0);
			buffer = next;
		} else {
			buffer.copyWithin(0, start, end);
		}
		start = 0;
		end = used;
		buffer.set(chunk, end);
		end += chunk.length;
	};

	// Pull one complete length-prefixed message off the front of the buffered
	// region [start, end), or return null when the bytes don't yet hold a full
	// prefix+message.
	const readMessage = () => {
		let length = 0;
		let scale = 1;
		let offset = start;
		let complete = false;
		// offset only ever advances by 1 from start, so it lands on `end` exactly;
		// `!==` (rather than `<`) avoids a phantom read one past the end being
		// indistinguishable from the real terminating-byte case.
		while (offset !== end) {
			const byte = buffer[offset];
			length += (byte & 0x7f) * scale;
			offset += 1;
			if ((byte & 0x80) === 0) {
				complete = true;
				break;
			}
			scale *= 0x80;
		}
		if (!complete) {
			return null;
		}
		if (length > maxMessageSize) {
			throw new Error(
				`Protobuf message exceeds maxMessageSize (${maxMessageSize} bytes)`,
			);
		}
		if (end - offset < length) {
			return null;
		}
		const message = buffer.slice(offset, offset + length);
		start = offset + length;
		return message;
	};

	const transform = (chunk, enqueue) => {
		append(chunk);
		let message = readMessage();
		while (message !== null) {
			enqueue(message);
			message = readMessage();
		}
	};

	const flush = () => {
		if (end - start > 0) {
			throw new Error(
				"Protobuf unframe stream ended with an incomplete message",
			);
		}
	};

	return createTransformStream(transform, flush, streamOptions);
};
