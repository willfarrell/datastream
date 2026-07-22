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
	// Buffer incoming chunks as a list instead of one growable byte buffer: the
	// varint length prefix is parsed across chunk boundaries and each message is
	// copied out exactly once. This keeps the hot path free of per-chunk realloc
	// and — just as importantly — free of capacity-reuse branches whose only effect
	// is speed (and which mutation testing cannot distinguish from the slow path).
	const pending = [];
	let pendingLen = 0;

	// Byte at absolute position `pos` (0 <= pos < pendingLen) across the chunks.
	const byteAt = (pos) => {
		let index = 0;
		while (pos >= pending[index].byteLength) {
			pos -= pending[index].byteLength;
			index += 1;
		}
		return pending[index][pos];
	};

	// Drop the first `count` bytes off the front of the buffered chunks.
	const skip = (count) => {
		while (count > 0) {
			const head = pending[0];
			const n = Math.min(head.byteLength, count);
			count -= n;
			pendingLen -= n;
			const rest = head.subarray(n);
			if (rest.byteLength === 0) {
				pending.shift();
			} else {
				pending[0] = rest;
			}
		}
	};

	// Copy the first `count` bytes off the front of the buffered chunks into a new
	// contiguous Uint8Array, consuming them.
	const take = (count) => {
		const out = new Uint8Array(count);
		let filled = 0;
		while (filled < count) {
			const head = pending[0];
			const n = Math.min(head.byteLength, count - filled);
			out.set(head.subarray(0, n), filled);
			filled += n;
			const rest = head.subarray(n);
			if (rest.byteLength === 0) {
				pending.shift();
			} else {
				pending[0] = rest;
			}
		}
		pendingLen -= count;
		return out;
	};

	// Pull one complete length-prefixed message off the front, or return null when
	// the buffered bytes don't yet hold a full prefix + body.
	const readMessage = () => {
		let length = 0;
		let scale = 1;
		let prefixBytes = 0;
		let complete = false;
		while (prefixBytes < pendingLen) {
			const byte = byteAt(prefixBytes);
			length += (byte & 0x7f) * scale;
			prefixBytes += 1;
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
		if (pendingLen - prefixBytes < length) {
			return null;
		}
		skip(prefixBytes);
		return take(length);
	};

	const transform = (chunk, enqueue) => {
		pending.push(chunk);
		pendingLen += chunk.byteLength;
		let message = readMessage();
		while (message !== null) {
			enqueue(message);
			message = readMessage();
		}
	};

	const flush = () => {
		if (pendingLen > 0) {
			throw new Error(
				"Protobuf unframe stream ended with an incomplete message",
			);
		}
	};

	return createTransformStream(transform, flush, streamOptions);
};
