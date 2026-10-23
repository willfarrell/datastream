// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream } from "@datastream/core";

const encode = (type, chunk, enqueue) =>
	enqueue(type.encode(type.create(chunk)).finish());
export const protobufEncodeStream = ({ Type } = {}, streamOptions = {}) => {
	const transform =
		typeof Type === "function"
			? async (chunk, enqueue) => encode(await Type(chunk), chunk, enqueue)
			: (chunk, enqueue) => encode(Type, chunk, enqueue);
	return createTransformStream(transform, streamOptions);
};

// Default per-message ceiling: a hostile length prefix (unframe) or oversized
// payload (decode) must not force an unbounded buffer/decode. null opts out.
const DEFAULT_MAX_MESSAGE_SIZE = 64 * 1024 * 1024;

export const protobufDecodeStream = (
	{ Type, payload, maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE } = {},
	streamOptions = {},
) => {
	const getPayload = payload ?? ((chunk) => chunk);
	// Checked per message, not cumulatively: a cumulative cap with any finite
	// default would eventually kill every long-lived consumer.
	const messageLimit = maxMessageSize ?? Number.POSITIVE_INFINITY;
	const checkedPayload = (chunk) => {
		const bytes = getPayload(chunk);
		if (bytes.length > messageLimit) {
			throw new RangeError(
				`Protobuf message exceeds maxMessageSize (${maxMessageSize} bytes)`,
			);
		}
		return bytes;
	};
	const transform =
		typeof Type === "function"
			? async (chunk, enqueue) => {
					const bytes = checkedPayload(chunk);
					enqueue((await Type(chunk)).decode(bytes));
				}
			: (chunk, enqueue) => enqueue(Type.decode(checkedPayload(chunk)));
	return createTransformStream(transform, streamOptions);
};

// Base-128 varint, matching the protobuf length-delimited wire format. Uses
// arithmetic (not bitwise) so lengths beyond 2^31 encode/decode correctly.
// Bytes needed to encode `value` as a varint (1 for 0..127, 2 for ..16383).
const varintLength = (value) => {
	let n = 1;
	let v = value;
	while (v > 0x7f) {
		v = Math.floor(v / 0x80);
		n += 1;
	}
	return n;
};
// Writes `value` as a varint into `out` from index 0; the caller sized it with
// varintLength so the last byte is the one without a continuation bit.
const writeVarint = (out, value) => {
	let i = 0;
	let v = value;
	while (v > 0x7f) {
		out[i] = (v % 0x80) | 0x80;
		v = Math.floor(v / 0x80);
		i += 1;
	}
	out[i] = v;
};

export const protobufLengthPrefixFrameStream = (
	_options = {},
	streamOptions = {},
) => {
	const transform = (chunk, enqueue) => {
		const prefixLength = varintLength(chunk.length);
		const framed = new Uint8Array(prefixLength + chunk.length);
		writeVarint(framed, chunk.length);
		framed.set(chunk, prefixLength);
		enqueue(framed);
	};
	return createTransformStream(transform, streamOptions);
};

export const protobufLengthPrefixUnframeStream = (
	{ maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE } = {},
	streamOptions = {},
) => {
	const messageLimit = maxMessageSize ?? Number.POSITIVE_INFINITY;
	// Buffer incoming chunks as a list instead of one growable byte buffer: the
	// varint length prefix is parsed across chunk boundaries and each message is
	// copied out exactly once. This keeps the hot path free of per-chunk realloc
	// and — just as importantly — free of capacity-reuse branches whose only effect
	// is speed (and which mutation testing cannot distinguish from the slow path).
	const pending = [];
	let pendingLen = 0;
	// Bytes already consumed off the front of pending[0]: tracked as an offset so
	// consuming never re-slices the head chunk.
	let headOffset = 0;

	// Byte at absolute position `pos` (0 <= pos < pendingLen) across the chunks.
	const byteAt = (pos) => {
		pos += headOffset;
		let index = 0;
		while (pos >= pending[index].byteLength) {
			pos -= pending[index].byteLength;
			index += 1;
		}
		return pending[index][pos];
	};

	// Consume `n` bytes off the front of pending[0] (n is at most what is left of
	// it), dropping the chunk once it is used up.
	const consume = (n) => {
		headOffset += n;
		pendingLen -= n;
		if (headOffset === pending[0].byteLength) {
			pending.shift();
			headOffset = 0;
		}
	};

	// Drop the first `count` bytes off the front of the buffered chunks.
	const skip = (count) => {
		while (count > 0) {
			const n = Math.min(pending[0].byteLength - headOffset, count);
			count -= n;
			consume(n);
		}
	};

	// Copy the first `count` bytes off the front of the buffered chunks into a new
	// contiguous Uint8Array, consuming them.
	const take = (count) => {
		const out = new Uint8Array(count);
		let filled = 0;
		while (filled < count) {
			const head = pending[0];
			const n = Math.min(head.byteLength - headOffset, count - filled);
			out.set(head.subarray(headOffset, headOffset + n), filled);
			filled += n;
			consume(n);
		}
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
			// A varint is at most 10 bytes; longer overflows `length` to NaN and
			// silently drops every following message.
			if (prefixBytes === 10) {
				throw new Error("Protobuf length prefix exceeds 10 bytes");
			}
			scale *= 0x80;
		}
		if (!complete) {
			return null;
		}
		if (length > messageLimit) {
			throw new RangeError(
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
