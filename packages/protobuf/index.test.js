// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, ok, strictEqual } from "node:assert";
import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	protobufDecodeStream,
	protobufEncodeStream,
	protobufLengthPrefixFrameStream,
	protobufLengthPrefixUnframeStream,
} from "@datastream/protobuf";
import protobuf from "protobufjs";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

const Type = new protobuf.Type("Msg")
	.add(new protobuf.Field("id", 1, "int32"))
	.add(new protobuf.Field("name", 2, "string"));

const messages = [
	{ id: 1, name: "alpha" },
	{ id: 2, name: "beta" },
	{ id: 3, name: "gamma" },
];

// A message whose encoded form is >127 bytes, forcing a multi-byte varint
// length prefix (exercises the encodeVarint loop and split-prefix framing).
const bigMessage = { id: 42, name: "x".repeat(200) };

const toPlain = (m) => ({ id: m.id, name: m.name });

const encodeAll = (input, options) =>
	streamToArray(
		pipejoin([createReadableStream(input), protobufEncodeStream(options)]),
	);

const concat = (chunks) => {
	const total = chunks.reduce((sum, c) => sum + c.length, 0);
	const out = new Uint8Array(total);
	let offset = 0;
	for (const c of chunks) {
		out.set(c, offset);
		offset += c.length;
	}
	return out;
};

// *** protobufEncodeStream *** //
test(`${variant}: protobufEncodeStream encodes objects to bytes (static Type)`, async () => {
	const encoded = await encodeAll(messages, { Type });
	strictEqual(encoded.length, 3);
	for (const bytes of encoded) {
		strictEqual(bytes instanceof Uint8Array, true);
	}
	// Round-trips back to the originals.
	deepStrictEqual(
		encoded.map((b) => toPlain(Type.decode(b))),
		messages,
	);
});

test(`${variant}: protobufEncodeStream accepts a sync function Type`, async () => {
	const encoded = await encodeAll(messages, { Type: () => Type });
	deepStrictEqual(
		encoded.map((b) => toPlain(Type.decode(b))),
		messages,
	);
});

test(`${variant}: protobufEncodeStream accepts an async function Type`, async () => {
	const encoded = await encodeAll(messages, { Type: async () => Type });
	deepStrictEqual(
		encoded.map((b) => toPlain(Type.decode(b))),
		messages,
	);
});

test(`${variant}: protobufEncodeStream honors streamOptions`, async () => {
	const encoded = await encodeAll(messages, { Type }, {});
	const stream = protobufEncodeStream({ Type }, { highWaterMark: 1 });
	strictEqual(typeof stream.pipe, "function");
	strictEqual(encoded.length, 3);
});

test(`${variant}: protobufEncodeStream constructs with no arguments`, () => {
	const stream = protobufEncodeStream();
	strictEqual(typeof stream.pipe, "function");
});

// *** protobufDecodeStream *** //
test(`${variant}: protobufDecodeStream decodes bytes to objects`, async () => {
	const encoded = await encodeAll(messages, { Type });
	const decoded = await streamToArray(
		pipejoin([createReadableStream(encoded), protobufDecodeStream({ Type })]),
	);
	deepStrictEqual(decoded.map(toPlain), messages);
});

test(`${variant}: protobufDecodeStream extracts bytes via payload`, async () => {
	const encoded = await encodeAll(messages, { Type });
	const wrapped = encoded.map((data) => ({ data }));
	const decoded = await streamToArray(
		pipejoin([
			createReadableStream(wrapped),
			protobufDecodeStream({ Type, payload: (chunk) => chunk.data }),
		]),
	);
	deepStrictEqual(decoded.map(toPlain), messages);
});

test(`${variant}: protobufDecodeStream allows input within maxOutputSize`, async () => {
	const encoded = await encodeAll(messages, { Type });
	const decoded = await streamToArray(
		pipejoin([
			createReadableStream(encoded),
			protobufDecodeStream({ Type, maxOutputSize: 1024 }),
		]),
	);
	deepStrictEqual(decoded.map(toPlain), messages);
});

test(`${variant}: protobufDecodeStream allows input exactly at maxOutputSize`, async () => {
	const encoded = await encodeAll(messages, { Type });
	// Cumulative input size equal to the ceiling must be accepted (boundary is
	// strictly-greater, not greater-or-equal).
	const exact = encoded.reduce((sum, b) => sum + b.length, 0);
	const decoded = await streamToArray(
		pipejoin([
			createReadableStream(encoded),
			protobufDecodeStream({ Type, maxOutputSize: exact }),
		]),
	);
	deepStrictEqual(decoded.map(toPlain), messages);
});

test(`${variant}: protobufDecodeStream throws when input exceeds maxOutputSize`, async () => {
	const encoded = await encodeAll(messages, { Type });
	try {
		await pipeline([
			createReadableStream(encoded),
			protobufDecodeStream({ Type, maxOutputSize: 1 }),
		]);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxOutputSize"));
	}
});

test(`${variant}: protobufDecodeStream constructs with no arguments`, () => {
	const stream = protobufDecodeStream();
	strictEqual(typeof stream.pipe, "function");
});

// *** length-prefix framing *** //
test(`${variant}: frame then unframe round-trips messages (one chunk each)`, async () => {
	const encoded = await encodeAll(messages, { Type });
	const frames = await streamToArray(
		pipejoin([
			createReadableStream(encoded),
			protobufLengthPrefixFrameStream(),
		]),
	);
	strictEqual(frames.length, 3);
	const unframed = await streamToArray(
		pipejoin([
			createReadableStream(frames),
			protobufLengthPrefixUnframeStream(),
		]),
	);
	deepStrictEqual(
		unframed.map((b) => toPlain(Type.decode(b))),
		messages,
	);
});

test(`${variant}: frame uses a single-byte varint prefix for a 127-byte message`, async () => {
	// 127 (0x7f) is the largest value that fits in a single varint byte; the
	// encodeVarint loop must stop at v > 0x7f (a >= boundary would spill into a
	// second 0x80-continuation byte).
	const body = new Uint8Array(127).fill(7);
	const frames = await streamToArray(
		pipejoin([createReadableStream([body]), protobufLengthPrefixFrameStream()]),
	);
	strictEqual(frames.length, 1);
	strictEqual(frames[0].length, 128); // prefix(1) + body(127)
	const unframed = await streamToArray(
		pipejoin([
			createReadableStream(frames),
			protobufLengthPrefixUnframeStream(),
		]),
	);
	deepStrictEqual(unframed, [body]);
});

test(`${variant}: unframe allows a message exactly at maxMessageSize`, async () => {
	// A message whose length equals the ceiling must pass (boundary is
	// strictly-greater).
	const body = new Uint8Array(50).fill(3);
	const frames = await streamToArray(
		pipejoin([createReadableStream([body]), protobufLengthPrefixFrameStream()]),
	);
	const unframed = await streamToArray(
		pipejoin([
			createReadableStream(frames),
			protobufLengthPrefixUnframeStream({ maxMessageSize: 50 }),
		]),
	);
	deepStrictEqual(unframed, [body]);
});

test(`${variant}: unframe reassembles messages split across byte-sized chunks`, async () => {
	const encoded = await encodeAll([bigMessage, ...messages], { Type });
	const frames = await streamToArray(
		pipejoin([
			createReadableStream(encoded),
			protobufLengthPrefixFrameStream(),
		]),
	);
	// One contiguous buffer, then feed it one byte at a time so that both the
	// multi-byte length prefix and the message body straddle chunk boundaries.
	const stream = concat(frames);
	const byteChunks = Array.from(stream, (b) => Uint8Array.of(b));
	const unframed = await streamToArray(
		pipejoin([
			createReadableStream(byteChunks),
			protobufLengthPrefixUnframeStream({ maxMessageSize: 4096 }),
		]),
	);
	deepStrictEqual(
		unframed.map((b) => toPlain(Type.decode(b))),
		[bigMessage, ...messages],
	);
});

test(`${variant}: unframe reassembles a body split mid-chunk with trailing frames`, async () => {
	// Two chunks where the split falls inside bigMessage's body, so a single
	// message is assembled from a partial first chunk plus a second chunk that
	// also carries the following frames. This drives the multi-chunk copy path:
	// the body spans two buffered chunks and the second chunk's tail (the next
	// frames) must be retained, not discarded.
	const encoded = await encodeAll([bigMessage, ...messages], { Type });
	const frames = await streamToArray(
		pipejoin([
			createReadableStream(encoded),
			protobufLengthPrefixFrameStream(),
		]),
	);
	const stream = concat(frames);
	// 100 lands inside bigMessage's body (2-byte prefix + ~200-byte body), and the
	// remainder carries the rest of that body plus all three trailing frames.
	const splitAt = 100;
	const chunks = [stream.subarray(0, splitAt), stream.subarray(splitAt)];
	const unframed = await streamToArray(
		pipejoin([
			createReadableStream(chunks),
			protobufLengthPrefixUnframeStream({ maxMessageSize: 4096 }),
		]),
	);
	deepStrictEqual(
		unframed.map((b) => toPlain(Type.decode(b))),
		[bigMessage, ...messages],
	);
});

test(`${variant}: unframe reassembles many small messages fed one byte at a time`, async () => {
	// Many small messages, fed one byte at a time, so every prefix and body
	// straddles chunk boundaries and the buffered-chunk list is repeatedly drained
	// down to a partial message and refilled.
	const small = Array.from({ length: 20 }, (_, i) => ({
		id: i,
		name: `m${i}`,
	}));
	const encoded = await encodeAll(small, { Type });
	const frames = await streamToArray(
		pipejoin([
			createReadableStream(encoded),
			protobufLengthPrefixFrameStream(),
		]),
	);
	const byteChunks = Array.from(concat(frames), (b) => Uint8Array.of(b));
	const unframed = await streamToArray(
		pipejoin([
			createReadableStream(byteChunks),
			protobufLengthPrefixUnframeStream({ maxMessageSize: 4096 }),
		]),
	);
	deepStrictEqual(
		unframed.map((b) => toPlain(Type.decode(b))),
		small,
	);
});

test(`${variant}: unframe throws when a message exceeds maxMessageSize`, async () => {
	const encoded = await encodeAll([bigMessage], { Type });
	const frames = await streamToArray(
		pipejoin([
			createReadableStream(encoded),
			protobufLengthPrefixFrameStream(),
		]),
	);
	try {
		await pipeline([
			createReadableStream(frames),
			protobufLengthPrefixUnframeStream({ maxMessageSize: 8 }),
		]);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxMessageSize"));
	}
});

test(`${variant}: unframe throws when the stream ends mid-message`, async () => {
	// Prefix declares 5 bytes but only 2 follow.
	const truncated = Uint8Array.of(5, 1, 2);
	try {
		await pipeline([
			createReadableStream([truncated]),
			protobufLengthPrefixUnframeStream(),
		]);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("incomplete message"));
	}
});

test(`${variant}: frame honors streamOptions and constructs with no arguments`, async () => {
	const stream = protobufLengthPrefixFrameStream(undefined, {});
	strictEqual(typeof stream.pipe, "function");
	const encoded = await encodeAll(messages, { Type });
	const frames = await streamToArray(
		pipejoin([
			createReadableStream(encoded),
			protobufLengthPrefixFrameStream({}, {}),
		]),
	);
	strictEqual(frames.length, 3);
});
