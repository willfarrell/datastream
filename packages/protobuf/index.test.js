// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import {
	createReadableStream,
	isWritable,
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
import { variant } from "../variant.js";

describe(`@datastream/protobuf (${variant})`, () => {
	const Type = new protobuf.Type("Msg")
		.add(new protobuf.Field("id", 1, "int32"))
		.add(new protobuf.Field("name", 2, "string"));

	const messages = [
		{ id: 1, name: "alpha" },
		{ id: 2, name: "beta" },
		{ id: 3, name: "gamma" },
	];

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

	test(`protobufEncodeStream encodes objects to bytes (static Type)`, async () => {
		const encoded = await encodeAll(messages, { Type });
		strictEqual(encoded.length, 3);
		for (const bytes of encoded) {
			strictEqual(bytes instanceof Uint8Array, true);
		}
		deepStrictEqual(
			encoded.map((b) => toPlain(Type.decode(b))),
			messages,
		);
	});

	test(`protobufEncodeStream accepts a sync function Type`, async () => {
		const encoded = await encodeAll(messages, { Type: () => Type });
		deepStrictEqual(
			encoded.map((b) => toPlain(Type.decode(b))),
			messages,
		);
	});

	test(`protobufEncodeStream accepts an async function Type`, async () => {
		const encoded = await encodeAll(messages, { Type: async () => Type });
		deepStrictEqual(
			encoded.map((b) => toPlain(Type.decode(b))),
			messages,
		);
	});

	test(`protobufEncodeStream honors streamOptions`, async () => {
		const encoded = await encodeAll(messages, { Type }, {});
		const stream = protobufEncodeStream({ Type }, { highWaterMark: 1 });
		strictEqual(isWritable(stream), true);
		strictEqual(encoded.length, 3);
	});

	test(`protobufEncodeStream constructs with no arguments`, () => {
		const stream = protobufEncodeStream();
		strictEqual(isWritable(stream), true);
	});

	test(`protobufDecodeStream decodes bytes to objects`, async () => {
		const encoded = await encodeAll(messages, { Type });
		const decoded = await streamToArray(
			pipejoin([createReadableStream(encoded), protobufDecodeStream({ Type })]),
		);
		deepStrictEqual(decoded.map(toPlain), messages);
	});

	test(`protobufDecodeStream accepts a sync function Type`, async () => {
		const encoded = await encodeAll(messages, { Type });
		const decoded = await streamToArray(
			pipejoin([
				createReadableStream(encoded),
				protobufDecodeStream({ Type: () => Type }),
			]),
		);
		deepStrictEqual(decoded.map(toPlain), messages);
	});

	test(`protobufDecodeStream accepts an async function Type`, async () => {
		const encoded = await encodeAll(messages, { Type });
		const decoded = await streamToArray(
			pipejoin([
				createReadableStream(encoded),
				protobufDecodeStream({ Type: async () => Type }),
			]),
		);
		deepStrictEqual(decoded.map(toPlain), messages);
	});

	test(`protobufDecodeStream extracts bytes via payload`, async () => {
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

	const SizeType = { decode: (bytes) => bytes.length };

	test(`protobufDecodeStream maxMessageSize is per message, not cumulative`, async () => {
		const encoded = await encodeAll(messages, { Type });
		const largest = Math.max(...encoded.map((b) => b.length));
		const total = encoded.reduce((sum, b) => sum + b.length, 0);
		ok(total > largest);
		const decoded = await streamToArray(
			pipejoin([
				createReadableStream(encoded),
				protobufDecodeStream({ Type, maxMessageSize: largest }),
			]),
		);
		deepStrictEqual(decoded.map(toPlain), messages);
	});

	test(`protobufDecodeStream throws a RangeError when a message exceeds maxMessageSize`, async () => {
		const encoded = await encodeAll([messages[0], bigMessage], { Type });
		const limit = encoded[1].length - 1;
		await rejects(
			pipeline([
				createReadableStream(encoded),
				protobufDecodeStream({ Type, maxMessageSize: limit }),
			]),
			{
				name: "RangeError",
				message: `Protobuf message exceeds maxMessageSize (${limit} bytes)`,
			},
		);
	});

	test(`protobufDecodeStream accepts a message exactly at the 64MiB default maxMessageSize`, async () => {
		const decoded = await streamToArray(
			pipejoin([
				createReadableStream([new Uint8Array(67_108_864)]),
				protobufDecodeStream({ Type: SizeType }),
			]),
		);
		deepStrictEqual(decoded, [67_108_864]);
	});

	test(`protobufDecodeStream rejects a message over the 64MiB default maxMessageSize`, async () => {
		await rejects(
			pipeline([
				createReadableStream([new Uint8Array(67_108_865)]),
				protobufDecodeStream({ Type: SizeType, maxMessageSize: undefined }),
			]),
			{
				name: "RangeError",
				message: "Protobuf message exceeds maxMessageSize (67108864 bytes)",
			},
		);
	});

	test(`protobufDecodeStream maxMessageSize: null disables the limit`, async () => {
		const decoded = await streamToArray(
			pipejoin([
				createReadableStream([new Uint8Array(67_108_865)]),
				protobufDecodeStream({ Type: SizeType, maxMessageSize: null }),
			]),
		);
		deepStrictEqual(decoded, [67_108_865]);
	});

	test(`protobufDecodeStream constructs with no arguments`, () => {
		const stream = protobufDecodeStream();
		strictEqual(isWritable(stream), true);
	});

	test(`frame then unframe round-trips messages (one chunk each)`, async () => {
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

	test(`frame uses a single-byte varint prefix for a 127-byte message`, async () => {
		const body = new Uint8Array(127).fill(7);
		const frames = await streamToArray(
			pipejoin([
				createReadableStream([body]),
				protobufLengthPrefixFrameStream(),
			]),
		);
		strictEqual(frames.length, 1);
		strictEqual(frames[0].length, 128);
		const unframed = await streamToArray(
			pipejoin([
				createReadableStream(frames),
				protobufLengthPrefixUnframeStream(),
			]),
		);
		deepStrictEqual(unframed, [body]);
	});

	test(`unframe allows a message exactly at maxMessageSize`, async () => {
		const body = new Uint8Array(50).fill(3);
		const frames = await streamToArray(
			pipejoin([
				createReadableStream([body]),
				protobufLengthPrefixFrameStream(),
			]),
		);
		const unframed = await streamToArray(
			pipejoin([
				createReadableStream(frames),
				protobufLengthPrefixUnframeStream({ maxMessageSize: 50 }),
			]),
		);
		deepStrictEqual(unframed, [body]);
	});

	test(`unframe reassembles messages split across byte-sized chunks`, async () => {
		const encoded = await encodeAll([bigMessage, ...messages], { Type });
		const frames = await streamToArray(
			pipejoin([
				createReadableStream(encoded),
				protobufLengthPrefixFrameStream(),
			]),
		);
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

	test(`unframe reassembles a body split mid-chunk with trailing frames`, async () => {
		const encoded = await encodeAll([bigMessage, ...messages], { Type });
		const frames = await streamToArray(
			pipejoin([
				createReadableStream(encoded),
				protobufLengthPrefixFrameStream(),
			]),
		);
		const stream = concat(frames);
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

	test(`unframe reassembles many small messages fed one byte at a time`, async () => {
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

	test(`unframe throws when a message exceeds maxMessageSize`, async () => {
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
			strictEqual(e.name, "RangeError");
			strictEqual(
				e.message,
				"Protobuf message exceeds maxMessageSize (8 bytes)",
			);
		}
	});

	test(`unframe throws on a length prefix longer than 10 bytes`, async () => {
		const overlong = new Uint8Array(200).fill(0x80);
		overlong[199] = 0x01;
		try {
			await pipeline([
				createReadableStream([overlong, Uint8Array.of(2, 0xaa, 0xbb)]),
				protobufLengthPrefixUnframeStream({ maxMessageSize: 1024 }),
			]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("length prefix"));
		}
	});

	test(`unframe accepts a 10-byte length prefix but not 11`, async () => {
		const prefix = (size) => {
			const bytes = new Uint8Array(size).fill(0x80);
			bytes[size - 1] = 0x00;
			return bytes;
		};
		const unframed = await streamToArray(
			pipejoin([
				createReadableStream([prefix(10)]),
				protobufLengthPrefixUnframeStream(),
			]),
		);
		deepStrictEqual(unframed, [new Uint8Array(0)]);
		try {
			await pipeline([
				createReadableStream([prefix(11)]),
				protobufLengthPrefixUnframeStream(),
			]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("length prefix"));
		}
	});

	test(`unframe throws when the stream ends mid-message`, async () => {
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

	test(`frame honors streamOptions and constructs with no arguments`, async () => {
		const stream = protobufLengthPrefixFrameStream(undefined, {});
		strictEqual(isWritable(stream), true);
		const encoded = await encodeAll(messages, { Type });
		const frames = await streamToArray(
			pipejoin([
				createReadableStream(encoded),
				protobufLengthPrefixFrameStream({}, {}),
			]),
		);
		strictEqual(frames.length, 3);
	});
	const varint = (n) => {
		const bytes = [];
		while (n > 0x7f) {
			bytes.push((n % 0x80) | 0x80);
			n = Math.floor(n / 0x80);
		}
		bytes.push(n);
		return Uint8Array.from(bytes);
	};
	const incomplete = /ended with an incomplete message/;

	test(`unframe rejects a frame over the 64MiB default maxMessageSize`, async () => {
		await rejects(
			pipeline([
				createReadableStream([varint(67_108_865)]),
				protobufLengthPrefixUnframeStream(),
			]),
			{
				name: "RangeError",
				message: "Protobuf message exceeds maxMessageSize (67108864 bytes)",
			},
		);
	});

	test(`unframe accepts a frame exactly at the 64MiB default maxMessageSize`, async () => {
		await rejects(
			pipeline([
				createReadableStream([varint(67_108_864)]),
				protobufLengthPrefixUnframeStream(),
			]),
			incomplete,
		);
	});

	test(`unframe maxMessageSize: null disables the limit`, async () => {
		await rejects(
			pipeline([
				createReadableStream([varint(2 ** 40)]),
				protobufLengthPrefixUnframeStream({ maxMessageSize: null }),
			]),
			incomplete,
		);
	});

	test(`protobufLengthPrefixUnframeStream skips a straddling prefix after a mid-chunk message`, async () => {
		const body = new Uint8Array(200).fill(0x42);
		const chunks = [
			Uint8Array.from([0x01, 0x41, 0xc8]),
			Uint8Array.from([0x01, ...body]),
		];
		const out = await streamToArray(
			pipejoin([
				createReadableStream(chunks),
				protobufLengthPrefixUnframeStream(),
			]),
		);
		deepStrictEqual(
			out.map((m) => Array.from(m)),
			[[0x41], Array.from(body)],
		);
	});

	test(`protobufLengthPrefixFrameStream writes multi-byte varint prefixes`, async () => {
		const bodies = [new Uint8Array(128).fill(1), new Uint8Array(16384).fill(2)];
		const framed = await streamToArray(
			pipejoin([
				createReadableStream(bodies),
				protobufLengthPrefixFrameStream(),
			]),
		);
		deepStrictEqual(Array.from(framed[0].subarray(0, 2)), [0x80, 0x01]);
		strictEqual(framed[0].length, 130);
		deepStrictEqual(Array.from(framed[1].subarray(0, 3)), [0x80, 0x80, 0x01]);
		strictEqual(framed[1].length, 16387);
		const unframed = await streamToArray(
			pipejoin([
				createReadableStream(framed),
				protobufLengthPrefixUnframeStream(),
			]),
		);
		deepStrictEqual(unframed, bodies);
	});
});
