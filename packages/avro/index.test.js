// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, rejects, strictEqual, throws } from "node:assert";
import test, { describe } from "node:test";
import { avroDecodeStream, avroEncodeStream } from "@datastream/avro";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { kafkaCommitStream } from "@datastream/kafka";
import {
	confluentUnframeStream,
	glueUnframeStream,
} from "@datastream/schema-registry";
import avro from "avsc";

describe("@datastream/avro", () => {
	const schema = {
		type: "record",
		name: "Msg",
		fields: [
			{ name: "id", type: "int" },
			{ name: "name", type: "string" },
		],
	};
	const type = avro.Type.forSchema(schema);

	const messages = [
		{ id: 1, name: "alpha" },
		{ id: 2, name: "béta" },
		{ id: 3, name: "gamma" },
	];

	// Decoded avsc records are instances of a generated class; compare as plain.
	const toPlain = (m) => ({ id: m.id, name: m.name });

	const encodeAll = (input, options, streamOptions) =>
		streamToArray(
			pipejoin([
				createReadableStream(input),
				avroEncodeStream(options, streamOptions),
			]),
		);
	const decodeAll = (input, options) =>
		streamToArray(
			pipejoin([createReadableStream(input), avroDecodeStream(options)]),
		);

	// *** avroEncodeStream *** //
	test(`avroEncodeStream encodes objects to Avro binary (schema object)`, async () => {
		const encoded = await encodeAll(messages, { schema });
		strictEqual(encoded.length, 3);
		for (const bytes of encoded) {
			strictEqual(bytes instanceof Uint8Array, true);
		}
		// id 1 -> zigzag 0x02; "alpha" -> length 5 zigzag 0x0a + utf8
		deepStrictEqual([...encoded[0]], [0x02, 0x0a, 97, 108, 112, 104, 97]);
		deepStrictEqual(
			encoded.map((b) => toPlain(type.fromBuffer(b))),
			messages,
		);
	});

	test(`avroEncodeStream accepts a JSON string schema`, async () => {
		const encoded = await encodeAll(messages, {
			schema: JSON.stringify(schema),
		});
		deepStrictEqual(
			encoded.map((b) => toPlain(type.fromBuffer(b))),
			messages,
		);
	});

	test(`avroEncodeStream accepts an avsc Type`, async () => {
		const encoded = await encodeAll(messages, { schema: type });
		deepStrictEqual(
			encoded.map((b) => toPlain(type.fromBuffer(b))),
			messages,
		);
	});

	test(`avroEncodeStream resolves a lazy schema once, when the first chunk arrives`, async () => {
		let calls = 0;
		const stream = avroEncodeStream({
			schema: () => {
				calls++;
				return schema;
			},
		});
		// Not resolved at construction: upstream may not have produced it yet.
		strictEqual(calls, 0);
		const encoded = await streamToArray(
			pipejoin([createReadableStream(messages), stream]),
		);
		strictEqual(calls, 1);
		deepStrictEqual(
			encoded.map((b) => toPlain(type.fromBuffer(b))),
			messages,
		);
	});

	test(`avroEncodeStream compiles a concrete schema at construction`, () => {
		throws(() => avroEncodeStream({ schema: { type: "nope" } }), {
			message: 'unknown type: "nope"',
		});
	});

	test(`avroEncodeStream throws when schema is missing`, () => {
		throws(() => avroEncodeStream(), {
			name: "TypeError",
			message: "avroEncodeStream: schema required",
		});
	});

	test(`avroEncodeStream errors when a lazy schema resolves to nothing`, async () => {
		await rejects(
			pipeline([
				createReadableStream(messages),
				avroEncodeStream({ schema: () => undefined }),
			]),
			{ name: "TypeError", message: "avroEncodeStream: schema required" },
		);
	});

	test(`avroEncodeStream errors the stream on an invalid record, naming each bad field`, async () => {
		await rejects(
			pipeline([
				createReadableStream([messages[0], { id: "x", name: 1 }]),
				avroEncodeStream({ schema }),
			]),
			{
				name: "TypeError",
				message:
					"avroEncodeStream: record does not match schema at $.id, $.name",
			},
		);
	});

	test(`avroEncodeStream reports the root path for a non-record value`, async () => {
		await rejects(
			pipeline([
				createReadableStream([1]),
				avroEncodeStream({ schema: { type: "string" } }),
			]),
			{
				name: "TypeError",
				message: "avroEncodeStream: record does not match schema at $",
			},
		);
	});

	test(`avroEncodeStream honors streamOptions`, async () => {
		const encoded = await encodeAll(messages, { schema }, {});
		strictEqual(encoded.length, 3);
	});

	// *** avroDecodeStream *** //
	test(`avroDecodeStream decodes one record per chunk`, async () => {
		const encoded = messages.map((m) => type.toBuffer(m));
		const decoded = await decodeAll(encoded, { schema });
		deepStrictEqual(decoded.map(toPlain), messages);
	});

	test(`avroDecodeStream decodes plain Uint8Array views (not just Buffers)`, async () => {
		// A view into a larger buffer at a non-zero offset, as an unframe stream
		// would emit via subarray(). avsc 5 needs Buffer methods for strings.
		const encoded = messages.map((m) => {
			const bytes = type.toBuffer(m);
			const backing = new Uint8Array(bytes.length + 5);
			backing.set(bytes, 5);
			return backing.subarray(5);
		});
		const decoded = await decodeAll(encoded, { schema });
		deepStrictEqual(decoded.map(toPlain), messages);
	});

	test(`avroDecodeStream resolves a lazy schema once`, async () => {
		let calls = 0;
		const stream = avroDecodeStream({
			schema: () => {
				calls++;
				return JSON.stringify(schema);
			},
		});
		strictEqual(calls, 0);
		const decoded = await streamToArray(
			pipejoin([
				createReadableStream(messages.map((m) => type.toBuffer(m))),
				stream,
			]),
		);
		strictEqual(calls, 1);
		deepStrictEqual(decoded.map(toPlain), messages);
	});

	test(`avroDecodeStream throws when schema is missing`, () => {
		throws(() => avroDecodeStream(), {
			name: "TypeError",
			message: "avroDecodeStream: schema required",
		});
	});

	test(`avroDecodeStream errors the stream on a truncated record`, async () => {
		await rejects(
			pipeline([
				createReadableStream([new Uint8Array([0x02, 0x0a, 0x41])]),
				avroDecodeStream({ schema }),
			]),
			(e) => {
				strictEqual(e.message, "avroDecodeStream: truncated buffer");
				strictEqual(e.cause.message, "truncated buffer");
				return true;
			},
		);
	});

	test(`avroDecodeStream errors the stream on trailing bytes`, async () => {
		await rejects(
			pipeline([
				createReadableStream([new Uint8Array([0x02, 0x00, 0xff])]),
				avroDecodeStream({ schema }),
			]),
			{ message: "avroDecodeStream: trailing data" },
		);
	});

	test(`avroDecodeStream decodes an ArrayBuffer chunk`, async () => {
		const bytes = type.toBuffer(messages[1]);
		const arrayBuffer = bytes.buffer.slice(
			bytes.byteOffset,
			bytes.byteOffset + bytes.length,
		);
		const decoded = await decodeAll([arrayBuffer], { schema });
		deepStrictEqual(decoded.map(toPlain), [messages[1]]);
	});

	test(`avroDecodeStream decodes a DataView chunk at a non-zero offset`, async () => {
		const bytes = type.toBuffer(messages[1]);
		const backing = new Uint8Array(bytes.length + 3);
		backing.set(bytes, 3);
		const decoded = await decodeAll(
			[new DataView(backing.buffer, 3, bytes.length)],
			{ schema },
		);
		deepStrictEqual(decoded.map(toPlain), [messages[1]]);
	});

	test(`avroDecodeStream rejects a string chunk with a TypeError`, async () => {
		await rejects(
			pipeline([createReadableStream(["abc"]), avroDecodeStream({ schema })]),
			{
				name: "TypeError",
				message:
					"avroDecodeStream: chunk must be a Uint8Array, ArrayBuffer view, or ArrayBuffer",
			},
		);
	});

	test(`avroDecodeStream awaits a lazy schema that returns a Promise, once`, async () => {
		let calls = 0;
		const decoded = await decodeAll(
			messages.map((m) => type.toBuffer(m)),
			{
				schema: async () => {
					calls++;
					return schema;
				},
			},
		);
		strictEqual(calls, 1);
		deepStrictEqual(decoded.map(toPlain), messages);
	});

	test(`avroEncodeStream awaits a lazy schema that returns a Promise`, async () => {
		const encoded = await encodeAll(messages, {
			schema: () => Promise.resolve(JSON.stringify(schema)),
		});
		deepStrictEqual(
			encoded.map((b) => toPlain(type.fromBuffer(b))),
			messages,
		);
	});

	test(`avroDecodeStream errors when a lazy schema resolves to nothing`, async () => {
		await rejects(
			pipeline([
				createReadableStream([type.toBuffer(messages[0])]),
				avroDecodeStream({ schema: async () => undefined }),
			]),
			{ name: "TypeError", message: "avroDecodeStream: schema required" },
		);
	});

	// *** Limits *** //
	const fixed = (size) => ({ type: "fixed", name: "Blob", size });

	test(`avroDecodeStream rejects a chunk over the 64MiB default maxMessageSize`, async () => {
		const limit = 64 * 1024 * 1024;
		const [ok] = await decodeAll([new Uint8Array(limit)], {
			schema: fixed(limit),
		});
		strictEqual(ok.length, limit);
		await rejects(
			pipeline([
				createReadableStream([new Uint8Array(limit + 1)]),
				avroDecodeStream({ schema: fixed(limit + 1) }),
			]),
			{
				name: "RangeError",
				message:
					"avroDecodeStream: message exceeds maxMessageSize (67108864 bytes)",
			},
		);
	});

	test(`avroDecodeStream honors maxMessageSize`, async () => {
		const [ok] = await decodeAll([new Uint8Array(3)], {
			schema: fixed(3),
			maxMessageSize: 3,
		});
		strictEqual(ok.length, 3);
		await rejects(
			pipeline([
				createReadableStream([new Uint8Array(4)]),
				avroDecodeStream({ schema: fixed(4), maxMessageSize: 3 }),
			]),
			{
				name: "RangeError",
				message: "avroDecodeStream: message exceeds maxMessageSize (3 bytes)",
			},
		);
	});

	test(`avroDecodeStream with maxMessageSize null imposes no ceiling`, async () => {
		const size = 64 * 1024 * 1024 + 1;
		const [ok] = await decodeAll([new Uint8Array(size)], {
			schema: fixed(size),
			maxMessageSize: null,
		});
		strictEqual(ok.length, size);
	});

	// zigzag varint, as Avro encodes longs (block counts and sizes).
	const long = (n) => {
		const out = [];
		let v = n < 0 ? -n * 2 - 1 : n * 2;
		while (v > 0x7f) {
			out.push((v % 0x80) | 0x80);
			v = Math.floor(v / 0x80);
		}
		out.push(v);
		return out;
	};
	// A 5-byte message claiming 50M items: avsc 5 alone allocates ~750MB.
	const bomb = new Uint8Array([...long(50_000_000), 0]);
	const bombMessage =
		"avroDecodeStream: block count 50000000 exceeds the 1 bytes left";

	test(`avroDecodeStream rejects an array block count larger than the bytes left`, async () => {
		await rejects(
			pipeline([
				createReadableStream([bomb]),
				avroDecodeStream({ schema: { type: "array", items: "null" } }),
			]),
			(e) => {
				strictEqual(e.message, bombMessage);
				strictEqual(e.cause.name, "RangeError");
				return true;
			},
		);
	});

	test(`avroDecodeStream rejects a map block count larger than the bytes left`, async () => {
		await rejects(
			pipeline([
				createReadableStream([bomb]),
				avroDecodeStream({ schema: { type: "map", values: "null" } }),
			]),
			{ message: bombMessage },
		);
	});

	test(`avroDecodeStream guards arrays and maps nested in records, unions, arrays and maps`, async () => {
		const nested = [
			// record field
			[
				{
					type: "record",
					name: "R",
					fields: [{ name: "a", type: { type: "array", items: "null" } }],
				},
				[],
			],
			// union branch 1
			[["null", { type: "map", values: "null" }], long(1)],
			// array of arrays: one outer item, then the inner bomb
			[
				{ type: "array", items: { type: "array", items: "null" } },
				[...long(1)],
			],
			// map of arrays: one entry with key "k"
			[
				{ type: "map", values: { type: "array", items: "null" } },
				[...long(1), ...long(1), 0x6b],
			],
		];
		for (const [nestedSchema, prefix] of nested) {
			await rejects(
				pipeline([
					createReadableStream([new Uint8Array([...prefix, ...bomb])]),
					avroDecodeStream({ schema: nestedSchema }),
				]),
				{ message: bombMessage },
			);
		}
	});

	test(`avroDecodeStream guards recursive schemas`, async () => {
		const tree = {
			type: "record",
			name: "Tree",
			fields: [
				{ name: "id", type: "int" },
				{ name: "children", type: { type: "array", items: "Tree" } },
				{ name: "labels", type: { type: "map", values: "Tree" } },
			],
		};
		const treeType = avro.Type.forSchema(tree);
		const value = {
			id: 1,
			children: [
				{ id: 2, children: [], labels: {} },
				{
					id: 3,
					children: [],
					labels: { x: { id: 4, children: [], labels: {} } },
				},
			],
			labels: {},
		};
		const [decoded] = await decodeAll([treeType.toBuffer(value)], {
			schema: tree,
		});
		deepStrictEqual(JSON.parse(JSON.stringify(decoded)), value);
		await rejects(
			pipeline([
				createReadableStream([
					new Uint8Array([...long(1), ...long(1), ...long(2), ...bomb]),
				]),
				avroDecodeStream({ schema: tree }),
			]),
			{ message: bombMessage },
		);
	});

	test(`avroDecodeStream reads negative (sized) array and map blocks`, async () => {
		// [1, 2] as one block of -2 items, 2 bytes long
		const [array] = await decodeAll(
			[new Uint8Array([...long(-2), ...long(2), ...long(1), ...long(2), 0])],
			{ schema: { type: "array", items: "int" } },
		);
		deepStrictEqual(array, [1, 2]);
		// { a: 1, b: 2 } as one block of -2 entries, 6 bytes long, then { c: 3 }
		const [map] = await decodeAll(
			[
				new Uint8Array([
					...long(-2),
					...long(6),
					...long(1),
					0x61,
					...long(1),
					...long(1),
					0x62,
					...long(2),
					...long(1),
					...long(1),
					0x63,
					...long(3),
					0,
				]),
			],
			{ schema: { type: "map", values: "int" } },
		);
		deepStrictEqual({ ...map }, { a: 1, b: 2, c: 3 });
	});

	test(`avroDecodeStream reports an array missing its closing block as truncated`, async () => {
		// One boolean item, then no closing 0 block: reading the next count runs
		// past the end, and avsc reports the truncation.
		await rejects(
			pipeline([
				createReadableStream([new Uint8Array([...long(1), 1])]),
				avroDecodeStream({ schema: { type: "array", items: "boolean" } }),
			]),
			{ message: "avroDecodeStream: truncated buffer" },
		);
	});

	test(`avroDecodeStream rejects arrays of zero-width items longer than the bytes left`, async () => {
		// [null] is count 1 with 1 byte left (the closing 0): allowed. [null,
		// null] encodes as 2 bytes (count 2, then the closing 0): rejected, a
		// documented limitation of the block-count guard.
		const nulls = { type: "array", items: "null" };
		deepStrictEqual(
			(
				await decodeAll([avro.Type.forSchema(nulls).toBuffer([null])], {
					schema: nulls,
				})
			)[0],
			[null],
		);
		await rejects(
			pipeline([
				createReadableStream([
					avro.Type.forSchema(nulls).toBuffer([null, null]),
				]),
				avroDecodeStream({ schema: nulls }),
			]),
			{ message: "avroDecodeStream: block count 2 exceeds the 1 bytes left" },
		);
	});

	// *** Per-message schemas (resolveSchema) *** //
	const v2 = {
		type: "record",
		name: "Msg",
		fields: [
			{ name: "id", type: "int" },
			{ name: "name", type: "string" },
			{ name: "tags", type: { type: "array", items: "string" } },
		],
	};
	const typeV2 = avro.Type.forSchema(v2);
	const confluentFrame = (schemaId, bytes) => {
		const frame = new Uint8Array(5 + bytes.length);
		new DataView(frame.buffer).setUint32(1, schemaId, false);
		frame.set(bytes, 5);
		return frame;
	};
	// Shaped like kafkaConsumeStream's messages.
	const kafkaMessage = (offset, value) => ({
		topic: "orders",
		partition: 0,
		offset,
		key: null,
		value,
		headers: {},
		timestamp: "1700000000000",
	});
	// Stands in for confluentSchemaRegistryResolver: same argument and result.
	const fakeRegistry = (schemas) => {
		const lookups = [];
		const resolveSchema = async (schemaId) => {
			lookups.push(schemaId);
			return {
				schemaId,
				schemaDefinition: JSON.stringify(schemas[schemaId]),
				dataFormat: "AVRO",
				references: [],
			};
		};
		return { resolveSchema, lookups };
	};
	const kafkaMessages = [
		kafkaMessage("0", confluentFrame(1, type.toBuffer(messages[0]))),
		kafkaMessage(
			"1",
			confluentFrame(2, typeV2.toBuffer({ ...messages[1], tags: ["x"] })),
		),
		kafkaMessage("2", confluentFrame(1, type.toBuffer(messages[2]))),
		kafkaMessage(
			"3",
			confluentFrame(2, typeV2.toBuffer({ ...messages[0], tags: [] })),
		),
	];

	test(`kafka messages -> confluentUnframeStream -> avroDecodeStream({ resolveSchema }) decodes per schema id and keeps the metadata`, async () => {
		const { resolveSchema, lookups } = fakeRegistry({ 1: schema, 2: v2 });
		const out = await streamToArray(
			pipejoin([
				createReadableStream(kafkaMessages),
				confluentUnframeStream(),
				avroDecodeStream({ resolveSchema }),
			]),
		);
		deepStrictEqual(
			out.map(({ value, ...rest }) => ({ ...rest, value: { ...value } })),
			[
				{ ...kafkaMessage("0"), schemaId: 1, value: messages[0] },
				{
					...kafkaMessage("1"),
					schemaId: 2,
					value: { ...messages[1], tags: ["x"] },
				},
				{ ...kafkaMessage("2"), schemaId: 1, value: messages[2] },
				{
					...kafkaMessage("3"),
					schemaId: 2,
					value: { ...messages[0], tags: [] },
				},
			],
		);
		// Compiled types are cached per id: one lookup each.
		deepStrictEqual(lookups, [1, 2]);
	});

	test(`kafka messages decoded with resolveSchema can still be committed by kafkaCommitStream`, async () => {
		const { resolveSchema } = fakeRegistry({ 1: schema, 2: v2 });
		const committed = [];
		const consumer = {
			events: { STOP: "consumer.stop" },
			on: () => () => {},
			commitOffsets: async (offsets) => {
				committed.push(...offsets);
			},
		};
		await pipeline([
			createReadableStream(kafkaMessages),
			confluentUnframeStream(),
			avroDecodeStream({ resolveSchema }),
			await kafkaCommitStream({ consumer }),
		]);
		deepStrictEqual(committed, [
			{ topic: "orders", partition: 0, offset: "4" },
		]);
	});

	test(`avroDecodeStream resolveSchema uses glueUnframeStream's schemaVersionId`, async () => {
		const uuid = "12345678-1234-1234-1234-1234567890ab";
		const lookups = [];
		// 0x03 magic, 0x00 = uncompressed, the 16 UUID bytes, then the record.
		const uuidBytes = uuid
			.replaceAll("-", "")
			.match(/../g)
			.map((hex) => Number.parseInt(hex, 16));
		const frame = new Uint8Array([
			0x03,
			0x00,
			...uuidBytes,
			...type.toBuffer(messages[0]),
		]);
		const out = await streamToArray(
			pipejoin([
				createReadableStream([frame]),
				glueUnframeStream(),
				avroDecodeStream({
					// Plain schema objects work as well as registry results.
					resolveSchema: async (id) => {
						lookups.push(id);
						return schema;
					},
				}),
			]),
		);
		deepStrictEqual(lookups, [uuid]);
		deepStrictEqual(Object.keys(out[0]), [
			"schemaVersionId",
			"compression",
			"value",
		]);
		deepStrictEqual(toPlain(out[0].value), messages[0]);
	});

	test(`avroDecodeStream resolveSchema accepts a JSON string or an avsc Type`, async () => {
		const envelopes = [
			{ schemaId: 1, payload: type.toBuffer(messages[0]) },
			{ schemaId: 2, payload: type.toBuffer(messages[1]) },
		];
		const out = await streamToArray(
			pipejoin([
				createReadableStream(envelopes),
				avroDecodeStream({
					resolveSchema: (id) => (id === 1 ? JSON.stringify(schema) : type),
				}),
			]),
		);
		deepStrictEqual(
			out.map((m) => ({ schemaId: m.schemaId, value: toPlain(m.value) })),
			[
				{ schemaId: 1, value: messages[0] },
				{ schemaId: 2, value: messages[1] },
			],
		);
	});

	test(`avroDecodeStream errors when resolveSchema resolves to nothing`, async () => {
		await rejects(
			pipeline([
				createReadableStream([{ schemaId: 1, payload: new Uint8Array([2]) }]),
				avroDecodeStream({ resolveSchema: async () => undefined }),
			]),
			{ name: "TypeError", message: "avroDecodeStream: schema required" },
		);
	});

	test(`avroDecodeStream resolveSchema rejects an envelope without payload bytes`, async () => {
		const resolveSchema = async () => schema;
		await rejects(
			pipeline([
				createReadableStream([{ schemaId: 1, payload: "abc" }]),
				avroDecodeStream({ resolveSchema }),
			]),
			{
				name: "TypeError",
				message:
					"avroDecodeStream: payload must be a Uint8Array, ArrayBuffer view, or ArrayBuffer",
			},
		);
	});

	test(`avroDecodeStream resolveSchema applies maxMessageSize and the block-count guard to payloads`, async () => {
		await rejects(
			pipeline([
				createReadableStream([{ schemaId: 1, payload: new Uint8Array(4) }]),
				avroDecodeStream({ resolveSchema: () => fixed(4), maxMessageSize: 3 }),
			]),
			{
				name: "RangeError",
				message: "avroDecodeStream: message exceeds maxMessageSize (3 bytes)",
			},
		);
		await rejects(
			pipeline([
				createReadableStream([{ schemaId: 1, payload: bomb }]),
				avroDecodeStream({
					resolveSchema: () => ({ type: "array", items: "null" }),
				}),
			]),
			{ message: bombMessage },
		);
	});

	test(`avroDecodeStream caches at most 1000 compiled types, evicting the oldest`, async () => {
		const lookups = [];
		const resolveSchema = (id) => {
			lookups.push(id);
			return '"int"';
		};
		const ids = [...Array.from({ length: 1000 }, (_, i) => i), 0, 1000, 1, 0];
		await pipeline([
			createReadableStream(
				ids.map((schemaId) => ({ schemaId, payload: new Uint8Array([0]) })),
			),
			avroDecodeStream({ resolveSchema }),
		]);
		strictEqual(lookups.length, 1002);
		deepStrictEqual(lookups.slice(1000), [1000, 0]);
	});

	test(`avroDecodeStream rejects schema together with resolveSchema`, () => {
		throws(() => avroDecodeStream({ schema, resolveSchema: () => schema }), {
			name: "TypeError",
			message: "avroDecodeStream: pass schema or resolveSchema, not both",
		});
	});

	test(`encode then decode round-trips`, async () => {
		const decoded = await streamToArray(
			pipejoin([
				createReadableStream(messages),
				avroEncodeStream({ schema }),
				avroDecodeStream({ schema }),
			]),
		);
		deepStrictEqual(decoded.map(toPlain), messages);
	});
});
