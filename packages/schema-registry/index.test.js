import { deepStrictEqual, ok, rejects, strictEqual, throws } from "node:assert";
import test, { describe } from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	confluentFrameStream,
	confluentSchemaRegistryResolver,
	confluentUnframeStream,
	glueFrameStream,
	glueUnframeStream,
} from "@datastream/schema-registry";
import { variant } from "../variant.js";

describe(`@datastream/schema-registry (${variant})`, () => {
	const helloWorld = new TextEncoder().encode("hello world");
	const schemaUuid = "12345678-1234-1234-1234-1234567890ab";

	// *** Confluent *** //
	test(`confluentFrameStream prepends 0x00 + uint32 BE schema id`, async () => {
		const streams = [
			createReadableStream([helloWorld]),
			confluentFrameStream({ schemaId: 257 }),
		];
		const out = await streamToArray(pipejoin(streams));
		strictEqual(out.length, 1);
		strictEqual(out[0][0], 0x00);
		// 257 = 0x00000101
		strictEqual(out[0][1], 0x00);
		strictEqual(out[0][2], 0x00);
		strictEqual(out[0][3], 0x01);
		strictEqual(out[0][4], 0x01);
		strictEqual(out[0].byteLength, 5 + helloWorld.byteLength);
	});

	test(`confluentFrameStream encodes schemaId big-endian for >24-bit values`, async () => {
		const streams = [
			createReadableStream([new Uint8Array([0xaa])]),
			confluentFrameStream({ schemaId: 0x12345678 }),
		];
		const out = await streamToArray(pipejoin(streams));
		deepStrictEqual(
			Array.from(out[0].slice(0, 5)),
			[0x00, 0x12, 0x34, 0x56, 0x78],
		);
	});

	test(`confluent frame -> unframe round-trip emits envelope with schemaId + payload`, async () => {
		const unframe = confluentUnframeStream();
		const streams = [
			createReadableStream([helloWorld]),
			confluentFrameStream({ schemaId: 42 }),
			unframe,
		];
		const out = await streamToArray(pipejoin(streams));
		strictEqual(out.length, 1);
		strictEqual(out[0].schemaId, 42);
		deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
		// .result() still works for compatibility with the csvDetect-style readers.
		strictEqual(unframe.result().value.schemaId, 42);
	});

	test(`confluentUnframeStream rejects frames missing the magic byte`, async () => {
		const bogus = new Uint8Array([0x01, 0, 0, 0, 0, 0x68, 0x69]);
		try {
			await pipeline([createReadableStream([bogus]), confluentUnframeStream()]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("magic byte"));
		}
	});

	// *** Glue uncompressed *** //
	test(`glueFrameStream prepends 0x03 + compression byte + uuid`, async () => {
		const streams = [
			createReadableStream([helloWorld]),
			glueFrameStream({ schemaVersionId: schemaUuid }),
		];
		const out = await streamToArray(pipejoin(streams));
		strictEqual(out[0][0], 0x03);
		strictEqual(out[0][1], 0x00); // compression none
		strictEqual(out[0].byteLength, 18 + helloWorld.byteLength);
	});

	test(`glue frame -> unframe round-trip emits { schemaVersionId, compression, payload }`, async () => {
		const unframe = glueUnframeStream();
		const streams = [
			createReadableStream([helloWorld]),
			glueFrameStream({ schemaVersionId: schemaUuid }),
			unframe,
		];
		const out = await streamToArray(pipejoin(streams));
		strictEqual(out[0].schemaVersionId, schemaUuid);
		strictEqual(out[0].compression, "none");
		deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
		strictEqual(unframe.result().value.schemaVersionId, schemaUuid);
		strictEqual(unframe.result().value.compression, "none");
	});

	// *** Glue zlib *** //
	test(`glue frame -> unframe round-trip (zlib compressed)`, async () => {
		const payload = new TextEncoder().encode("a".repeat(2048));
		const unframe = glueUnframeStream();
		const streams = [
			createReadableStream([payload]),
			glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
			unframe,
		];
		const out = await streamToArray(pipejoin(streams));
		strictEqual(out[0].compression, "zlib");
		deepStrictEqual(Array.from(out[0].payload), Array.from(payload));
	});

	test(`glueUnframeStream enforces maxOutputSize with a RangeError`, async () => {
		const payload = new TextEncoder().encode("a".repeat(2048));
		await rejects(
			pipeline([
				createReadableStream([payload]),
				glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
				glueUnframeStream({ maxOutputSize: 100 }),
			]),
			{
				name: "RangeError",
				message: "schema-registry: maxOutputSize exceeded (100)",
			},
		);
	});

	test(`glueUnframeStream rejects output over the 256MiB default maxOutputSize`, async () => {
		const payload = new Uint8Array(268_435_457);
		await rejects(
			pipeline([
				createReadableStream([payload]),
				glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
				glueUnframeStream({ maxOutputSize: undefined }),
			]),
			{
				name: "RangeError",
				message: "schema-registry: maxOutputSize exceeded (268435456)",
			},
		);
	});

	test(`glueUnframeStream rejects unknown compression byte`, async () => {
		const bogus = new Uint8Array(18 + 4);
		bogus[0] = 0x03;
		bogus[1] = 0x09; // unsupported
		try {
			await pipeline([createReadableStream([bogus]), glueUnframeStream()]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("unsupported compression"));
		}
	});

	// *** input validation *** //
	test(`confluentFrameStream rejects non-integer / out-of-range schemaId`, () => {
		for (const bad of [undefined, "1", -1, 1.5, null, 0x100000000]) {
			try {
				confluentFrameStream({ schemaId: bad });
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("schemaId"));
			}
		}
	});

	test(`glueFrameStream rejects missing schemaVersionId`, () => {
		try {
			glueFrameStream({});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("schemaVersionId"));
		}
	});

	test(`glueFrameStream rejects unsupported compression`, () => {
		try {
			glueFrameStream({ schemaVersionId: schemaUuid, compression: "gzip" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("compression"));
		}
	});

	test(`glueFrameStream rejects malformed schemaVersionId UUID`, () => {
		for (const bad of [
			"not-a-uuid",
			"zzzzzzzz-1234-1234-1234-1234567890ab", // non-hex
			"1234567812341234123412345678ab", // too short
		]) {
			try {
				glueFrameStream({ schemaVersionId: bad });
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("UUID"));
			}
		}
	});

	test(`glueUnframeStream rejects frames missing the magic byte`, async () => {
		const bogus = new Uint8Array(20);
		bogus[0] = 0x07;
		try {
			await pipeline([createReadableStream([bogus]), glueUnframeStream()]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("magic byte"));
		}
	});

	// *** asBytes input-type handling (finding: numeric chunk -> zero-filled buffer) *** //
	test(`confluentUnframeStream accepts string chunks`, async () => {
		// Build a framed Confluent envelope, then feed it back as a latin1 string.
		const framed = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 7 }),
			]),
		);
		const asString = Array.from(framed[0])
			.map((b) => String.fromCharCode(b))
			.join("");
		// Re-encode via TextEncoder would corrupt high bytes; helloWorld is ASCII so
		// round-trips, and the header bytes are < 0x80, so a latin1 string is exact.
		const out = await streamToArray(
			pipejoin([createReadableStream([asString]), confluentUnframeStream()]),
		);
		strictEqual(out.length, 1);
		strictEqual(out[0].schemaId, 7);
		deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
	});

	test(`confluentUnframeStream accepts ArrayBuffer view with non-zero byteOffset`, async () => {
		const framed = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 9 }),
			]),
		);
		// Place the frame inside a larger buffer at a non-zero offset, then hand a
		// subarray view (byteOffset > 0) to the unframe stream.
		const backing = new Uint8Array(framed[0].byteLength + 3);
		backing.set(framed[0], 3);
		const view = backing.subarray(3);
		const out = await streamToArray(
			pipejoin([createReadableStream([view]), confluentUnframeStream()]),
		);
		strictEqual(out.length, 1);
		strictEqual(out[0].schemaId, 9);
		deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
	});

	test(`confluentFrameStream throws on a numeric chunk instead of framing zero bytes`, async () => {
		try {
			await pipeline([
				createReadableStream([5]),
				confluentFrameStream({ schemaId: 1 }),
			]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("chunk must be"));
		}
	});

	// *** Glue zlib: input written to DecompressionStream in 16KiB slices *** //
	// A browser DecompressionStream inflates each written chunk synchronously,
	// so the frame must be fed in slices with the limit checked in between.
	// A fake DecompressionStream records each write and expands it by `ratio`.
	const withFakeDecompressionStream = async (ratio, fn) => {
		const original = globalThis.DecompressionStream;
		const writes = [];
		globalThis.DecompressionStream = class extends TransformStream {
			constructor() {
				super({
					transform(chunk, controller) {
						writes.push(chunk.byteLength);
						controller.enqueue(new Uint8Array(chunk.byteLength * ratio));
					},
				});
			}
		};
		try {
			await fn(writes);
		} finally {
			globalThis.DecompressionStream = original;
		}
	};
	const glueZlibFrame = (payload) => {
		const frame = new Uint8Array(18 + payload.byteLength);
		frame[0] = 0x03;
		frame[1] = 0x05;
		frame.set(payload, 18);
		return frame;
	};

	test(`glueUnframeStream writes zlib input to the decompressor in 16KiB slices`, async () => {
		await withFakeDecompressionStream(1, async (writes) => {
			const payload = new Uint8Array(40_000).map((_, i) => i % 251);
			const out = await streamToArray(
				pipejoin([
					createReadableStream([glueZlibFrame(payload)]),
					glueUnframeStream({ maxOutputSize: null }),
				]),
			);
			deepStrictEqual(writes, [16_384, 16_384, 7_232]);
			strictEqual(out[0].payload.byteLength, 40_000);
		});
	});

	test(`glueUnframeStream writes no empty trailing slice for a 16KiB-multiple zlib input`, async () => {
		await withFakeDecompressionStream(1, async (writes) => {
			const out = await streamToArray(
				pipejoin([
					createReadableStream([glueZlibFrame(new Uint8Array(32_768))]),
					glueUnframeStream({ maxOutputSize: null }),
				]),
			);
			deepStrictEqual(writes, [16_384, 16_384]);
			strictEqual(out[0].payload.byteLength, 32_768);
		});
	});

	test(`glueUnframeStream stops feeding a zlib bomb once maxOutputSize is exceeded`, async () => {
		await withFakeDecompressionStream(1000, async (writes) => {
			// 1MiB of input = 64 slices; each slice inflates to ~16MB.
			const frame = glueZlibFrame(new Uint8Array(1024 * 1024));
			await rejects(
				pipeline([
					createReadableStream([frame]),
					glueUnframeStream({ maxOutputSize: 1024 * 1024 }),
				]),
				{
					name: "RangeError",
					message: "schema-registry: maxOutputSize exceeded (1048576)",
				},
			);
			ok(writes.length <= 2, `wrote ${writes.length} slices`);
		});
	});

	// *** Glue zlib failure / round-trip edge cases *** //
	test(`glueUnframeStream rejects a malformed zlib payload`, async () => {
		// magic 0x03 + compression 0x05 (zlib) + 16-byte UUID + garbage payload.
		const frame = new Uint8Array(18 + 4);
		frame[0] = 0x03;
		frame[1] = 0x05;
		frame.set([0xde, 0xad, 0xbe, 0xef], 18);
		try {
			await pipeline([createReadableStream([frame]), glueUnframeStream()]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e instanceof Error);
			ok(!e.message.includes("Should have thrown"));
		}
	});

	test(`glue frame -> unframe round-trip (zlib, empty payload)`, async () => {
		const empty = new Uint8Array(0);
		const out = await streamToArray(
			pipejoin([
				createReadableStream([empty]),
				glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
				glueUnframeStream(),
			]),
		);
		strictEqual(out.length, 1);
		strictEqual(out[0].compression, "zlib");
		strictEqual(out[0].payload.byteLength, 0);
	});

	test(`glueFrameStream deflate enforces maxOutputSize with a RangeError`, async () => {
		const payload = new TextEncoder().encode("a".repeat(4096));
		await rejects(
			pipeline([
				createReadableStream([payload]),
				glueFrameStream({
					schemaVersionId: schemaUuid,
					compression: "zlib",
					maxOutputSize: 10,
				}),
			]),
			{
				name: "RangeError",
				message: "schema-registry: maxOutputSize exceeded (10)",
			},
		);
	});

	test(`glueFrameStream deflate with maxOutputSize: null imposes no ceiling`, async () => {
		const payload = new TextEncoder().encode("a".repeat(4096));
		const [frame] = await streamToArray(
			pipejoin([
				createReadableStream([payload]),
				glueFrameStream({
					schemaVersionId: schemaUuid,
					compression: "zlib",
					maxOutputSize: null,
				}),
			]),
		);
		const [out] = await streamToArray(
			pipejoin([createReadableStream([frame]), glueUnframeStream()]),
		);
		deepStrictEqual(Array.from(out.payload), Array.from(payload));
	});

	// *** .result() reports every distinct id instead of throwing on mixed ids *** //
	test(`confluentUnframeStream: pipeline() resolves with the distinct schemaIds when ids are mixed`, async () => {
		const [frameA] = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 1 }),
			]),
		);
		const [frameB] = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 2 }),
			]),
		);
		const result = await pipeline([
			createReadableStream([frameA, frameB, frameA]),
			confluentUnframeStream(),
		]);
		deepStrictEqual(result, {
			confluentSchemaId: {
				schemaId: 1,
				schemaIds: [1, 2],
				untrackedSchemaIds: 0,
			},
		});
	});

	// *** one chunk = one frame: emitted immediately, not held for the next chunk *** //
	// A live consumer (e.g. Kafka) may not send another message for a long time;
	// the latest message must reach downstream without waiting for a successor.
	const emitsBeforeNextChunk = async (frame, unframe) => {
		let emitted;
		const seen = new Promise((resolve) => {
			emitted = resolve;
		});
		let outcome;
		async function* source() {
			yield frame;
			let timer;
			outcome = await Promise.race([
				seen.then(() => "emitted"),
				new Promise((resolve) => {
					timer = setTimeout(resolve, 200, "held");
				}),
			]);
			clearTimeout(timer);
		}
		const out = [];
		for await (const envelope of pipejoin([
			createReadableStream(source()),
			unframe,
		])) {
			out.push(envelope);
			emitted();
		}
		return { outcome, out };
	};

	test(`confluentUnframeStream emits a frame's envelope before the next chunk arrives`, async () => {
		const [frame] = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 9 }),
			]),
		);
		const { outcome, out } = await emitsBeforeNextChunk(
			frame,
			confluentUnframeStream(),
		);
		strictEqual(outcome, "emitted");
		strictEqual(out.length, 1);
		strictEqual(out[0].schemaId, 9);
		deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
	});

	test(`glueUnframeStream emits a frame's envelope before the next chunk arrives`, async () => {
		const [frame] = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
			]),
		);
		const { outcome, out } = await emitsBeforeNextChunk(
			frame,
			glueUnframeStream(),
		);
		strictEqual(outcome, "emitted");
		strictEqual(out.length, 1);
		strictEqual(out[0].schemaVersionId, schemaUuid);
		deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
	});

	test(`glueUnframeStream: pipeline() resolves with the distinct schemaVersionIds when ids are mixed`, async () => {
		const idA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
		const idB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
		const [frameA] = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: idA }),
			]),
		);
		const [frameB] = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: idB, compression: "zlib" }),
			]),
		);
		const result = await pipeline([
			createReadableStream([frameA, frameB, frameA]),
			glueUnframeStream(),
		]);
		deepStrictEqual(result, {
			glueSchemaVersionId: {
				schemaVersionId: idA,
				compression: "none",
				schemaVersionIds: [idA, idB],
				untrackedSchemaVersionIds: 0,
			},
		});
	});

	// *** large frames: one chunk = one frame, no heuristic re-splitting *** //
	test(`confluent frame -> unframe round-trips a single >16KB frame delivered as one chunk`, async () => {
		// 40KB payload whose bytes at framed offsets 16384 and 32768 are 0x00: the
		// old magic-byte re-splitting heuristic mis-split exactly this shape.
		const big = new Uint8Array(40 * 1024);
		for (let i = 0; i < big.byteLength; i++) big[i] = i % 251;
		big[16384 - 5] = 0x00;
		big[32768 - 5] = 0x00;
		const framed = await streamToArray(
			pipejoin([
				createReadableStream([big]),
				confluentFrameStream({ schemaId: 123 }),
			]),
		);
		ok(framed[0].byteLength > 16384);
		const out = await streamToArray(
			pipejoin([createReadableStream([framed[0]]), confluentUnframeStream()]),
		);
		strictEqual(out.length, 1);
		strictEqual(out[0].schemaId, 123);
		strictEqual(out[0].payload.byteLength, big.byteLength);
		deepStrictEqual(Array.from(out[0].payload), Array.from(big));
	});

	test(`glue frame -> unframe round-trips a single >16KB frame delivered as one chunk`, async () => {
		const big = new Uint8Array(40 * 1024);
		for (let i = 0; i < big.byteLength; i++) big[i] = (i * 7) % 251;
		// 0x03 (the Glue magic byte) at framed offsets 16384 and 32768.
		big[16384 - 18] = 0x03;
		big[32768 - 18] = 0x03;
		const framed = await streamToArray(
			pipejoin([
				createReadableStream([big]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
			]),
		);
		ok(framed[0].byteLength > 16384);
		const out = await streamToArray(
			pipejoin([createReadableStream([framed[0]]), glueUnframeStream()]),
		);
		strictEqual(out.length, 1);
		strictEqual(out[0].schemaVersionId, schemaUuid);
		strictEqual(out[0].payload.byteLength, big.byteLength);
		deepStrictEqual(Array.from(out[0].payload), Array.from(big));
	});

	// *** envelope shape protects against per-message schemaVersionId drift *** //
	test(`glueUnframeStream envelope correctly pairs payload with its own schemaVersionId`, async () => {
		const idA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
		const idB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
		const frameA = await streamToArray(
			pipejoin([
				createReadableStream([new TextEncoder().encode("payload-a")]),
				glueFrameStream({ schemaVersionId: idA }),
			]),
		);
		const frameB = await streamToArray(
			pipejoin([
				createReadableStream([new TextEncoder().encode("payload-b")]),
				glueFrameStream({ schemaVersionId: idB }),
			]),
		);
		const envelopes = await streamToArray(
			pipejoin([
				createReadableStream([frameA[0], frameB[0]]),
				glueUnframeStream(),
			]),
		);
		strictEqual(envelopes.length, 2);
		strictEqual(envelopes[0].schemaVersionId, idA);
		strictEqual(envelopes[1].schemaVersionId, idB);
		deepStrictEqual(
			new TextDecoder().decode(envelopes[0].payload),
			"payload-a",
		);
		deepStrictEqual(
			new TextDecoder().decode(envelopes[1].payload),
			"payload-b",
		);
	});

	// *** UUID regex anchoring: ^ and $ must both match (not substring) *** //
	test(`glueFrameStream rejects a 33-char hex string (fails $ anchor on UUID regex)`, () => {
		// After replaceAll("-",""), 33 hex chars is not 32 - both anchors must hold.
		const tooLong = "0123456789abcdef0123456789abcdef0"; // 33 hex chars, no hyphens
		try {
			glueFrameStream({ schemaVersionId: tooLong });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("UUID"), `Expected UUID error, got: ${e.message}`);
		}
		const alsoTooLong = "012345678901234567890123456789abc"; // 33 hex chars
		try {
			glueFrameStream({ schemaVersionId: alsoTooLong });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("UUID"), `Expected UUID error, got: ${e.message}`);
		}
	});

	// *** asBytes: ArrayBuffer.isView path (non-Uint8Array typed array) *** //
	test(`confluentFrameStream accepts an Int16Array (ArrayBuffer.isView branch)`, async () => {
		// Int16Array is an ArrayBuffer view but NOT a Uint8Array - hits ArrayBuffer.isView branch.
		const int16 = new Int16Array([1, 2, 3, 4]);
		const out = await streamToArray(
			pipejoin([
				createReadableStream([int16]),
				confluentFrameStream({ schemaId: 55 }),
			]),
		);
		strictEqual(out.length, 1);
		strictEqual(out[0][0], 0x00); // magic byte
		strictEqual(out[0].byteLength, 5 + int16.byteLength);
	});

	// *** asBytes: plain ArrayBuffer path *** //
	test(`confluentFrameStream accepts a plain ArrayBuffer`, async () => {
		// Plain ArrayBuffer (not a view) hits the instanceof ArrayBuffer branch.
		const buf = new Uint8Array([0x61, 0x62, 0x63]).buffer; // "abc"
		const out = await streamToArray(
			pipejoin([
				createReadableStream([buf]),
				confluentFrameStream({ schemaId: 77 }),
			]),
		);
		strictEqual(out.length, 1);
		strictEqual(out[0][0], 0x00);
		strictEqual(out[0].byteLength, 5 + 3);
		// Verify payload bytes are "abc"
		strictEqual(out[0][5], 0x61);
		strictEqual(out[0][6], 0x62);
		strictEqual(out[0][7], 0x63);
	});

	// *** asBytes: Uint8Array fast-path returns the chunk identity *** //
	test(`asBytes Uint8Array fast-path preserves the original bytes exactly`, async () => {
		// Exercise the first branch in asBytes: Uint8Array -> return chunk directly.
		const bytes = new Uint8Array([0x41, 0x42, 0x43]);
		const out = await streamToArray(
			pipejoin([
				createReadableStream([bytes]),
				confluentFrameStream({ schemaId: 0 }),
			]),
		);
		strictEqual(out[0][5], 0x41);
		strictEqual(out[0][6], 0x42);
		strictEqual(out[0][7], 0x43);
	});

	// *** confluentFrameStream schemaId boundary values *** //
	test(`confluentFrameStream accepts schemaId === 0 (minimum valid, exact boundary)`, async () => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 0 }),
			]),
		);
		strictEqual(out[0][1], 0x00);
		strictEqual(out[0][2], 0x00);
		strictEqual(out[0][3], 0x00);
		strictEqual(out[0][4], 0x00);
	});

	test(`confluentFrameStream accepts schemaId === 0xffffffff (maximum valid, exact boundary)`, async () => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 0xffffffff }),
			]),
		);
		strictEqual(out[0][1], 0xff);
		strictEqual(out[0][2], 0xff);
		strictEqual(out[0][3], 0xff);
		strictEqual(out[0][4], 0xff);
	});

	test(`confluentFrameStream rejects schemaId === 0x100000000 (one above max)`, () => {
		try {
			confluentFrameStream({ schemaId: 0x100000000 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("schemaId"),
				`Expected schemaId error, got: ${e.message}`,
			);
		}
	});

	// *** confluentFrameStream typeof validation: number type check *** //
	test(`confluentFrameStream rejects schemaId that is a number string (type check)`, () => {
		try {
			confluentFrameStream({ schemaId: "42" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("schemaId"),
				`Expected schemaId error, got: ${e.message}`,
			);
		}
	});

	// *** confluentFrameStream uses big-endian encoding (setUint32 false) *** //
	test(`confluentFrameStream header bytes are big-endian (setUint32 little=false)`, async () => {
		// 0x01020304 big-endian => bytes [0x01, 0x02, 0x03, 0x04]
		const out = await streamToArray(
			pipejoin([
				createReadableStream([new Uint8Array([0xff])]),
				confluentFrameStream({ schemaId: 0x01020304 }),
			]),
		);
		strictEqual(out[0][1], 0x01);
		strictEqual(out[0][2], 0x02);
		strictEqual(out[0][3], 0x03);
		strictEqual(out[0][4], 0x04);
	});

	// *** confluentUnframeStream: frame exactly 4 bytes -> too short *** //
	test(`confluentUnframeStream rejects a frame that is exactly 4 bytes (byteLength < 5)`, async () => {
		const short = new Uint8Array([0x00, 0x00, 0x00, 0x01]); // valid magic, but only 4 bytes
		try {
			await pipeline([createReadableStream([short]), confluentUnframeStream()]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("magic byte"),
				`Expected magic byte error, got: ${e.message}`,
			);
		}
	});

	// *** confluentUnframeStream: schemaId read as big-endian (getUint32 false) *** //
	test(`confluentUnframeStream reads schemaId as big-endian (not little-endian)`, async () => {
		// magic 0x00 + [0x01, 0x02, 0x03, 0x04] big-endian = 0x01020304
		const frame = new Uint8Array([0x00, 0x01, 0x02, 0x03, 0x04, 0xaa]);
		const out = await streamToArray(
			pipejoin([createReadableStream([frame]), confluentUnframeStream()]),
		);
		strictEqual(out[0].schemaId, 0x01020304);
		strictEqual(out[0].payload[0], 0xaa);
	});

	// *** confluentUnframeStream distinct schemaIds tracking *** //
	test(`confluentUnframeStream.result() succeeds when exactly 1 distinct schemaId is seen`, async () => {
		const unframe = confluentUnframeStream();
		const frameA = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 5 }),
			]),
		);
		const frameB = await streamToArray(
			pipejoin([
				createReadableStream([new Uint8Array([0x01])]),
				confluentFrameStream({ schemaId: 5 }), // same id
			]),
		);
		await streamToArray(
			pipejoin([createReadableStream([frameA[0], frameB[0]]), unframe]),
		);
		const result = unframe.result();
		deepStrictEqual(result.value, {
			schemaId: 5,
			schemaIds: [5],
			untrackedSchemaIds: 0,
		});
	});

	test(`confluentUnframeStream.result() lists exactly 2 distinct schemaIds in first-seen order`, async () => {
		const frameA = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 10 }),
			]),
		);
		const frameB = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 11 }),
			]),
		);
		const unframe = confluentUnframeStream();
		await streamToArray(
			pipejoin([createReadableStream([frameA[0], frameB[0]]), unframe]),
		);
		deepStrictEqual(unframe.result().value, {
			schemaId: 11,
			schemaIds: [10, 11],
			untrackedSchemaIds: 0,
		});
	});

	// *** confluentUnframeStream.result() uses resultKey when provided *** //
	test(`confluentUnframeStream.result() uses custom resultKey`, async () => {
		const unframe = confluentUnframeStream({ resultKey: "myKey" });
		await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 3 }),
				unframe,
			]),
		);
		strictEqual(unframe.result().key, "myKey");
	});

	test(`confluentUnframeStream.result() defaults resultKey to "confluentSchemaId"`, async () => {
		const unframe = confluentUnframeStream();
		await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 3 }),
				unframe,
			]),
		);
		strictEqual(unframe.result().key, "confluentSchemaId");
	});

	// *** confluentFrameStream.result() resultKey *** //
	test(`confluentFrameStream.result() uses custom resultKey`, async () => {
		const stream = confluentFrameStream({
			schemaId: 7,
			resultKey: "myConfluentKey",
		});
		await streamToArray(pipejoin([createReadableStream([helloWorld]), stream]));
		strictEqual(stream.result().key, "myConfluentKey");
	});

	test(`confluentFrameStream.result() defaults to "confluentFrameSchemaId"`, async () => {
		const stream = confluentFrameStream({ schemaId: 7 });
		await streamToArray(pipejoin([createReadableStream([helloWorld]), stream]));
		strictEqual(stream.result().key, "confluentFrameSchemaId");
		strictEqual(stream.result().value.schemaId, 7);
	});

	// *** collectStream maxOutputSize boundary: total > maxOutputSize, not >= *** //
	test(`glueUnframeStream allows decompressed output exactly equal to maxOutputSize`, async () => {
		const payload = new TextEncoder().encode("a".repeat(100));
		const out = await streamToArray(
			pipejoin([
				createReadableStream([payload]),
				glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
				glueUnframeStream({ maxOutputSize: 100 }),
			]),
		);
		strictEqual(out[0].payload.byteLength, 100);
	});

	// *** collectStream: maxOutputSize null means no limit *** //
	test(`glueUnframeStream with default maxOutputSize handles payloads without throwing`, async () => {
		const payload = new TextEncoder().encode("test payload");
		const out = await streamToArray(
			pipejoin([
				createReadableStream([payload]),
				glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
				glueUnframeStream(),
			]),
		);
		strictEqual(out.length, 1);
		deepStrictEqual(Array.from(out[0].payload), Array.from(payload));
	});

	// *** one chunk = one frame: a frame split across chunks is rejected, not reassembled *** //
	test(`confluentUnframeStream rejects a frame split into 2 chunks`, async () => {
		const framed = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 200 }),
			]),
		);
		const full = framed[0];
		const headerChunk = full.slice(0, 5);
		const payloadChunk = full.slice(5);
		// The 5-byte header chunk parses as its own (empty-payload) frame; the
		// payload-only chunk then fails the magic-byte check instead of being
		// silently glued back on.
		try {
			await pipeline([
				createReadableStream([headerChunk, payloadChunk]),
				confluentUnframeStream(),
			]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("magic byte"), e.message);
		}
	});

	test(`glueUnframeStream rejects a frame split into 2 chunks`, async () => {
		const bigPayload = new Uint8Array(20);
		for (let i = 0; i < 20; i++) bigPayload[i] = i + 1;
		const framed = await streamToArray(
			pipejoin([
				createReadableStream([bigPayload]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
			]),
		);
		const full = framed[0];
		// The 18-byte header chunk parses as its own (empty-payload) frame; the
		// payload-only chunk then fails the magic-byte check.
		try {
			await pipeline([
				createReadableStream([full.slice(0, 18), full.slice(18)]),
				glueUnframeStream(),
			]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("magic byte"), e.message);
		}
	});

	// *** glueUnframeStream: frame exactly 17 bytes -> too short *** //
	test(`glueUnframeStream rejects a frame that is exactly 17 bytes (byteLength < 18)`, async () => {
		const short = new Uint8Array(17);
		short[0] = 0x03;
		try {
			await pipeline([createReadableStream([short]), glueUnframeStream()]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("magic byte"),
				`Expected magic byte error, got: ${e.message}`,
			);
		}
	});

	// *** glueUnframeStream distinct schemaVersionIds tracking *** //
	test(`glueUnframeStream.result() succeeds when exactly 1 distinct schemaVersionId seen`, async () => {
		const frameA = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
			]),
		);
		const frameB = await streamToArray(
			pipejoin([
				createReadableStream([new Uint8Array([0x42])]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
			]),
		);
		const unframe = glueUnframeStream();
		await streamToArray(
			pipejoin([createReadableStream([frameA[0], frameB[0]]), unframe]),
		);
		const result = unframe.result();
		deepStrictEqual(result.value, {
			schemaVersionId: schemaUuid,
			compression: "none",
			schemaVersionIds: [schemaUuid],
			untrackedSchemaVersionIds: 0,
		});
	});

	test(`glueUnframeStream.result() lists exactly 2 distinct schemaVersionIds in first-seen order`, async () => {
		const idA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaa00";
		const idB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb00";
		const frameA = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: idA }),
			]),
		);
		const frameB = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: idB }),
			]),
		);
		const unframe = glueUnframeStream();
		await streamToArray(
			pipejoin([createReadableStream([frameA[0], frameB[0]]), unframe]),
		);
		deepStrictEqual(unframe.result().value, {
			schemaVersionId: idB,
			compression: "none",
			schemaVersionIds: [idA, idB],
			untrackedSchemaVersionIds: 0,
		});
	});

	// *** glueUnframeStream.result() resultKey *** //
	test(`glueUnframeStream.result() uses custom resultKey`, async () => {
		const unframe = glueUnframeStream({ resultKey: "myGlueKey" });
		await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
				unframe,
			]),
		);
		strictEqual(unframe.result().key, "myGlueKey");
	});

	test(`glueUnframeStream.result() defaults to "glueSchemaVersionId"`, async () => {
		const unframe = glueUnframeStream();
		await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
				unframe,
			]),
		);
		strictEqual(unframe.result().key, "glueSchemaVersionId");
	});

	// *** glueFrameStream.result() resultKey *** //
	test(`glueFrameStream.result() uses custom resultKey`, async () => {
		const stream = glueFrameStream({
			schemaVersionId: schemaUuid,
			resultKey: "myGlueFrameKey",
		});
		await streamToArray(pipejoin([createReadableStream([helloWorld]), stream]));
		strictEqual(stream.result().key, "myGlueFrameKey");
	});

	test(`glueFrameStream.result() defaults to "glueFrameSchemaVersionId"`, async () => {
		const stream = glueFrameStream({ schemaVersionId: schemaUuid });
		await streamToArray(pipejoin([createReadableStream([helloWorld]), stream]));
		strictEqual(stream.result().key, "glueFrameSchemaVersionId");
		strictEqual(stream.result().value.schemaVersionId, schemaUuid);
	});

	// *** glueUnframeStream: unsupported compression byte error message includes zero-padded hex *** //
	test(`glueUnframeStream unsupported compression error message includes zero-padded hex byte`, async () => {
		// byte 0x09 -> padStart(2,"0") gives "09", not "9"
		const frame = new Uint8Array(18 + 1);
		frame[0] = 0x03;
		frame[1] = 0x09;
		try {
			await pipeline([createReadableStream([frame]), glueUnframeStream()]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("0x09"),
				`Expected '0x09' in error message, got: ${e.message}`,
			);
		}
	});

	// *** header-length check is `< headerSize`: an exactly-headerSize chunk is a valid frame *** //
	test(`confluentUnframeStream accepts a frame chunk of exactly 5 bytes as new-frame start`, async () => {
		const emptyPayload = new Uint8Array(0);
		const framed = await streamToArray(
			pipejoin([
				createReadableStream([emptyPayload]),
				confluentFrameStream({ schemaId: 500 }),
			]),
		);
		strictEqual(framed[0].byteLength, 5);
		const out = await streamToArray(
			pipejoin([createReadableStream([framed[0]]), confluentUnframeStream()]),
		);
		strictEqual(out.length, 1);
		strictEqual(out[0].schemaId, 500);
		strictEqual(out[0].payload.byteLength, 0);
	});

	test(`glueUnframeStream accepts a frame chunk of exactly 18 bytes as new-frame start`, async () => {
		const emptyPayload = new Uint8Array(0);
		const framed = await streamToArray(
			pipejoin([
				createReadableStream([emptyPayload]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
			]),
		);
		strictEqual(framed[0].byteLength, 18);
		const out = await streamToArray(
			pipejoin([createReadableStream([framed[0]]), glueUnframeStream()]),
		);
		strictEqual(out.length, 1);
		strictEqual(out[0].schemaVersionId, schemaUuid);
		strictEqual(out[0].payload.byteLength, 0);
	});

	// *** flush does nothing on empty stream *** //
	test(`confluentUnframeStream flush does nothing when stream had no input`, async () => {
		const out = await streamToArray(
			pipejoin([createReadableStream([]), confluentUnframeStream()]),
		);
		strictEqual(out.length, 0);
	});

	test(`glueUnframeStream flush does nothing when stream had no input`, async () => {
		const out = await streamToArray(
			pipejoin([createReadableStream([]), glueUnframeStream()]),
		);
		strictEqual(out.length, 0);
	});

	// *** Error messages must be non-empty strings (StringLiteral mutants) *** //
	test(`confluentFrameStream schemaId validation error message is non-empty`, () => {
		throws(() => confluentFrameStream({ schemaId: undefined }), {
			message:
				"confluentFrameStream: schemaId must be an unsigned 32-bit integer",
		});
	});

	test(`confluentUnframeStream missing magic byte error message is non-empty`, async () => {
		const bogus = new Uint8Array([0x01, 0, 0, 0, 0, 0x68]);
		await rejects(
			pipeline([createReadableStream([bogus]), confluentUnframeStream()]),
			{
				message:
					"confluentUnframeStream: missing 0x00 magic byte / frame is too short (each chunk must be one whole frame)",
			},
		);
	});

	// *** envelope contains both schemaId and payload fields (not empty object) *** //
	test(`confluentUnframeStream envelope has schemaId and payload fields`, async () => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 999 }),
				confluentUnframeStream(),
			]),
		);
		ok("schemaId" in out[0], "envelope should have schemaId");
		ok("payload" in out[0], "envelope should have payload");
		strictEqual(out[0].schemaId, 999);
	});

	// *** confluentFrameStream value object has schemaId field *** //
	test(`confluentFrameStream.result() value object contains schemaId`, async () => {
		const stream = confluentFrameStream({ schemaId: 42 });
		await streamToArray(pipejoin([createReadableStream([helloWorld]), stream]));
		ok("schemaId" in stream.result().value, "value should have schemaId");
		strictEqual(stream.result().value.schemaId, 42);
	});

	// *** glueUnframeStream value object has schemaVersionId and compression *** //
	test(`glueUnframeStream.result() value object contains schemaVersionId and compression`, async () => {
		const unframe = glueUnframeStream();
		await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
				unframe,
			]),
		);
		ok("schemaVersionId" in unframe.result().value);
		ok("compression" in unframe.result().value);
		strictEqual(unframe.result().value.schemaVersionId, schemaUuid);
	});

	// *** confluentUnframeStream.result() value object has schemaId field *** //
	test(`confluentUnframeStream.result() value has schemaId field (not empty object)`, async () => {
		const unframe = confluentUnframeStream();
		await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 88 }),
				unframe,
			]),
		);
		ok("schemaId" in unframe.result().value);
		strictEqual(unframe.result().value.schemaId, 88);
	});

	// *** bytesToUuid: padStart with "0" is needed for single-digit hex bytes *** //
	test(`glueUnframeStream decodes UUID bytes with single-digit hex values (padStart needed)`, async () => {
		// "00010203-0405-0607-0809-0a0b0c0d0e0f" has bytes 0x00..0x0f requiring padStart(2,"0")
		const uuidWithSmallBytes = "00010203-0405-0607-0809-0a0b0c0d0e0f";
		const out = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: uuidWithSmallBytes }),
				glueUnframeStream(),
			]),
		);
		strictEqual(out[0].schemaVersionId, uuidWithSmallBytes);
	});

	// *** bytesToUuid loop: i < 16 not <= 16 (exactly 16 bytes needed) *** //
	test(`glueUnframeStream decodes all 16 UUID bytes correctly (loop bound i < 16)`, async () => {
		// All-FF UUID - if loop ran 17 iterations (i <= 16) it would read out-of-bounds bytes.
		const allFF = "ffffffff-ffff-ffff-ffff-ffffffffffff";
		const out = await streamToArray(
			pipejoin([
				createReadableStream([new Uint8Array([0x42])]),
				glueFrameStream({ schemaVersionId: allFF }),
				glueUnframeStream(),
			]),
		);
		strictEqual(out[0].schemaVersionId, allFF);
	});

	// *** named exports only (no default export) *** //
	test(`has no default export`, async () => {
		const mod = await import("@datastream/schema-registry");
		strictEqual(mod.default, undefined);
	});

	// *** a chunk < headerSize is a truncated frame even if it starts with magic *** //
	test(`confluentUnframeStream rejects a chunk shorter than 5 bytes starting with 0x00`, async () => {
		try {
			await pipeline([
				createReadableStream([new Uint8Array([0x00, 0x00, 0x00])]),
				confluentUnframeStream(),
			]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("magic byte"), e.message);
		}
	});

	test(`glueUnframeStream rejects a chunk shorter than 18 bytes starting with 0x03`, async () => {
		const short = new Uint8Array(10);
		short[0] = 0x03;
		try {
			await pipeline([createReadableStream([short]), glueUnframeStream()]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("magic byte"), e.message);
		}
	});

	// *** collectStream: maxOutputSize null/undefined -> no limit applied *** //
	test(`confluentUnframeStream initial value has schemaId as null before any frames`, async () => {
		// Test that result().value.schemaId is null (not undefined) before frames processed.
		// With the { schemaId: null } mutation -> {}, value.schemaId would be undefined.
		const unframe = confluentUnframeStream();
		await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 50 }),
				unframe,
			]),
		);
		strictEqual(unframe.result().value.schemaId, 50);
	});

	test(`glueUnframeStream initial value fields present after processing`, async () => {
		// Test that result().value has schemaVersionId and compression (not an empty object).
		// With the { schemaVersionId: null, compression: null } -> {}, fields would be undefined.
		const unframe = glueUnframeStream();
		await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
				unframe,
			]),
		);
		const v = unframe.result().value;
		strictEqual(v.schemaVersionId, schemaUuid);
		strictEqual(v.compression, "none");
	});

	// *** confluentUnframeStream initial value has schemaId as null (not undefined) *** //
	test(`confluentUnframeStream.result() value.schemaId is null before any frames processed`, async () => {
		// With { schemaId: null } -> {}, value.schemaId would be undefined, not null.
		const unframe = confluentUnframeStream();
		const result = unframe.result();
		deepStrictEqual(result.value, {
			schemaId: null,
			schemaIds: [],
			untrackedSchemaIds: 0,
		});
	});

	// *** glueUnframeStream initial value fields are null (not undefined) *** //
	test(`glueUnframeStream.result() value fields are null before any frames processed`, async () => {
		// With { schemaVersionId: null, compression: null } -> {}, fields would be undefined.
		const unframe = glueUnframeStream();
		const result = unframe.result();
		deepStrictEqual(result.value, {
			schemaVersionId: null,
			compression: null,
			schemaVersionIds: [],
			untrackedSchemaVersionIds: 0,
		});
	});

	// *** consecutive chunks are independent frames ***
	// A second frame that is EXACTLY headerSize bytes (empty payload) is its own
	// envelope, never folded into the previous frame.
	test(`confluentUnframeStream treats an exactly-5-byte second frame as a new frame`, async () => {
		const frameA = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				confluentFrameStream({ schemaId: 811 }),
			]),
		);
		// Second frame: empty payload => exactly 5 bytes, distinct schemaId.
		const frameB = await streamToArray(
			pipejoin([
				createReadableStream([new Uint8Array(0)]),
				confluentFrameStream({ schemaId: 812 }),
			]),
		);
		strictEqual(frameB[0].byteLength, 5);
		const out = await streamToArray(
			pipejoin([
				createReadableStream([frameA[0], frameB[0]]),
				confluentUnframeStream(),
			]),
		);
		// Correct: TWO distinct envelopes, one per chunk.
		strictEqual(out.length, 2);
		strictEqual(out[0].schemaId, 811);
		deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
		strictEqual(out[1].schemaId, 812);
		strictEqual(out[1].payload.byteLength, 0);
	});

	// *** maxOutputSize: null disables the ceiling ***
	// null maps to Infinity; a mutant that let null through to the comparison
	// (`total > null` => `total > 0`) would throw for any non-empty output.
	test(`glueUnframeStream with maxOutputSize: null imposes no ceiling`, async () => {
		const payload = new TextEncoder().encode("a".repeat(2048));
		const out = await streamToArray(
			pipejoin([
				createReadableStream([payload]),
				glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
				glueUnframeStream({ maxOutputSize: null }),
			]),
		);
		strictEqual(out.length, 1);
		strictEqual(out[0].compression, "zlib");
		deepStrictEqual(Array.from(out[0].payload), Array.from(payload));
	});
	// *** maxSchemaIds bounds the distinct-id list in long-lived consumers;
	// ids past the cap are counted, not recorded. null = unlimited. *** //
	const confluentFrame = (schemaId) => {
		const frame = new Uint8Array(6);
		new DataView(frame.buffer).setUint32(1, schemaId, false);
		return frame;
	};
	const glueFrame = (n) => {
		const frame = new Uint8Array(19);
		frame[0] = 0x03;
		frame[17] = n;
		return frame;
	};
	const glueId = (n) =>
		`00000000-0000-0000-0000-0000000000${n.toString(16).padStart(2, "0")}`;
	const unframeIds = async (stream, frames) => {
		await pipeline([createReadableStream(frames), stream]);
		return stream.result().value;
	};

	test(`confluentUnframeStream stops recording ids past maxSchemaIds and counts them`, async () => {
		const value = await unframeIds(
			confluentUnframeStream({ maxSchemaIds: 2 }),
			[1, 2, 3, 3, 1].map(confluentFrame),
		);
		deepStrictEqual(value, {
			schemaId: 1,
			schemaIds: [1, 2],
			untrackedSchemaIds: 2,
		});
	});

	test(`confluentUnframeStream records at most 1000 distinct ids by default`, async () => {
		const value = await unframeIds(
			confluentUnframeStream(),
			Array.from({ length: 1001 }, (_, i) => confluentFrame(i)),
		);
		strictEqual(value.schemaIds.length, 1000);
		strictEqual(value.schemaIds[999], 999);
		strictEqual(value.untrackedSchemaIds, 1);
	});

	test(`confluentUnframeStream maxSchemaIds: null records every id`, async () => {
		const value = await unframeIds(
			confluentUnframeStream({ maxSchemaIds: null }),
			Array.from({ length: 1001 }, (_, i) => confluentFrame(i)),
		);
		strictEqual(value.schemaIds.length, 1001);
		strictEqual(value.untrackedSchemaIds, 0);
	});

	test(`glueUnframeStream stops recording ids past maxSchemaIds and counts them`, async () => {
		const value = await unframeIds(
			glueUnframeStream({ maxSchemaIds: 2 }),
			[1, 2, 3, 3, 1].map(glueFrame),
		);
		deepStrictEqual(value, {
			schemaVersionId: glueId(1),
			compression: "none",
			schemaVersionIds: [glueId(1), glueId(2)],
			untrackedSchemaVersionIds: 2,
		});
	});

	test(`glueUnframeStream records at most 1000 distinct ids by default`, async () => {
		const frames = Array.from({ length: 1001 }, (_, i) => {
			const frame = glueFrame(i % 256);
			frame[16] = i >> 8;
			return frame;
		});
		const value = await unframeIds(glueUnframeStream(), frames);
		strictEqual(value.schemaVersionIds.length, 1000);
		strictEqual(value.untrackedSchemaVersionIds, 1);
	});

	test(`glueUnframeStream maxSchemaIds: null records every id`, async () => {
		const frames = Array.from({ length: 1001 }, (_, i) => {
			const frame = glueFrame(i % 256);
			frame[16] = i >> 8;
			return frame;
		});
		const value = await unframeIds(
			glueUnframeStream({ maxSchemaIds: null }),
			frames,
		);
		strictEqual(value.schemaVersionIds.length, 1001);
		strictEqual(value.untrackedSchemaVersionIds, 0);
	});

	// *** Message envelopes (e.g. kafkaConsumeStream output) *** //
	// Shaped like kafkaConsumeStream's chunks: the frame is in `value`, and
	// kafkaCommitStream needs topic/partition/offset to survive unframing.
	const kafkaMessage = (value, offset = "5") => ({
		topic: "orders",
		partition: 2,
		offset,
		key: null,
		value,
		headers: { trace: "abc" },
		timestamp: "1700000000000",
	});

	test(`confluentUnframeStream emits exactly { schemaId, payload } for a bytes chunk`, async () => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([new Uint8Array([0, 0, 0, 0, 7, 0xaa])]),
				confluentUnframeStream(),
			]),
		);
		deepStrictEqual(Object.keys(out[0]), ["schemaId", "payload"]);
	});

	test(`confluentUnframeStream unframes an envelope's value and keeps its other fields`, async () => {
		const frame = new Uint8Array([0, 0, 0, 0, 7, 0xaa, 0xbb]);
		const unframe = confluentUnframeStream();
		const out = await streamToArray(
			pipejoin([
				createReadableStream([kafkaMessage(frame), kafkaMessage(frame, "6")]),
				unframe,
			]),
		);
		deepStrictEqual(out, [
			{
				...kafkaMessage(frame),
				schemaId: 7,
				payload: new Uint8Array([0xaa, 0xbb]),
			},
			{
				...kafkaMessage(frame, "6"),
				schemaId: 7,
				payload: new Uint8Array([0xaa, 0xbb]),
			},
		]);
		deepStrictEqual(unframe.result().value.schemaIds, [7]);
	});

	test(`confluentUnframeStream rejects an envelope whose value is not bytes (e.g. a tombstone)`, async () => {
		await rejects(
			pipeline([
				createReadableStream([kafkaMessage(null)]),
				confluentUnframeStream(),
			]),
			{
				name: "TypeError",
				message:
					"schema-registry: chunk must be a Uint8Array, ArrayBuffer view, ArrayBuffer, or string",
			},
		);
	});

	test(`glueUnframeStream unframes an envelope's value and keeps its other fields`, async () => {
		const [frame] = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
			]),
		);
		const out = await streamToArray(
			pipejoin([
				createReadableStream([kafkaMessage(frame)]),
				glueUnframeStream(),
			]),
		);
		deepStrictEqual(out, [
			{
				...kafkaMessage(frame),
				schemaVersionId: schemaUuid,
				compression: "none",
				payload: helloWorld,
			},
		]);
	});

	test(`glueUnframeStream emits exactly { schemaVersionId, compression, payload } for a bytes chunk`, async () => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([helloWorld]),
				glueFrameStream({ schemaVersionId: schemaUuid }),
				glueUnframeStream(),
			]),
		);
		deepStrictEqual(Object.keys(out[0]), [
			"schemaVersionId",
			"compression",
			"payload",
		]);
	});

	test(`glueUnframeStream rejects an envelope whose value is not bytes`, async () => {
		await rejects(
			pipeline([
				createReadableStream([kafkaMessage(null)]),
				glueUnframeStream(),
			]),
			{
				name: "TypeError",
				message:
					"schema-registry: chunk must be a Uint8Array, ArrayBuffer view, ArrayBuffer, or string",
			},
		);
	});
});

describe(`confluentSchemaRegistryResolver (${variant})`, () => {
	const url = "https://registry.example.com";
	const avroSchema = '{"type":"string"}';

	// Fake fetch recording every call; `respond(id)` builds the Response.
	const fakeFetch = (respond = () => Response.json({ schema: avroSchema })) => {
		const calls = [];
		const fetch = async (requestUrl, init) => {
			calls.push({ url: requestUrl, init });
			return respond(Number(requestUrl.split("/").pop()));
		};
		return { fetch, calls };
	};

	test(`confluentSchemaRegistryResolver GETs /schemas/ids/{id} and returns schema metadata`, async () => {
		const { fetch, calls } = fakeFetch();
		const headers = { Authorization: "Basic abc" };
		const resolve = confluentSchemaRegistryResolver({ url, headers, fetch });
		deepStrictEqual(await resolve(42), {
			schemaId: 42,
			schemaDefinition: avroSchema,
			dataFormat: "AVRO",
			references: [],
		});
		strictEqual(calls.length, 1);
		strictEqual(calls[0].url, `${url}/schemas/ids/42`);
		deepStrictEqual(Object.keys(calls[0].init), ["headers", "signal"]);
		strictEqual(calls[0].init.headers, headers);
		ok(calls[0].init.signal instanceof AbortSignal);
	});

	test(`confluentSchemaRegistryResolver returns the registry's schema references`, async () => {
		const references = [{ name: "Address", subject: "address", version: 3 }];
		const { fetch } = fakeFetch(() =>
			Response.json({ schema: avroSchema, references }),
		);
		const resolve = confluentSchemaRegistryResolver({ url, fetch });
		deepStrictEqual((await resolve(1)).references, references);
	});

	test(`confluentSchemaRegistryResolver keeps the base url's path and query string`, async () => {
		const { fetch, calls } = fakeFetch();
		await confluentSchemaRegistryResolver({
			url: `${url}/?tenant=a`,
			fetch,
		})(7);
		await confluentSchemaRegistryResolver({
			url: `${url}/registry?tenant=a&b=1`,
			fetch,
		})(8);
		await confluentSchemaRegistryResolver({ url: `${url}?tenant=a`, fetch })(9);
		deepStrictEqual(
			calls.map((c) => c.url),
			[
				`${url}/schemas/ids/7?tenant=a`,
				`${url}/registry/schemas/ids/8?tenant=a&b=1`,
				`${url}/schemas/ids/9?tenant=a`,
			],
		);
	});

	// Waits for the request's signal, like a real fetch to a hung registry.
	const hangingFetch = (calls) => async (requestUrl, init) => {
		calls.push({ url: requestUrl, init });
		return new Promise((_resolve, reject) => {
			init.signal.addEventListener("abort", () => reject(init.signal.reason));
		});
	};

	test(`confluentSchemaRegistryResolver times a lookup out after 30s by default`, async (t) => {
		const timeout = t.mock.method(AbortSignal, "timeout");
		const { fetch, calls } = fakeFetch();
		await confluentSchemaRegistryResolver({ url, fetch })(1);
		strictEqual(timeout.mock.callCount(), 1);
		deepStrictEqual(timeout.mock.calls[0].arguments, [30_000]);
		strictEqual(calls[0].init.signal, timeout.mock.calls[0].result);
	});

	test(`confluentSchemaRegistryResolver rejects a lookup that exceeds timeout, then retries`, async () => {
		const calls = [];
		let hang = true;
		const { fetch: respond } = fakeFetch();
		const fetch = (requestUrl, init) =>
			hang ? hangingFetch(calls)(requestUrl, init) : respond(requestUrl, init);
		const resolve = confluentSchemaRegistryResolver({ url, fetch, timeout: 5 });
		await rejects(resolve(1), { name: "TimeoutError" });
		// The timed-out lookup must not stay in the in-flight map.
		hang = false;
		strictEqual((await resolve(1)).schemaId, 1);
		strictEqual(calls.length, 1);
	});

	test(`confluentSchemaRegistryResolver aborts a lookup when signal aborts`, async () => {
		const calls = [];
		const controller = new AbortController();
		const resolve = confluentSchemaRegistryResolver({
			url,
			fetch: hangingFetch(calls),
			signal: controller.signal,
		});
		const pending = resolve(1);
		await new Promise((r) => setTimeout(r, 1));
		const reason = new Error("shutting down");
		controller.abort(reason);
		await rejects(pending, (e) => e === reason);
		// Combined with the timeout, not passed through as-is.
		ok(calls[0].init.signal !== controller.signal);
	});

	test(`confluentSchemaRegistryResolver still times out when a signal is passed`, async () => {
		const resolve = confluentSchemaRegistryResolver({
			url,
			fetch: hangingFetch([]),
			signal: new AbortController().signal,
			timeout: 5,
		});
		await rejects(resolve(1), { name: "TimeoutError" });
	});

	// A Response whose JSON body is exactly `size` bytes, sent in two chunks so
	// the limit has to count across chunks.
	const sizedResponse = (size) => {
		const json = new TextEncoder().encode(
			JSON.stringify({ schema: "a".repeat(size - 13) }),
		);
		const half = Math.floor(size / 2);
		return new Response(
			new ReadableStream({
				start(controller) {
					controller.enqueue(json.subarray(0, half));
					controller.enqueue(json.subarray(half));
					controller.close();
				},
			}),
		);
	};

	test(`confluentSchemaRegistryResolver caps the response body at 1MiB by default`, async () => {
		const resolve = confluentSchemaRegistryResolver({
			url,
			fetch: async (requestUrl) =>
				sizedResponse(requestUrl.endsWith("/1") ? 1_048_576 : 1_048_577),
		});
		strictEqual((await resolve(1)).schemaDefinition.length, 1_048_576 - 13);
		await rejects(resolve(2), {
			name: "RangeError",
			message:
				"confluentSchemaRegistryResolver: schema 2 response exceeds maxBodySize (1048576 bytes)",
		});
	});

	test(`confluentSchemaRegistryResolver honors maxBodySize and releases the body`, async () => {
		let cancelled = false;
		const resolve = confluentSchemaRegistryResolver({
			url,
			maxBodySize: 20,
			fetch: async () =>
				new Response(
					new ReadableStream({
						start(controller) {
							controller.enqueue(new TextEncoder().encode('{"schema":'));
							controller.enqueue(new TextEncoder().encode('"0123456789"}'));
						},
						cancel() {
							cancelled = true;
						},
					}),
				),
		});
		await rejects(resolve(3), {
			name: "RangeError",
			message:
				"confluentSchemaRegistryResolver: schema 3 response exceeds maxBodySize (20 bytes)",
		});
		strictEqual(cancelled, true);
	});

	test(`confluentSchemaRegistryResolver rejects a 2xx response with no body`, async () => {
		const resolve = confluentSchemaRegistryResolver({
			url,
			fetch: async () => new Response(null, { status: 200 }),
		});
		await rejects(resolve(4), {
			name: "Error",
			message:
				"confluentSchemaRegistryResolver: schema 4 lookup returned no body",
		});
	});

	test(`confluentSchemaRegistryResolver with maxBodySize null reads any size`, async () => {
		const resolve = confluentSchemaRegistryResolver({
			url,
			maxBodySize: null,
			fetch: async () => sizedResponse(2_000_000),
		});
		strictEqual((await resolve(1)).schemaDefinition.length, 2_000_000 - 13);
	});

	test(`confluentSchemaRegistryResolver maps schemaType to dataFormat`, async () => {
		const { fetch } = fakeFetch((id) =>
			Response.json({
				schema: "syntax",
				schemaType: id === 1 ? "PROTOBUF" : "JSON",
			}),
		);
		const resolve = confluentSchemaRegistryResolver({ url, fetch });
		strictEqual((await resolve(1)).dataFormat, "PROTOBUF");
		strictEqual((await resolve(2)).dataFormat, "JSON");
	});

	test(`confluentSchemaRegistryResolver drops a trailing slash from url`, async () => {
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({
			url: `${url}/registry/`,
			fetch,
		});
		await resolve(7);
		strictEqual(calls[0].url, `${url}/registry/schemas/ids/7`);
	});

	test(`confluentSchemaRegistryResolver defaults to globalThis.fetch`, async (t) => {
		const { fetch, calls } = fakeFetch();
		t.mock.method(globalThis, "fetch", fetch);
		const resolve = confluentSchemaRegistryResolver({ url });
		strictEqual((await resolve(3)).schemaId, 3);
		strictEqual(calls.length, 1);
	});

	test(`confluentSchemaRegistryResolver rejects a missing or relative url`, () => {
		const message =
			"confluentSchemaRegistryResolver: url must be an absolute URL";
		throws(() => confluentSchemaRegistryResolver(), {
			name: "TypeError",
			message,
		});
		throws(() => confluentSchemaRegistryResolver({ url: "/schemas" }), {
			name: "TypeError",
			message,
		});
		throws(() => confluentSchemaRegistryResolver({ url: new URL(url) }), {
			name: "TypeError",
			message,
		});
	});

	test(`confluentSchemaRegistryResolver rejects ids that are not unsigned 32-bit integers`, async () => {
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({ url, fetch });
		for (const id of [-1, 1.5, "1", 2 ** 32, Number.NaN, undefined]) {
			await rejects(resolve(id), {
				name: "TypeError",
				message:
					"confluentSchemaRegistryResolver: schemaId must be an unsigned 32-bit integer",
			});
		}
		strictEqual(calls.length, 0);
		// Both ends of the range are valid.
		strictEqual((await resolve(0)).schemaId, 0);
		strictEqual((await resolve(0xffffffff)).schemaId, 0xffffffff);
	});

	test(`confluentSchemaRegistryResolver errors with the status on a non-2xx response and releases the body`, async () => {
		let cancelled = false;
		const body = new ReadableStream({
			cancel() {
				cancelled = true;
			},
		});
		const { fetch } = fakeFetch(() => new Response(body, { status: 404 }));
		const resolve = confluentSchemaRegistryResolver({ url, fetch });
		await rejects(resolve(5), (e) => {
			strictEqual(
				e.message,
				"confluentSchemaRegistryResolver: schema 5 lookup failed (HTTP 404)",
			);
			deepStrictEqual(e.cause, { status: 404 });
			return true;
		});
		strictEqual(cancelled, true);
	});

	test(`confluentSchemaRegistryResolver handles a non-2xx response with no body`, async () => {
		const { fetch } = fakeFetch(() => new Response(null, { status: 503 }));
		const resolve = confluentSchemaRegistryResolver({ url, fetch });
		await rejects(resolve(5), {
			message:
				"confluentSchemaRegistryResolver: schema 5 lookup failed (HTTP 503)",
		});
	});

	test(`confluentSchemaRegistryResolver does not cache a failure`, async () => {
		let status = 500;
		const { fetch, calls } = fakeFetch(() =>
			status === 500
				? new Response(null, { status })
				: Response.json({ schema: avroSchema }),
		);
		const resolve = confluentSchemaRegistryResolver({ url, fetch });
		await rejects(resolve(1));
		status = 200;
		strictEqual((await resolve(1)).schemaDefinition, avroSchema);
		strictEqual(calls.length, 2);
	});

	test(`confluentSchemaRegistryResolver caches indefinitely by default`, async (t) => {
		t.mock.timers.enable({ apis: ["Date"], now: 1_000 });
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({ url, fetch });
		const first = await resolve(1);
		t.mock.timers.tick(1_000_000);
		strictEqual(await resolve(1), first);
		strictEqual(calls.length, 1);
	});

	test(`confluentSchemaRegistryResolver serves from cache within a positive cacheExpiry`, async (t) => {
		t.mock.timers.enable({ apis: ["Date"], now: 1_000 });
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({
			url,
			fetch,
			cacheExpiry: 10,
		});
		await resolve(1);
		t.mock.timers.tick(9);
		await resolve(1);
		strictEqual(calls.length, 1);
		t.mock.timers.tick(1); // expiry === now: stale
		await resolve(1);
		strictEqual(calls.length, 2);
	});

	test(`confluentSchemaRegistryResolver treats cacheExpiry 0 as a finite TTL`, async (t) => {
		t.mock.timers.enable({ apis: ["Date"], now: 1_000 });
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({
			url,
			fetch,
			cacheExpiry: 0,
		});
		await resolve(1);
		await resolve(1);
		strictEqual(calls.length, 2);
	});

	test(`confluentSchemaRegistryResolver dedupes concurrent in-flight lookups`, async () => {
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({ url, fetch });
		const [a, b] = await Promise.all([resolve(1), resolve(1)]);
		strictEqual(a, b);
		strictEqual(calls.length, 1);
		// In-flight entry is cleared once settled; the cache answers from here.
		await resolve(2);
		strictEqual(calls.length, 2);
	});

	test(`confluentSchemaRegistryResolver evicts the oldest entry past maxCacheSize`, async () => {
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({
			url,
			fetch,
			maxCacheSize: 2,
		});
		await resolve(1);
		await resolve(2);
		await resolve(3); // evicts 1
		await resolve(2);
		await resolve(3);
		await resolve(1); // re-fetch, evicts 2
		await resolve(3);
		deepStrictEqual(
			calls.map((c) => c.url.split("/").pop()),
			["1", "2", "3", "1"],
		);
	});

	test(`confluentSchemaRegistryResolver defaults maxCacheSize to 1000`, async () => {
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({ url, fetch });
		for (let id = 0; id < 1000; id++) await resolve(id);
		await resolve(0);
		strictEqual(calls.length, 1000);
		await resolve(1000); // evicts 0
		await resolve(1);
		strictEqual(calls.length, 1001);
		await resolve(0);
		strictEqual(calls.length, 1002);
	});

	test(`confluentSchemaRegistryResolver with maxCacheSize 0 does not cache`, async () => {
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({
			url,
			fetch,
			maxCacheSize: 0,
		});
		await resolve(1);
		await resolve(1);
		strictEqual(calls.length, 2);
	});

	test(`confluentSchemaRegistryResolver with maxCacheSize null never evicts`, async () => {
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({
			url,
			fetch,
			maxCacheSize: null,
		});
		for (let id = 0; id < 1001; id++) await resolve(id);
		await resolve(0);
		strictEqual(calls.length, 1001);
	});

	test(`confluentSchemaRegistryResolver re-queues a refreshed key at the FIFO tail`, async (t) => {
		t.mock.timers.enable({ apis: ["Date"], now: 1_000 });
		const { fetch, calls } = fakeFetch();
		const resolve = confluentSchemaRegistryResolver({
			url,
			fetch,
			cacheExpiry: 10,
			maxCacheSize: 3,
		});
		await resolve(1); // expires 1010
		t.mock.timers.tick(5);
		await resolve(2); // expires 1015
		t.mock.timers.tick(7);
		await resolve(1); // stale -> refreshed, now newest
		await resolve(3);
		await resolve(4); // evicts the oldest: 2
		await resolve(1); // still cached
		deepStrictEqual(
			calls.map((c) => c.url.split("/").pop()),
			["1", "2", "1", "3", "4"],
		);
	});
});
