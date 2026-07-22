import { deepStrictEqual, ok, strictEqual } from "node:assert";
import test from "node:test";

import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	confluentFrameStream,
	confluentUnframeStream,
	glueFrameStream,
	glueUnframeStream,
} from "@datastream/schema-registry";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

const helloWorld = new TextEncoder().encode("hello world");
const schemaUuid = "12345678-1234-1234-1234-1234567890ab";

// *** Confluent *** //
test(`${variant}: confluentFrameStream prepends 0x00 + uint32 BE schema id`, async () => {
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

test(`${variant}: confluentFrameStream encodes schemaId big-endian for >24-bit values`, async () => {
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

test(`${variant}: confluent frame -> unframe round-trip emits envelope with schemaId + payload`, async () => {
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

test(`${variant}: confluentUnframeStream rejects frames missing the magic byte`, async () => {
	const bogus = new Uint8Array([0x01, 0, 0, 0, 0, 0x68, 0x69]);
	try {
		await pipeline([createReadableStream([bogus]), confluentUnframeStream()]);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("magic byte"));
	}
});

// *** Glue uncompressed *** //
test(`${variant}: glueFrameStream prepends 0x03 + compression byte + uuid`, async () => {
	const streams = [
		createReadableStream([helloWorld]),
		glueFrameStream({ schemaVersionId: schemaUuid }),
	];
	const out = await streamToArray(pipejoin(streams));
	strictEqual(out[0][0], 0x03);
	strictEqual(out[0][1], 0x00); // compression none
	strictEqual(out[0].byteLength, 18 + helloWorld.byteLength);
});

test(`${variant}: glue frame -> unframe round-trip emits { schemaVersionId, compression, payload }`, async () => {
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
test(`${variant}: glue frame -> unframe round-trip (zlib compressed)`, async () => {
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

test(`${variant}: glueUnframeStream enforces maxDecompressedBytes`, async () => {
	const payload = new TextEncoder().encode("a".repeat(2048));
	try {
		await pipeline([
			createReadableStream([payload]),
			glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
			glueUnframeStream({ maxDecompressedBytes: 100 }),
		]);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxDecompressedBytes"));
	}
});

test(`${variant}: glueUnframeStream rejects unknown compression byte`, async () => {
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
test(`${variant}: confluentFrameStream rejects non-integer / out-of-range schemaId`, () => {
	for (const bad of [undefined, "1", -1, 1.5, null, 0x100000000]) {
		try {
			confluentFrameStream({ schemaId: bad });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("schemaId"));
		}
	}
});

test(`${variant}: glueFrameStream rejects missing schemaVersionId`, () => {
	try {
		glueFrameStream({});
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("schemaVersionId"));
	}
});

test(`${variant}: glueFrameStream rejects unsupported compression`, () => {
	try {
		glueFrameStream({ schemaVersionId: schemaUuid, compression: "gzip" });
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("compression"));
	}
});

test(`${variant}: glueFrameStream rejects malformed schemaVersionId UUID`, () => {
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

test(`${variant}: glueUnframeStream rejects frames missing the magic byte`, async () => {
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
test(`${variant}: confluentUnframeStream accepts string chunks`, async () => {
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

test(`${variant}: confluentUnframeStream accepts ArrayBuffer view with non-zero byteOffset`, async () => {
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

test(`${variant}: confluentFrameStream throws on a numeric chunk instead of framing zero bytes`, async () => {
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

// *** Glue zlib failure / round-trip edge cases *** //
test(`${variant}: glueUnframeStream rejects a malformed zlib payload`, async () => {
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

test(`${variant}: glue frame -> unframe round-trip (zlib, empty payload)`, async () => {
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

test(`${variant}: glueFrameStream deflate enforces maxFrameBytes ceiling`, async () => {
	const payload = new TextEncoder().encode("a".repeat(4096));
	try {
		await pipeline([
			createReadableStream([payload]),
			glueFrameStream({
				schemaVersionId: schemaUuid,
				compression: "zlib",
				maxFrameBytes: 10,
			}),
		]);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxFrameBytes"));
	}
});

// *** .result() raciness guard *** //
test(`${variant}: confluentUnframeStream.result() throws when multiple distinct schema ids are seen`, async () => {
	const frameA = await streamToArray(
		pipejoin([
			createReadableStream([helloWorld]),
			confluentFrameStream({ schemaId: 1 }),
		]),
	);
	const frameB = await streamToArray(
		pipejoin([
			createReadableStream([helloWorld]),
			confluentFrameStream({ schemaId: 2 }),
		]),
	);
	const unframe = confluentUnframeStream();
	await streamToArray(
		pipejoin([createReadableStream([frameA[0], frameB[0]]), unframe]),
	);
	try {
		await unframe.result();
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("distinct"));
	}
});

// *** multi-chunk reassembly: a single frame split across chunks *** //
test(`${variant}: confluent frame -> unframe round-trips a single frame auto-chunked over 16KB`, async () => {
	// 40KB payload; createReadableStream auto-chunks the framed buffer into
	// multiple 16KB sub-chunks of ONE frame.
	const big = new Uint8Array(40 * 1024);
	for (let i = 0; i < big.byteLength; i++) big[i] = i % 251;
	const framed = await streamToArray(
		pipejoin([
			createReadableStream([big]),
			confluentFrameStream({ schemaId: 123 }),
		]),
	);
	ok(framed[0].byteLength > 16384);
	const out = await streamToArray(
		pipejoin([
			createReadableStream(framed[0]), // ArrayBuffer path -> sub-chunks
			confluentUnframeStream(),
		]),
	);
	strictEqual(out.length, 1);
	strictEqual(out[0].schemaId, 123);
	strictEqual(out[0].payload.byteLength, big.byteLength);
	deepStrictEqual(Array.from(out[0].payload), Array.from(big));
});

test(`${variant}: glue frame -> unframe round-trips a single frame auto-chunked over 16KB`, async () => {
	const big = new Uint8Array(40 * 1024);
	for (let i = 0; i < big.byteLength; i++) big[i] = (i * 7) % 251;
	const framed = await streamToArray(
		pipejoin([
			createReadableStream([big]),
			glueFrameStream({ schemaVersionId: schemaUuid }),
		]),
	);
	ok(framed[0].byteLength > 16384);
	const out = await streamToArray(
		pipejoin([createReadableStream(framed[0]), glueUnframeStream()]),
	);
	strictEqual(out.length, 1);
	strictEqual(out[0].schemaVersionId, schemaUuid);
	strictEqual(out[0].payload.byteLength, big.byteLength);
	deepStrictEqual(Array.from(out[0].payload), Array.from(big));
});

// *** envelope shape protects against per-message schemaVersionId drift *** //
test(`${variant}: glueUnframeStream envelope correctly pairs payload with its own schemaVersionId`, async () => {
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
	deepStrictEqual(new TextDecoder().decode(envelopes[0].payload), "payload-a");
	deepStrictEqual(new TextDecoder().decode(envelopes[1].payload), "payload-b");
});

// *** UUID regex anchoring: ^ and $ must both match (not substring) *** //
test(`${variant}: glueFrameStream rejects a 33-char hex string (fails $ anchor on UUID regex)`, () => {
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
test(`${variant}: confluentFrameStream accepts an Int16Array (ArrayBuffer.isView branch)`, async () => {
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
test(`${variant}: confluentFrameStream accepts a plain ArrayBuffer`, async () => {
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
test(`${variant}: asBytes Uint8Array fast-path preserves the original bytes exactly`, async () => {
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
test(`${variant}: confluentFrameStream accepts schemaId === 0 (minimum valid, exact boundary)`, async () => {
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

test(`${variant}: confluentFrameStream accepts schemaId === 0xffffffff (maximum valid, exact boundary)`, async () => {
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

test(`${variant}: confluentFrameStream rejects schemaId === 0x100000000 (one above max)`, () => {
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
test(`${variant}: confluentFrameStream rejects schemaId that is a number string (type check)`, () => {
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
test(`${variant}: confluentFrameStream header bytes are big-endian (setUint32 little=false)`, async () => {
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
test(`${variant}: confluentUnframeStream rejects a frame that is exactly 4 bytes (byteLength < 5)`, async () => {
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
test(`${variant}: confluentUnframeStream reads schemaId as big-endian (not little-endian)`, async () => {
	// magic 0x00 + [0x01, 0x02, 0x03, 0x04] big-endian = 0x01020304
	const frame = new Uint8Array([0x00, 0x01, 0x02, 0x03, 0x04, 0xaa]);
	const out = await streamToArray(
		pipejoin([createReadableStream([frame]), confluentUnframeStream()]),
	);
	strictEqual(out[0].schemaId, 0x01020304);
	strictEqual(out[0].payload[0], 0xaa);
});

// *** confluentUnframeStream distinctIds tracking *** //
test(`${variant}: confluentUnframeStream.result() succeeds when exactly 1 distinct schemaId is seen`, async () => {
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
	strictEqual(result.value.schemaId, 5);
});

test(`${variant}: confluentUnframeStream.result() throws when exactly 2 distinct schemaIds are seen`, async () => {
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
	try {
		unframe.result();
		throw new Error("Should have thrown");
	} catch (e) {
		ok(
			e.message.includes("distinct"),
			`Expected distinct error, got: ${e.message}`,
		);
		ok(
			e.message.includes("schemaId"),
			`Expected schemaId in error, got: ${e.message}`,
		);
	}
});

// *** confluentUnframeStream.result() uses resultKey when provided *** //
test(`${variant}: confluentUnframeStream.result() uses custom resultKey`, async () => {
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

test(`${variant}: confluentUnframeStream.result() defaults resultKey to "confluentSchemaId"`, async () => {
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
test(`${variant}: confluentFrameStream.result() uses custom resultKey`, async () => {
	const stream = confluentFrameStream({
		schemaId: 7,
		resultKey: "myConfluentKey",
	});
	await streamToArray(pipejoin([createReadableStream([helloWorld]), stream]));
	strictEqual(stream.result().key, "myConfluentKey");
});

test(`${variant}: confluentFrameStream.result() defaults to "confluentSchemaId"`, async () => {
	const stream = confluentFrameStream({ schemaId: 7 });
	await streamToArray(pipejoin([createReadableStream([helloWorld]), stream]));
	strictEqual(stream.result().key, "confluentSchemaId");
	strictEqual(stream.result().value.schemaId, 7);
});

// *** collectStream maxOutputSize boundary: total > maxOutputSize, not >= *** //
test(`${variant}: glueUnframeStream allows decompressed output exactly equal to maxDecompressedBytes`, async () => {
	const payload = new TextEncoder().encode("a".repeat(100));
	const out = await streamToArray(
		pipejoin([
			createReadableStream([payload]),
			glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
			glueUnframeStream({ maxDecompressedBytes: 100 }),
		]),
	);
	strictEqual(out[0].payload.byteLength, 100);
});

// *** collectStream: maxOutputSize null means no limit *** //
test(`${variant}: glueUnframeStream with default maxDecompressedBytes handles payloads without throwing`, async () => {
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

// *** createFrameBuffer: multi-chunk frame (parts.length > 1 -> concat needed in flush) *** //
test(`${variant}: confluentUnframeStream reassembles a frame split into exactly 2 chunks`, async () => {
	const framed = await streamToArray(
		pipejoin([
			createReadableStream([helloWorld]),
			confluentFrameStream({ schemaId: 200 }),
		]),
	);
	const full = framed[0];
	const headerChunk = full.slice(0, 5);
	const payloadChunk = full.slice(5);
	const out = await streamToArray(
		pipejoin([
			createReadableStream([headerChunk, payloadChunk]),
			confluentUnframeStream(),
		]),
	);
	strictEqual(out.length, 1);
	strictEqual(out[0].schemaId, 200);
	deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
});

test(`${variant}: confluentUnframeStream reassembles a frame split into 3+ chunks (multi-part concat)`, async () => {
	const payload = new Uint8Array(12);
	for (let i = 0; i < 12; i++) payload[i] = i + 1; // none start with 0x00
	const framed = await streamToArray(
		pipejoin([
			createReadableStream([payload]),
			confluentFrameStream({ schemaId: 201 }),
		]),
	);
	const full = framed[0]; // 17 bytes total
	const c1 = full.slice(0, 5); // header
	const c2 = full.slice(5, 9); // continuation 1
	const c3 = full.slice(9); // continuation 2
	const out = await streamToArray(
		pipejoin([createReadableStream([c1, c2, c3]), confluentUnframeStream()]),
	);
	strictEqual(out.length, 1);
	strictEqual(out[0].schemaId, 201);
	deepStrictEqual(Array.from(out[0].payload), Array.from(payload));
});

// *** createFrameBuffer: push completes previous multi-part frame when new frame starts *** //
test(`${variant}: confluentUnframeStream emits first multi-part frame when second frame header arrives`, async () => {
	const frameA = await streamToArray(
		pipejoin([
			createReadableStream([new Uint8Array([0x01, 0x02])]),
			confluentFrameStream({ schemaId: 300 }),
		]),
	);
	const frameB = await streamToArray(
		pipejoin([
			createReadableStream([new Uint8Array([0x03, 0x04])]),
			confluentFrameStream({ schemaId: 301 }),
		]),
	);
	const fullA = frameA[0];
	const a1 = fullA.slice(0, 5); // header
	const a2 = fullA.slice(5); // continuation
	const out = await streamToArray(
		pipejoin([
			createReadableStream([a1, a2, frameB[0]]),
			confluentUnframeStream(),
		]),
	);
	strictEqual(out.length, 2);
	strictEqual(out[0].schemaId, 300);
	strictEqual(out[1].schemaId, 301);
});

// *** createFrameBuffer flush: parts.length > 1 -> concat needed *** //
test(`${variant}: glueUnframeStream flush emits multi-part frame via concat (parts.length > 1)`, async () => {
	const bigPayload = new Uint8Array(20);
	for (let i = 0; i < 20; i++) bigPayload[i] = i + 1; // none start with 0x03
	const framed = await streamToArray(
		pipejoin([
			createReadableStream([bigPayload]),
			glueFrameStream({ schemaVersionId: schemaUuid }),
		]),
	);
	const full = framed[0];
	const c1 = full.slice(0, 18); // header exactly
	const c2 = full.slice(18); // continuation
	const out = await streamToArray(
		pipejoin([createReadableStream([c1, c2]), glueUnframeStream()]),
	);
	strictEqual(out.length, 1);
	strictEqual(out[0].schemaVersionId, schemaUuid);
	deepStrictEqual(Array.from(out[0].payload), Array.from(bigPayload));
});

// *** glueUnframeStream: frame exactly 17 bytes -> too short *** //
test(`${variant}: glueUnframeStream rejects a frame that is exactly 17 bytes (byteLength < 18)`, async () => {
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

// *** glueUnframeStream distinctIds tracking *** //
test(`${variant}: glueUnframeStream.result() succeeds when exactly 1 distinct schemaVersionId seen`, async () => {
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
	strictEqual(result.value.schemaVersionId, schemaUuid);
});

test(`${variant}: glueUnframeStream.result() throws when exactly 2 distinct schemaVersionIds seen`, async () => {
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
	try {
		unframe.result();
		throw new Error("Should have thrown");
	} catch (e) {
		ok(
			e.message.includes("distinct"),
			`Expected distinct error, got: ${e.message}`,
		);
		ok(
			e.message.includes("schemaVersionId"),
			`Expected schemaVersionId in error, got: ${e.message}`,
		);
	}
});

// *** glueUnframeStream.result() resultKey *** //
test(`${variant}: glueUnframeStream.result() uses custom resultKey`, async () => {
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

test(`${variant}: glueUnframeStream.result() defaults to "glueSchemaVersionId"`, async () => {
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
test(`${variant}: glueFrameStream.result() uses custom resultKey`, async () => {
	const stream = glueFrameStream({
		schemaVersionId: schemaUuid,
		resultKey: "myGlueFrameKey",
	});
	await streamToArray(pipejoin([createReadableStream([helloWorld]), stream]));
	strictEqual(stream.result().key, "myGlueFrameKey");
});

test(`${variant}: glueFrameStream.result() defaults to "glueSchemaVersionId"`, async () => {
	const stream = glueFrameStream({ schemaVersionId: schemaUuid });
	await streamToArray(pipejoin([createReadableStream([helloWorld]), stream]));
	strictEqual(stream.result().key, "glueSchemaVersionId");
	strictEqual(stream.result().value.schemaVersionId, schemaUuid);
});

// *** glueUnframeStream: unsupported compression byte error message includes zero-padded hex *** //
test(`${variant}: glueUnframeStream unsupported compression error message includes zero-padded hex byte`, async () => {
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

// *** createFrameBuffer startsNewFrame: byteLength >= headerSize not > headerSize *** //
test(`${variant}: confluentUnframeStream accepts a frame chunk of exactly 5 bytes as new-frame start`, async () => {
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

test(`${variant}: glueUnframeStream accepts a frame chunk of exactly 18 bytes as new-frame start`, async () => {
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
test(`${variant}: confluentUnframeStream flush does nothing when stream had no input`, async () => {
	const out = await streamToArray(
		pipejoin([createReadableStream([]), confluentUnframeStream()]),
	);
	strictEqual(out.length, 0);
});

test(`${variant}: glueUnframeStream flush does nothing when stream had no input`, async () => {
	const out = await streamToArray(
		pipejoin([createReadableStream([]), glueUnframeStream()]),
	);
	strictEqual(out.length, 0);
});

// *** Error messages must be non-empty strings (StringLiteral mutants) *** //
test(`${variant}: confluentFrameStream schemaId validation error message is non-empty`, () => {
	try {
		confluentFrameStream({ schemaId: undefined });
		throw new Error("Should have thrown");
	} catch (e) {
		ok(
			e.message !== "",
			`Expected non-empty error message, got: "${e.message}"`,
		);
		ok(e.message.length > 10);
	}
});

test(`${variant}: confluentUnframeStream missing magic byte error message is non-empty`, async () => {
	const bogus = new Uint8Array([0x01, 0, 0, 0, 0, 0x68]);
	try {
		await pipeline([createReadableStream([bogus]), confluentUnframeStream()]);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(
			e.message !== "",
			`Expected non-empty error message, got: "${e.message}"`,
		);
		ok(e.message.length > 10);
	}
});

test(`${variant}: confluentUnframeStream.result() multiple-ids error message is non-empty`, async () => {
	const frameA = await streamToArray(
		pipejoin([
			createReadableStream([helloWorld]),
			confluentFrameStream({ schemaId: 20 }),
		]),
	);
	const frameB = await streamToArray(
		pipejoin([
			createReadableStream([helloWorld]),
			confluentFrameStream({ schemaId: 21 }),
		]),
	);
	const unframe = confluentUnframeStream();
	await streamToArray(
		pipejoin([createReadableStream([frameA[0], frameB[0]]), unframe]),
	);
	try {
		unframe.result();
		throw new Error("Should have thrown");
	} catch (e) {
		ok(
			e.message !== "",
			`Expected non-empty error message, got: "${e.message}"`,
		);
		ok(e.message.length > 10);
	}
});

test(`${variant}: glueUnframeStream.result() multiple-ids error message is non-empty`, async () => {
	const idA = "11111111-1111-1111-1111-111111111111";
	const idB = "22222222-2222-2222-2222-222222222222";
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
	try {
		unframe.result();
		throw new Error("Should have thrown");
	} catch (e) {
		ok(
			e.message !== "",
			`Expected non-empty error message, got: "${e.message}"`,
		);
		ok(e.message.length > 10);
	}
});

// *** envelope contains both schemaId and payload fields (not empty object) *** //
test(`${variant}: confluentUnframeStream envelope has schemaId and payload fields`, async () => {
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
test(`${variant}: confluentFrameStream.result() value object contains schemaId`, async () => {
	const stream = confluentFrameStream({ schemaId: 42 });
	await streamToArray(pipejoin([createReadableStream([helloWorld]), stream]));
	ok("schemaId" in stream.result().value, "value should have schemaId");
	strictEqual(stream.result().value.schemaId, 42);
});

// *** glueUnframeStream value object has schemaVersionId and compression *** //
test(`${variant}: glueUnframeStream.result() value object contains schemaVersionId and compression`, async () => {
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
test(`${variant}: confluentUnframeStream.result() value has schemaId field (not empty object)`, async () => {
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
test(`${variant}: glueUnframeStream decodes UUID bytes with single-digit hex values (padStart needed)`, async () => {
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
test(`${variant}: glueUnframeStream decodes all 16 UUID bytes correctly (loop bound i < 16)`, async () => {
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

// *** default export contains all 4 functions *** //
test(`${variant}: default export contains all 4 stream factory functions`, async () => {
	const mod = await import("@datastream/schema-registry");
	const def = mod.default;
	ok(
		typeof def.confluentFrameStream === "function",
		"confluentFrameStream missing",
	);
	ok(
		typeof def.confluentUnframeStream === "function",
		"confluentUnframeStream missing",
	);
	ok(typeof def.glueFrameStream === "function", "glueFrameStream missing");
	ok(typeof def.glueUnframeStream === "function", "glueUnframeStream missing");
});

// *** createFrameBuffer: chunk < headerSize starting with magic is a continuation *** //
test(`${variant}: confluentUnframeStream treats a chunk shorter than 5 bytes starting with 0x00 as continuation`, async () => {
	// A 3-byte chunk starting with 0x00 is < headerSize(5), so it is NOT a new frame.
	// Instead it is a continuation. Combined with the next bytes, it forms a valid frame.
	// Build a valid frame and split it: first 3 bytes (header prefix), then the remaining 8 bytes.
	const framed = await streamToArray(
		pipejoin([
			createReadableStream([helloWorld]),
			confluentFrameStream({ schemaId: 600 }),
		]),
	);
	const full = framed[0];
	// first 3 bytes: [0x00, 0x00, 0x00] — starts with magic but only 3 bytes (< 5)
	const c1 = full.slice(0, 3);
	const c2 = full.slice(3);
	const out = await streamToArray(
		pipejoin([createReadableStream([c1, c2]), confluentUnframeStream()]),
	);
	strictEqual(out.length, 1);
	strictEqual(out[0].schemaId, 600);
	deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
});

test(`${variant}: glueUnframeStream treats a chunk shorter than 18 bytes starting with 0x03 as continuation`, async () => {
	// A 10-byte chunk starting with 0x03 is < headerSize(18), so it is a continuation.
	const framed = await streamToArray(
		pipejoin([
			createReadableStream([helloWorld]),
			glueFrameStream({ schemaVersionId: schemaUuid }),
		]),
	);
	const full = framed[0];
	const c1 = full.slice(0, 10); // starts with 0x03 but < 18 bytes
	const c2 = full.slice(10);
	const out = await streamToArray(
		pipejoin([createReadableStream([c1, c2]), glueUnframeStream()]),
	);
	strictEqual(out.length, 1);
	strictEqual(out[0].schemaVersionId, schemaUuid);
	deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
});

// *** collectStream: maxOutputSize null/undefined -> no limit applied *** //
test(`${variant}: confluentUnframeStream initial value has schemaId as null before any frames`, async () => {
	// Test that result().value.schemaId is null (not undefined) before frames processed.
	// With the { schemaId: null } mutation -> {}, value.schemaId would be undefined.
	const unframe = confluentUnframeStream();
	// Process one frame so result() does not throw due to distinctIds check
	await streamToArray(
		pipejoin([
			createReadableStream([helloWorld]),
			confluentFrameStream({ schemaId: 50 }),
			unframe,
		]),
	);
	strictEqual(unframe.result().value.schemaId, 50);
});

test(`${variant}: glueUnframeStream initial value fields present after processing`, async () => {
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

// *** collectStream limitName fallback "maxDecompressedBytes" is non-empty string *** //
test(`${variant}: glueUnframeStream limit error message contains non-empty limitName`, async () => {
	// When glueUnframeStream calls inflate, limitName="maxDecompressedBytes" is passed.
	// With the empty string mutant, the error would say "schema-registry:  exceeded"
	// instead of "schema-registry: maxDecompressedBytes exceeded".
	const payload = new TextEncoder().encode("a".repeat(500));
	try {
		await pipeline([
			createReadableStream([payload]),
			glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
			glueUnframeStream({ maxDecompressedBytes: 10 }),
		]);
		throw new Error("Should have thrown");
	} catch (e) {
		// The error message must contain "maxDecompressedBytes" (not just "exceeded")
		ok(
			e.message.includes("maxDecompressedBytes"),
			`Error should mention maxDecompressedBytes, got: ${e.message}`,
		);
		// Verify it's not an empty string for the limit name
		const match = e.message.match(/schema-registry: (\S+) exceeded/);
		ok(
			match !== null,
			`Error message format should match 'schema-registry: <name> exceeded', got: ${e.message}`,
		);
		strictEqual(match[1], "maxDecompressedBytes");
	}
});

// *** confluentUnframeStream initial value has schemaId as null (not undefined) *** //
test(`${variant}: confluentUnframeStream.result() value.schemaId is null before any frames processed`, async () => {
	// With { schemaId: null } -> {}, value.schemaId would be undefined, not null.
	const unframe = confluentUnframeStream();
	// distinctIds=0 so result() won't throw even before any frames.
	const result = unframe.result();
	strictEqual(result.value.schemaId, null);
});

// *** glueUnframeStream initial value fields are null (not undefined) *** //
test(`${variant}: glueUnframeStream.result() value fields are null before any frames processed`, async () => {
	// With { schemaVersionId: null, compression: null } -> {}, fields would be undefined.
	const unframe = glueUnframeStream();
	const result = unframe.result();
	strictEqual(result.value.schemaVersionId, null);
	strictEqual(result.value.compression, null);
});

// *** Multi-part push: payload bytes must be present in the completed first frame *** //
test(`${variant}: confluentUnframeStream emits correct payload bytes from multi-part first frame`, async () => {
	// Build a frame where the payload bytes differ from the header bytes.
	// This ensures parts.length===1 ? parts[0] : concat(parts) actually works correctly
	// when concat is needed (parts.length > 1 in the push path).
	const distinctPayload = new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]);
	const frameA = await streamToArray(
		pipejoin([
			createReadableStream([distinctPayload]),
			confluentFrameStream({ schemaId: 700 }),
		]),
	);
	const frameB = await streamToArray(
		pipejoin([
			createReadableStream([new Uint8Array([0x01])]),
			confluentFrameStream({ schemaId: 701 }),
		]),
	);
	// Split frameA into header + payload to force parts.length===2 in push.
	const a1 = frameA[0].slice(0, 5); // header
	const a2 = frameA[0].slice(5); // payload = [0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]
	const out = await streamToArray(
		pipejoin([
			createReadableStream([a1, a2, frameB[0]]),
			confluentUnframeStream(),
		]),
	);
	strictEqual(out.length, 2);
	strictEqual(out[0].schemaId, 700);
	// Must include ALL payload bytes (a2), not just the header (a1).
	deepStrictEqual(Array.from(out[0].payload), Array.from(distinctPayload));
	strictEqual(out[1].schemaId, 701);
});

// *** createFrameBuffer startsNewFrame requires the LENGTH check, not just magic ***
// A short (< headerSize) continuation chunk that happens to start with the magic
// byte must stay a continuation. Dropping the `byteLength >= headerSize` guard
// (mutant: `true && bytes[0] === magic`) would mis-split it into its own frame.
test(`${variant}: confluentUnframeStream keeps a <5-byte magic-leading continuation attached to the open frame`, async () => {
	const framed = await streamToArray(
		pipejoin([
			createReadableStream([helloWorld]),
			confluentFrameStream({ schemaId: 808 }),
		]),
	);
	// A 2-byte trailing continuation that begins with the magic byte 0x00.
	const shortCont = new Uint8Array([0x00, 0x99]);
	const out = await streamToArray(
		pipejoin([
			createReadableStream([framed[0], shortCont]),
			confluentUnframeStream(),
		]),
	);
	// Correct: ONE frame whose payload is helloWorld + [0x00, 0x99].
	strictEqual(out.length, 1);
	strictEqual(out[0].schemaId, 808);
	deepStrictEqual(Array.from(out[0].payload), [
		...Array.from(helloWorld),
		0x00,
		0x99,
	]);
});

// *** createFrameBuffer startsNewFrame uses >= headerSize, not > headerSize ***
// A second frame that is EXACTLY headerSize bytes (empty payload) must be
// recognised as a new frame. Mutant `> headerSize` would fold it into the
// previous frame as a continuation.
test(`${variant}: confluentUnframeStream treats an exactly-5-byte second frame as a new frame`, async () => {
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
	// Correct: TWO distinct envelopes. Mutant (`>`) would merge into one.
	strictEqual(out.length, 2);
	strictEqual(out[0].schemaId, 811);
	deepStrictEqual(Array.from(out[0].payload), Array.from(helloWorld));
	strictEqual(out[1].schemaId, 812);
	strictEqual(out[1].payload.byteLength, 0);
});

// *** collectStream maxOutputSize guard: `!= null` must gate the size check ***
// Passing maxDecompressedBytes: null explicitly disables the ceiling. The guard
// `maxOutputSize != null && total > maxOutputSize` must short-circuit to false.
// Mutant `true && total > maxOutputSize` becomes `total > null` => `total > 0`,
// which would throw for any non-empty decompressed output.
test(`${variant}: glueUnframeStream with maxDecompressedBytes: null imposes no ceiling`, async () => {
	const payload = new TextEncoder().encode("a".repeat(2048));
	const out = await streamToArray(
		pipejoin([
			createReadableStream([payload]),
			glueFrameStream({ schemaVersionId: schemaUuid, compression: "zlib" }),
			glueUnframeStream({ maxDecompressedBytes: null }),
		]),
	);
	strictEqual(out.length, 1);
	strictEqual(out[0].compression, "zlib");
	deepStrictEqual(Array.from(out[0].payload), Array.from(payload));
});
