// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
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
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		// confluentUnframeStream validation
		"confluentUnframeStream: missing 0x00 magic byte / frame is too short",
		// glueUnframeStream validation
		"glueUnframeStream: missing 0x03 magic byte / frame is too short",
		// glueUnframeStream compression errors (dynamic message with hex byte)
		"unsupported compression",
		// glueUnframeStream decompressed size guard
		"maxDecompressedBytes",
		// glueFrameStream frame size guard
		"maxFrameBytes",
		// confluentFrameStream schemaId validation
		"confluentFrameStream: schemaId must be an unsigned 32-bit integer",
		// glueFrameStream schemaVersionId validation
		"glueFrameStream: schemaVersionId required",
		// glueFrameStream UUID validation
		"UUID",
		// glueFrameStream compression validation
		"unsupported compression",
		// asBytes chunk type validation
		"schema-registry: chunk must be a Uint8Array, ArrayBuffer view, ArrayBuffer, or string",
		// confluentUnframeStream.result() / glueUnframeStream.result() distinct-id guard
		"distinct",
		// zlib decompression failures (malformed payload)
		"unexpected end of file",
		"invalid stored block lengths",
		"incorrect header check",
		"invalid block type",
		"invalid distance too far back",
		"invalid literal/length code",
		"invalid distance code",
		"too many length or distance symbols",
		"invalid code lengths set",
		"invalid bit length repeat",
		"over-subscribed dynamic bit lengths tree",
		"incomplete dynamic bit lengths tree",
		"empty distance tree with lengths",
		"over-subscribed literal/length tree",
		"incomplete literal/length tree",
	];
	for (const msg of expectedErrors) {
		if (e.message.includes(msg)) return;
	}
	console.error(input, e);
	throw e;
};

// *** confluentFrameStream *** //
test("fuzz confluentFrameStream w/ random payload + schemaId", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 0, maxLength: 256 }),
			fc.integer({ min: 0, max: 0xffffffff }),
			async (payload, schemaId) => {
				try {
					const streams = [
						createReadableStream([payload]),
						confluentFrameStream({ schemaId }),
					];
					await streamToArray(pipejoin(streams));
				} catch (e) {
					catchError({ payload, schemaId }, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** confluentUnframeStream *** //
test("fuzz confluentUnframeStream w/ random bytes", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 0, maxLength: 256 }),
			async (input) => {
				try {
					await pipeline([
						createReadableStream([input]),
						confluentUnframeStream(),
					]);
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** confluent frame -> unframe roundtrip *** //
test("fuzz confluent frame -> unframe roundtrip", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 0, maxLength: 256 }),
			fc.integer({ min: 0, max: 0xffffffff }),
			async (payload, schemaId) => {
				try {
					const streams = [
						createReadableStream([payload]),
						confluentFrameStream({ schemaId }),
						confluentUnframeStream(),
					];
					await streamToArray(pipejoin(streams));
				} catch (e) {
					catchError({ payload, schemaId }, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** glueFrameStream *** //
test("fuzz glueFrameStream w/ random payload + UUID", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 0, maxLength: 256 }),
			fc.uuid(),
			async (payload, schemaVersionId) => {
				try {
					const streams = [
						createReadableStream([payload]),
						glueFrameStream({ schemaVersionId }),
					];
					await streamToArray(pipejoin(streams));
				} catch (e) {
					catchError({ payload, schemaVersionId }, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** glueUnframeStream *** //
test("fuzz glueUnframeStream w/ random bytes", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 0, maxLength: 256 }),
			async (input) => {
				try {
					await pipeline([createReadableStream([input]), glueUnframeStream()]);
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** glue frame -> unframe roundtrip (none compression) *** //
test("fuzz glue frame -> unframe roundtrip (none)", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 0, maxLength: 256 }),
			fc.uuid(),
			async (payload, schemaVersionId) => {
				try {
					const streams = [
						createReadableStream([payload]),
						glueFrameStream({ schemaVersionId }),
						glueUnframeStream(),
					];
					await streamToArray(pipejoin(streams));
				} catch (e) {
					catchError({ payload, schemaVersionId }, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** glue frame -> unframe roundtrip (zlib compression) *** //
test("fuzz glue frame -> unframe roundtrip (zlib)", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 0, maxLength: 256 }),
			fc.uuid(),
			async (payload, schemaVersionId) => {
				try {
					const streams = [
						createReadableStream([payload]),
						glueFrameStream({ schemaVersionId, compression: "zlib" }),
						glueUnframeStream(),
					];
					await streamToArray(pipejoin(streams));
				} catch (e) {
					catchError({ payload, schemaVersionId }, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});
