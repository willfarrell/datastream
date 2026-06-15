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
	protobufDecodeStream,
	protobufEncodeStream,
	protobufLengthPrefixFrameStream,
	protobufLengthPrefixUnframeStream,
} from "@datastream/protobuf";
import fc from "fast-check";
import protobuf from "protobufjs";

const Type = new protobuf.Type("Msg")
	.add(new protobuf.Field("id", 1, "int32"))
	.add(new protobuf.Field("name", 2, "string"));

const catchError = (input, e) => {
	if (
		e.message.includes("maxMessageSize") ||
		e.message.includes("maxOutputSize") ||
		e.message.includes("incomplete message") ||
		/invalid wire type/i.test(e.message) ||
		/invalid tag/i.test(e.message) ||
		/invalid varint/i.test(e.message) ||
		/invalid end group/i.test(e.message) ||
		/index out of range/i.test(e.message) ||
		/max depth exceeded/i.test(e.message) ||
		/illegal/i.test(e.message)
	) {
		return;
	}
	console.error(input, e);
	throw e;
};

// *** protobufEncodeStream + protobufDecodeStream round-trip *** //
test("fuzz encode→decode round-trip with random {id, name}", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					id: fc.integer({ min: -2147483648, max: 2147483647 }),
					name: fc.string(),
				}),
				{ minLength: 1 },
			),
			async (input) => {
				try {
					const encoded = await streamToArray(
						pipejoin([
							createReadableStream(input),
							protobufEncodeStream({ Type }),
						]),
					);
					const decoded = await streamToArray(
						pipejoin([
							createReadableStream(encoded),
							protobufDecodeStream({ Type }),
						]),
					);
					for (let i = 0; i < input.length; i++) {
						const got = decoded[i];
						const want = input[i];
						if (got.id !== want.id || got.name !== want.name) {
							throw new Error(
								`Round-trip mismatch at ${i}: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`,
							);
						}
					}
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{ numRuns: 1_000, verbose: 2, examples: [] },
	);
});

// *** protobufDecodeStream with random bytes *** //
test("fuzz protobufDecodeStream with random byte inputs", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.uint8Array({ minLength: 0, maxLength: 64 }), {
				minLength: 1,
			}),
			async (chunks) => {
				try {
					await pipeline([
						createReadableStream(chunks),
						protobufDecodeStream({ Type }),
					]);
				} catch (e) {
					catchError(chunks, e);
				}
			},
		),
		{ numRuns: 1_000, verbose: 2, examples: [] },
	);
});

// *** protobufDecodeStream maxOutputSize guard *** //
test("fuzz protobufDecodeStream with maxOutputSize", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					id: fc.integer({ min: 0, max: 100 }),
					name: fc.string({ maxLength: 32 }),
				}),
				{ minLength: 1, maxLength: 10 },
			),
			fc.integer({ min: 1, max: 512 }),
			async (input, maxOutputSize) => {
				try {
					const encoded = await streamToArray(
						pipejoin([
							createReadableStream(input),
							protobufEncodeStream({ Type }),
						]),
					);
					await pipeline([
						createReadableStream(encoded),
						protobufDecodeStream({ Type, maxOutputSize }),
					]);
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{ numRuns: 1_000, verbose: 2, examples: [] },
	);
});

// *** protobufLengthPrefixUnframeStream with random byte chunks *** //
test("fuzz protobufLengthPrefixUnframeStream with random byte chunks", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.uint8Array({ minLength: 0, maxLength: 64 }), {
				minLength: 1,
			}),
			async (chunks) => {
				try {
					await pipeline([
						createReadableStream(chunks),
						protobufLengthPrefixUnframeStream(),
					]);
				} catch (e) {
					catchError(chunks, e);
				}
			},
		),
		{ numRuns: 1_000, verbose: 2, examples: [] },
	);
});

// *** protobufLengthPrefixUnframeStream maxMessageSize guard *** //
test("fuzz protobufLengthPrefixUnframeStream with maxMessageSize", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.uint8Array({ minLength: 1, maxLength: 64 }), {
				minLength: 1,
				maxLength: 8,
			}),
			fc.integer({ min: 1, max: 128 }),
			async (chunks, maxMessageSize) => {
				try {
					await pipeline([
						createReadableStream(chunks),
						protobufLengthPrefixUnframeStream({ maxMessageSize }),
					]);
				} catch (e) {
					catchError(chunks, e);
				}
			},
		),
		{ numRuns: 1_000, verbose: 2, examples: [] },
	);
});

// *** frame→unframe round-trip with random payloads *** //
test("fuzz frame→unframe round-trip with random payloads", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.uint8Array({ minLength: 0, maxLength: 128 }), {
				minLength: 1,
				maxLength: 16,
			}),
			async (payloads) => {
				try {
					const framed = await streamToArray(
						pipejoin([
							createReadableStream(payloads),
							protobufLengthPrefixFrameStream(),
						]),
					);
					const unframed = await streamToArray(
						pipejoin([
							createReadableStream(framed),
							protobufLengthPrefixUnframeStream(),
						]),
					);
					if (unframed.length !== payloads.length) {
						throw new Error(
							`Length mismatch: got ${unframed.length}, want ${payloads.length}`,
						);
					}
					for (let i = 0; i < payloads.length; i++) {
						const got = unframed[i];
						const want = payloads[i];
						if (got.length !== want.length) {
							throw new Error(
								`Payload length mismatch at ${i}: got ${got.length}, want ${want.length}`,
							);
						}
						for (let j = 0; j < want.length; j++) {
							if (got[j] !== want[j]) {
								throw new Error(`Byte mismatch at ${i}[${j}]`);
							}
						}
					}
				} catch (e) {
					catchError(payloads, e);
				}
			},
		),
		{ numRuns: 1_000, verbose: 2, examples: [] },
	);
});
