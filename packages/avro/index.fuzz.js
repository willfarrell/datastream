// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import test from "node:test";
import { avroDecodeStream, avroEncodeStream } from "@datastream/avro";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import fc from "fast-check";

const schema = {
	type: "record",
	name: "Msg",
	fields: [
		{ name: "id", type: "int" },
		{ name: "name", type: "string" },
		{ name: "tag", type: ["null", "string"] },
	],
};

const catchError = (input, e) => {
	if (
		e.message.startsWith("avroEncodeStream: record does not match schema") ||
		e.message === "avroDecodeStream: truncated buffer" ||
		e.message === "avroDecodeStream: trailing data" ||
		e.message.startsWith("avroDecodeStream: invalid union index")
	) {
		return;
	}
	console.error(input, e);
	throw e;
};

test("fuzz encode→decode round-trip with random records", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					id: fc.integer({ min: -2147483648, max: 2147483647 }),
					name: fc.string(),
					tag: fc.option(fc.string(), { nil: null }),
				}),
				{ minLength: 1 },
			),
			async (input) => {
				try {
					const decoded = await streamToArray(
						pipejoin([
							createReadableStream(input),
							avroEncodeStream({ schema }),
							avroDecodeStream({ schema }),
						]),
					);
					for (let i = 0; i < input.length; i++) {
						const got = decoded[i];
						const want = input[i];
						if (
							got.id !== want.id ||
							got.name !== want.name ||
							got.tag !== want.tag
						) {
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

test("fuzz avroEncodeStream with arbitrary values", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.anything(), { minLength: 1 }),
			async (input) => {
				try {
					await pipeline([
						createReadableStream(
							input.filter((v) => v !== null && v !== undefined),
						),
						avroEncodeStream({ schema }),
					]);
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{ numRuns: 1_000, verbose: 2, examples: [] },
	);
});

test("fuzz avroDecodeStream with random byte inputs", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.uint8Array({ minLength: 0, maxLength: 64 }), {
				minLength: 1,
			}),
			async (chunks) => {
				try {
					await pipeline([
						createReadableStream(chunks),
						avroDecodeStream({ schema }),
					]);
				} catch (e) {
					catchError(chunks, e);
				}
			},
		),
		{ numRuns: 1_000, verbose: 2, examples: [] },
	);
});
