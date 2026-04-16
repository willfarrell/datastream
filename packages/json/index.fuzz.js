import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	streamToArray,
} from "@datastream/core";
import {
	jsonFormatStream,
	jsonParseStream,
	ndjsonFormatStream,
	ndjsonParseStream,
} from "@datastream/json";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

// *** ndjsonParseStream *** //
test("fuzz ndjsonParseStream w/ string input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (input) => {
			try {
				const streams = [createReadableStream(input), ndjsonParseStream()];
				const stream = pipejoin(streams);
				await streamToArray(stream);
			} catch (e) {
				catchError(input, e);
			}
		}),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

test("fuzz ndjsonParseStream w/ ndjson-like input", async () => {
	const ndjsonArb = fc
		.array(fc.jsonValue(), { minLength: 1, maxLength: 20 })
		.map((values) => values.map((v) => JSON.stringify(v)).join("\n"));

	await fc.assert(
		fc.asyncProperty(ndjsonArb, async (input) => {
			try {
				const streams = [createReadableStream(input), ndjsonParseStream()];
				const stream = pipejoin(streams);
				await streamToArray(stream);
			} catch (e) {
				catchError(input, e);
			}
		}),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** ndjsonFormatStream *** //
test("fuzz ndjsonFormatStream w/ object input", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.object({ maxDepth: 2 }), { minLength: 1, maxLength: 20 }),
			async (input) => {
				try {
					const streams = [createReadableStream(input), ndjsonFormatStream()];
					const stream = pipejoin(streams);
					await streamToArray(stream);
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

// *** jsonParseStream *** //
test("fuzz jsonParseStream w/ string input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (input) => {
			try {
				const streams = [createReadableStream(input), jsonParseStream()];
				const stream = pipejoin(streams);
				await streamToArray(stream);
			} catch (e) {
				catchError(input, e);
			}
		}),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

test("fuzz jsonParseStream w/ json array input", async () => {
	const jsonArrayArb = fc
		.array(fc.object({ maxDepth: 2 }), { minLength: 0, maxLength: 20 })
		.map((arr) => JSON.stringify(arr));

	await fc.assert(
		fc.asyncProperty(jsonArrayArb, async (input) => {
			try {
				const streams = [createReadableStream(input), jsonParseStream()];
				const stream = pipejoin(streams);
				await streamToArray(stream);
			} catch (e) {
				catchError(input, e);
			}
		}),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** jsonFormatStream *** //
test("fuzz jsonFormatStream w/ object input", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.object({ maxDepth: 2 }), { minLength: 1, maxLength: 20 }),
			async (input) => {
				try {
					const streams = [createReadableStream(input), jsonFormatStream()];
					const stream = pipejoin(streams);
					await streamToArray(stream);
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
