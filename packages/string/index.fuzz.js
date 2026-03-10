import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	stringCountStream,
	stringLengthStream,
	stringMinimumChunkSize,
	stringMinimumFirstChunkSize,
	stringReadableStream,
	stringReplaceStream,
	stringSkipConsecutiveDuplicates,
	stringSplitStream,
} from "@datastream/string";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

// *** stringReadableStream *** //
test("fuzz stringReadableStream w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (input) => {
			try {
				const stream = stringReadableStream(input);
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

// *** stringLengthStream *** //
test("fuzz stringLengthStream w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.array(fc.string()), async (input) => {
			try {
				const streams = [createReadableStream(input), stringLengthStream()];
				await pipeline(streams);
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

// *** stringCountStream *** //
test("fuzz stringCountStream w/ substr", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.string(), { minLength: 1 }),
			fc.string({ minLength: 1 }),
			async (input, substr) => {
				try {
					const streams = [
						createReadableStream(input),
						stringCountStream({ substr }),
					];
					await pipeline(streams);
				} catch (e) {
					catchError({ input, substr }, e);
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

// *** stringSkipConsecutiveDuplicates *** //
test("fuzz stringSkipConsecutiveDuplicates w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.array(fc.string()), async (input) => {
			try {
				const streams = [
					createReadableStream(input),
					stringSkipConsecutiveDuplicates(),
				];
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

// *** stringReplaceStream *** //
test("fuzz stringReplaceStream w/ pattern and replacement", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.string(), { minLength: 1 }),
			fc.string({ minLength: 1 }),
			fc.string(),
			async (input, pattern, replacement) => {
				try {
					const streams = [
						createReadableStream(input),
						stringReplaceStream({ pattern, replacement }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError({ input, pattern, replacement }, e);
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

// *** stringSplitStream *** //
test("fuzz stringSplitStream w/ separator", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.string(), { minLength: 1 }),
			fc.string({ minLength: 1 }),
			async (input, separator) => {
				try {
					const streams = [
						createReadableStream(input),
						stringSplitStream({ separator }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError({ input, separator }, e);
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

// *** stringMinimumFirstChunkSize *** //
test("fuzz stringMinimumFirstChunkSize w/ chunkSize", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.string(), { minLength: 1 }),
			fc.nat({ max: 10000 }),
			async (input, chunkSize) => {
				try {
					const streams = [
						createReadableStream(input),
						stringMinimumFirstChunkSize({ chunkSize }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError({ input, chunkSize }, e);
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

// *** stringMinimumChunkSize *** //
test("fuzz stringMinimumChunkSize w/ chunkSize", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.string(), { minLength: 1 }),
			fc.nat({ max: 10000 }),
			async (input, chunkSize) => {
				try {
					const streams = [
						createReadableStream(input),
						stringMinimumChunkSize({ chunkSize }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError({ input, chunkSize }, e);
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
