import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	objectBatchStream,
	objectCountStream,
	objectFromEntriesStream,
	objectKeyJoinStream,
	objectKeyMapStream,
	objectKeyValueStream,
	objectKeyValuesStream,
	objectOmitStream,
	objectPickStream,
	objectReadableStream,
	objectSkipConsecutiveDuplicatesStream,
} from "@datastream/object";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		"Expected chunk to be array, use with objectBatchStream",
	];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

// *** objectReadableStream *** //
test("fuzz objectReadableStream w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.anything().filter((v) => v !== null)),
			async (input) => {
				try {
					const stream = objectReadableStream(input);
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

// *** objectCountStream *** //
test("fuzz objectCountStream w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.anything().filter((v) => v !== null)),
			async (input) => {
				try {
					const streams = [createReadableStream(input), objectCountStream()];
					await pipeline(streams);
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

// *** objectPickStream *** //
test("fuzz objectPickStream w/ keys", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.object({ maxDepth: 0 }), { minLength: 1 }),
			fc.array(fc.string(), { minLength: 1, maxLength: 5 }),
			async (input, keys) => {
				try {
					const streams = [
						createReadableStream(input),
						objectPickStream({ keys }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError({ input, keys }, e);
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

// *** objectOmitStream *** //
test("fuzz objectOmitStream w/ keys", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.object({ maxDepth: 0 }), { minLength: 1 }),
			fc.array(fc.string(), { minLength: 1, maxLength: 5 }),
			async (input, keys) => {
				try {
					const streams = [
						createReadableStream(input),
						objectOmitStream({ keys }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError({ input, keys }, e);
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

// *** objectKeyMapStream *** //
test("fuzz objectKeyMapStream w/ keys", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.object({ maxDepth: 0 }), { minLength: 1 }),
			fc.object({ maxDepth: 0 }),
			async (input, keys) => {
				try {
					const streams = [
						createReadableStream(input),
						objectKeyMapStream({ keys }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError({ input, keys }, e);
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

// *** objectBatchStream *** //
test("fuzz objectBatchStream w/ keys", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.record({ group: fc.string(), value: fc.anything() }), {
				minLength: 1,
			}),
			async (input) => {
				try {
					const streams = [
						createReadableStream(input),
						objectBatchStream({ keys: ["group"] }),
					];
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

// *** objectKeyValueStream *** //
test("fuzz objectKeyValueStream w/ key/value", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					k: fc.string(),
					v: fc.anything(),
				}),
				{ minLength: 1 },
			),
			async (input) => {
				try {
					const streams = [
						createReadableStream(input),
						objectKeyValueStream({ key: "k", value: "v" }),
					];
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

// *** objectKeyValuesStream *** //
test("fuzz objectKeyValuesStream w/ key/values", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					k: fc.string(),
					a: fc.anything(),
					b: fc.anything(),
				}),
				{ minLength: 1 },
			),
			async (input) => {
				try {
					const streams = [
						createReadableStream(input),
						objectKeyValuesStream({ key: "k", values: ["a", "b"] }),
					];
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

// *** objectKeyJoinStream *** //
test("fuzz objectKeyJoinStream w/ keys", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					first: fc.string(),
					last: fc.string(),
					other: fc.anything(),
				}),
				{ minLength: 1 },
			),
			fc.string({ minLength: 1, maxLength: 3 }),
			async (input, separator) => {
				try {
					const streams = [
						createReadableStream(input),
						objectKeyJoinStream({
							keys: { name: ["first", "last"] },
							separator,
						}),
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

// *** objectFromEntriesStream *** //
test("fuzz objectFromEntriesStream w/ keys", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.string(), { minLength: 1, maxLength: 5 }),
			async (keys) => {
				try {
					const input = [keys.map((_, i) => `val${i}`)];
					const streams = [
						createReadableStream(input),
						objectFromEntriesStream({ keys }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError(keys, e);
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

// *** objectSkipConsecutiveDuplicatesStream *** //
test("fuzz objectSkipConsecutiveDuplicatesStream w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.array(fc.object({ maxDepth: 0 })), async (input) => {
			try {
				const streams = [
					createReadableStream(input),
					objectSkipConsecutiveDuplicatesStream(),
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
