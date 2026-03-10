import test from "node:test";
import {
	createPassThroughStream,
	createReadableStream,
	createTransformStream,
	makeOptions,
	pipeline,
	streamToArray,
	streamToObject,
	streamToString,
} from "@datastream/core";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = ["ERR_STREAM_NULL_VALUES"];
	if (expectedErrors.includes(e.code)) {
		return;
	}
	console.error(input, e);
	throw e;
};

// *** createReadableStream *** //
test("fuzz createReadableStream w/ string input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (input) => {
			try {
				const stream = createReadableStream(input);
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

test("fuzz createReadableStream w/ array of strings", async () => {
	await fc.assert(
		fc.asyncProperty(fc.array(fc.string()), async (input) => {
			try {
				const stream = createReadableStream(input);
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

test("fuzz createReadableStream w/ array of anything", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.anything().filter((v) => v !== null)),
			async (input) => {
				try {
					const stream = createReadableStream(input);
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

// *** streamToString *** //
test("fuzz streamToString w/ string input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (input) => {
			try {
				const stream = createReadableStream(input);
				await streamToString(stream);
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

// *** streamToObject *** //
test("fuzz streamToObject w/ object input", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.object(), { minLength: 1, maxLength: 5 }),
			async (input) => {
				try {
					const stream = createReadableStream(input);
					await streamToObject(stream);
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

// *** pipeline *** //
test("fuzz pipeline w/ passthrough", async () => {
	await fc.assert(
		fc.asyncProperty(fc.array(fc.string()), async (input) => {
			try {
				const streams = [
					createReadableStream(input),
					createPassThroughStream(),
				];
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

test("fuzz pipeline w/ transform", async () => {
	await fc.assert(
		fc.asyncProperty(fc.array(fc.string()), async (input) => {
			try {
				const streams = [createReadableStream(input), createTransformStream()];
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

// *** makeOptions *** //
test("fuzz makeOptions w/ anything", async () => {
	await fc.assert(
		fc.asyncProperty(fc.anything(), async (input) => {
			try {
				makeOptions(input);
			} catch (e) {
				if (e instanceof TypeError) {
					return;
				}
				catchError(input, e);
			}
		}),
		{
			numRuns: 10_000,
			verbose: 2,
			examples: [],
		},
	);
});
