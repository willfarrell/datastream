import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	streamToArray,
} from "@datastream/core";
import {
	csvCoerceValuesStream,
	csvDetectDelimitersStream,
	csvDetectHeaderStream,
	csvFormatStream,
	csvParseStream,
	csvQuotedParser,
	csvRemoveEmptyRowsStream,
	csvRemoveMalformedRowsStream,
	csvUnquotedParser,
} from "@datastream/csv";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

// *** csvQuotedParser *** //
test("fuzz csvQuotedParser w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (input) => {
			try {
				csvQuotedParser(input, {}, true);
			} catch (e) {
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

test("fuzz csvQuotedParser w/ delimiterChar", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.string(),
			fc.string({ minLength: 1, maxLength: 3 }),
			async (input, delimiterChar) => {
				try {
					csvQuotedParser(input, { delimiterChar }, true);
				} catch (e) {
					catchError({ input, delimiterChar }, e);
				}
			},
		),
		{
			numRuns: 10_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** csvUnquotedParser *** //
test("fuzz csvUnquotedParser w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (input) => {
			try {
				csvUnquotedParser(input, {}, true);
			} catch (e) {
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

// *** csvParseStream *** //
test("fuzz csvParseStream w/ string input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (input) => {
			try {
				const streams = [createReadableStream(input), csvParseStream()];
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

test("fuzz csvParseStream w/ csv-like input", async () => {
	const csvArb = fc
		.array(
			fc.array(fc.string({ maxLength: 20 }), {
				minLength: 1,
				maxLength: 10,
			}),
			{ minLength: 1, maxLength: 20 },
		)
		.map((rows) => `${rows.map((row) => row.join(",")).join("\r\n")}\r\n`);

	await fc.assert(
		fc.asyncProperty(csvArb, async (input) => {
			try {
				const streams = [createReadableStream(input), csvParseStream()];
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

// *** csvDetectDelimitersStream *** //
test("fuzz csvDetectDelimitersStream w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (input) => {
			try {
				const streams = [
					createReadableStream(input),
					csvDetectDelimitersStream(),
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

// *** csvDetectHeaderStream *** //
test("fuzz csvDetectHeaderStream w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (input) => {
			try {
				const streams = [createReadableStream(input), csvDetectHeaderStream()];
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

// *** csvRemoveEmptyRowsStream *** //
test("fuzz csvRemoveEmptyRowsStream w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.array(fc.string(), { maxLength: 5 })),
			async (input) => {
				try {
					const streams = [
						createReadableStream(input),
						csvRemoveEmptyRowsStream(),
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

// *** csvRemoveMalformedRowsStream *** //
test("fuzz csvRemoveMalformedRowsStream w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.array(fc.string(), { minLength: 1, maxLength: 5 }), {
				minLength: 1,
			}),
			async (input) => {
				try {
					const streams = [
						createReadableStream(input),
						csvRemoveMalformedRowsStream(),
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

// *** csvCoerceValuesStream *** //
test("fuzz csvCoerceValuesStream w/ input", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.object({ maxDepth: 0 }), { minLength: 1 }),
			async (input) => {
				try {
					const streams = [
						createReadableStream(input),
						csvCoerceValuesStream(),
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

// *** csvFormatStream *** //
test("fuzz csvFormatStream w/ object input", async () => {
	const objArb = fc.record({
		a: fc.string(),
		b: fc.string(),
		c: fc.string(),
	});
	await fc.assert(
		fc.asyncProperty(
			fc.array(objArb, { minLength: 1, maxLength: 20 }),
			async (input) => {
				try {
					const streams = [createReadableStream(input), csvFormatStream()];
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
