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
	csvInjectHeaderStream,
	csvObjectToArrayStream,
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

// Splitting the input into arbitrary string or byte chunks must not change
// what the detect -> header -> parse pipeline produces.
test("fuzz csv pipeline is independent of chunking", async () => {
	const run = async (chunks) => {
		const detect = csvDetectDelimitersStream();
		const lazy = {
			delimiterChar: () => detect.result().value.delimiterChar,
			newlineChar: () => detect.result().value.newlineChar,
			quoteChar: () => detect.result().value.quoteChar,
		};
		const header = csvDetectHeaderStream(lazy);
		const parse = csvParseStream(lazy);
		const rows = await streamToArray(
			pipejoin([createReadableStream(chunks), detect, header, parse]),
		);
		return {
			detected: detect.result().value,
			header: header.result().value.header,
			rows,
			errors: parse.result().value,
		};
	};
	const split = (input, cuts) => {
		const at = [...new Set(cuts.map((c) => c % (input.length + 1)))].sort(
			(a, b) => a - b,
		);
		const chunks = [];
		let prev = 0;
		for (const cut of at) {
			chunks.push(input.slice(prev, cut));
			prev = cut;
		}
		chunks.push(input.slice(prev));
		return chunks;
	};
	await fc.assert(
		fc.asyncProperty(
			fc.string({
				unit: fc.constantFrom(
					"a",
					"é",
					",",
					";",
					'"',
					"'",
					"\r",
					"\n",
					"\uFEFF",
				),
				maxLength: 60,
			}),
			fc.array(fc.nat(), { maxLength: 10 }),
			fc.boolean(),
			async (input, cuts, asBytes) => {
				const expected = await run([input]);
				const source = asBytes ? new TextEncoder().encode(input) : input;
				const chunks = split(source, cuts);
				const actual = await run(chunks);
				// Detection only sees the data buffered up to the first line, so it may
				// legitimately differ with chunking; compare the rest only when it matches.
				if (
					JSON.stringify(actual.detected) !== JSON.stringify(expected.detected)
				)
					return;
				if (JSON.stringify(actual) !== JSON.stringify(expected)) {
					throw new Error(
						`chunked output differs: ${JSON.stringify({ input, chunks: chunks.map(String), expected, actual })}`,
					);
				}
			},
		),
		{
			numRuns: 5_000,
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
test("fuzz csvFormatStream w/ array input", async () => {
	const rowArb = fc.array(fc.string(), { minLength: 1, maxLength: 10 });
	await fc.assert(
		fc.asyncProperty(
			fc.array(rowArb, { minLength: 1, maxLength: 20 }),
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

test("fuzz csvFormatStream w/ object input via compose", async () => {
	const objArb = fc.record({
		a: fc.string(),
		b: fc.string(),
		c: fc.string(),
	});
	const headers = ["a", "b", "c"];
	await fc.assert(
		fc.asyncProperty(
			fc.array(objArb, { minLength: 1, maxLength: 20 }),
			async (input) => {
				try {
					const streams = [
						createReadableStream(input),
						csvObjectToArrayStream({ headers }),
						csvInjectHeaderStream({ header: headers }),
						csvFormatStream(),
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
