import test from "node:test";
import {
	indexedDBReadStream,
	indexedDBWriteStream,
} from "@datastream/indexeddb";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		"indexedDBReadStream: Not supported",
		"indexedDBWriteStream: Not supported",
	];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

// *** indexedDBReadStream *** //
test("fuzz indexedDBReadStream w/ random options", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.record({
				db: fc.option(fc.anything()),
				store: fc.option(fc.string({ minLength: 0, maxLength: 100 })),
				index: fc.option(fc.string({ minLength: 0, maxLength: 100 })),
				key: fc.option(fc.anything()),
			}),
			async (options) => {
				try {
					await indexedDBReadStream(options);
				} catch (e) {
					catchError(options, e);
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

// *** indexedDBWriteStream *** //
test("fuzz indexedDBWriteStream w/ random options", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.record({
				db: fc.option(fc.anything()),
				store: fc.option(fc.string({ minLength: 0, maxLength: 100 })),
			}),
			async (options) => {
				try {
					await indexedDBWriteStream(options);
				} catch (e) {
					catchError(options, e);
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
