import test from "node:test";
import { createReadableStream, pipeline } from "@datastream/core";
import { digestStream } from "@datastream/digest";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

// *** digestStream *** //
test("fuzz digestStream w/ string input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.array(fc.string(), { minLength: 1 }), async (input) => {
			try {
				const streams = [
					createReadableStream(input),
					digestStream({ algorithm: "SHA2-256" }),
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

test("fuzz digestStream w/ algorithm", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.constantFrom("SHA2-256", "SHA2-384", "SHA2-512"),
			fc.array(fc.string(), { minLength: 1 }),
			async (algorithm, input) => {
				try {
					const streams = [
						createReadableStream(input),
						digestStream({ algorithm }),
					];
					await pipeline(streams);
				} catch (e) {
					catchError({ algorithm, input }, e);
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
