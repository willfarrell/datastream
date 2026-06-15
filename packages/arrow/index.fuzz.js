import test from "node:test";
import {
	arrowBatchFromArrayStream,
	arrowBatchFromObjectStream,
	arrowDetectSchemaStream,
	arrowToArrayStream,
	arrowToObjectStream,
} from "@datastream/arrow";
import {
	createReadableStream,
	pipejoin,
	streamToArray,
} from "@datastream/core";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

// Helper: run arrowDetectSchemaStream in isolation and return the detected schema.
// Returns null when the input produces no detectable schema (e.g. empty stream or
// all-empty objects).
const detectSchema = async (input) => {
	const detect = arrowDetectSchemaStream({ sampleSize: 10 });
	await streamToArray(pipejoin([createReadableStream(input), detect]));
	return detect.result().value.schema;
};

// *** arrowDetectSchemaStream *** //
test("fuzz arrowDetectSchemaStream w/ object array input", async () => {
	await fc.assert(
		fc.asyncProperty(fc.array(fc.object({ maxDepth: 0 })), async (input) => {
			try {
				await detectSchema(input);
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

// *** arrowBatchFromObjectStream via detect *** //
// Skip when the detected schema is null (empty stream) or has no fields (all-empty
// objects were sampled), as those are valid edge-cases tested by the unit suite.
test("fuzz arrowBatchFromObjectStream w/ detected schema", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.object({ maxDepth: 0 }), { minLength: 1 }),
			async (input) => {
				try {
					const schema = await detectSchema(input);
					if (schema === null || schema.fields.length === 0) {
						return;
					}
					const streams = [
						createReadableStream(input),
						arrowBatchFromObjectStream({ schema, batchSize: 50 }),
					];
					await streamToArray(pipejoin(streams));
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

// *** arrowBatchFromArrayStream via detect *** //
test("fuzz arrowBatchFromArrayStream w/ detected schema", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.array(fc.oneof(fc.integer(), fc.string(), fc.boolean()), {
					minLength: 1,
					maxLength: 5,
				}),
				{ minLength: 1 },
			),
			async (input) => {
				try {
					const schema = await detectSchema(input);
					if (schema === null || schema.fields.length === 0) {
						return;
					}
					const streams = [
						createReadableStream(input),
						arrowBatchFromArrayStream({ schema, batchSize: 50 }),
					];
					await streamToArray(pipejoin(streams));
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

// *** detect -> batch -> arrowToObjectStream roundtrip *** //
test("fuzz arrowToObjectStream roundtrip w/ detected schema", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.object({ maxDepth: 0 }), { minLength: 1 }),
			async (input) => {
				try {
					const schema = await detectSchema(input);
					if (schema === null || schema.fields.length === 0) {
						return;
					}
					const streams = [
						createReadableStream(input),
						arrowBatchFromObjectStream({ schema, batchSize: 50 }),
						arrowToObjectStream(),
					];
					await streamToArray(pipejoin(streams));
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

// *** detect -> batch -> arrowToArrayStream roundtrip *** //
test("fuzz arrowToArrayStream roundtrip w/ detected schema", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.object({ maxDepth: 0 }), { minLength: 1 }),
			async (input) => {
				try {
					const schema = await detectSchema(input);
					if (schema === null || schema.fields.length === 0) {
						return;
					}
					const streams = [
						createReadableStream(input),
						arrowBatchFromObjectStream({ schema, batchSize: 50 }),
						arrowToArrayStream(),
					];
					await streamToArray(pipejoin(streams));
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
