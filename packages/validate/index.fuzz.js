import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	streamToArray,
} from "@datastream/core";
import { transpileSchema, validateStream } from "@datastream/validate";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

const schema = transpileSchema({
	type: "object",
	properties: {
		name: { type: "string" },
		age: { type: "number" },
		active: { type: "boolean" },
	},
	required: ["name"],
	additionalProperties: true,
});

// *** validateStream w/ valid objects *** //
test("fuzz validateStream w/ valid-ish objects", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					name: fc.string(),
					age: fc.option(fc.integer()),
					active: fc.option(fc.boolean()),
				}),
				{ minLength: 1 },
			),
			async (input) => {
				try {
					const streams = [
						createReadableStream(input),
						validateStream({ schema, onErrorEnqueue: true }),
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

// *** validateStream w/ anything *** //
test("fuzz validateStream w/ anything", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.anything().filter((v) => v !== null),
				{ minLength: 1 },
			),
			async (input) => {
				try {
					const streams = [
						createReadableStream(input),
						validateStream({ schema, onErrorEnqueue: true }),
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

// *** validateStream w/ onErrorEnqueue false *** //
test("fuzz validateStream w/ onErrorEnqueue false", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.anything().filter((v) => v !== null),
				{ minLength: 1 },
			),
			async (input) => {
				try {
					const streams = [
						createReadableStream(input),
						validateStream({ schema, onErrorEnqueue: false }),
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
