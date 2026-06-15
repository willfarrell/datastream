import test from "node:test";
import { arrowBatchFromObjectStream } from "@datastream/arrow";
import { createReadableStream, pipeline } from "@datastream/core";
import {
	duckdbAppenderStream,
	duckdbArrowInsertStream,
	duckdbConnect,
} from "@datastream/duckdb";
import { Field, Int32, Schema, Utf8 } from "apache-arrow";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		// Invalid table name (non-string or empty string)
		"duckdb: identifier must be a non-empty string",
		// DuckDB type coercion failures (e.g. out-of-range float → INTEGER)
		"Failed to cast value:",
		// Cannot store objects/arrays/symbols/functions into typed columns
		"Cannot create values of type ANY. Specify a specific type.",
		// Column count mismatch between batch and table
		"column count mismatch",
	];
	for (const prefix of expectedErrors) {
		if (e.message?.includes(prefix) || e.message === prefix) {
			return;
		}
	}
	console.error(input, e);
	throw e;
};

// Fixed in-memory schema for the shared users table used across fuzz iterations.
const arrowSchema = new Schema([
	new Field("id", new Int32(), true),
	new Field("name", new Utf8(), true),
]);

// *** duckdbAppenderStream — fuzz random object rows *** //
test("fuzz duckdbAppenderStream w/ random object rows", async () => {
	const db = await duckdbConnect();
	await db.run("CREATE TABLE fuzz_appender (id INTEGER, name VARCHAR)");

	await fc.assert(
		fc.asyncProperty(
			// Generate arrays of objects with arbitrary id/name values (including
			// edge cases like null, undefined, wrong types, booleans, etc.)
			fc.array(
				fc.record({
					id: fc.oneof(
						fc.integer(),
						fc.float(),
						fc.string(),
						fc.boolean(),
						fc.constant(null),
						fc.constant(undefined),
					),
					name: fc.oneof(
						fc.string(),
						fc.integer(),
						fc.constant(null),
						fc.constant(undefined),
					),
				}),
				{ minLength: 1, maxLength: 20 },
			),
			async (input) => {
				try {
					await pipeline([
						createReadableStream(input),
						await duckdbAppenderStream({ db, table: "fuzz_appender" }),
					]);
					// Reset table for next iteration so row counts don't grow unbounded.
					await db.run("DELETE FROM fuzz_appender");
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 200,
			verbose: 2,
			examples: [],
		},
	);

	db.closeSync();
});

// *** duckdbAppenderStream — fuzz random table names and row shapes *** //
test("fuzz duckdbAppenderStream w/ random table name", async () => {
	await fc.assert(
		fc.asyncProperty(
			// Table names — many will be invalid (non-string, empty, etc.)
			fc.oneof(
				fc.string({ minLength: 0, maxLength: 50 }),
				fc.integer(),
				fc.constant(null),
				fc.constant(undefined),
			),
			fc.array(
				fc.record({
					id: fc.integer({ min: 0, max: 9_999 }),
					name: fc.string({ minLength: 0, maxLength: 20 }),
				}),
				{ minLength: 1, maxLength: 5 },
			),
			async (table, input) => {
				const db = await duckdbConnect();
				// Only create the table when the name is a valid non-empty string so
				// we exercise both valid and invalid-name paths cleanly.
				if (typeof table === "string" && table.length > 0) {
					try {
						await db.run(
							`CREATE TABLE "${table.replaceAll('"', '""')}" (id INTEGER, name VARCHAR)`,
						);
					} catch (_) {
						// Some random strings produce invalid SQL even after quoting;
						// skip table creation and let the appender surface the error.
					}
				}
				try {
					await pipeline([
						createReadableStream(input),
						await duckdbAppenderStream({ db, table }),
					]);
				} catch (e) {
					catchError({ table, input }, e);
				}
				db.closeSync();
			},
		),
		{
			numRuns: 200,
			verbose: 2,
			examples: [],
		},
	);
});

// *** duckdbArrowInsertStream — fuzz random Arrow record batches *** //
test("fuzz duckdbArrowInsertStream w/ random object rows via Arrow batches", async () => {
	const db = await duckdbConnect();
	await db.run("CREATE TABLE fuzz_arrow (id INTEGER, name VARCHAR)");

	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					id: fc.oneof(fc.integer({ min: 0, max: 9_999 }), fc.constant(null)),
					name: fc.oneof(fc.string({ maxLength: 30 }), fc.constant(null)),
				}),
				{ minLength: 1, maxLength: 50 },
			),
			fc.integer({ min: 1, max: 100 }),
			async (input, batchSize) => {
				try {
					await pipeline([
						createReadableStream(input),
						arrowBatchFromObjectStream({ schema: arrowSchema, batchSize }),
						await duckdbArrowInsertStream({ db, table: "fuzz_arrow" }),
					]);
					await db.run("DELETE FROM fuzz_arrow");
				} catch (e) {
					catchError({ input, batchSize }, e);
				}
			},
		),
		{
			numRuns: 200,
			verbose: 2,
			examples: [],
		},
	);

	db.closeSync();
});
