// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { strictEqual } from "node:assert";
import test from "node:test";
import { gcpBigQueryInsertStream } from "@datastream/gcp/bigquery";
import fc from "fast-check";

const tick = () => new Promise((resolve) => setImmediate(resolve));

class PartialFailureError extends Error {
	constructor(errors) {
		super("insert failed");
		this.name = "PartialFailureError";
		this.errors = errors;
	}
}

// outcomes[call]: "ok", "partial" (rows at odd positions fail) or "fail"
// (whole request); each insert settles after `delay` ticks.
const fakeTable = (outcomes, delay) => {
	const table = {
		inserted: [],
		inFlight: [],
		insert: async (rows) => {
			const outcome = outcomes[table.calls++ % outcomes.length];
			table.inFlight = rows;
			for (let i = 0; i < delay; i++) await tick();
			table.inFlight = [];
			if (outcome === "fail") throw new Error("503 Service Unavailable");
			const failed = rows.filter((_row, index) => index % 2 === 1);
			table.inserted.push(...rows.filter((_row, index) => index % 2 === 0));
			if (outcome === "partial" && failed.length) {
				throw new PartialFailureError(
					failed.map((row) => ({ row, errors: [{ reason: "invalid" }] })),
				);
			}
			if (outcome === "ok") table.inserted.push(...failed);
		},
		calls: 0,
	};
	return table;
};

// Every written row is inserted, handed back (err.failedMessages), rejected
// up front (oversize RangeError), or in an insert still in flight at teardown.
test("fuzz gcpBigQueryInsertStream batch accounting", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.string({ maxLength: 120 }), { maxLength: 40 }),
			fc.integer({ min: 1, max: 6 }),
			fc.integer({ min: 70, max: 500 }),
			fc.array(fc.constantFrom("ok", "ok", "partial", "fail"), {
				minLength: 1,
				maxLength: 5,
			}),
			fc.nat({ max: 3 }),
			fc.option(fc.nat({ max: 40 })),
			async (values, batchSize, maxBatchBytes, outcomes, delay, destroyAt) => {
				const table = fakeTable(outcomes, delay);
				const stream = gcpBigQueryInsertStream({
					table,
					batchSize,
					maxBatchBytes,
				});
				let error;
				stream.on("error", (e) => {
					error ??= e;
				});
				// Not events.once: it rejects on 'error'.
				const closed = new Promise((resolve) => stream.once("close", resolve));
				let written = 0;
				for (const [index, value] of values.entries()) {
					if (stream.destroyed) break;
					written++;
					const done = new Promise((resolve) =>
						stream.write({ value }, resolve),
					);
					// Tear down while this write's insert may still be in flight.
					if (index === destroyAt) {
						await tick();
						break;
					}
					await done;
				}
				if (destroyAt === null) stream.end();
				else stream.destroy();
				await closed;
				const inFlight = table.inFlight.length;
				const rejected = error?.name === "RangeError" ? 1 : 0;
				strictEqual(
					table.inserted.length +
						(error?.failedMessages?.length ?? 0) +
						rejected +
						inFlight,
					written,
				);
				if (destroyAt === null && !error) {
					strictEqual(table.inserted.length, written);
				}
			},
		),
		{ numRuns: 1_000, verbose: 2 },
	);
});
