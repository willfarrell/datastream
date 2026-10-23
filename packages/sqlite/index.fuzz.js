// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { strictEqual } from "node:assert";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { sqliteInsertStream, sqliteQueryStream } from "@datastream/sqlite";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		"sqlite: identifier must be a non-empty string",
		"sqliteInsertStream: at least one column required",
		"sqliteInsertStream: columns required for array rows",
	];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	// Row-shape errors name the offending keys or counts.
	if (
		e.message.startsWith("sqliteInsertStream: row has keys not in columns: ") ||
		e.message.startsWith("sqliteInsertStream: array row has ")
	) {
		return;
	}
	// SQLite rejected the statement (no such table/column, a NUL truncating the
	// SQL text, ...), a value could not be bound, or a named param is unknown.
	if (
		e.code === "ERR_SQLITE_ERROR" ||
		e.code === "ERR_INVALID_ARG_TYPE" ||
		e.code === "ERR_INVALID_STATE"
	) {
		return;
	}
	console.error(input, e);
	throw e;
};

const quote = (name) => `"${name.replaceAll('"', '""')}"`;

// ---------------------------------------------------------------------------
// fuzz: identifiers never escape their quotes
// ---------------------------------------------------------------------------

test("fuzz sqliteInsertStream w/ random identifiers", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.string({ minLength: 0, maxLength: 30 }),
			fc.string({ minLength: 0, maxLength: 30 }),
			async (table, column) => {
				const db = new DatabaseSync(":memory:");
				db.exec("CREATE TABLE victim (v)");
				try {
					db.exec(`CREATE TABLE ${quote(table)} (${quote(column)})`);
				} catch {
					// Some names (NUL, sqlite_ prefix, ...) can't be created; the stream
					// must still fail cleanly against them.
				}
				try {
					await pipeline([
						createReadableStream([{ [column]: 1 }]),
						sqliteInsertStream({ db, table }),
					]);
				} catch (e) {
					catchError({ table, column }, e);
				}
				strictEqual(db.isTransaction, false);
				// A broken-out identifier would have dropped or altered this table.
				strictEqual(db.prepare("SELECT count(*) AS n FROM victim").get().n, 0);
				db.close();
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [['x"; DROP TABLE victim; --', 'c" ) VALUES (1); --']],
		},
	);
});

// ---------------------------------------------------------------------------
// fuzz: random rows round-trip or roll back entirely
// ---------------------------------------------------------------------------

test("fuzz sqliteInsertStream w/ random rows", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.oneof(
					fc.record(
						{
							a: fc.oneof(fc.integer(), fc.string(), fc.constant(null)),
							b: fc.oneof(fc.double({ noNaN: true }), fc.string()),
						},
						{ requiredKeys: [] },
					),
					fc.array(fc.oneof(fc.integer(), fc.string()), { maxLength: 3 }),
					// not bindable: exercises the mid-stream rollback path
					fc.record({ a: fc.object() }),
				),
				{ maxLength: 20 },
			),
			async (rows) => {
				const db = new DatabaseSync(":memory:");
				db.exec("CREATE TABLE t (a, b)");
				let ok = true;
				try {
					await pipeline([
						createReadableStream(rows),
						sqliteInsertStream({ db, table: "t" }),
					]);
				} catch (e) {
					ok = false;
					catchError(rows, e);
				}
				strictEqual(db.isTransaction, false);
				const stored = await streamToArray(
					sqliteQueryStream({ db, sql: "SELECT * FROM t" }),
				);
				// All or nothing.
				strictEqual(stored.length, ok ? rows.length : 0);
				db.close();
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// ---------------------------------------------------------------------------
// fuzz: query params
// ---------------------------------------------------------------------------

test("fuzz sqliteQueryStream w/ random params", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.oneof(
				fc.array(fc.oneof(fc.integer(), fc.string(), fc.constant(null)), {
					maxLength: 3,
				}),
				fc.dictionary(fc.string({ maxLength: 5 }), fc.integer(), {
					maxKeys: 3,
				}),
				fc.constant(null),
			),
			async (params) => {
				const db = new DatabaseSync(":memory:");
				try {
					await streamToArray(
						sqliteQueryStream({ db, sql: "SELECT ? AS x", params }),
					);
				} catch (e) {
					catchError(params, e);
				}
				db.close();
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});
