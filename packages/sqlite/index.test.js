// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, rejects, strictEqual, throws } from "node:assert";
import { DatabaseSync } from "node:sqlite";
import test, { describe } from "node:test";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { sqliteInsertStream, sqliteQueryStream } from "@datastream/sqlite";

// node:sqlite rows have a null prototype; spread them for deepStrictEqual.
const plain = (rows) => rows.map((row) => ({ ...row }));
const selectAll = (db, table = "t") =>
	plain(db.prepare(`SELECT * FROM "${table}"`).all());

const makeDb = () => {
	const db = new DatabaseSync(":memory:");
	db.exec("CREATE TABLE t (id INTEGER, name TEXT)");
	return db;
};

const seed = (db) => {
	const insert = db.prepare("INSERT INTO t (id, name) VALUES (?, ?)");
	insert.run(1, "a");
	insert.run(2, "b");
	insert.run(3, "c");
};

describe("@datastream/sqlite", () => {
	describe("sqliteQueryStream", () => {
		test("streams rows as objects", async () => {
			const db = makeDb();
			seed(db);
			const stream = sqliteQueryStream({
				db,
				sql: "SELECT id, name FROM t ORDER BY id",
			});
			deepStrictEqual(plain(await streamToArray(stream)), [
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
				{ id: 3, name: "c" },
			]);
		});

		test("binds positional params from an array", async () => {
			const db = makeDb();
			seed(db);
			const stream = sqliteQueryStream({
				db,
				sql: "SELECT name FROM t WHERE id > ? AND id < ? ORDER BY id",
				params: [1, 3],
			});
			deepStrictEqual(plain(await streamToArray(stream)), [{ name: "b" }]);
		});

		test("binds named params from an object", async () => {
			const db = makeDb();
			seed(db);
			const stream = sqliteQueryStream({
				db,
				sql: "SELECT name FROM t WHERE id = $id",
				params: { id: 3 },
			});
			deepStrictEqual(plain(await streamToArray(stream)), [{ name: "c" }]);
		});

		test("never binds a leading object in positional params as named", () => {
			const db = makeDb();
			// e.g. untrusted JSON `{}` in the first slot would otherwise shift 2
			// into the first placeholder.
			throws(
				() =>
					sqliteQueryStream({
						db,
						sql: "SELECT ? AS a, ? AS b",
						params: [{}, 2],
					}),
				{
					code: "ERR_INVALID_ARG_TYPE",
					message: "Provided value cannot be bound to SQLite parameter 1.",
				},
			);
		});

		test("treats null params as no params", async () => {
			const db = makeDb();
			// Without the guard, null is bound as a positional NULL and a query
			// with no placeholders fails with "column index out of range".
			const stream = sqliteQueryStream({
				db,
				sql: "SELECT 1 AS x",
				params: null,
			});
			deepStrictEqual(plain(await streamToArray(stream)), [{ x: 1 }]);
		});

		test("passes streamOptions to the readable", async () => {
			const db = makeDb();
			const stream = sqliteQueryStream(
				{ db, sql: "SELECT 1 AS x" },
				{ highWaterMark: 7 },
			);
			strictEqual(stream.readableHighWaterMark, 7);
			deepStrictEqual(plain(await streamToArray(stream)), [{ x: 1 }]);
		});

		test("throws on invalid sql", () => {
			const db = makeDb();
			throws(() => sqliteQueryStream({ db, sql: "SELEC nope" }), {
				code: "ERR_SQLITE_ERROR",
			});
		});

		test("early destroy releases the statement iterator", async () => {
			const db = makeDb();
			seed(db);
			const stream = sqliteQueryStream({
				db,
				sql: "SELECT id FROM t ORDER BY id",
			});
			const rows = [];
			for await (const row of stream) {
				rows.push(row.id);
				break;
			}
			deepStrictEqual(rows, [1]);
			strictEqual(stream.destroyed, true);
			// A still-open read cursor would hold a SHARED lock; with the iterator
			// released, DDL on the same table succeeds.
			db.exec("DROP TABLE t");
		});
	});

	describe("sqliteInsertStream", () => {
		test("inserts object rows, deriving columns from the first row", async () => {
			const db = makeDb();
			await pipeline([
				createReadableStream([
					{ id: 1, name: "a" },
					{ name: "b", id: 2 },
				]),
				sqliteInsertStream({ db, table: "t" }),
			]);
			deepStrictEqual(selectAll(db), [
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
			]);
			strictEqual(db.isTransaction, false);
		});

		test("inserts missing object keys as NULL", async () => {
			const db = makeDb();
			await pipeline([
				createReadableStream([{ id: 1, name: "a" }, { id: 2 }]),
				sqliteInsertStream({ db, table: "t" }),
			]);
			deepStrictEqual(selectAll(db), [
				{ id: 1, name: "a" },
				{ id: 2, name: null },
			]);
		});

		test("errors on keys missing from the derived columns", async () => {
			const db = new DatabaseSync(":memory:");
			db.exec("CREATE TABLE t (a, b, c)");
			await rejects(
				pipeline([
					createReadableStream([{ a: 1 }, { a: 2, b: 3, c: 4 }]),
					sqliteInsertStream({ db, table: "t" }),
				]),
				{
					name: "TypeError",
					message: 'sqliteInsertStream: row has keys not in columns: "b", "c"',
				},
			);
			deepStrictEqual(selectAll(db), []);
			strictEqual(db.isTransaction, false);
		});

		test("ignores inherited properties of object rows", async () => {
			const db = new DatabaseSync(":memory:");
			db.exec('CREATE TABLE t ("constructor", "toString")');
			await pipeline([
				createReadableStream([{}]),
				sqliteInsertStream({
					db,
					table: "t",
					columns: ["constructor", "toString"],
				}),
			]);
			deepStrictEqual(selectAll(db), [{ constructor: null, toString: null }]);
		});

		test("inserts an explicitly undefined value as NULL", async () => {
			const db = makeDb();
			await pipeline([
				createReadableStream([{ id: 1, name: undefined }]),
				sqliteInsertStream({ db, table: "t" }),
			]);
			deepStrictEqual(selectAll(db), [{ id: 1, name: null }]);
		});

		test("inserts object rows into only the given columns", async () => {
			const db = makeDb();
			await pipeline([
				createReadableStream([{ id: 1, name: "a", extra: true }]),
				sqliteInsertStream({ db, table: "t", columns: ["id"] }),
			]);
			deepStrictEqual(selectAll(db), [{ id: 1, name: null }]);
		});

		test("inserts array rows in the given column order", async () => {
			const db = makeDb();
			await pipeline([
				createReadableStream([
					["a", 1],
					["b", 2],
				]),
				sqliteInsertStream({ db, table: "t", columns: ["name", "id"] }),
			]);
			deepStrictEqual(selectAll(db), [
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
			]);
		});

		test("array rows shorter than columns error and commit nothing", async () => {
			const db = makeDb();
			await rejects(
				pipeline([
					createReadableStream([[1, "a"], [2]]),
					sqliteInsertStream({ db, table: "t", columns: ["id", "name"] }),
				]),
				{
					name: "TypeError",
					message: "sqliteInsertStream: array row has 1 values, expected 2",
				},
			);
			deepStrictEqual(selectAll(db), []);
			strictEqual(db.isTransaction, false);
		});

		test("array rows longer than columns error and commit nothing", async () => {
			const db = makeDb();
			await rejects(
				pipeline([
					createReadableStream([[1, "a", true]]),
					sqliteInsertStream({ db, table: "t", columns: ["id", "name"] }),
				]),
				{
					name: "TypeError",
					message: "sqliteInsertStream: array row has 3 values, expected 2",
				},
			);
			deepStrictEqual(selectAll(db), []);
			strictEqual(db.isTransaction, false);
		});

		test("array rows without columns error and commit nothing", async () => {
			const db = makeDb();
			await rejects(
				pipeline([
					createReadableStream([[1, "a"]]),
					sqliteInsertStream({ db, table: "t" }),
				]),
				{
					name: "TypeError",
					message: "sqliteInsertStream: columns required for array rows",
				},
			);
			deepStrictEqual(selectAll(db), []);
			strictEqual(db.isTransaction, false);
		});

		test("rejects an empty column list", async () => {
			const db = makeDb();
			throws(() => sqliteInsertStream({ db, table: "t", columns: [] }), {
				name: "TypeError",
				message: "sqliteInsertStream: at least one column required",
			});
			await rejects(
				pipeline([
					createReadableStream([{}]),
					sqliteInsertStream({ db, table: "t" }),
				]),
				{
					name: "TypeError",
					message: "sqliteInsertStream: at least one column required",
				},
			);
			strictEqual(db.isTransaction, false);
		});

		test("rejects empty and non-string identifiers", () => {
			const db = makeDb();
			const error = {
				name: "TypeError",
				message: "sqlite: identifier must be a non-empty string",
			};
			throws(() => sqliteInsertStream({ db, table: "" }), error);
			throws(() => sqliteInsertStream({ db, table: 1 }), error);
			throws(() => sqliteInsertStream({ db }), error);
			throws(
				() => sqliteInsertStream({ db, table: "t", columns: ["id", ""] }),
				error,
			);
			throws(
				() => sqliteInsertStream({ db, table: "t", columns: [null] }),
				error,
			);
		});

		test("quotes identifiers so injection attempts are inert", async () => {
			const db = new DatabaseSync(":memory:");
			const table = 'x" (a) VALUES (1); DROP TABLE victim; --';
			const column = 'c"; DROP TABLE victim; --';
			db.exec("CREATE TABLE victim (v)");
			db.exec(
				`CREATE TABLE "${table.replaceAll('"', '""')}" ("${column.replaceAll('"', '""')}")`,
			);
			await pipeline([
				createReadableStream([{ [column]: 1 }]),
				sqliteInsertStream({ db, table }),
			]);
			deepStrictEqual(selectAll(db, table.replaceAll('"', '""')), [
				{ [column]: 1 },
			]);
			deepStrictEqual(selectAll(db, "victim"), []);
		});

		test("rolls back every row when a write fails mid-stream", async () => {
			const db = makeDb();
			await rejects(
				pipeline([
					createReadableStream([
						{ id: 1, name: "a" },
						{ id: 2, name: "b" },
						// A nested object cannot be bound to a parameter.
						{ id: 3, name: {} },
					]),
					sqliteInsertStream({ db, table: "t" }),
				]),
				{ code: "ERR_INVALID_ARG_TYPE" },
			);
			deepStrictEqual(selectAll(db), []);
			strictEqual(db.isTransaction, false);
		});

		test("never binds a first-column object as named params", async () => {
			const db = makeDb();
			// node:sqlite reads a leading plain object as named parameters, which
			// would shift "b" into the id column instead of failing.
			await rejects(
				pipeline([
					createReadableStream([{ id: {}, name: "b" }]),
					sqliteInsertStream({ db, table: "t" }),
				]),
				{
					code: "ERR_INVALID_ARG_TYPE",
					message: "Provided value cannot be bound to SQLite parameter 1.",
				},
			);
			deepStrictEqual(selectAll(db), []);
		});

		test("rolls back when the upstream errors", async () => {
			const db = makeDb();
			const source = createReadableStream();
			const insert = sqliteInsertStream({ db, table: "t" });
			const done = pipeline([source, insert]);
			source.push({ id: 1, name: "a" });
			await new Promise((resolve) => setImmediate(resolve));
			strictEqual(db.isTransaction, true);
			source.destroy(new Error("upstream"));
			await rejects(done, { message: "upstream" });
			deepStrictEqual(selectAll(db), []);
			strictEqual(db.isTransaction, false);
		});

		test("rolls back when destroyed without an error", async () => {
			const db = makeDb();
			const insert = sqliteInsertStream({ db, table: "t" });
			insert.write({ id: 1, name: "a" });
			strictEqual(db.isTransaction, true);
			insert.destroy();
			await new Promise((resolve) => insert.once("close", resolve));
			deepStrictEqual(selectAll(db), []);
			strictEqual(db.isTransaction, false);
		});

		test("rolls back on streamOptions.signal abort", async () => {
			const db = makeDb();
			const controller = new AbortController();
			const insert = sqliteInsertStream(
				{ db, table: "t" },
				{ signal: controller.signal },
			);
			insert.on("error", () => {});
			insert.write({ id: 1, name: "a" });
			strictEqual(db.isTransaction, true);
			controller.abort();
			await new Promise((resolve) => insert.once("close", resolve));
			strictEqual(insert.errored.name, "AbortError");
			deepStrictEqual(selectAll(db), []);
			strictEqual(db.isTransaction, false);
		});

		test("rolls back when COMMIT fails", async () => {
			const db = new DatabaseSync(":memory:");
			db.exec("PRAGMA foreign_keys = ON");
			db.exec("CREATE TABLE p (id INTEGER PRIMARY KEY)");
			db.exec(
				"CREATE TABLE c (pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
			);
			await rejects(
				pipeline([
					createReadableStream([{ pid: 1 }]),
					sqliteInsertStream({ db, table: "c" }),
				]),
				{ code: "ERR_SQLITE_ERROR", message: "FOREIGN KEY constraint failed" },
			);
			deepStrictEqual(selectAll(db, "c"), []);
			strictEqual(db.isTransaction, false);
		});

		test("swallows a failing ROLLBACK during teardown", async () => {
			const db = makeDb();
			const insert = sqliteInsertStream({ db, table: "t" });
			insert.write({ id: 1, name: "a" });
			// The caller closed the database first, so ROLLBACK throws.
			db.close();
			insert.destroy();
			await new Promise((resolve) => insert.once("close", resolve));
			strictEqual(db.isOpen, false);
		});

		test("does not roll back a caller transaction begun after commit", async () => {
			const db = makeDb();
			const insert = sqliteInsertStream({ db, table: "t" });
			insert.once("finish", () => {
				db.exec("BEGIN");
				db.exec("INSERT INTO t (id, name) VALUES (9, 'z')");
			});
			insert.end({ id: 1, name: "a" });
			await new Promise((resolve) => insert.once("close", resolve));
			strictEqual(db.isTransaction, true);
			db.exec("COMMIT");
			deepStrictEqual(selectAll(db), [
				{ id: 1, name: "a" },
				{ id: 9, name: "z" },
			]);
		});

		test("leaves a caller's open transaction alone when BEGIN fails", async () => {
			const db = makeDb();
			db.exec("BEGIN");
			db.exec("INSERT INTO t (id, name) VALUES (9, 'z')");
			await rejects(
				pipeline([
					createReadableStream([{ id: 1, name: "a" }]),
					sqliteInsertStream({ db, table: "t" }),
				]),
				{
					name: "Error",
					message:
						"sqlite: connection already has an open transaction; SQLite allows one writer per database file, so run insert streams on it one after another",
				},
			);
			strictEqual(db.isTransaction, true);
			db.exec("COMMIT");
			deepStrictEqual(selectAll(db), [{ id: 9, name: "z" }]);
		});

		test("a second insert stream on one connection errors early", async () => {
			const db = makeDb();
			const first = sqliteInsertStream({ db, table: "t" });
			first.write({ id: 1, name: "a" });
			strictEqual(db.isTransaction, true);
			await rejects(
				pipeline([
					createReadableStream([{ id: 2, name: "b" }]),
					sqliteInsertStream({ db, table: "t" }),
				]),
				{
					name: "Error",
					message:
						"sqlite: connection already has an open transaction; SQLite allows one writer per database file, so run insert streams on it one after another",
				},
			);
			// The failed stream never owned the transaction, so it must not roll
			// back the first stream's rows.
			strictEqual(db.isTransaction, true);
			first.end({ id: 3, name: "c" });
			await new Promise((resolve) => first.once("close", resolve));
			deepStrictEqual(selectAll(db), [
				{ id: 1, name: "a" },
				{ id: 3, name: "c" },
			]);
			strictEqual(db.isTransaction, false);
		});

		test("empty stream commits nothing and never touches the table", async () => {
			const db = new DatabaseSync(":memory:");
			await pipeline([
				createReadableStream([]),
				sqliteInsertStream({ db, table: "missing" }),
			]);
			strictEqual(db.isTransaction, false);
		});

		test("prepare errors for a missing table leave no transaction open", async () => {
			const db = new DatabaseSync(":memory:");
			await rejects(
				pipeline([
					createReadableStream([{ id: 1 }]),
					sqliteInsertStream({ db, table: "missing" }),
				]),
				{ code: "ERR_SQLITE_ERROR", message: "no such table: missing" },
			);
			strictEqual(db.isTransaction, false);
		});

		test("passes streamOptions to the writable", () => {
			const db = makeDb();
			const insert = sqliteInsertStream(
				{ db, table: "t" },
				{ highWaterMark: 3 },
			);
			strictEqual(insert.writableHighWaterMark, 3);
			insert.destroy();
		});
	});
});
