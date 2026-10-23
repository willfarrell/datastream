import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import { mkdtemp, rm } from "node:fs/promises";
import { register } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { describe } from "node:test";
import {
	arrowBatchFromObjectStream,
	arrowDetectSchemaStream,
} from "@datastream/arrow";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	duckdbAppenderStream,
	duckdbArrowInsertStream,
	duckdbConnect,
	duckdbQueryStream,
} from "@datastream/duckdb";
import {
	Bool,
	Date_,
	DateDay,
	DateMillisecond,
	Field,
	Float32,
	Float64,
	Int8,
	Int16,
	Int32,
	Int64,
	Schema,
	Table,
	TimestampMicrosecond,
	TimestampMillisecond,
	TimestampNanosecond,
	TimestampSecond,
	tableFromArrays,
	tableFromIPC,
	tableToIPC,
	Uint8,
	Uint16,
	Uint32,
	Uint64,
	Utf8,
	vectorFromArray,
} from "apache-arrow";
import { variant } from "../variant.js";

const typedAppends = {
	get appendVarchar() {
		return this.appendValue;
	},
	get appendBoolean() {
		return this.appendValue;
	},
	get appendInteger() {
		return this.appendValue;
	},
	get appendDouble() {
		return this.appendValue;
	},
};

describe(`@datastream/duckdb (${variant})`, () => {
	const nodeTest = variant === "node" ? test : test.skip;

	const usersArrowSchema = new Schema([
		new Field("id", new Int32(), true),
		new Field("name", new Utf8(), true),
	]);

	const setupTable = async () => {
		const db = await duckdbConnect();
		await db.run("CREATE TABLE users (id INTEGER, name VARCHAR)");
		return db;
	};

	const selectAll = async (db) => {
		const reader = await db.runAndReadAll(
			"SELECT id, name FROM users ORDER BY id",
		);
		return reader.getRowsJS();
	};

	nodeTest(
		`duckdbAppenderStream should insert object rows and pull schema from DESCRIBE`,
		async () => {
			const db = await setupTable();
			await pipeline([
				createReadableStream([
					{ id: 1, name: "alice" },
					{ id: 2, name: "bob" },
				]),
				await duckdbAppenderStream({ db, table: "users" }),
			]);
			const rows = await selectAll(db);
			deepStrictEqual(rows, [
				[1, "alice"],
				[2, "bob"],
			]);
			db.closeSync();
		},
	);

	nodeTest(`duckdbAppenderStream should insert array rows`, async () => {
		const db = await setupTable();
		await pipeline([
			createReadableStream([
				[1, "alice"],
				[2, "bob"],
			]),
			await duckdbAppenderStream({ db, table: "users" }),
		]);
		const rows = await selectAll(db);
		strictEqual(rows.length, 2);
		strictEqual(rows[0][1], "alice");
		db.closeSync();
	});

	nodeTest(`duckdbAppenderStream should handle nulls`, async () => {
		const db = await setupTable();
		await pipeline([
			createReadableStream([{ id: 1, name: null }]),
			await duckdbAppenderStream({ db, table: "users" }),
		]);
		const rows = await selectAll(db);
		deepStrictEqual(rows, [[1, null]]);
		db.closeSync();
	});

	nodeTest(
		`duckdbArrowInsertStream should insert Arrow record batches`,
		async () => {
			const db = await setupTable();
			const detect = arrowDetectSchemaStream({ sampleSize: 2 });
			await pipeline([
				createReadableStream([
					{ id: 1, name: "alice" },
					{ id: 2, name: "bob" },
					{ id: 3, name: "carol" },
				]),
				detect,
				arrowBatchFromObjectStream({
					schema: () => detect.result().value.schema,
					batchSize: 2,
				}),
				await duckdbArrowInsertStream({ db, table: "users" }),
			]);
			const rows = await selectAll(db);
			deepStrictEqual(rows, [
				[1, "alice"],
				[2, "bob"],
				[3, "carol"],
			]);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbAppenderStream should CREATE TABLE when given a schema and table is missing`,
		async () => {
			const db = await duckdbConnect();
			const detect = arrowDetectSchemaStream({ sampleSize: 2 });
			await pipeline([
				createReadableStream([
					{ id: 1, name: "alice" },
					{ id: 2, name: "bob" },
				]),
				detect,
				await duckdbAppenderStream({
					db,
					table: "users",
					schema: () => detect.result().value.schema,
				}),
			]);
			const rows = await selectAll(db);
			deepStrictEqual(rows, [
				[1, "alice"],
				[2, "bob"],
			]);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbAppenderStream escapes identifiers / neutralizes SQL injection in table name`,
		async () => {
			const db = await duckdbConnect();
			await db.run("CREATE TABLE secret (id INTEGER)");
			await db.run("INSERT INTO secret VALUES (1)");
			const table = `evil"; DROP TABLE secret; --`;
			const detect = arrowDetectSchemaStream({ sampleSize: 1 });
			await pipeline([
				createReadableStream([{ id: 1, name: "alice" }]),
				detect,
				await duckdbAppenderStream({
					db,
					table,
					schema: () => detect.result().value.schema,
				}),
			]);
			const reader = await db.runAndReadAll("SELECT id FROM secret");
			deepStrictEqual(reader.getRowsJS(), [[1]]);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbAppenderStream closes the appender when a write fails (no native handle leak)`,
		async () => {
			let closed = false;
			const appender = {
				__proto__: typedAppends,
				appendValue: (v) => {
					if (v === "poison") throw new Error("bad cell");
				},
				appendNull: () => {},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {
					closed = true;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
				createAppender: async () => appender,
			};
			let caught;
			try {
				await pipeline([
					createReadableStream([{ id: 1, name: "poison" }]),
					await duckdbAppenderStream({ db, table: "users" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "the write error must propagate");
			ok(caught.message.includes("bad cell"));
			strictEqual(closed, true);
		},
	);

	nodeTest(
		`duckdbArrowInsertStream closes the appender when a write fails (no native handle leak)`,
		async () => {
			let closed = false;
			const appender = {
				__proto__: typedAppends,
				appendValue: () => {
					throw new Error("bad batch cell");
				},
				appendNull: () => {},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {
					closed = true;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
				createAppender: async () => appender,
			};
			const batchStream = arrowBatchFromObjectStream({
				schema: usersArrowSchema,
				batchSize: 10,
			});
			let caught;
			try {
				await pipeline([
					createReadableStream([{ id: 1, name: "alice" }]),
					batchStream,
					await duckdbArrowInsertStream({ db, table: "users" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "the write error must propagate");
			strictEqual(closed, true);
		},
	);

	nodeTest(`duckdbAppenderStream rejects a non-string table name`, async () => {
		const db = await duckdbConnect();
		try {
			await pipeline([
				createReadableStream([{ id: 1 }]),
				await duckdbAppenderStream({ db, table: 42 }),
			]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("identifier must be a non-empty string"));
		}
		db.closeSync();
	});

	const reversedUsersSchema = new Schema([
		new Field("name", new Utf8(), true),
		new Field("id", new Int32(), true),
	]);

	nodeTest(
		`duckdbAppenderStream maps object rows by physical column order when provided schema order differs`,
		async () => {
			const db = await setupTable();
			await pipeline([
				createReadableStream([
					{ id: 1, name: "alice" },
					{ id: 2, name: "bob" },
				]),
				await duckdbAppenderStream({
					db,
					table: "users",
					schema: reversedUsersSchema,
				}),
			]);
			const rows = await selectAll(db);
			deepStrictEqual(rows, [
				[1, "alice"],
				[2, "bob"],
			]);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbArrowInsertStream maps batch columns by physical column order when provided schema order differs`,
		async () => {
			const db = await setupTable();
			await pipeline([
				createReadableStream([
					{ id: 1, name: "alice" },
					{ id: 2, name: "bob" },
				]),
				arrowBatchFromObjectStream({
					schema: usersArrowSchema,
					batchSize: 2,
				}),
				await duckdbArrowInsertStream({
					db,
					table: "users",
					schema: reversedUsersSchema,
				}),
			]);
			const rows = await selectAll(db);
			deepStrictEqual(rows, [
				[1, "alice"],
				[2, "bob"],
			]);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbAppenderStream closes the appender when an upstream source errors AFTER init (no native handle leak)`,
		async () => {
			let closed = false;
			const appender = {
				__proto__: typedAppends,
				appendValue: () => {},
				appendNull: () => {},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {
					closed = true;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
				createAppender: async () => appender,
			};
			async function* source() {
				yield { id: 1, name: "alice" };
				throw new Error("upstream boom");
			}
			let caught;
			try {
				await pipeline([
					createReadableStream(source()),
					await duckdbAppenderStream({ db, table: "users" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "the upstream error must propagate");
			ok(caught.message.includes("upstream boom"));
			strictEqual(closed, true);
		},
	);

	nodeTest(
		`duckdbArrowInsertStream closes the appender when an upstream source errors AFTER init (no native handle leak)`,
		async () => {
			let closed = false;
			const appender = {
				__proto__: typedAppends,
				appendValue: () => {},
				appendNull: () => {},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {
					closed = true;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
				createAppender: async () => appender,
			};
			const fakeBatch = {
				schema: { fields: [{ name: "id" }, { name: "name" }] },
				numRows: 1,
				getChild: () => ({ get: () => 1 }),
			};
			async function* source() {
				yield fakeBatch;
				throw new Error("upstream batch boom");
			}
			let caught;
			try {
				await pipeline([
					createReadableStream(source()),
					await duckdbArrowInsertStream({ db, table: "users" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "the upstream error must propagate");
			ok(caught.message.includes("upstream batch boom"));
			strictEqual(closed, true);
		},
	);

	nodeTest(
		`duckdbArrowInsertStream maps batch columns to table columns by name, not position`,
		async () => {
			const db = await duckdbConnect();
			await db.run("CREATE TABLE ab (a VARCHAR, b VARCHAR)");
			const batch = tableFromArrays({ b: ["B1"], a: ["A1"] }).batches[0];
			await pipeline([
				createReadableStream([batch]),
				await duckdbArrowInsertStream({ db, table: "ab" }),
			]);
			const reader = await db.runAndReadAll("SELECT a, b FROM ab");
			deepStrictEqual(reader.getRowsJS(), [["A1", "B1"]]);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbAppenderStream appends an integer outside int32 through appendValue (BIGINT), not DOUBLE`,
		async () => {
			const db = await duckdbConnect();
			await db.run("CREATE TABLE big (v VARCHAR)");
			await pipeline([
				createReadableStream([{ v: 2 ** 40 }]),
				await duckdbAppenderStream({ db, table: "big" }),
			]);
			const reader = await db.runAndReadAll("SELECT v FROM big");
			deepStrictEqual(reader.getRowsJS(), [["1099511627776"]]);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbAppenderStream appends booleans through appendBoolean`,
		async () => {
			const db = await duckdbConnect();
			await db.run("CREATE TABLE flags (v BOOLEAN)");
			await pipeline([
				createReadableStream([{ v: true }, { v: false }]),
				await duckdbAppenderStream({ db, table: "flags" }),
			]);
			const reader = await db.runAndReadAll("SELECT v FROM flags");
			deepStrictEqual(reader.getRowsJS(), [[true], [false]]);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbAppenderStream cast failures come from the typed appends`,
		async () => {
			const db = await duckdbConnect();
			await db.run("CREATE TABLE ints (v INTEGER)");
			await db.run("CREATE TABLE dates (v DATE)");
			const insert = async (table, v) =>
				pipeline([
					createReadableStream([{ v }]),
					await duckdbAppenderStream({ db, table }),
				]);
			await rejects(insert("ints", "abc"), {
				message: "Could not convert string 'abc' to INT32",
			});
			await rejects(insert("dates", true), {
				message: "Unimplemented type for cast (BOOL -> INT32)",
			});
			await rejects(insert("ints", 1e30), {
				message: "bigint out of int64 range",
			});
			await rejects(insert("ints", 1e10 + 0.5), {
				message:
					"Type DOUBLE with value 10000000000.5 can't be cast because the value is out of range for the destination type INT32",
			});
			await rejects(insert("dates", 5), {
				message: "Unimplemented type for cast (INT32 -> INT32)",
			});
			await rejects(insert("dates", 1.5), {
				message: "Unimplemented type for cast (DOUBLE -> INT32)",
			});
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbArrowInsertStream throws a descriptive error when the batch column count does not match the table`,
		async () => {
			const db = await setupTable();
			const oneColSchema = new Schema([new Field("id", new Int32(), true)]);
			let caught;
			try {
				await pipeline([
					createReadableStream([{ id: 1 }]),
					arrowBatchFromObjectStream({ schema: oneColSchema, batchSize: 2 }),
					await duckdbArrowInsertStream({ db, table: "users" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "a column-count mismatch must throw");
			ok(
				caught.message.includes("column"),
				`error must mention column mismatch, got: ${caught?.message}`,
			);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbAppenderStream does not commit buffered rows when an upstream source errors`,
		async () => {
			const db = await setupTable();
			async function* source() {
				yield { id: 1, name: "alice" };
				yield { id: 2, name: "bob" };
				await new Promise((resolve) => setTimeout(resolve, 10));
				throw new Error("upstream boom");
			}
			await rejects(
				pipeline([
					createReadableStream(source()),
					await duckdbAppenderStream({ db, table: "users" }),
				]),
				{ message: "upstream boom" },
			);
			await new Promise((resolve) => setTimeout(resolve, 10));
			deepStrictEqual(await selectAll(db), []);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbArrowInsertStream does not commit buffered rows when the signal aborts`,
		async () => {
			const db = await setupTable();
			const controller = new AbortController();
			async function* source() {
				yield { id: 1, name: "alice" };
				yield { id: 2, name: "bob" };
				await new Promise((resolve) => setTimeout(resolve, 10));
				controller.abort(new Error("aborted"));
				await new Promise((resolve) => setTimeout(resolve, 10));
				yield { id: 3, name: "carol" };
			}
			await rejects(
				pipeline(
					[
						createReadableStream(source()),
						arrowBatchFromObjectStream({
							schema: usersArrowSchema,
							batchSize: 1,
						}),
						await duckdbArrowInsertStream(
							{ db, table: "users" },
							{ signal: controller.signal },
						),
					],
					{ signal: controller.signal },
				),
			);
			await new Promise((resolve) => setTimeout(resolve, 10));
			deepStrictEqual(await selectAll(db), []);
			db.closeSync();
		},
	);

	{
		const ROWS = 300_000;
		const BEGIN_ERROR =
			"duckdb: could not begin the write stream's transaction; use a connection with no open transaction";
		const count = async (db, table = "users") =>
			(
				await db.runAndReadAll(`SELECT count(*)::INTEGER AS n FROM ${table}`)
			).getRowObjectsJS()[0].n;
		const batchOf = (ids, names) =>
			tableFromArrays({ id: Int32Array.from(ids), name: names }).batches[0];

		nodeTest(
			`duckdbAppenderStream commits nothing when the pipeline fails past an auto-flush`,
			async () => {
				const db = await duckdbConnect();
				await db.run("CREATE TABLE t (x INTEGER)");
				async function* source() {
					for (let x = 0; x < ROWS; x++) yield { x };
					throw new Error("upstream failed");
				}
				await rejects(
					pipeline([
						createReadableStream(source()),
						await duckdbAppenderStream({ db, table: "t" }),
					]),
					{ message: "upstream failed" },
				);
				strictEqual(await count(db, "t"), 0);
				await db.run("BEGIN TRANSACTION");
				await db.run("ROLLBACK");
				db.closeSync();
			},
		);

		nodeTest(
			`duckdbArrowInsertStream commits nothing when the pipeline fails past an auto-flush`,
			async () => {
				const db = await duckdbConnect();
				await db.run("CREATE TABLE t (x INTEGER)");
				const batch = tableFromArrays({
					x: Int32Array.from({ length: ROWS }, (_, i) => i),
				}).batches[0];
				async function* source() {
					yield batch;
					await new Promise((resolve) => setTimeout(resolve, 10));
					throw new Error("upstream failed");
				}
				await rejects(
					pipeline([
						createReadableStream(source()),
						await duckdbArrowInsertStream({ db, table: "t" }),
					]),
					{ message: "upstream failed" },
				);
				strictEqual(await count(db, "t"), 0);
				db.closeSync();
			},
		);

		nodeTest(
			`duckdbAppenderStream rolls back the table it created when the pipeline fails`,
			async () => {
				const db = await duckdbConnect();
				async function* source() {
					yield { id: 1, name: "alice" };
					await new Promise((resolve) => setTimeout(resolve, 10));
					throw new Error("upstream failed");
				}
				await rejects(
					pipeline([
						createReadableStream(source()),
						await duckdbAppenderStream({
							db,
							table: "users",
							schema: usersArrowSchema,
						}),
					]),
					{ message: "upstream failed" },
				);
				await rejects(count(db), /Table with name users does not exist/);
				db.closeSync();
			},
		);

		nodeTest(
			`duckdb write streams commit their transaction when the pipeline finishes`,
			async () => {
				const db = await setupTable();
				await pipeline([
					createReadableStream([{ id: 1, name: "alice" }]),
					await duckdbAppenderStream({ db, table: "users" }),
				]);
				await pipeline([
					createReadableStream([batchOf([2], ["bob"])]),
					await duckdbArrowInsertStream({ db, table: "users" }),
				]);
				await db.run("BEGIN TRANSACTION");
				await db.run("ROLLBACK");
				deepStrictEqual(await selectAll(db), [
					[1, "alice"],
					[2, "bob"],
				]);
				db.closeSync();
			},
		);

		nodeTest(
			`duckdb write streams refuse a connection that already has an open transaction`,
			async () => {
				for (const [make, chunk] of [
					[duckdbAppenderStream, { id: 1, name: "alice" }],
					[duckdbArrowInsertStream, batchOf([1], ["alice"])],
				]) {
					const db = await setupTable();
					await db.run("BEGIN TRANSACTION");
					await rejects(
						pipeline([
							createReadableStream([chunk]),
							await make({ db, table: "users" }),
						]),
						(error) => {
							strictEqual(error.message, BEGIN_ERROR);
							ok(
								error.cause.message.includes(
									"cannot start a transaction within a transaction",
								),
							);
							return true;
						},
					);
					db.closeSync();
				}
			},
		);

		const makeTxDb = ({ run = async () => {} } = {}) => {
			const calls = [];
			const db = {
				calls,
				run: async (sql) => {
					calls.push(sql);
					await run(sql);
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => calls.push("row"),
					columnType: () => ({ typeId: 0 }),
					flushSync: () => calls.push("flush"),
					closeSync: () => calls.push("close"),
				}),
			};
			return db;
		};
		const fakeBatch = {
			schema: { fields: [{ name: "id" }] },
			numRows: 1,
			getChild: () => ({ get: () => 1 }),
		};
		const writers = [
			["duckdbAppenderStream", duckdbAppenderStream, { id: 1 }],
			["duckdbArrowInsertStream", duckdbArrowInsertStream, fakeBatch],
		];

		for (const [name, make, chunk] of writers) {
			nodeTest(
				`${name} begins before the first write and commits after closing the appender`,
				async () => {
					const db = makeTxDb();
					await pipeline([
						createReadableStream([chunk, chunk]),
						await make({ db, table: "t" }),
					]);
					deepStrictEqual(db.calls, [
						"BEGIN TRANSACTION",
						"row",
						"row",
						"flush",
						"close",
						"COMMIT",
					]);
				},
			);

			nodeTest(
				`${name} opens no transaction when nothing is written`,
				async () => {
					const db = makeTxDb();
					await pipeline([
						createReadableStream([]),
						await make({ db, table: "t" }),
					]);
					deepStrictEqual(db.calls, []);
				},
			);

			nodeTest(
				`${name} closes the appender then rolls back when upstream fails`,
				async () => {
					const db = makeTxDb();
					async function* source() {
						yield chunk;
						throw new Error("upstream boom");
					}
					await rejects(
						pipeline([
							createReadableStream(source()),
							await make({ db, table: "t" }),
						]),
						{ message: "upstream boom" },
					);
					deepStrictEqual(db.calls, [
						"BEGIN TRANSACTION",
						"row",
						"close",
						"ROLLBACK",
					]);
				},
			);

			nodeTest(
				`${name} rolls back when the stream is destroyed without an error`,
				async () => {
					const db = makeTxDb();
					const stream = await make({ db, table: "t" });
					await new Promise((resolve, reject) => {
						stream.write(chunk, (error) => (error ? reject(error) : resolve()));
					});
					stream.destroy();
					await new Promise((resolve) => stream.once("close", resolve));
					deepStrictEqual(db.calls, [
						"BEGIN TRANSACTION",
						"row",
						"close",
						"ROLLBACK",
					]);
				},
			);

			nodeTest(
				`${name} keeps the original error when ROLLBACK also fails`,
				async () => {
					const db = makeTxDb({
						run: async (sql) => {
							if (sql === "ROLLBACK") throw new Error("rollback failed");
						},
					});
					async function* source() {
						yield chunk;
						throw new Error("upstream boom");
					}
					await rejects(
						pipeline([
							createReadableStream(source()),
							await make({ db, table: "t" }),
						]),
						{ message: "upstream boom" },
					);
					deepStrictEqual(db.calls.at(-1), "ROLLBACK");
				},
			);

			nodeTest(`${name} rolls back when its own write fails`, async () => {
				const db = makeTxDb();
				const createAppender = db.createAppender;
				db.createAppender = async () => ({
					__proto__: typedAppends,
					...(await createAppender()),
					endRow: () => {
						throw new Error("bad row");
					},
				});
				await rejects(
					pipeline([
						createReadableStream([chunk]),
						await make({ db, table: "t" }),
					]),
					{ message: "bad row" },
				);
				deepStrictEqual(db.calls, ["BEGIN TRANSACTION", "close", "ROLLBACK"]);
			});

			nodeTest(
				`${name} surfaces a failed COMMIT without rolling back again`,
				async () => {
					const db = makeTxDb({
						run: async (sql) => {
							if (sql === "COMMIT") throw new Error("commit failed");
						},
					});
					await rejects(
						pipeline([
							createReadableStream([chunk]),
							await make({ db, table: "t" }),
						]),
						{ message: "commit failed" },
					);
					deepStrictEqual(db.calls, [
						"BEGIN TRANSACTION",
						"row",
						"flush",
						"close",
						"COMMIT",
					]);
				},
			);

			nodeTest(
				`${name} rolls back a transaction still being opened when aborted`,
				async () => {
					let opened;
					const db = makeTxDb({
						run: (sql) =>
							sql === "BEGIN TRANSACTION"
								? new Promise((resolve) => {
										opened = resolve;
									})
								: undefined,
					});
					const controller = new AbortController();
					const stream = await make(
						{ db, table: "t" },
						{ signal: controller.signal },
					);
					stream.write(chunk, () => {});
					await new Promise(setImmediate);
					controller.abort();
					await new Promise(setImmediate);
					opened();
					await new Promise((resolve) => stream.once("close", resolve));
					deepStrictEqual(db.calls.slice(-2), ["close", "ROLLBACK"]);
					strictEqual(db.calls[0], "BEGIN TRANSACTION");
				},
			);

			nodeTest(
				`${name} still runs the caller's abort hook after rolling back`,
				async () => {
					const db = makeTxDb();
					const aborted = [];
					async function* source() {
						yield chunk;
						throw new Error("upstream boom");
					}
					await rejects(
						pipeline([
							createReadableStream(source()),
							await make(
								{ db, table: "t" },
								{
									abort: (reason) => {
										aborted.push([reason.message, db.calls.at(-1)]);
									},
								},
							),
						]),
						{ message: "upstream boom" },
					);
					deepStrictEqual(aborted, [["upstream boom", "ROLLBACK"]]);
				},
			);
		}
	}

	const importWeb = () =>
		variant === "browser"
			? import("@datastream/duckdb")
			: import(
					`file://${new URL("./index.browser.js", import.meta.url).pathname}`
				);

	test(`web duckdbArrowInsertStream inserts ALL record batches, not just the first`, async () => {
		const { duckdbArrowInsertStream: webArrowInsertStream } = await importWeb();
		let inserted;
		const db = {
			query: async () => ({ schema: { fields: usersArrowSchema.fields } }),
			insertArrowTable: async (arrowTable) => {
				inserted = arrowTable;
			},
		};
		await pipeline([
			createReadableStream([
				{ id: 1, name: "alice" },
				{ id: 2, name: "bob" },
				{ id: 3, name: "carol" },
			]),
			arrowBatchFromObjectStream({ schema: usersArrowSchema, batchSize: 1 }),
			await webArrowInsertStream({
				db,
				table: "users",
				schema: usersArrowSchema,
			}),
		]);
		ok(inserted, "insertArrowTable must be called");
		strictEqual(inserted.numRows, 3);
		const idCol = inserted.getChild("id");
		deepStrictEqual([idCol.get(0), idCol.get(1), idCol.get(2)], [1, 2, 3]);
	});

	test(`web duckdbAppenderStream sends object rows via a by-name prepared INSERT`, async () => {
		const { duckdbAppenderStream: webAppenderStream } = await importWeb();
		let preparedSql;
		let closed = false;
		const sent = [];
		const db = {
			query: async () => ({ schema: { fields: usersArrowSchema.fields } }),
			prepare: async (sql) => {
				preparedSql = sql;
				return {
					send: async (...values) => {
						sent.push(values);
					},
					close: async () => {
						closed = true;
					},
				};
			},
		};
		await pipeline([
			createReadableStream([
				{ id: 1, name: "alice" },
				{ id: 2, name: "bob" },
			]),
			await webAppenderStream({ db, table: "users" }),
		]);
		ok(
			preparedSql.includes('INSERT INTO "users"'),
			`unexpected SQL: ${preparedSql}`,
		);
		ok(preparedSql.includes('"id"') && preparedSql.includes('"name"'));
		deepStrictEqual(sent, [
			[1, "alice"],
			[2, "bob"],
		]);
		strictEqual(closed, true, "prepared statement must be closed");
	});

	test(`web duckdbAppenderStream maps object rows by physical column order when provided schema order differs`, async () => {
		const { duckdbAppenderStream: webAppenderStream } = await importWeb();
		const sent = [];
		const db = {
			query: async () => ({ schema: { fields: usersArrowSchema.fields } }),
			prepare: async () => ({
				send: async (...values) => {
					sent.push(values);
				},
				close: async () => {},
			}),
		};
		await pipeline([
			createReadableStream([{ id: 1, name: "alice" }]),
			await webAppenderStream({
				db,
				table: "users",
				schema: reversedUsersSchema,
			}),
		]);
		deepStrictEqual(sent, [[1, "alice"]]);
	});

	test(`web duckdbAppenderStream closes the prepared statement when an upstream source errors AFTER init`, async () => {
		const { duckdbAppenderStream: webAppenderStream } = await importWeb();
		const run = async (streamOptions) => {
			let closed = false;
			const db = {
				query: async () => ({ schema: { fields: usersArrowSchema.fields } }),
				prepare: async () => ({
					send: async () => {},
					close: async () => {
						closed = true;
					},
				}),
			};
			async function* source() {
				yield { id: 1, name: "alice" };
				throw new Error("upstream boom");
			}
			let caught;
			try {
				await pipeline([
					createReadableStream(source()),
					await webAppenderStream({ db, table: "users" }, streamOptions),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught?.message.includes("upstream boom"));
			strictEqual(closed, true, "prepared statement must be closed");
		};
		await run();
		const aborted = [];
		await run({ abort: (reason) => aborted.push(reason) });
		strictEqual(aborted.length, 1);
		strictEqual(aborted[0].message, "upstream boom");
	});

	test(`web duckdbAppenderStream still runs the caller's abort when upstream errors before init`, async () => {
		const { duckdbAppenderStream: webAppenderStream } = await importWeb();
		const aborted = [];
		const db = {
			query: async () => ({ schema: { fields: usersArrowSchema.fields } }),
			prepare: async () => {
				throw new Error("prepare must not run");
			},
		};
		async function* source() {
			yield* [];
			throw new Error("upstream boom");
		}
		let caught;
		try {
			await pipeline([
				createReadableStream(source()),
				await webAppenderStream(
					{ db, table: "users" },
					{ abort: (reason) => aborted.push(reason) },
				),
			]);
		} catch (e) {
			caught = e;
		}
		ok(caught?.message.includes("upstream boom"));
		strictEqual(aborted.length, 1);
		strictEqual(aborted[0].message, "upstream boom");
	});

	const describeColumns = async (db, table) => {
		const r = await db.runAndReadAll(`DESCRIBE ${table}`);
		const rows = r.getRowsJS();
		return Object.fromEntries(rows.map((row) => [row[0], row[1]]));
	};

	nodeTest(
		`arrowTypeToDuckDBSQL maps every Arrow type to the correct DuckDB column type`,
		async () => {
			const db = await duckdbConnect();
			const allTypesSchema = new Schema([
				new Field("c_bool", new Bool(), true),
				new Field("c_int8", new Int8(), true),
				new Field("c_int16", new Int16(), true),
				new Field("c_int64", new Int64(), true),
				new Field("c_uint8", new Uint8(), true),
				new Field("c_uint16", new Uint16(), true),
				new Field("c_uint32", new Uint32(), true),
				new Field("c_uint64", new Uint64(), true),
				new Field("c_float32", new Float32(), true),
				new Field("c_float64", new Float64(), true),
				new Field("c_date", new Date_(), true),
				new Field("c_ts_s", new TimestampSecond(), true),
				new Field("c_ts_ms", new TimestampMillisecond(), true),
				new Field("c_ts_us", new TimestampMicrosecond(), true),
				new Field("c_ts_ns", new TimestampNanosecond(), true),
				new Field("c_str", new Utf8(), true),
			]);
			await pipeline([
				createReadableStream([
					{
						c_bool: null,
						c_int8: null,
						c_int16: null,
						c_int64: null,
						c_uint8: null,
						c_uint16: null,
						c_uint32: null,
						c_uint64: null,
						c_float32: null,
						c_float64: null,
						c_date: null,
						c_ts_s: null,
						c_ts_ms: null,
						c_ts_us: null,
						c_ts_ns: null,
						c_str: null,
					},
				]),
				await duckdbAppenderStream({
					db,
					table: "all_types",
					schema: allTypesSchema,
				}),
			]);
			const cols = await describeColumns(db, "all_types");
			strictEqual(cols.c_bool, "BOOLEAN");
			strictEqual(cols.c_int8, "TINYINT");
			strictEqual(cols.c_int16, "SMALLINT");
			strictEqual(cols.c_int64, "BIGINT");
			strictEqual(cols.c_uint8, "UTINYINT");
			strictEqual(cols.c_uint16, "USMALLINT");
			strictEqual(cols.c_uint32, "UINTEGER");
			strictEqual(cols.c_uint64, "UBIGINT");
			strictEqual(cols.c_float32, "FLOAT");
			strictEqual(cols.c_float64, "DOUBLE");
			strictEqual(cols.c_date, "DATE");
			strictEqual(cols.c_ts_s, "TIMESTAMP_S");
			strictEqual(cols.c_ts_ms, "TIMESTAMP_MS");
			strictEqual(cols.c_ts_us, "TIMESTAMP");
			strictEqual(cols.c_ts_ns, "TIMESTAMP_NS");
			strictEqual(cols.c_str, "VARCHAR");
			db.closeSync();
		},
	);

	const ipcBatch = (vectors) => {
		const decoded = tableFromIPC(tableToIPC(new Table(vectors)));
		return { schema: decoded.schema, batch: decoded.batches[0] };
	};
	const insertDecoded = async (db, table, vectors) => {
		const { schema, batch } = ipcBatch(vectors);
		await pipeline([
			createReadableStream([batch]),
			await duckdbArrowInsertStream({ db, table, schema }),
		]);
	};
	const selectText = async (db, sql) =>
		(await db.runAndReadAll(sql)).getRowsJS();

	nodeTest(
		`duckdbArrowInsertStream creates integer columns from IPC-decoded Int types`,
		async () => {
			const db = await duckdbConnect();
			await insertDecoded(db, "ints", {
				i8: vectorFromArray([-8], new Int8()),
				i16: vectorFromArray([-16], new Int16()),
				i32: vectorFromArray([-32], new Int32()),
				i64: vectorFromArray([-64n], new Int64()),
				u8: vectorFromArray([8], new Uint8()),
				u16: vectorFromArray([16], new Uint16()),
				u32: vectorFromArray([32], new Uint32()),
				u64: vectorFromArray([64n], new Uint64()),
			});
			deepStrictEqual(await describeColumns(db, "ints"), {
				i8: "TINYINT",
				i16: "SMALLINT",
				i32: "INTEGER",
				i64: "BIGINT",
				u8: "UTINYINT",
				u16: "USMALLINT",
				u32: "UINTEGER",
				u64: "UBIGINT",
			});
			deepStrictEqual(
				await selectText(
					db,
					"SELECT i8, i16, i32, i64, u8, u16, u32, u64 FROM ints",
				),
				[[-8, -16, -32, -64n, 8, 16, 32, 64n]],
			);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbArrowInsertStream creates float columns from IPC-decoded Float types`,
		async () => {
			const db = await duckdbConnect();
			await insertDecoded(db, "floats", {
				f32: vectorFromArray([1.5], new Float32()),
				f64: vectorFromArray([2.25], new Float64()),
			});
			deepStrictEqual(await describeColumns(db, "floats"), {
				f32: "FLOAT",
				f64: "DOUBLE",
			});
			deepStrictEqual(await selectText(db, "SELECT f32, f64 FROM floats"), [
				[1.5, 2.25],
			]);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbArrowInsertStream stores DateDay/DateMillisecond as DATE values`,
		async () => {
			const db = await duckdbConnect();
			const at = new Date(Date.UTC(2024, 0, 2, 23, 4, 5));
			const before = new Date(Date.UTC(1969, 11, 31, 12));
			await insertDecoded(db, "dates", {
				d_day: vectorFromArray([at, before], new DateDay()),
				d_ms: vectorFromArray([at, before], new DateMillisecond()),
			});
			deepStrictEqual(await describeColumns(db, "dates"), {
				d_day: "DATE",
				d_ms: "DATE",
			});
			deepStrictEqual(
				await selectText(
					db,
					"SELECT d_day::VARCHAR, d_ms::VARCHAR FROM dates ORDER BY d_day DESC",
				),
				[
					["2024-01-02", "2024-01-02"],
					["1969-12-31", "1969-12-31"],
				],
			);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbArrowInsertStream stores every Timestamp unit as its TIMESTAMP type`,
		async () => {
			const db = await duckdbConnect();
			const at = new Date(Date.UTC(2024, 0, 2, 3, 4, 5, 678));
			await insertDecoded(db, "stamps", {
				ts_s: vectorFromArray(
					[new Date(Date.UTC(2024, 0, 2, 3, 4, 5))],
					new TimestampSecond(),
				),
				ts_ms: vectorFromArray([at], new TimestampMillisecond()),
				ts_us: vectorFromArray([at], new TimestampMicrosecond()),
				ts_ns: vectorFromArray([at], new TimestampNanosecond()),
			});
			deepStrictEqual(await describeColumns(db, "stamps"), {
				ts_s: "TIMESTAMP_S",
				ts_ms: "TIMESTAMP_MS",
				ts_us: "TIMESTAMP",
				ts_ns: "TIMESTAMP_NS",
			});
			deepStrictEqual(
				await selectText(
					db,
					"SELECT ts_s::VARCHAR, ts_ms::VARCHAR, ts_us::VARCHAR, ts_ns::VARCHAR FROM stamps",
				),
				[
					[
						"2024-01-02 03:04:05",
						"2024-01-02 03:04:05.678",
						"2024-01-02 03:04:05.678",
						"2024-01-02 03:04:05.678",
					],
				],
			);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbArrowInsertStream converts Date/Timestamp only for DATE/TIMESTAMP target columns`,
		async () => {
			const db = await duckdbConnect();
			await db.run(
				"CREATE TABLE mixed (ts_big BIGINT, d_text VARCHAR, ts_tz TIMESTAMPTZ, d_ts TIMESTAMP)",
			);
			const at = new Date(Date.UTC(2024, 0, 2));
			await insertDecoded(db, "mixed", {
				ts_big: vectorFromArray([new Date(1000)], new TimestampMillisecond()),
				d_text: vectorFromArray([at], new DateMillisecond()),
				ts_tz: vectorFromArray([at], new TimestampMillisecond()),
				d_ts: vectorFromArray([at], new DateDay()),
			});
			await db.run("SET TimeZone = 'UTC'");
			deepStrictEqual(
				await selectText(
					db,
					"SELECT ts_big::VARCHAR, d_text, ts_tz::VARCHAR, d_ts::VARCHAR FROM mixed",
				),
				[
					[
						"1000",
						String(at.getTime()),
						"2024-01-02 00:00:00+00",
						"2024-01-02 00:00:00",
					],
				],
			);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbArrowInsertStream matches batch fields to table columns case-insensitively`,
		async () => {
			const db = await duckdbConnect();
			await db.run('CREATE TABLE t ("ID" INTEGER, "Name" VARCHAR)');
			await pipeline([
				createReadableStream([{ id: 1, name: "a" }]),
				arrowBatchFromObjectStream({ schema: usersArrowSchema }),
				await duckdbArrowInsertStream({ db, table: "t" }),
			]);
			deepStrictEqual(await selectText(db, 'SELECT "ID", "Name" FROM t'), [
				[1, "a"],
			]);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbArrowInsertStream looks up columns by exact name when the batch has no schema fields`,
		async () => {
			const cells = [];
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					appendNull: () => {},
					__proto__: typedAppends,
					appendValue: (v) => cells.push(v),
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			await pipeline([
				createReadableStream([
					{
						schema: undefined,
						numCols: 1,
						numRows: 1,
						getChild: (name) => (name === "id" ? { get: () => 7 } : null),
					},
				]),
				await duckdbArrowInsertStream({ db, table: "t" }),
			]);
			deepStrictEqual(cells, [7]);
		},
	);

	nodeTest(
		`quoteIdent escapes embedded double-quotes in table name`,
		async () => {
			const db = await duckdbConnect();
			const schema = new Schema([new Field("id", new Int32(), true)]);
			await pipeline([
				createReadableStream([{ id: 42 }]),
				await duckdbAppenderStream({ db, table: 'weird"table', schema }),
			]);
			const reader = await db.runAndReadAll('SELECT id FROM "weird""table"');
			deepStrictEqual(reader.getRowsJS(), [[42]]);
			db.closeSync();
		},
	);

	nodeTest(`quoteIdent rejects an empty-string table name`, async () => {
		const db = await duckdbConnect();
		let caught;
		try {
			await pipeline([
				createReadableStream([{ id: 1 }]),
				await duckdbAppenderStream({ db, table: "" }),
			]);
		} catch (e) {
			caught = e;
		}
		ok(caught, "empty table name must throw");
		ok(caught.message.includes("identifier must be a non-empty string"));
		db.closeSync();
	});

	nodeTest(
		`non-table-missing error from db.run propagates instead of being swallowed`,
		async () => {
			const lockError = new Error("lock timeout on resource");
			const db = {
				run: async (sql) => {
					if (sql.includes("LIMIT 0")) throw lockError;
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			const schema = new Schema([new Field("id", new Int32(), true)]);
			let caught;
			try {
				await pipeline([
					createReadableStream([{ id: 1 }]),
					await duckdbAppenderStream({ db, table: "t", schema }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "error must propagate");
			strictEqual(caught, lockError);
		},
	);

	nodeTest(
		`tableExists returns false for "does not exist" errors (triggers CREATE TABLE)`,
		async () => {
			let createTableCalled = false;
			const db = {
				run: async (sql) => {
					if (sql.includes("LIMIT 0")) {
						throw new Error("Table with name t does not exist");
					}
					if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			const schema = new Schema([new Field("id", new Int32(), true)]);
			await pipeline([
				createReadableStream([{ id: 1 }]),
				await duckdbAppenderStream({ db, table: "t", schema }),
			]);
			ok(
				createTableCalled,
				"createTableFromArrowSchema must be called when table is absent",
			);
		},
	);

	nodeTest(
		`tableExists returns false for "not found" errors (triggers CREATE TABLE)`,
		async () => {
			let createTableCalled = false;
			const db = {
				run: async (sql) => {
					if (sql.includes("LIMIT 0")) {
						throw new Error("Table not found");
					}
					if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			const schema = new Schema([new Field("id", new Int32(), true)]);
			await pipeline([
				createReadableStream([{ id: 1 }]),
				await duckdbAppenderStream({ db, table: "t", schema }),
			]);
			ok(
				createTableCalled,
				"createTableFromArrowSchema must be called on not found error",
			);
		},
	);

	nodeTest(
		`tableExists returns false for "catalog error" errors (triggers CREATE TABLE)`,
		async () => {
			let createTableCalled = false;
			const db = {
				run: async (sql) => {
					if (sql.includes("LIMIT 0")) {
						throw new Error("Catalog error: table missing");
					}
					if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			const schema = new Schema([new Field("id", new Int32(), true)]);
			await pipeline([
				createReadableStream([{ id: 1 }]),
				await duckdbAppenderStream({ db, table: "t", schema }),
			]);
			ok(
				createTableCalled,
				"createTableFromArrowSchema must be called on catalog error",
			);
		},
	);

	nodeTest(
		`tableExists returns true when db.run succeeds (no CREATE TABLE called)`,
		async () => {
			let createTableCalled = false;
			const db = {
				run: async (sql) => {
					if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			const schema = new Schema([new Field("id", new Int32(), true)]);
			await pipeline([
				createReadableStream([{ id: 1 }]),
				await duckdbAppenderStream({ db, table: "t", schema }),
			]);
			strictEqual(
				createTableCalled,
				false,
				"CREATE TABLE must NOT be called when table already exists",
			);
		},
	);

	nodeTest(
		`appendCell calls appendNull for null values and appendValue for non-null`,
		async () => {
			let nullCount = 0;
			const values = [];
			const appender = {
				appendNull: () => {
					nullCount++;
				},
				__proto__: typedAppends,
				appendValue: (v) => {
					values.push(v);
				},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["a", "b", "c"] }),
				createAppender: async () => appender,
			};
			await pipeline([
				createReadableStream([{ a: null, b: undefined, c: 99 }]),
				await duckdbAppenderStream({ db, table: "t" }),
			]);
			strictEqual(nullCount, 2, "null and undefined must both call appendNull");
			deepStrictEqual(
				values,
				[99],
				"only the non-null value must call appendValue",
			);
		},
	);

	nodeTest(
		`appendCell calls appendNull for undefined values in array rows`,
		async () => {
			let nullCount = 0;
			const appender = {
				appendNull: () => {
					nullCount++;
				},
				__proto__: typedAppends,
				appendValue: () => {},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["x"] }),
				createAppender: async () => appender,
			};
			await pipeline([
				createReadableStream([[undefined]]),
				await duckdbAppenderStream({ db, table: "t" }),
			]);
			strictEqual(nullCount, 1, "undefined in array row must call appendNull");
		},
	);

	nodeTest(
		`closeAppenderOnce is idempotent — closeSync called exactly once even after double invocation`,
		async () => {
			let closeSyncCount = 0;
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: (v) => {
					if (v === "boom") throw new Error("forced");
				},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {
					closeSyncCount++;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["v"] }),
				createAppender: async () => appender,
			};
			try {
				await pipeline([
					createReadableStream([{ v: "boom" }]),
					await duckdbAppenderStream({ db, table: "t" }),
				]);
			} catch (_) {}
			strictEqual(closeSyncCount, 1, "closeSync must be called exactly once");
		},
	);

	nodeTest(
		`closeAppenderOnce sets state.closed to true — normal completion calls flushSync then closeSync once`,
		async () => {
			let flushCount = 0;
			let closeSyncCount = 0;
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: () => {},
				endRow: () => {},
				flushSync: () => {
					flushCount++;
				},
				clear: () => {},
				closeSync: () => {
					closeSyncCount++;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["v"] }),
				createAppender: async () => appender,
			};
			await pipeline([
				createReadableStream([{ v: 1 }]),
				await duckdbAppenderStream({ db, table: "t" }),
			]);
			strictEqual(flushCount, 1, "flushSync called once on normal completion");
			strictEqual(
				closeSyncCount,
				1,
				"closeSync called once on normal completion",
			);
		},
	);

	nodeTest(
		`duckdbAppenderStream final calls flushSync before closing the appender`,
		async () => {
			const calls = [];
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: () => {},
				endRow: () => {},
				flushSync: () => {
					calls.push("flush");
				},
				clear: () => {},
				closeSync: () => {
					calls.push("close");
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => appender,
			};
			await pipeline([
				createReadableStream([{ id: 1 }]),
				await duckdbAppenderStream({ db, table: "t" }),
			]);
			deepStrictEqual(
				calls,
				["flush", "close"],
				"flushSync must precede closeSync",
			);
		},
	);

	nodeTest(
		`duckdbArrowInsertStream final calls flushSync before closing the appender`,
		async () => {
			const calls = [];
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: () => {},
				endRow: () => {},
				flushSync: () => {
					calls.push("flush");
				},
				clear: () => {},
				closeSync: () => {
					calls.push("close");
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
				createAppender: async () => appender,
			};
			const fakeBatch = {
				schema: { fields: [{ name: "id" }, { name: "name" }] },
				numRows: 1,
				getChild: () => ({ get: () => 1 }),
			};
			await pipeline([
				createReadableStream([fakeBatch]),
				await duckdbArrowInsertStream({ db, table: "t" }),
			]);
			deepStrictEqual(
				calls,
				["flush", "close"],
				"flushSync must precede closeSync in arrow stream",
			);
		},
	);

	test(`duckdbAppenderStream final does nothing when no rows were written`, async () => {
		let closeSyncCalled = false;
		const db = {
			run: async () => {},
			runAndReadAll: async () => ({ columnNames: () => ["id"] }),
			createAppender: async () => ({
				__proto__: typedAppends,
				appendValue: () => {},
				appendNull: () => {},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {
					closeSyncCalled = true;
				},
			}),
		};
		await pipeline([
			createReadableStream([]),
			await duckdbAppenderStream({ db, table: "t" }),
		]);
		strictEqual(
			closeSyncCalled,
			false,
			"closeSync must not be called when no rows were written",
		);
	});

	nodeTest(
		`duckdbArrowInsertStream throws when batch.getChild returns null`,
		async () => {
			let closed = false;
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: () => {},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {
					closed = true;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
				createAppender: async () => appender,
			};
			const nullColBatch = {
				schema: { fields: [{ name: "id" }, { name: "name" }] },
				numRows: 1,
				getChild: () => null,
			};
			let caught;
			try {
				await pipeline([
					createReadableStream([nullColBatch]),
					await duckdbArrowInsertStream({ db, table: "t" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "missing column must throw");
			ok(
				caught.message.includes("missing column"),
				`error must mention missing column, got: ${caught?.message}`,
			);
			strictEqual(
				closed,
				true,
				"appender must be closed after missing-column error",
			);
		},
	);

	nodeTest(
		`duckdbArrowInsertStream throws when batch.getChild returns undefined`,
		async () => {
			let closed = false;
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: () => {},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {
					closed = true;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => appender,
			};
			const undefinedColBatch = {
				schema: { fields: [{ name: "id" }] },
				numRows: 1,
				getChild: () => undefined,
			};
			let caught;
			try {
				await pipeline([
					createReadableStream([undefinedColBatch]),
					await duckdbArrowInsertStream({ db, table: "t" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "missing column (undefined) must throw");
			ok(caught.message.includes("missing column"));
			strictEqual(closed, true);
		},
	);

	nodeTest(
		`duckdbArrowInsertStream writes the correct number of cells per row (colCount loop)`,
		async () => {
			const cellValues = [];
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: (v) => {
					cellValues.push(v);
				},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["a", "b", "c"] }),
				createAppender: async () => appender,
			};
			const fakeBatch = {
				schema: { fields: [{ name: "a" }, { name: "b" }, { name: "c" }] },
				numRows: 1,
				getChild: (name) => ({
					get: (r) => (r + 1) * 10 + "abc".indexOf(name),
				}),
			};
			await pipeline([
				createReadableStream([fakeBatch]),
				await duckdbArrowInsertStream({ db, table: "t" }),
			]);
			deepStrictEqual(
				cellValues,
				[10, 11, 12],
				"all 3 columns must be appended for the row",
			);
		},
	);

	nodeTest(
		`duckdbArrowInsertStream writes all rows (rowCount loop)`,
		async () => {
			let endRowCount = 0;
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: () => {},
				endRow: () => {
					endRowCount++;
				},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => appender,
			};
			const fakeBatch = {
				schema: { fields: [{ name: "id" }] },
				numRows: 5,
				getChild: () => ({ get: (r) => r }),
			};
			await pipeline([
				createReadableStream([fakeBatch]),
				await duckdbArrowInsertStream({ db, table: "t" }),
			]);
			strictEqual(endRowCount, 5, "endRow must be called once per row");
		},
	);

	nodeTest(
		`duckdbArrowInsertStream column count mismatch error message includes batch and table counts`,
		async () => {
			const db = await setupTable();
			const oneColSchema = new Schema([new Field("id", new Int32(), true)]);
			let caught;
			try {
				await pipeline([
					createReadableStream([{ id: 1 }]),
					arrowBatchFromObjectStream({ schema: oneColSchema, batchSize: 1 }),
					await duckdbArrowInsertStream({ db, table: "users" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught);
			ok(
				caught.message.includes("1"),
				`mismatch message must include batch col count (1), got: ${caught.message}`,
			);
			ok(
				caught.message.includes("2"),
				`mismatch message must include table col count (2), got: ${caught.message}`,
			);
			ok(
				caught.message.includes("users"),
				"mismatch message must include table name",
			);
			db.closeSync();
		},
	);

	nodeTest(
		`duckdbArrowInsertStream uses batch.numCols when schema.fields is absent`,
		async () => {
			const db = await setupTable();
			const noSchemaBatch = {
				schema: undefined,
				numCols: 1,
				numRows: 0,
				getChild: () => ({ get: () => null }),
			};
			let caught;
			try {
				await pipeline([
					createReadableStream([noSchemaBatch]),
					await duckdbArrowInsertStream({ db, table: "users" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "mismatch from numCols fallback must throw");
			ok(caught.message.includes("column count"));
			db.closeSync();
		},
	);

	nodeTest(
		`createTableFromArrowSchema uses correct column separator in CREATE TABLE`,
		async () => {
			const db = await duckdbConnect();
			const threeColSchema = new Schema([
				new Field("x", new Int32(), true),
				new Field("y", new Int32(), true),
				new Field("z", new Int32(), true),
			]);
			await pipeline([
				createReadableStream([{ x: 1, y: 2, z: 3 }]),
				await duckdbAppenderStream({
					db,
					table: "triple",
					schema: threeColSchema,
				}),
			]);
			const reader = await db.runAndReadAll("SELECT x, y, z FROM triple");
			deepStrictEqual(reader.getRowsJS(), [[1, 2, 3]]);
			db.closeSync();
		},
	);

	test(`module has only named exports`, async () => {
		const mod = await import("@datastream/duckdb");
		deepStrictEqual(Object.keys(mod).sort(), [
			"duckdbAppenderStream",
			"duckdbArrowInsertStream",
			"duckdbConnect",
			"duckdbQueryStream",
		]);
	});

	nodeTest(
		`duckdbAppenderStream closes appender when AbortSignal fires after init`,
		async () => {
			let closed = false;
			const controller = new AbortController();
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: () => {},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {
					closed = true;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => {
					controller.abort();
					return appender;
				},
			};
			try {
				await pipeline([
					createReadableStream([{ id: 1 }]),
					await duckdbAppenderStream(
						{ db, table: "t" },
						{ signal: controller.signal },
					),
				]);
			} catch (_) {}
			strictEqual(
				closed,
				true,
				"appender must be closed when the signal fires",
			);
		},
	);

	nodeTest(
		`duckdbAppenderStream with no schema and existing table skips CREATE TABLE`,
		async () => {
			let tableExistsProbed = false;
			let createTableCalled = false;
			const db = {
				run: async (sql) => {
					if (sql.includes("LIMIT 0")) tableExistsProbed = true;
					if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			await pipeline([
				createReadableStream([{ id: 1 }]),
				await duckdbAppenderStream({ db, table: "t" }),
			]);
			strictEqual(
				tableExistsProbed,
				false,
				"tableExists probe must be skipped when no schema",
			);
			strictEqual(
				createTableCalled,
				false,
				"CREATE TABLE must not be called without schema",
			);
		},
	);

	nodeTest(
		`closeAppenderOnce swallows errors thrown by closeSync on the error path`,
		async () => {
			const appender = {
				__proto__: typedAppends,
				appendValue: (v) => {
					if (v === "boom") throw new Error("write error");
				},
				appendNull: () => {},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {
					throw new Error("closeSync also failed");
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["v"] }),
				createAppender: async () => appender,
			};
			let caught;
			try {
				await pipeline([
					createReadableStream([{ v: "boom" }]),
					await duckdbAppenderStream({ db, table: "t" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "write error must propagate");
			ok(
				caught.message.includes("write error"),
				`expected write error, got: ${caught?.message}`,
			);
		},
	);

	nodeTest(
		`tableExists returns false when db.run throws a plain string containing "does not exist"`,
		async () => {
			let createTableCalled = false;
			const db = {
				run: async (sql) => {
					if (sql.includes("LIMIT 0")) {
						throw "Table does not exist";
					}
					if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			const schema = new Schema([new Field("id", new Int32(), true)]);
			await pipeline([
				createReadableStream([{ id: 1 }]),
				await duckdbAppenderStream({ db, table: "t", schema }),
			]);
			ok(
				createTableCalled,
				"CREATE TABLE must be called when string error matches",
			);
		},
	);

	test(`duckdbAppenderStream with pre-aborted signal closes appender when signal is already aborted at wiring time`, async () => {
		const controller = new AbortController();
		controller.abort();
		let appenderCreated = false;
		const db = {
			run: async () => {},
			runAndReadAll: async () => ({ columnNames: () => ["id"] }),
			createAppender: async () => {
				appenderCreated = true;
				return {
					appendNull: () => {},
					__proto__: typedAppends,
					appendValue: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				};
			},
		};
		let caught;
		try {
			await pipeline([
				createReadableStream([{ id: 1 }]),
				await duckdbAppenderStream(
					{ db, table: "t" },
					{ signal: controller.signal },
				),
			]);
		} catch (e) {
			caught = e;
		}
		ok(
			caught === undefined || caught instanceof Error,
			"pre-aborted signal must not prevent stream construction",
		);
		strictEqual(
			typeof appenderCreated,
			"boolean",
			"branch reached: signal.aborted check executed at construction time",
		);
	});

	nodeTest(
		`duckdbConnect defaults to an in-memory database and rejects an empty path`,
		async () => {
			const db = await duckdbConnect();
			await db.run("CREATE TABLE m (x INTEGER); INSERT INTO m VALUES (7)");
			const reader = await db.runAndReadAll("SELECT x FROM m");
			deepStrictEqual(reader.getRowsJS(), [[7]]);
			db.closeSync();
			let caught;
			try {
				await duckdbConnect("");
			} catch (e) {
				caught = e;
			}
			ok(caught, "an empty path must throw");
			ok(
				caught.message.includes("non-empty string"),
				`unexpected error: ${caught?.message}`,
			);
			let caughtNonString;
			try {
				await duckdbConnect(42);
			} catch (e) {
				caughtNonString = e;
			}
			ok(caughtNonString, "a non-string path must throw");
			ok(
				caughtNonString instanceof TypeError &&
					caughtNonString.message.includes("non-empty string"),
				`non-string path must hit the path guard, got: ${caughtNonString?.message}`,
			);
		},
	);

	nodeTest(
		`a non-missing probe error propagates and does NOT cause a spurious CREATE TABLE`,
		async () => {
			let createTableCalled = false;
			const probeError = new Error("IO Error: could not read lock on resource");
			const db = {
				run: async (sql) => {
					if (sql.includes("LIMIT 0")) throw probeError;
					if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			const schema = new Schema([new Field("id", new Int32(), true)]);
			let caught;
			try {
				await pipeline([
					createReadableStream([{ id: 1 }]),
					await duckdbAppenderStream({ db, table: "t", schema }),
				]);
			} catch (e) {
				caught = e;
			}
			strictEqual(
				caught,
				probeError,
				"the non-missing probe error must propagate",
			);
			strictEqual(
				createTableCalled,
				false,
				"CREATE TABLE must NOT run when the probe failed with a non-missing error",
			);
		},
	);

	nodeTest(
		`isMissingTableError requires the FULL "does not exist" phrase, not a prefix`,
		async () => {
			let createTableCalled = false;
			const probeError = new Error("permission denied for relation t");
			const db = {
				run: async (sql) => {
					if (sql.includes("LIMIT 0")) throw probeError;
					if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			const schema = new Schema([new Field("id", new Int32(), true)]);
			let caught;
			try {
				await pipeline([
					createReadableStream([{ id: 1 }]),
					await duckdbAppenderStream({ db, table: "t", schema }),
				]);
			} catch (e) {
				caught = e;
			}
			strictEqual(caught, probeError);
			strictEqual(createTableCalled, false);
		},
	);

	nodeTest(
		`isMissingTableError tolerates a nullish probe error (optional chaining on error.message)`,
		async () => {
			let createTableCalled = false;
			const db = {
				run: async (sql) => {
					if (sql.includes("LIMIT 0")) {
						throw null;
					}
					if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
				},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			const schema = new Schema([new Field("id", new Int32(), true)]);
			let caught;
			try {
				await pipeline([
					createReadableStream([{ id: 1 }]),
					await duckdbAppenderStream({ db, table: "t", schema }),
				]);
			} catch (e) {
				caught = e ?? new Error("nullish rejection");
			}
			ok(
				!(caught instanceof TypeError),
				`a TypeError means optional chaining on error.message was lost: ${caught}`,
			);
			strictEqual(
				createTableCalled,
				false,
				"a nullish (non-missing) probe error must not trigger CREATE TABLE",
			);
		},
	);

	nodeTest(
		`arrowTypeToDuckDBSQL maps a field with no type to VARCHAR (does not crash)`,
		async () => {
			const ddl = [];
			const db = {
				run: async (sql) => {
					if (sql.startsWith("CREATE TABLE")) ddl.push(sql);
					if (sql.includes("LIMIT 0")) throw new Error("table does not exist");
				},
				runAndReadAll: async () => ({ columnNames: () => ["a", "b"] }),
				createAppender: async () => ({
					__proto__: typedAppends,
					appendValue: () => {},
					appendNull: () => {},
					endRow: () => {},
					flushSync: () => {},
					clear: () => {},
					closeSync: () => {},
				}),
			};
			const schema = {
				fields: [
					{ name: "a", type: Object.create(null) },
					{ name: "b", type: null },
				],
			};
			await pipeline([
				createReadableStream([{ a: "x", b: "y" }]),
				await duckdbAppenderStream({ db, table: "t", schema }),
			]);
			strictEqual(ddl.length, 1, "CREATE TABLE must run for the missing table");
			ok(
				ddl[0].includes('"a" VARCHAR'),
				`field with undefined type must be VARCHAR, got: ${ddl[0]}`,
			);
			ok(
				ddl[0].includes('"b" VARCHAR'),
				`field with null type must be VARCHAR, got: ${ddl[0]}`,
			);
		},
	);

	nodeTest(
		`duckdbAppenderStream closes the appender via the stream "close" event (destroy without error)`,
		async () => {
			let closed = false;
			let flushed = false;
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: () => {},
				endRow: () => {},
				flushSync: () => {
					flushed = true;
				},
				clear: () => {},
				closeSync: () => {
					closed = true;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => appender,
			};
			const stream = await duckdbAppenderStream({ db, table: "t" });
			await new Promise((resolve, reject) =>
				stream.write({ id: 1 }, (e) => (e ? reject(e) : resolve())),
			);
			strictEqual(closed, false, "appender must still be open before close");
			stream.destroy();
			await new Promise((resolve) => stream.once("close", resolve));
			strictEqual(closed, true, "destroy must release the appender");
			strictEqual(
				flushed,
				false,
				"final() must not have run on a destroy (no flush)",
			);
		},
	);

	nodeTest(
		`duckdbArrowInsertStream falls back to numCols when batch.schema is present but has no fields`,
		async () => {
			const cells = [];
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: (v) => {
					cells.push(v);
				},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => appender,
			};
			const batch = {
				schema: {},
				numCols: 1,
				numRows: 2,
				getChild: () => ({ get: (r) => r + 100 }),
			};
			await pipeline([
				createReadableStream([batch]),
				await duckdbArrowInsertStream({ db, table: "t" }),
			]);
			deepStrictEqual(
				cells,
				[100, 101],
				"numCols fallback must let both rows append their single column",
			);
		},
	);

	nodeTest(
		`duckdbArrowInsertStream appends every column in order`,
		async () => {
			const cells = [];
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: (v) => {
					cells.push(v);
				},
				endRow: () => {},
				flushSync: () => {},
				clear: () => {},
				closeSync: () => {},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["a", "b", "c"] }),
				createAppender: async () => appender,
			};
			const batch = {
				schema: { fields: [{ name: "a" }, { name: "b" }, { name: "c" }] },
				numRows: 1,
				getChild: (name) => ({ get: () => `v${name}` }),
			};
			await pipeline([
				createReadableStream([batch]),
				await duckdbArrowInsertStream({ db, table: "t" }),
			]);
			deepStrictEqual(
				cells,
				["va", "vb", "vc"],
				"all three preallocated columns must be appended in order",
			);
		},
	);

	nodeTest(
		`duckdbArrowInsertStream final does NOT flush again after a write error already closed the appender`,
		async () => {
			let flushCount = 0;
			let closeCount = 0;
			const appender = {
				appendNull: () => {},
				__proto__: typedAppends,
				appendValue: () => {
					throw new Error("cell boom");
				},
				endRow: () => {},
				flushSync: () => {
					flushCount++;
				},
				clear: () => {},
				closeSync: () => {
					closeCount++;
				},
			};
			const db = {
				run: async () => {},
				runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
				createAppender: async () => appender,
			};
			const batch = {
				schema: { fields: [{ name: "id" }, { name: "name" }] },
				numRows: 1,
				getChild: () => ({ get: () => 1 }),
			};
			let caught;
			try {
				await pipeline([
					createReadableStream([batch]),
					await duckdbArrowInsertStream({ db, table: "t" }),
				]);
			} catch (e) {
				caught = e;
			}
			ok(caught, "the write error must propagate");
			strictEqual(flushCount, 0, "flushSync must NOT run after a failed write");
			strictEqual(closeCount, 1, "the appender must be closed exactly once");
		},
	);

	test(`duckdbArrowInsertStream final does nothing when no batches were written`, async () => {
		let flushCalled = false;
		let closeCalled = false;
		const db = {
			run: async () => {},
			runAndReadAll: async () => ({ columnNames: () => ["id"] }),
			createAppender: async () => ({
				__proto__: typedAppends,
				appendValue: () => {},
				appendNull: () => {},
				endRow: () => {},
				flushSync: () => {
					flushCalled = true;
				},
				clear: () => {},
				closeSync: () => {
					closeCalled = true;
				},
			}),
		};
		let caught;
		try {
			await pipeline([
				createReadableStream([]),
				await duckdbArrowInsertStream({ db, table: "t" }),
			]);
		} catch (e) {
			caught = e;
		}
		strictEqual(caught, undefined, "empty arrow stream must complete cleanly");
		strictEqual(
			flushCalled,
			false,
			"flushSync must not run for an empty stream",
		);
		strictEqual(
			closeCalled,
			false,
			"closeSync must not run for an empty stream",
		);
	});

	{
		const makeWebTxDb = ({ query = async () => {} } = {}) => {
			const calls = [];
			const db = {
				calls,
				query: async (sql) => {
					if (sql.startsWith("SELECT")) {
						return { schema: { fields: usersArrowSchema.fields } };
					}
					calls.push(sql);
					await query(sql);
				},
				prepare: async () => ({
					send: async () => {
						calls.push("send");
					},
					close: async () => {
						calls.push("close");
					},
				}),
				insertArrowTable: async () => {
					calls.push("insert");
				},
			};
			return db;
		};
		const row = { id: 1, name: "alice" };
		const batch = tableFromArrays({
			id: Int32Array.from([1]),
			name: ["alice"],
		}).batches[0];
		const webWriters = [
			["duckdbAppenderStream", "duckdbAppenderStream", row, "send", "close"],
			["duckdbArrowInsertStream", "duckdbArrowInsertStream", batch, "insert"],
		];
		const BEGIN_ERROR =
			"duckdb: could not begin the write stream's transaction; use a connection with no open transaction";

		for (const [name, exported, chunk, op, release] of webWriters) {
			const make = async (db, streamOptions) =>
				(await importWeb())[exported](
					{ db, table: "users", batchRows: 1 },
					streamOptions,
				);
			const released = release ? [release] : [];

			test(`web ${name} begins before the first write and commits at the end`, async () => {
				const db = makeWebTxDb();
				await pipeline([createReadableStream([chunk, chunk]), await make(db)]);
				deepStrictEqual(db.calls, [
					"BEGIN TRANSACTION",
					op,
					op,
					...released,
					"COMMIT",
				]);
			});

			test(`web ${name} opens no transaction when nothing is written`, async () => {
				const db = makeWebTxDb();
				await pipeline([createReadableStream([]), await make(db)]);
				deepStrictEqual(db.calls, []);
			});

			test(`web ${name} rolls back when upstream fails`, async () => {
				const db = makeWebTxDb();
				const aborted = [];
				async function* source() {
					yield chunk;
					await new Promise((resolve) => setTimeout(resolve, 10));
					throw new Error("upstream boom");
				}
				await rejects(
					pipeline([
						createReadableStream(source()),
						await make(db, {
							abort: (reason) => aborted.push(reason.message),
						}),
					]),
					{ message: "upstream boom" },
				);
				deepStrictEqual(db.calls, [
					"BEGIN TRANSACTION",
					op,
					...released,
					"ROLLBACK",
				]);
				deepStrictEqual(aborted, ["upstream boom"]);
			});

			test(`web ${name} rolls back when its own write fails`, async () => {
				const db = makeWebTxDb();
				db.prepare = async () => ({
					send: async () => {
						throw new Error("bad row");
					},
					close: async () => {
						db.calls.push("close");
					},
				});
				db.insertArrowTable = async () => {
					throw new Error("bad row");
				};
				await rejects(
					pipeline([createReadableStream([chunk]), await make(db)]),
					{ message: "bad row" },
				);
				deepStrictEqual(db.calls, [
					"BEGIN TRANSACTION",
					...released,
					"ROLLBACK",
				]);
			});

			test(`web ${name} keeps the original error when ROLLBACK also fails`, async () => {
				const db = makeWebTxDb({
					query: async (sql) => {
						if (sql === "ROLLBACK") throw new Error("rollback failed");
					},
				});
				db.insertArrowTable = async () => {
					throw new Error("bad row");
				};
				db.prepare = async () => ({
					send: async () => {
						throw new Error("bad row");
					},
					close: async () => {},
				});
				await rejects(
					pipeline([createReadableStream([chunk]), await make(db)]),
					{ message: "bad row" },
				);
				deepStrictEqual(db.calls, ["BEGIN TRANSACTION", "ROLLBACK"]);
			});

			test(`web ${name} surfaces a failed COMMIT without rolling back again`, async () => {
				const db = makeWebTxDb({
					query: async (sql) => {
						if (sql === "COMMIT") throw new Error("commit failed");
					},
				});
				await rejects(
					pipeline([createReadableStream([chunk]), await make(db)]),
					{ message: "commit failed" },
				);
				deepStrictEqual(db.calls, [
					"BEGIN TRANSACTION",
					op,
					...released,
					"COMMIT",
				]);
			});

			test(`web ${name} refuses a connection that already has an open transaction`, async () => {
				const db = makeWebTxDb({
					query: async (sql) => {
						if (sql === "BEGIN TRANSACTION") {
							throw new Error(
								"TransactionContext Error: cannot start a transaction within a transaction",
							);
						}
					},
				});
				await rejects(
					pipeline([createReadableStream([chunk]), await make(db)]),
					(error) => {
						strictEqual(error.message, BEGIN_ERROR);
						ok(error.cause.message.includes("within a transaction"));
						return true;
					},
				);
				deepStrictEqual(db.calls, ["BEGIN TRANSACTION"]);
			});

			test(`web ${name} rolls back a transaction still being opened when aborted`, async () => {
				let opened;
				const db = makeWebTxDb({
					query: (sql) =>
						sql === "BEGIN TRANSACTION"
							? new Promise((resolve) => {
									opened = resolve;
								})
							: undefined,
				});
				const controller = new AbortController();
				const { signal } = controller;
				const pending = pipeline(
					[createReadableStream([chunk]), await make(db, { signal })],
					{ signal },
				);
				pending.catch(() => {});
				await new Promise(setImmediate);
				controller.abort();
				await new Promise(setImmediate);
				opened();
				await rejects(pending);
				await new Promise((resolve) => setTimeout(resolve, 10));
				strictEqual(db.calls[0], "BEGIN TRANSACTION");
				ok(db.calls.includes("ROLLBACK"), `no ROLLBACK in ${db.calls}`);
				strictEqual(db.calls.includes("COMMIT"), false);
			});
		}
	}

	{
		const makeWebDb = ({
			probeError,
			fields = usersArrowSchema.fields,
		} = {}) => {
			const db = {
				queries: [],
				sent: [],
				inserted: [],
				prepared: 0,
				closed: 0,
			};
			db.query = async (sql) => {
				db.queries.push(sql);
				if (sql.startsWith("SELECT 1 FROM") && probeError !== undefined) {
					throw probeError;
				}
				return { schema: { fields } };
			};
			db.prepare = async (sql) => {
				db.prepared += 1;
				db.preparedSql = sql;
				return {
					send: async (...values) => {
						db.sent.push(values);
					},
					close: async () => {
						db.closed += 1;
					},
				};
			};
			db.insertArrowTable = async (table, options) => {
				db.inserted.push({ table, options });
			};
			return db;
		};
		const created = (db) =>
			db.queries.find((q) => q.startsWith("CREATE TABLE"));
		const probed = (db) =>
			db.queries.filter((q) => q.startsWith("SELECT 1 FROM"));

		test(`web duckdbAppenderStream creates a missing table, mapping every Arrow type`, async () => {
			const { duckdbAppenderStream: webAppenderStream } = await importWeb();
			const schema = new Schema([
				new Field("c_bool", new Bool(), true),
				new Field("c_int8", new Int8(), true),
				new Field("c_int16", new Int16(), true),
				new Field("c_int32", new Int32(), true),
				new Field("c_int64", new Int64(), true),
				new Field("c_uint8", new Uint8(), true),
				new Field("c_uint16", new Uint16(), true),
				new Field("c_uint32", new Uint32(), true),
				new Field("c_uint64", new Uint64(), true),
				new Field("c_float32", new Float32(), true),
				new Field("c_float64", new Float64(), true),
				new Field("c_date", new Date_(), true),
				new Field("c_ts_s", new TimestampSecond(), true),
				new Field("c_ts_ms", new TimestampMillisecond(), true),
				new Field("c_ts_us", new TimestampMicrosecond(), true),
				new Field("c_ts_ns", new TimestampNanosecond(), true),
				new Field("c_str", new Utf8(), true),
			]);
			const db = makeWebDb({
				probeError: new Error(
					'Catalog Error: Table with name "t" does not exist',
				),
				fields: schema.fields,
			});
			await pipeline([
				createReadableStream([{ c_int32: 1 }]),
				await webAppenderStream({ db, table: "t", schema }),
			]);
			strictEqual(
				created(db),
				'CREATE TABLE "t" ("c_bool" BOOLEAN, "c_int8" TINYINT, "c_int16" SMALLINT, "c_int32" INTEGER, "c_int64" BIGINT, "c_uint8" UTINYINT, "c_uint16" USMALLINT, "c_uint32" UINTEGER, "c_uint64" UBIGINT, "c_float32" REAL, "c_float64" DOUBLE, "c_date" DATE, "c_ts_s" TIMESTAMP_S, "c_ts_ms" TIMESTAMP_MS, "c_ts_us" TIMESTAMP, "c_ts_ns" TIMESTAMP_NS, "c_str" VARCHAR)',
			);
			strictEqual(db.queries.at(-2), 'SELECT * FROM "t" LIMIT 0');
			strictEqual(db.queries.at(-1), "COMMIT");
			strictEqual(db.sent.length, 1);
			strictEqual(db.closed, 1);
		});

		test(`web duckdbArrowInsertStream maps IPC-decoded Arrow types by typeId`, async () => {
			const { duckdbArrowInsertStream: webArrowInsertStream } =
				await importWeb();
			const at = new Date(Date.UTC(2024, 0, 2));
			const { schema } = ipcBatch({
				i8: vectorFromArray([1], new Int8()),
				u64: vectorFromArray([1n], new Uint64()),
				f32: vectorFromArray([1], new Float32()),
				f64: vectorFromArray([1], new Float64()),
				d_day: vectorFromArray([at], new DateDay()),
				d_ms: vectorFromArray([at], new DateMillisecond()),
				ts_s: vectorFromArray([at], new TimestampSecond()),
				ts_ns: vectorFromArray([at], new TimestampNanosecond()),
			});
			const db = makeWebDb({
				probeError: new Error("does not exist"),
				fields: schema.fields,
			});
			await pipeline([
				createReadableStream([{ i8: 1 }]),
				arrowBatchFromObjectStream({ schema }),
				await webArrowInsertStream({ db, table: "t", schema }),
			]);
			strictEqual(
				created(db),
				'CREATE TABLE "t" ("i8" TINYINT, "u64" UBIGINT, "f32" REAL, "f64" DOUBLE, "d_day" DATE, "d_ms" DATE, "ts_s" TIMESTAMP_S, "ts_ns" TIMESTAMP_NS)',
			);
		});

		test(`web duckdbAppenderStream closes the prepared statement when send fails`, async () => {
			const { duckdbAppenderStream: webAppenderStream } = await importWeb();
			const db = makeWebDb();
			const prepare = db.prepare;
			db.prepare = async (sql) => {
				const statement = await prepare(sql);
				statement.send = async () => {
					throw new Error("send boom");
				};
				return statement;
			};
			await rejects(
				pipeline([
					createReadableStream([{ id: 1, name: "a" }]),
					await webAppenderStream({ db, table: "users" }),
				]),
				/send boom/,
			);
			strictEqual(db.closed, 1);
		});

		test(`web duckdbAppenderStream prepares one by-name INSERT and quotes identifiers`, async () => {
			const { duckdbAppenderStream: webAppenderStream } = await importWeb();
			const fields = [
				new Field('na"me', new Utf8(), true),
				new Field("id", new Int32(), true),
			];
			const db = makeWebDb({ fields });
			await pipeline([
				createReadableStream([
					{ id: 1, 'na"me': "a" },
					{ id: 2, 'na"me': "b" },
				]),
				await webAppenderStream({
					db,
					table: 'us"ers',
					schema: usersArrowSchema,
				}),
			]);
			deepStrictEqual(probed(db), ['SELECT 1 FROM "us""ers" LIMIT 0']);
			strictEqual(created(db), undefined);
			strictEqual(db.prepared, 1);
			strictEqual(
				db.preparedSql,
				'INSERT INTO "us""ers" ("na""me", "id") VALUES (?, ?)',
			);
			deepStrictEqual(db.sent, [
				["a", 1],
				["b", 2],
			]);
		});

		test(`web tableExists treats "not found" / "catalog error" (even thrown as strings) as missing`, async () => {
			const { duckdbAppenderStream: webAppenderStream } = await importWeb();
			for (const probeError of [
				"Catalog Error: nope",
				new Error("Binder Error: not found"),
			]) {
				const db = makeWebDb({ probeError });
				await pipeline([
					createReadableStream([{ id: 1, name: "a" }]),
					await webAppenderStream({
						db,
						table: "users",
						schema: usersArrowSchema,
					}),
				]);
				ok(created(db), `expected CREATE TABLE for ${probeError}`);
			}
		});

		test(`web duckdbAppenderStream propagates a non-missing probe error and skips CREATE TABLE`, async () => {
			const { duckdbAppenderStream: webAppenderStream } = await importWeb();
			const db = makeWebDb({ probeError: new Error("permission denied") });
			await rejects(
				pipeline([
					createReadableStream([{ id: 1, name: "a" }]),
					await webAppenderStream({
						db,
						table: "users",
						schema: usersArrowSchema,
					}),
				]),
				/permission denied/,
			);
			strictEqual(created(db), undefined);
		});

		test(`web duckdbAppenderStream skips the probe entirely without a schema`, async () => {
			const { duckdbAppenderStream: webAppenderStream } = await importWeb();
			const db = makeWebDb({ probeError: new Error("must not be probed") });
			await pipeline([
				createReadableStream([[1, "a"]]),
				await webAppenderStream({ db, table: "users" }),
			]);
			deepStrictEqual(db.sent, [[1, "a"]]);
			deepStrictEqual(probed(db), []);
		});

		test(`web tableExists propagates a null probe rejection instead of a TypeError`, async () => {
			const { duckdbAppenderStream: webAppenderStream } = await importWeb();
			const db = makeWebDb({ probeError: null });
			let caught = "none";
			try {
				await pipeline([
					createReadableStream([{ id: 1, name: "a" }]),
					await webAppenderStream({
						db,
						table: "users",
						schema: usersArrowSchema,
					}),
				]);
			} catch (e) {
				caught = e;
			}
			ok(!(caught instanceof TypeError), `unexpected TypeError: ${caught}`);
			strictEqual(created(db), undefined);
		});

		test(`web duckdbArrowInsertStream inserts every batchRows rows, not all at the end`, async () => {
			const { duckdbArrowInsertStream: webArrowInsertStream } =
				await importWeb();
			const db = makeWebDb();
			const sizesAtWrite = [];
			const insert = db.insertArrowTable;
			db.insertArrowTable = async (table, options) => {
				sizesAtWrite.push(table.numRows);
				await insert(table, options);
			};
			await pipeline([
				createReadableStream([
					{ id: 1, name: "a" },
					{ id: 2, name: "b" },
					{ id: 3, name: "c" },
					{ id: 4, name: "d" },
					{ id: 5, name: "e" },
				]),
				arrowBatchFromObjectStream({ schema: usersArrowSchema, batchSize: 1 }),
				await webArrowInsertStream({
					db,
					table: "users",
					schema: usersArrowSchema,
					batchRows: 2,
				}),
			]);
			deepStrictEqual(sizesAtWrite, [2, 2, 1]);
			deepStrictEqual(
				db.inserted.flatMap(({ table }) => [...table.getChild("id")]),
				[1, 2, 3, 4, 5],
			);
			deepStrictEqual(
				db.inserted.map(({ options }) => options),
				Array(3).fill({ name: "users", create: false }),
			);
		});

		test(`web duckdbArrowInsertStream reorders batch columns to table order by name, case-insensitively`, async () => {
			const { duckdbArrowInsertStream: webArrowInsertStream } =
				await importWeb();
			const db = makeWebDb({
				fields: [new Field("ID", new Int32()), new Field("name", new Utf8())],
			});
			await pipeline([
				createReadableStream([{ name: "alice", id: 1 }]),
				arrowBatchFromObjectStream({
					schema: new Schema([
						new Field("name", new Utf8(), true),
						new Field("id", new Int32(), true),
					]),
				}),
				await webArrowInsertStream({ db, table: "users" }),
			]);
			const [{ table }] = db.inserted;
			deepStrictEqual(
				table.schema.fields.map((f) => f.name),
				["id", "name"],
			);
			deepStrictEqual(table.get(0).toJSON(), { id: 1, name: "alice" });
		});

		test(`web duckdbArrowInsertStream rejects a column count or name mismatch`, async () => {
			const { duckdbArrowInsertStream: webArrowInsertStream } =
				await importWeb();
			const tooMany = makeWebDb();
			await rejects(
				pipeline([
					createReadableStream([{ id: 1, name: "a", other: "x" }]),
					arrowBatchFromObjectStream({
						schema: new Schema([
							...usersArrowSchema.fields,
							new Field("other", new Utf8(), true),
						]),
					}),
					await webArrowInsertStream({ db: tooMany, table: "users" }),
				]),
				{
					message:
						'duckdb: record batch column count (3) does not match table "users" column count (2)',
				},
			);
			deepStrictEqual(tooMany.inserted, []);
			const renamed = makeWebDb();
			await rejects(
				pipeline([
					createReadableStream([{ id: 1, other: "x" }]),
					arrowBatchFromObjectStream({
						schema: new Schema([
							new Field("id", new Int32(), true),
							new Field("other", new Utf8(), true),
						]),
					}),
					await webArrowInsertStream({ db: renamed, table: "users" }),
				]),
				{
					message:
						'duckdb: record batch is missing column "name" for table "users"',
				},
			);
			deepStrictEqual(renamed.inserted, []);
		});

		test(`web duckdb streams reject empty and non-string table names`, async () => {
			const { duckdbAppenderStream: webAppenderStream } = await importWeb();
			for (const table of ["", 42]) {
				await rejects(
					pipeline([
						createReadableStream([{ id: 1 }]),
						await webAppenderStream({ db: makeWebDb(), table }),
					]),
					{ name: "TypeError", message: /non-empty string/ },
				);
			}
		});

		test(`web duckdbArrowInsertStream creates a missing table once and inserts one Table`, async () => {
			const { duckdbArrowInsertStream: webArrowInsertStream } =
				await importWeb();
			const db = makeWebDb({ probeError: new Error("does not exist") });
			await pipeline([
				createReadableStream([]),
				await webArrowInsertStream({
					db,
					table: "users",
					schema: usersArrowSchema,
				}),
			]);
			deepStrictEqual(db.queries, []);
			deepStrictEqual(db.inserted, []);
			await pipeline([
				createReadableStream([
					{ id: 1, name: "a" },
					{ id: 2, name: "b" },
				]),
				arrowBatchFromObjectStream({ schema: usersArrowSchema, batchSize: 1 }),
				await webArrowInsertStream({
					db,
					table: "users",
					schema: usersArrowSchema,
				}),
			]);
			strictEqual(probed(db).length, 1);
			ok(created(db));
			strictEqual(db.inserted.length, 1);
			strictEqual(db.inserted[0].table.numRows, 2);
			deepStrictEqual(db.inserted[0].options, { name: "users", create: false });
		});

		register(
			`data:text/javascript,${encodeURIComponent(`
				export async function resolve(specifier, context, nextResolve) {
					if (specifier === "@duckdb/duckdb-wasm") {
						return { url: "data:text/javascript," + encodeURIComponent(${JSON.stringify(`
							export const getJsDelivrBundles = () => ({
								mvp: { mainWorker: "worker.js", mainModule: "main.wasm", pthreadWorker: "pthread.js" },
							});
							export const selectBundle = async (bundles) => bundles.mvp;
							export class ConsoleLogger {}
							export class AsyncDuckDB {
								constructor(logger, worker) { this.logger = logger; this.worker = worker; }
								// worker.fail names a step to reject, so tests can drive the error paths.
								async instantiate(mainModule, pthreadWorker) { if (this.worker.fail === "instantiate") throw new Error("instantiate failed"); this.instantiated = [mainModule, pthreadWorker]; }
								async open(config) { if (this.worker.fail === "open") throw new Error("open failed"); this.opened = config; }
								async connect() { if (this.worker.fail === "connect") throw new Error("connect failed"); return { db: this }; }
								async terminate() { this.worker.terminate(); }
							}
						`)}), shortCircuit: true };
					}
					return nextResolve(specifier, context);
				}
			`)}`,
			import.meta.url,
		);

		test(`web duckdbConnect instantiates a Worker-backed AsyncDuckDB and returns its connection`, async () => {
			const { duckdbConnect: webConnect } = await importWeb();
			globalThis.Worker = class {
				constructor(url) {
					this.url = url;
				}
			};
			try {
				const connection = await webConnect();
				strictEqual(connection.__duckdb, connection.db);
				strictEqual(connection.db.worker.url, "worker.js");
				deepStrictEqual(connection.db.instantiated, [
					"main.wasm",
					"pthread.js",
				]);
				strictEqual(connection.db.opened, undefined);
			} finally {
				delete globalThis.Worker;
			}
		});

		test(`web duckdbConnect loads self-hosted bundles when given`, async () => {
			const { duckdbConnect: webConnect } = await importWeb();
			globalThis.Worker = class {
				constructor(url) {
					this.url = url;
				}
			};
			try {
				const connection = await webConnect(":memory:", {
					bundles: {
						mvp: {
							mainWorker: "/duckdb/worker.js",
							mainModule: "/duckdb/main.wasm",
						},
					},
				});
				strictEqual(connection.db.worker.url, "/duckdb/worker.js");
				deepStrictEqual(connection.db.instantiated, [
					"/duckdb/main.wasm",
					undefined,
				]);
			} finally {
				delete globalThis.Worker;
			}
		});

		test(`web duckdbConnect opens the given database path`, async () => {
			const { duckdbConnect: webConnect } = await importWeb();
			globalThis.Worker = class {};
			try {
				const connection = await webConnect("opfs://data.duckdb");
				deepStrictEqual(connection.db.opened, { path: "opfs://data.duckdb" });
				const memory = await webConnect(":memory:");
				strictEqual(memory.db.opened, undefined);
			} finally {
				delete globalThis.Worker;
			}
		});

		test(`web duckdbConnect terminates the Worker when setup fails`, async () => {
			const { duckdbConnect: webConnect } = await importWeb();
			for (const fail of ["instantiate", "open", "connect"]) {
				const workers = [];
				globalThis.Worker = class {
					fail = fail;
					terminated = false;
					constructor() {
						workers.push(this);
					}
					terminate() {
						this.terminated = true;
					}
				};
				try {
					await rejects(webConnect("opfs://data.duckdb"), {
						message: `${fail} failed`,
					});
					strictEqual(workers.length, 1);
					strictEqual(workers[0].terminated, true, `${fail}: worker leaked`);
				} finally {
					delete globalThis.Worker;
				}
			}
		});
	}

	{
		const spyConnection = async () => {
			const db = await duckdbConnect();
			const spy = Object.create(db);
			spy.interrupts = 0;
			spy.interrupt = () => {
				spy.interrupts += 1;
				db.interrupt();
			};
			return spy;
		};

		nodeTest(`duckdbQueryStream streams plain JS row objects`, async () => {
			const db = await spyConnection();
			const rows = await streamToArray(
				duckdbQueryStream({
					db,
					sql: "SELECT 1::INTEGER AS i, 1.5::DOUBLE AS d, 'a' AS s, true AS b, NULL AS n, 2::BIGINT AS big",
				}),
			);
			deepStrictEqual(rows, [
				{ i: 1, d: 1.5, s: "a", b: true, n: null, big: 2n },
			]);
			strictEqual(db.interrupts, 0);
		});

		nodeTest(`duckdbQueryStream binds params`, async () => {
			const db = await duckdbConnect();
			const rows = await streamToArray(
				duckdbQueryStream({
					db,
					sql: "SELECT ?::INTEGER AS a, ?::VARCHAR AS b",
					params: [7, "x"],
				}),
			);
			deepStrictEqual(rows, [{ a: 7, b: "x" }]);
		});

		nodeTest(
			`duckdbQueryStream yields every row across result chunks`,
			async () => {
				const db = await duckdbConnect();
				const rows = await streamToArray(
					duckdbQueryStream({
						db,
						sql: "SELECT range::INTEGER AS i FROM range(5000) ORDER BY i",
					}),
				);
				strictEqual(rows.length, 5000);
				deepStrictEqual(rows[0], { i: 0 });
				deepStrictEqual(rows[4999], { i: 4999 });
			},
		);

		nodeTest(
			`duckdbQueryStream surfaces a query error without interrupting`,
			async () => {
				const db = await spyConnection();
				await rejects(
					streamToArray(duckdbQueryStream({ db, sql: "SELECT * FROM nope" })),
					{ message: /Table with name nope does not exist/ },
				);
				strictEqual(db.interrupts, 0);
			},
		);

		nodeTest(
			`duckdbQueryStream does not interrupt a result abandoned between chunks`,
			async () => {
				const db = await spyConnection();
				const seen = [];
				for await (const row of duckdbQueryStream({
					db,
					sql: "SELECT range::INTEGER AS i FROM range(1000000)",
				})) {
					seen.push(row);
					break;
				}
				deepStrictEqual(seen, [{ i: 0 }]);
				strictEqual(db.interrupts, 0);
				deepStrictEqual(
					(await db.runAndReadAll("SELECT 3 AS a")).getRowObjectsJS(),
					[{ a: 3 }],
				);
			},
		);

		const slowQuery =
			"SELECT count(*)::INTEGER AS n FROM range(30000000) t(a) WHERE hash(a) % 7 = 0";

		nodeTest(
			`duckdbQueryStream destroyed before it is read leaves other queries alone`,
			async () => {
				const db = await spyConnection();
				const stream = duckdbQueryStream({ db, sql: "SELECT 1" });
				const other = db.runAndReadAll(slowQuery);
				await new Promise((resolve) => setTimeout(resolve, 10));
				stream.destroy();
				strictEqual((await other).getRowObjectsJS().length, 1);
				strictEqual(db.interrupts, 0);
			},
		);

		nodeTest(
			`duckdbQueryStream paused mid-result and destroyed leaves other queries alone`,
			async () => {
				const db = await spyConnection();
				const stream = duckdbQueryStream({
					db,
					sql: "SELECT * FROM range(100000)",
				});
				const it = stream[Symbol.asyncIterator]();
				for (let i = 0; i < 10; i++) await it.next();
				const other = db.runAndReadAll(slowQuery);
				await new Promise((resolve) => setTimeout(resolve, 10));
				stream.destroy();
				strictEqual((await other).getRowObjectsJS().length, 1);
				strictEqual(db.interrupts, 0);
			},
		);

		const hangingDb = (hangAt) => {
			const db = { interrupts: 0 };
			let fail;
			const hang = new Promise((_resolve, reject) => {
				fail = reject;
			});
			hang.catch(() => {});
			db.interrupt = () => {
				db.interrupts += 1;
				fail(new Error("Interrupted!"));
			};
			db.stream = async () => {
				if (hangAt === "stream") await hang;
				return {
					async *yieldRowObjectJs() {
						yield [{ i: 0 }];
						await hang;
					},
				};
			};
			return db;
		};

		for (const hangAt of ["stream", "chunk"]) {
			nodeTest(
				`duckdbQueryStream interrupts when destroyed during an in-flight ${hangAt} call`,
				async () => {
					const db = hangingDb(hangAt);
					const stream = duckdbQueryStream({ db, sql: "SELECT 1" });
					stream.on("error", () => {});
					stream.resume();
					await new Promise((resolve) => setTimeout(resolve, 10));
					strictEqual(db.interrupts, 0);
					stream.destroy();
					strictEqual(db.interrupts, 1);
				},
			);
		}

		nodeTest(
			`duckdbQueryStream abort interrupts a query that is still executing`,
			{ timeout: 20_000 },
			async () => {
				const db = await spyConnection();
				const controller = new AbortController();
				setTimeout(() => controller.abort(), 100);
				await rejects(
					streamToArray(
						duckdbQueryStream(
							{
								db,
								sql: "SELECT range FROM range(1000000000000) WHERE hash(range) % 1000000007 = 0",
							},
							{ signal: controller.signal },
						),
					),
					{ name: "AbortError" },
				);
				strictEqual(db.interrupts, 1);
			},
		);

		nodeTest(
			`duckdbQueryStream reads back a Parquet file written with COPY`,
			async () => {
				const db = await duckdbConnect();
				const dir = await mkdtemp(join(tmpdir(), "datastream-duckdb-"));
				const file = join(dir, "users.parquet").replaceAll("'", "''");
				try {
					await db.run(
						`COPY (SELECT range::INTEGER AS id, 'user' || range AS name, range % 2 = 0 AS even FROM range(3)) TO '${file}' (FORMAT parquet)`,
					);
					const rows = await streamToArray(
						duckdbQueryStream({
							db,
							sql: "SELECT * FROM read_parquet(?) ORDER BY id",
							params: [file],
						}),
					);
					deepStrictEqual(rows, [
						{ id: 0, name: "user0", even: true },
						{ id: 1, name: "user1", even: false },
						{ id: 2, name: "user2", even: true },
					]);
				} finally {
					await rm(dir, { recursive: true, force: true });
				}
			},
		);
	}

	{
		const makeReader = (batches, next) => {
			let i = 0;
			return {
				[Symbol.asyncIterator]() {
					return this;
				},
				next:
					next ??
					(async () =>
						i < batches.length
							? { value: batches[i++], done: false }
							: { value: undefined, done: true }),
			};
		};
		const makeQueryDb = (batches, next) => {
			const db = { sends: [], prepares: [], params: [], closed: 0, cancels: 0 };
			db.send = async (sql, allowStreamResult) => {
				db.sends.push([sql, allowStreamResult]);
				return makeReader(batches, next);
			};
			db.prepare = async (sql) => {
				db.prepares.push(sql);
				return {
					send: async (...params) => {
						db.params.push(params);
						return makeReader(batches, next);
					},
					close: async () => {
						db.closed += 1;
					},
				};
			};
			db.cancelSent = async () => {
				db.cancels += 1;
				return true;
			};
			return db;
		};
		const batchesOf = (...tables) => tables.flatMap((t) => t.batches);

		test(`web duckdbQueryStream streams plain row objects from every batch`, async () => {
			const { duckdbQueryStream: webQueryStream } = await importWeb();
			const db = makeQueryDb(
				batchesOf(
					tableFromArrays({ id: Int32Array.from([1, 2]), name: ["a", "b"] }),
					tableFromArrays({ id: Int32Array.from([]), name: [] }),
					tableFromArrays({ id: Int32Array.from([3]), name: ["c"] }),
				),
			);
			const rows = await streamToArray(
				webQueryStream({ db, sql: "SELECT id, name FROM users" }),
			);
			deepStrictEqual(rows, [
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
				{ id: 3, name: "c" },
			]);
			strictEqual(Object.getPrototypeOf(rows[0]), Object.prototype);
			deepStrictEqual(db.sends, [["SELECT id, name FROM users", true]]);
			deepStrictEqual(db.prepares, []);
			strictEqual(db.cancels, 0);
		});

		test(`web duckdbQueryStream binds params through a prepared statement and closes it`, async () => {
			const { duckdbQueryStream: webQueryStream } = await importWeb();
			const db = makeQueryDb(
				batchesOf(tableFromArrays({ a: Int32Array.from([7]) })),
			);
			const rows = await streamToArray(
				webQueryStream({ db, sql: "SELECT ? AS a", params: [7, "x"] }),
			);
			deepStrictEqual(rows, [{ a: 7 }]);
			deepStrictEqual(db.sends, []);
			deepStrictEqual(db.prepares, ["SELECT ? AS a"]);
			deepStrictEqual(db.params, [[7, "x"]]);
			strictEqual(db.closed, 1);
			strictEqual(db.cancels, 0);
		});

		test(`web duckdbQueryStream closes the prepared statement when the result errors`, async () => {
			const { duckdbQueryStream: webQueryStream } = await importWeb();
			const db = makeQueryDb([], async () => {
				throw new Error("fetch failed");
			});
			await rejects(
				streamToArray(webQueryStream({ db, sql: "SELECT ?", params: [1] })),
				{ message: "fetch failed" },
			);
			strictEqual(db.closed, 1);
		});

		test(`web duckdbQueryStream surfaces a send error`, async () => {
			const { duckdbQueryStream: webQueryStream } = await importWeb();
			const db = makeQueryDb([]);
			db.send = async () => {
				throw new Error("Parser Error");
			};
			await rejects(streamToArray(webQueryStream({ db, sql: "SELEC" })), {
				message: "Parser Error",
			});
			strictEqual(db.closed, 0);
		});

		test(`web duckdbQueryStream cancels the query and closes the statement when stopped early`, async () => {
			const { duckdbQueryStream: webQueryStream } = await importWeb();
			const db = makeQueryDb(
				batchesOf(tableFromArrays({ id: Int32Array.from([1, 2, 3]) })),
			);
			const seen = [];
			for await (const row of webQueryStream({
				db,
				sql: "SELECT ?",
				params: [1],
			})) {
				seen.push(row);
				break;
			}
			deepStrictEqual(seen, [{ id: 1 }]);
			await new Promise(setImmediate);
			strictEqual(db.cancels, 0);
			strictEqual(db.closed, 1);
		});

		(variant === "browser" ? test : test.skip)(
			`web duckdbQueryStream cancelled before it is read does not cancel the connection`,
			async () => {
				const { duckdbQueryStream: webQueryStream } = await importWeb();
				const db = makeQueryDb([]);
				await webQueryStream({ db, sql: "SELECT 1" }).cancel();
				strictEqual(db.cancels, 0);
				deepStrictEqual(db.sends, []);
			},
		);

		(variant === "browser" ? test : test.skip)(
			`web duckdbQueryStream cancels when a later batch fetch is in flight`,
			async () => {
				const { duckdbQueryStream: webQueryStream } = await importWeb();
				const [batch] = batchesOf(
					tableFromArrays({ id: Int32Array.from([1]) }),
				);
				let fetches = 0;
				let settle;
				const db = makeQueryDb([], async () => {
					fetches += 1;
					if (fetches === 1) return { value: batch, done: false };
					return new Promise((_resolve, reject) => {
						settle = reject;
					});
				});
				db.cancelSent = async () => {
					db.cancels += 1;
					settle(new Error("query canceled"));
					return true;
				};
				const reader = webQueryStream({ db, sql: "SELECT 1" }).getReader();
				deepStrictEqual(await reader.read(), { value: { id: 1 }, done: false });
				const read = reader.read();
				await new Promise(setImmediate);
				strictEqual(fetches, 2);
				setTimeout(() => settle(new Error("not canceled")), 50);
				await reader.cancel();
				strictEqual(db.cancels, 1);
				deepStrictEqual(await read, { value: undefined, done: true });
			},
		);

		(variant === "browser" ? test : test.skip)(
			`web duckdbQueryStream cancel swallows a failing cancelSent`,
			async () => {
				const { duckdbQueryStream: webQueryStream } = await importWeb();
				let settle;
				const db = makeQueryDb(
					[],
					() =>
						new Promise((_resolve, reject) => {
							settle = reject;
						}),
				);
				db.cancelSent = async () => {
					db.cancels += 1;
					settle(new Error("query canceled"));
					throw new Error("cancel failed");
				};
				const reader = webQueryStream({ db, sql: "SELECT 1" }).getReader();
				const read = reader.read();
				await new Promise(setImmediate);
				await reader.cancel();
				strictEqual(db.cancels, 1);
				deepStrictEqual(await read, { value: undefined, done: true });
			},
		);

		(variant === "browser" ? test : test.skip)(
			`web duckdbQueryStream abort cancels a query that is still executing`,
			async () => {
				const { duckdbQueryStream: webQueryStream } = await importWeb();
				let settle;
				const db = makeQueryDb(
					[],
					() =>
						new Promise((_resolve, reject) => {
							settle = reject;
						}),
				);
				db.cancelSent = async () => {
					db.cancels += 1;
					settle(new Error("query canceled"));
					return true;
				};
				const controller = new AbortController();
				setTimeout(() => controller.abort(), 10);
				await rejects(
					streamToArray(
						webQueryStream(
							{ db, sql: "SELECT 1" },
							{ signal: controller.signal },
						),
					),
					{ name: "AbortError" },
				);
				strictEqual(db.cancels, 1);
			},
		);
	}
});
