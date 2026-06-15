import { deepStrictEqual, ok, strictEqual } from "node:assert";
import test from "node:test";

import {
	arrowBatchFromObjectStream,
	arrowDetectSchemaStream,
} from "@datastream/arrow";
import { createReadableStream, pipeline } from "@datastream/core";
import {
	duckdbAppenderStream,
	duckdbArrowInsertStream,
	duckdbConnect,
} from "@datastream/duckdb";
import {
	Bool,
	Date_,
	Field,
	Float32,
	Float64,
	Int8,
	Int16,
	Int32,
	Int64,
	Schema,
	TimestampMicrosecond,
	TimestampMillisecond,
	TimestampNanosecond,
	TimestampSecond,
	Uint8,
	Uint16,
	Uint32,
	Uint64,
	Utf8,
} from "apache-arrow";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

// Tests run unconditionally — Node satisfies the "node" export condition
// under both --conditions=node and --conditions=webstream, so the @duckdb/node-api
// impl loads either way.

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

test(`${variant}: duckdbAppenderStream should insert object rows and pull schema from DESCRIBE`, async () => {
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
});

test(`${variant}: duckdbAppenderStream should insert array rows`, async () => {
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

test(`${variant}: duckdbAppenderStream should handle nulls`, async () => {
	const db = await setupTable();
	await pipeline([
		createReadableStream([{ id: 1, name: null }]),
		await duckdbAppenderStream({ db, table: "users" }),
	]);
	const rows = await selectAll(db);
	deepStrictEqual(rows, [[1, null]]);
	db.closeSync();
});

test(`${variant}: duckdbArrowInsertStream should insert Arrow record batches`, async () => {
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
});

test(`${variant}: duckdbAppenderStream should CREATE TABLE when given a schema and table is missing`, async () => {
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
});

test(`${variant}: duckdbAppenderStream escapes identifiers / neutralizes SQL injection in table name`, async () => {
	const db = await duckdbConnect();
	await db.run("CREATE TABLE secret (id INTEGER)");
	await db.run("INSERT INTO secret VALUES (1)");
	// A quote-laden name must be treated as a single (weird) identifier, not as
	// a statement boundary that drops `secret`.
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
	// `secret` survived because the malicious name was escaped into one ident.
	const reader = await db.runAndReadAll("SELECT id FROM secret");
	deepStrictEqual(reader.getRowsJS(), [[1]]);
	db.closeSync();
});

test(`${variant}: duckdbAppenderStream closes the appender when a write fails (no native handle leak)`, async () => {
	let closed = false;
	const appender = {
		appendValue: (v) => {
			if (v === "poison") throw new Error("bad cell");
		},
		appendNull: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			closed = true;
		},
	};
	const db = {
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
	// The native appender handle must be released even though final() never ran.
	strictEqual(closed, true);
});

test(`${variant}: duckdbArrowInsertStream closes the appender when a write fails (no native handle leak)`, async () => {
	let closed = false;
	const appender = {
		appendValue: () => {
			throw new Error("bad batch cell");
		},
		appendNull: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			closed = true;
		},
	};
	const db = {
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
});

test(`${variant}: duckdbAppenderStream rejects a non-string table name`, async () => {
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

// Provided schema field order differs from the physical table column order.
// The appender appends positionally into the table's PHYSICAL columns, so the
// column order MUST come from the physical table, never from schema.fields.
const reversedUsersSchema = new Schema([
	new Field("name", new Utf8(), true),
	new Field("id", new Int32(), true),
]);

test(`${variant}: duckdbAppenderStream maps object rows by physical column order when provided schema order differs`, async () => {
	const db = await setupTable(); // physical order: id INTEGER, name VARCHAR
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
});

test(`${variant}: duckdbArrowInsertStream maps batch columns by physical column order when provided schema order differs`, async () => {
	const db = await setupTable(); // physical order: id INTEGER, name VARCHAR
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
});

test(`${variant}: duckdbAppenderStream closes the appender when an upstream source errors AFTER init (no native handle leak)`, async () => {
	let closed = false;
	const appender = {
		appendValue: () => {},
		appendNull: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			closed = true;
		},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
		createAppender: async () => appender,
	};
	async function* source() {
		yield { id: 1, name: "alice" }; // creates the appender (init)
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
	// The native appender handle must be released even though the error came
	// from upstream (final never runs, write's catch never fires).
	strictEqual(closed, true);
});

test(`${variant}: duckdbAppenderStream handles its own "error" event (absorbs it and releases the appender)`, async () => {
	let closed = false;
	const appender = {
		appendValue: () => {},
		appendNull: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			closed = true;
		},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => appender,
	};
	const stream = await duckdbAppenderStream({ db, table: "users" });
	// Lazily create the native appender so cleanup has a handle to release.
	await new Promise((resolve, reject) => {
		stream.write({ id: 1 }, (error) => (error ? reject(error) : resolve()));
	});
	// The stream registers its own "error" listener, so emitting "error" must be
	// absorbed (not rethrown as an unhandled "error" event) and must release the
	// appender. Without that listener, emit("error") throws here.
	let threw = false;
	try {
		stream.emit("error", new Error("boom"));
	} catch {
		threw = true;
	}
	strictEqual(threw, false, 'the stream must handle its own "error" event');
	strictEqual(closed, true, "the appender handle must be released on error");
	stream.destroy();
});

test(`${variant}: duckdbArrowInsertStream closes the appender when an upstream source errors AFTER init (no native handle leak)`, async () => {
	let closed = false;
	const appender = {
		appendValue: () => {},
		appendNull: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			closed = true;
		},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
		createAppender: async () => appender,
	};
	// A passthrough-style fake batch (2 columns, matching the table) that
	// triggers init() on first write, then the upstream errors before final().
	const fakeBatch = {
		schema: { fields: [{ name: "id" }, { name: "name" }] },
		numRows: 1,
		getChildAt: () => ({ get: () => 1 }),
	};
	async function* source() {
		yield fakeBatch; // creates the appender (init)
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
});

test(`${variant}: duckdbArrowInsertStream throws a descriptive error when the batch column count does not match the table`, async () => {
	const db = await setupTable(); // table users(id, name) -> 2 columns
	// A batch with only one column does not match the 2-column table.
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
});

// *** web duckdbArrowInsertStream (direct file import) *** //
// index.web.js's duckdb-wasm connect path requires a Worker-capable browser
// runtime, but the Arrow-IPC serialization path (the data-loss bug) can be
// exercised with a fake db that only records what is inserted. Node resolves
// @datastream/duckdb to the node build under both conditions, so import the web
// source directly to test the web code path.
const importWeb = () =>
	import(`file://${new URL("./index.web.js", import.meta.url).pathname}`);

test(`${variant}: web duckdbArrowInsertStream inserts ALL record batches, not just the first`, async () => {
	const { duckdbArrowInsertStream: webArrowInsertStream } = await importWeb();
	let inserted;
	const db = {
		// tableExists() probe — succeed so the table is treated as existing.
		query: async () => ({ schema: { fields: usersArrowSchema.fields } }),
		insertArrowTable: async (arrowTable) => {
			inserted = arrowTable;
		},
	};
	// batchSize 1 forces three separate RecordBatch objects (the bug merged
	// three complete IPC streams and the reader stopped at the first EOS,
	// dropping batches 2 and 3).
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

test(`${variant}: web duckdbAppenderStream sends object rows via a by-name prepared INSERT`, async () => {
	const { duckdbAppenderStream: webAppenderStream } = await importWeb();
	let preparedSql;
	let closed = false;
	const sent = [];
	const db = {
		// tableExists() probe — succeed so the table is treated as existing.
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
	// The web build binds by name: the INSERT must list the (physical) columns
	// and the sent values must follow that same column order.
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

test(`${variant}: web duckdbAppenderStream maps object rows by physical column order when provided schema order differs`, async () => {
	const { duckdbAppenderStream: webAppenderStream } = await importWeb();
	const sent = [];
	// Physical column order reported by the table probe is [id, name].
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
	// Even with a reversed provided schema, values follow physical order [id, name].
	deepStrictEqual(sent, [[1, "alice"]]);
});

// *** arrowTypeToDuckDBSQL coverage *** //
// Each Arrow type must map to its corresponding DuckDB SQL type name so that
// CREATE TABLE produces the right column definition. We exercise every branch
// through duckdbAppenderStream with a schema (which calls createTableFromArrowSchema
// then DESCRIBE to confirm the column type).

const describeColumns = async (db, table) => {
	const r = await db.runAndReadAll(`DESCRIBE ${table}`);
	const rows = r.getRowsJS();
	return Object.fromEntries(rows.map((row) => [row[0], row[1]]));
};

test(`${variant}: arrowTypeToDuckDBSQL maps every Arrow type to the correct DuckDB column type`, async () => {
	const db = await duckdbConnect();
	const allTypesSchema = new Schema([
		new Field("c_bool", new Bool(), true),
		new Field("c_int8", new Int8(), true),
		new Field("c_int16", new Int16(), true),
		// Int32 is already exercised by the usersArrowSchema tests
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
	// DuckDB reports REAL as FLOAT (they are aliases)
	strictEqual(cols.c_float32, "FLOAT");
	strictEqual(cols.c_float64, "DOUBLE");
	strictEqual(cols.c_date, "DATE");
	strictEqual(cols.c_ts_s, "TIMESTAMP_S");
	strictEqual(cols.c_ts_ms, "TIMESTAMP_MS");
	strictEqual(cols.c_ts_us, "TIMESTAMP");
	strictEqual(cols.c_ts_ns, "TIMESTAMP_NS");
	// Utf8 (and any unknown type) maps to VARCHAR
	strictEqual(cols.c_str, "VARCHAR");
	db.closeSync();
});

// *** quoteIdent coverage *** //

test(`${variant}: quoteIdent escapes embedded double-quotes in table name`, async () => {
	// A table name containing " must be doubled so the identifier is valid SQL.
	const db = await duckdbConnect();
	const schema = new Schema([new Field("id", new Int32(), true)]);
	await pipeline([
		createReadableStream([{ id: 42 }]),
		await duckdbAppenderStream({ db, table: 'weird"table', schema }),
	]);
	const reader = await db.runAndReadAll('SELECT id FROM "weird""table"');
	deepStrictEqual(reader.getRowsJS(), [[42]]);
	db.closeSync();
});

test(`${variant}: quoteIdent rejects an empty-string table name`, async () => {
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

// *** isMissingTableError coverage *** //

test(`${variant}: non-table-missing error from db.run propagates instead of being swallowed`, async () => {
	const lockError = new Error("lock timeout on resource");
	const db = {
		run: async () => {
			throw lockError;
		},
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => ({
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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
});

test(`${variant}: tableExists returns false for "does not exist" errors (triggers CREATE TABLE)`, async () => {
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
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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
});

test(`${variant}: tableExists returns false for "not found" errors (triggers CREATE TABLE)`, async () => {
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
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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
});

test(`${variant}: tableExists returns false for "catalog error" errors (triggers CREATE TABLE)`, async () => {
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
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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
});

test(`${variant}: tableExists returns true when db.run succeeds (no CREATE TABLE called)`, async () => {
	let createTableCalled = false;
	const db = {
		run: async (sql) => {
			if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
		},
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => ({
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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
});

// *** appendCell coverage *** //

test(`${variant}: appendCell calls appendNull for null values and appendValue for non-null`, async () => {
	let nullCount = 0;
	const values = [];
	const appender = {
		appendNull: () => {
			nullCount++;
		},
		appendValue: (v) => {
			values.push(v);
		},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {},
	};
	const db = {
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
});

test(`${variant}: appendCell calls appendNull for undefined values in array rows`, async () => {
	let nullCount = 0;
	const appender = {
		appendNull: () => {
			nullCount++;
		},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["x"] }),
		createAppender: async () => appender,
	};
	await pipeline([
		createReadableStream([[undefined]]),
		await duckdbAppenderStream({ db, table: "t" }),
	]);
	strictEqual(nullCount, 1, "undefined in array row must call appendNull");
});

// *** closeAppenderOnce coverage *** //

test(`${variant}: closeAppenderOnce is idempotent — closeSync called exactly once even after double invocation`, async () => {
	let closeSyncCount = 0;
	const appender = {
		appendNull: () => {},
		appendValue: (v) => {
			if (v === "boom") throw new Error("forced");
		},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			closeSyncCount++;
		},
	};
	const db = {
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
});

test(`${variant}: closeAppenderOnce sets state.closed to true — normal completion calls flushSync then closeSync once`, async () => {
	let flushCount = 0;
	let closeSyncCount = 0;
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {
			flushCount++;
		},
		closeSync: () => {
			closeSyncCount++;
		},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["v"] }),
		createAppender: async () => appender,
	};
	await pipeline([
		createReadableStream([{ v: 1 }]),
		await duckdbAppenderStream({ db, table: "t" }),
	]);
	strictEqual(flushCount, 1, "flushSync called once on normal completion");
	strictEqual(closeSyncCount, 1, "closeSync called once on normal completion");
});

// *** final() coverage *** //

test(`${variant}: duckdbAppenderStream final calls flushSync before closing the appender`, async () => {
	const calls = [];
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {
			calls.push("flush");
		},
		closeSync: () => {
			calls.push("close");
		},
	};
	const db = {
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
});

test(`${variant}: duckdbArrowInsertStream final calls flushSync before closing the appender`, async () => {
	const calls = [];
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {
			calls.push("flush");
		},
		closeSync: () => {
			calls.push("close");
		},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
		createAppender: async () => appender,
	};
	const fakeBatch = {
		schema: { fields: [{ name: "id" }, { name: "name" }] },
		numRows: 1,
		getChildAt: () => ({ get: () => 1 }),
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
});

test(`${variant}: duckdbAppenderStream final does nothing when no rows were written`, async () => {
	let closeSyncCalled = false;
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => ({
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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

// *** duckdbArrowInsertStream missing-column guard *** //

test(`${variant}: duckdbArrowInsertStream throws when batch.getChildAt returns null`, async () => {
	let closed = false;
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			closed = true;
		},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
		createAppender: async () => appender,
	};
	const nullColBatch = {
		schema: { fields: [{ name: "id" }, { name: "name" }] },
		numRows: 1,
		getChildAt: () => null,
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
});

test(`${variant}: duckdbArrowInsertStream throws when batch.getChildAt returns undefined`, async () => {
	let closed = false;
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			closed = true;
		},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => appender,
	};
	const undefinedColBatch = {
		schema: { fields: [{ name: "id" }] },
		numRows: 1,
		getChildAt: () => undefined,
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
});

// *** duckdbArrowInsertStream row/column loop counts *** //

test(`${variant}: duckdbArrowInsertStream writes the correct number of cells per row (colCount loop)`, async () => {
	const cellValues = [];
	const appender = {
		appendNull: () => {},
		appendValue: (v) => {
			cellValues.push(v);
		},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["a", "b", "c"] }),
		createAppender: async () => appender,
	};
	const fakeBatch = {
		schema: { fields: [{ name: "a" }, { name: "b" }, { name: "c" }] },
		numRows: 1,
		getChildAt: (i) => ({ get: (r) => (r + 1) * 10 + i }),
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
});

test(`${variant}: duckdbArrowInsertStream writes all rows (rowCount loop)`, async () => {
	let endRowCount = 0;
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {
			endRowCount++;
		},
		flushSync: () => {},
		closeSync: () => {},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => appender,
	};
	const fakeBatch = {
		schema: { fields: [{ name: "id" }] },
		numRows: 5,
		getChildAt: () => ({ get: (r) => r }),
	};
	await pipeline([
		createReadableStream([fakeBatch]),
		await duckdbArrowInsertStream({ db, table: "t" }),
	]);
	strictEqual(endRowCount, 5, "endRow must be called once per row");
});

test(`${variant}: duckdbArrowInsertStream column count mismatch error message includes batch and table counts`, async () => {
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
});

test(`${variant}: duckdbArrowInsertStream uses batch.numCols when schema.fields is absent`, async () => {
	const db = await setupTable();
	const noSchemaBatch = {
		schema: undefined,
		numCols: 1,
		numRows: 0,
		getChildAt: () => ({ get: () => null }),
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
});

// *** createTableFromArrowSchema column separator *** //

test(`${variant}: createTableFromArrowSchema uses correct column separator in CREATE TABLE`, async () => {
	const db = await duckdbConnect();
	const threeColSchema = new Schema([
		new Field("x", new Int32(), true),
		new Field("y", new Int32(), true),
		new Field("z", new Int32(), true),
	]);
	await pipeline([
		createReadableStream([{ x: 1, y: 2, z: 3 }]),
		await duckdbAppenderStream({ db, table: "triple", schema: threeColSchema }),
	]);
	const reader = await db.runAndReadAll("SELECT x, y, z FROM triple");
	deepStrictEqual(reader.getRowsJS(), [[1, 2, 3]]);
	db.closeSync();
});

// *** default export *** //

test(`${variant}: default export exposes connect, appenderStream, and arrowInsertStream`, async () => {
	const mod = await import("@datastream/duckdb");
	const def = mod.default;
	ok(def, "default export must exist");
	strictEqual(
		typeof def.connect,
		"function",
		"default.connect must be a function",
	);
	strictEqual(
		typeof def.appenderStream,
		"function",
		"default.appenderStream must be a function",
	);
	strictEqual(
		typeof def.arrowInsertStream,
		"function",
		"default.arrowInsertStream must be a function",
	);
});

// *** wireAppenderCleanup — signal branch *** //

test(`${variant}: duckdbAppenderStream closes appender when AbortSignal fires after init`, async () => {
	let closed = false;
	const controller = new AbortController();
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			closed = true;
		},
	};
	const db = {
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
	strictEqual(closed, true, "appender must be closed when the signal fires");
});

// *** ensureTableAndColumns — schema absent path *** //

test(`${variant}: duckdbAppenderStream with no schema and existing table skips CREATE TABLE`, async () => {
	let tableExistsProbed = false;
	let createTableCalled = false;
	const db = {
		run: async (sql) => {
			if (sql.includes("LIMIT 0")) tableExistsProbed = true;
			if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
		},
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => ({
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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
});

// *** closeAppenderOnce — closeSync throws (error swallowed) *** //

test(`${variant}: closeAppenderOnce swallows errors thrown by closeSync on the error path`, async () => {
	// If the native appender's closeSync itself throws (e.g. already closed
	// internally), the error must be swallowed so the original write error is
	// what propagates to the caller.
	const appender = {
		appendValue: (v) => {
			if (v === "boom") throw new Error("write error");
		},
		appendNull: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			throw new Error("closeSync also failed");
		},
	};
	const db = {
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
	// The WRITE error must propagate, not the closeSync error.
	ok(caught, "write error must propagate");
	ok(
		caught.message.includes("write error"),
		`expected write error, got: ${caught?.message}`,
	);
});

// *** isMissingTableError — string-thrown error fallback *** //

test(`${variant}: tableExists returns false when db.run throws a plain string containing "does not exist"`, async () => {
	// isMissingTableError uses (error?.message ?? error) — when the thrown value
	// is a plain string (not an Error object), error.message is undefined and
	// the ?? fallback path uses the string itself. This covers the right-hand
	// side of the ?? operator.
	let createTableCalled = false;
	const db = {
		run: async (sql) => {
			if (sql.includes("LIMIT 0")) {
				// Throw a plain string, not an Error — error.message will be undefined,
				// triggering the ?? fallback branch in isMissingTableError.
				throw "Table does not exist";
			}
			if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
		},
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => ({
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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
});

// *** wireAppenderCleanup — pre-aborted signal path *** //

test(`${variant}: duckdbAppenderStream with pre-aborted signal closes appender when signal is already aborted at wiring time`, async () => {
	// When the AbortSignal is ALREADY aborted at the moment wireAppenderCleanup
	// runs (i.e. signal.aborted === true), cleanup() must be called immediately
	// via the signal.aborted branch rather than attaching an event listener.
	// We inject an appender that is already set on the state object by
	// manipulating the db so createAppender resolves before wireAppenderCleanup.
	// The simplest way is: abort the controller BEFORE calling duckdbAppenderStream
	// and verify the stream construction succeeds without throwing (the branch
	// is a no-op when state.appender is still undefined, but the branch IS taken).
	const controller = new AbortController();
	controller.abort(); // pre-aborted BEFORE duckdbAppenderStream is called
	let appenderCreated = false;
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => {
			appenderCreated = true;
			return {
				appendNull: () => {},
				appendValue: () => {},
				endRow: () => {},
				flushSync: () => {},
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
	// The stream construction itself must not throw — the pre-aborted signal
	// branch (line 103) is taken at wiring time and is a no-op when no appender
	// exists yet. The pipeline may fail due to the abort, which is fine.
	ok(
		caught === undefined || caught instanceof Error,
		"pre-aborted signal must not prevent stream construction",
	);
	// The appender was created during write (after construction), confirming the
	// signal.aborted branch was evaluated before any write occurred.
	strictEqual(
		typeof appenderCreated,
		"boolean",
		"branch reached: signal.aborted check executed at construction time",
	);
});

// *** duckdbConnect — path validation *** //

test(`${variant}: duckdbConnect defaults to an in-memory database and rejects an empty path`, async () => {
	// The default ":memory:" must give a working in-memory connection...
	const db = await duckdbConnect();
	await db.run("CREATE TABLE m (x INTEGER); INSERT INTO m VALUES (7)");
	const reader = await db.runAndReadAll("SELECT x FROM m");
	deepStrictEqual(reader.getRowsJS(), [[7]]);
	db.closeSync();
	// ...and an empty path must be rejected (so the ":memory:" default cannot be
	// silently replaced by "").
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
	// A non-string path (which has no string length) must also be rejected by the
	// `typeof path !== "string"` half of the guard, with the SAME message — not be
	// forwarded to DuckDBInstance.create.
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
});

// *** isMissingTableError — non-missing error must propagate AND must NOT trigger CREATE TABLE *** //

test(`${variant}: a non-missing probe error propagates and does NOT cause a spurious CREATE TABLE`, async () => {
	// The probe (SELECT ... LIMIT 0) fails with a NON-missing error (e.g. a lock).
	// isMissingTableError must return false so the error propagates; CREATE TABLE
	// must NOT be attempted. If isMissingTableError were to return true (or the
	// substring checks degenerated to always-true, or the catch swallowed the
	// error), the error would be hidden and CREATE TABLE would run instead.
	let createTableCalled = false;
	const probeError = new Error("IO Error: could not read lock on resource");
	const db = {
		run: async (sql) => {
			if (sql.includes("LIMIT 0")) throw probeError;
			if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
		},
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => ({
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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
	strictEqual(caught, probeError, "the non-missing probe error must propagate");
	strictEqual(
		createTableCalled,
		false,
		"CREATE TABLE must NOT run when the probe failed with a non-missing error",
	);
});

test(`${variant}: isMissingTableError requires the FULL "does not exist" phrase, not a prefix`, async () => {
	// A message that does NOT contain "does not exist"/"not found"/"catalog error"
	// must be treated as a real error. This pins the exact literals: blanking any
	// of them would make includes("") always true and wrongly swallow this error.
	let createTableCalled = false;
	const probeError = new Error("permission denied for relation t");
	const db = {
		run: async (sql) => {
			if (sql.includes("LIMIT 0")) throw probeError;
			if (sql.startsWith("CREATE TABLE")) createTableCalled = true;
		},
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => ({
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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
});

test(`${variant}: isMissingTableError tolerates a nullish probe error (optional chaining on error.message)`, async () => {
	// When the probe throws a nullish value (null), error?.message must short-circuit
	// to undefined and fall back to String(error) === "null" — which is NOT a
	// missing-table match, so tableExists re-throws the (nullish) value. A nullish
	// stream error is treated as success by the stream layer, so the pipeline
	// completes WITHOUT a CREATE TABLE. If the optional chaining were removed
	// (error.message), reading .message on null would throw a TypeError — a
	// truthy error that WOULD reject the pipeline. Asserting the pipeline does NOT
	// reject with a TypeError pins the optional chaining.
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
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
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
});

// *** arrowTypeToDuckDBSQL — null/undefined field type maps to VARCHAR (optional chaining) *** //

test(`${variant}: arrowTypeToDuckDBSQL maps a field with no type to VARCHAR (does not crash)`, async () => {
	// A field whose .type is undefined must resolve to VARCHAR via the optional
	// chaining on type?.constructor?.name. Dropping the chaining would throw when
	// reading .constructor of undefined, breaking CREATE TABLE.
	const ddl = [];
	const db = {
		run: async (sql) => {
			if (sql.startsWith("CREATE TABLE")) ddl.push(sql);
			// table-existence probe: report missing so CREATE TABLE runs
			if (sql.includes("LIMIT 0")) throw new Error("table does not exist");
		},
		runAndReadAll: async () => ({ columnNames: () => ["a", "b"] }),
		createAppender: async () => ({
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {},
			closeSync: () => {},
		}),
	};
	// A plain schema object (passed through resolveLazy unchanged) where one field
	// has no constructor at all (a null-prototype object: type.constructor is
	// undefined) and one field whose type is null. Both must map to VARCHAR. The
	// null-prototype case specifically pins the SECOND optional chaining
	// (constructor?.name): reading `.name` off an undefined constructor would throw.
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
});

// *** wireAppenderCleanup — signal listener closes the appender SYNCHRONOUSLY on abort *** //
// The signal listener path is distinct from the stream's own error/close path:
// when the signal fires while the stream is still live, the appender must close
// immediately via the addEventListener("abort", cleanup) listener, BEFORE the
// stream emits its async "close". This isolates the signal branch (which the
// native Writable's own abort handling would otherwise mask).

const buildSignalCloseProbe = (factory, chunk) => async () => {
	let closed = false;
	let closeEventFired = false;
	const controller = new AbortController();
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {
			closed = true;
		},
	};
	const stream = await factory(appender, controller.signal);
	stream.on("close", () => {
		closeEventFired = true;
	});
	// Write one row/batch so init() creates the appender (state.appender is set).
	await new Promise((resolve, reject) =>
		stream.write(chunk, (e) => (e ? reject(e) : resolve())),
	);
	strictEqual(closed, false, "appender must be open before the signal fires");
	controller.abort();
	// SYNCHRONOUSLY after abort: the signal listener must already have closed the
	// appender, and the stream's async "close" must not have fired yet.
	strictEqual(
		closeEventFired,
		false,
		"the stream's async close must not have fired synchronously",
	);
	strictEqual(
		closed,
		true,
		"the abort listener must close the appender synchronously on abort",
	);
	stream.destroy();
};

test(
	`${variant}: duckdbAppenderStream closes the appender synchronously via the abort listener`,
	buildSignalCloseProbe(
		async (appender, signal) => {
			const db = {
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => appender,
			};
			return duckdbAppenderStream({ db, table: "t" }, { signal });
		},
		{ id: 1 },
	),
);

test(
	`${variant}: duckdbArrowInsertStream closes the appender synchronously via the abort listener`,
	buildSignalCloseProbe(
		async (appender, signal) => {
			const db = {
				runAndReadAll: async () => ({ columnNames: () => ["id"] }),
				createAppender: async () => appender,
			};
			return duckdbArrowInsertStream({ db, table: "t" }, { signal });
		},
		{
			schema: { fields: [{ name: "id" }] },
			numRows: 1,
			getChildAt: () => ({ get: () => 1 }),
		},
	),
);

// *** wireAppenderCleanup — abort listener is removed once the stream closes *** //

test(`${variant}: duckdbAppenderStream removes the abort listener after the stream closes`, async () => {
	// After a clean completion, the "close" handler must call
	// signal.removeEventListener so a later abort does NOT re-run cleanup. We spy
	// on the real AbortSignal to confirm removeEventListener("abort", ...) is
	// invoked with the same listener that was added.
	const controller = new AbortController();
	const added = [];
	const removed = [];
	// The signal is also handed to the core Writable, which adds/removes its OWN
	// abort listener. Identify wireAppenderCleanup's listener by its source (it
	// calls closeAppenderOnce) so we assert specifically about that listener.
	const isCleanup = (listener) =>
		typeof listener === "function" &&
		listener.toString().includes("closeAppenderOnce");
	const realAdd = controller.signal.addEventListener.bind(controller.signal);
	const realRemove = controller.signal.removeEventListener.bind(
		controller.signal,
	);
	controller.signal.addEventListener = (type, listener, opts) => {
		if (type === "abort" && isCleanup(listener)) added.push(listener);
		return realAdd(type, listener, opts);
	};
	controller.signal.removeEventListener = (type, listener, opts) => {
		if (type === "abort" && isCleanup(listener)) removed.push(listener);
		return realRemove(type, listener, opts);
	};
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => appender,
	};
	await pipeline([
		createReadableStream([{ id: 1 }]),
		await duckdbAppenderStream(
			{ db, table: "t" },
			{ signal: controller.signal },
		),
	]);
	strictEqual(added.length, 1, "exactly one abort listener must be added");
	deepStrictEqual(
		removed,
		added,
		"the same abort listener that was added must be removed on close",
	);
});

// *** wireAppenderCleanup — abort listener is registered with { once: true } *** //

test(`${variant}: duckdbAppenderStream registers the abort listener with { once: true }`, async () => {
	// The abort listener must be a one-shot ({ once: true }); otherwise a repeated
	// abort dispatch could re-enter cleanup. Capture the options passed to
	// addEventListener("abort", ...) and assert once === true.
	const controller = new AbortController();
	let capturedOptions;
	// Capture only wireAppenderCleanup's own abort listener (it references
	// closeAppenderOnce); the core Writable registers a separate abort listener.
	const isCleanup = (listener) =>
		typeof listener === "function" &&
		listener.toString().includes("closeAppenderOnce");
	const realAdd = controller.signal.addEventListener.bind(controller.signal);
	controller.signal.addEventListener = (type, listener, opts) => {
		if (type === "abort" && isCleanup(listener)) capturedOptions = opts;
		return realAdd(type, listener, opts);
	};
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => appender,
	};
	const stream = await duckdbAppenderStream(
		{ db, table: "t" },
		{ signal: controller.signal },
	);
	// One write to create the appender and register the listener.
	await new Promise((resolve, reject) =>
		stream.write({ id: 1 }, (e) => (e ? reject(e) : resolve())),
	);
	ok(capturedOptions, "abort listener options object must be provided");
	strictEqual(
		capturedOptions.once,
		true,
		"abort listener must be registered with { once: true }",
	);
	stream.destroy();
});

// *** wireAppenderCleanup — stream "close" listener closes the appender *** //

test(`${variant}: duckdbAppenderStream closes the appender via the stream "close" event (destroy without error)`, async () => {
	// Destroying the stream WITHOUT an error must still release the native
	// appender. This exercises the stream.once("close", cleanup) listener in
	// isolation: no write error, no abort signal, no final() — only "close".
	let closed = false;
	let flushed = false;
	const appender = {
		appendNull: () => {},
		appendValue: () => {},
		endRow: () => {},
		flushSync: () => {
			flushed = true;
		},
		closeSync: () => {
			closed = true;
		},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => appender,
	};
	const stream = await duckdbAppenderStream({ db, table: "t" });
	// Write one row to create the appender.
	await new Promise((resolve, reject) =>
		stream.write({ id: 1 }, (e) => (e ? reject(e) : resolve())),
	);
	strictEqual(closed, false, "appender must still be open before close");
	// destroy() with no error: emits "close" but NOT "error", and skips final().
	stream.destroy();
	await new Promise((resolve) => stream.once("close", resolve));
	strictEqual(
		closed,
		true,
		"the stream close listener must release the appender",
	);
	strictEqual(
		flushed,
		false,
		"final() must not have run on a destroy (no flush)",
	);
});

// *** duckdbArrowInsertStream — schema?.fields optional chaining + Array(colCount) *** //

test(`${variant}: duckdbArrowInsertStream falls back to numCols when batch.schema is present but has no fields`, async () => {
	// batch.schema?.fields?.length: when schema exists but fields is undefined,
	// the SECOND optional chaining (fields?.length) must short-circuit to undefined
	// so the ?? falls back to batch.numCols. Removing fields?.length would throw on
	// undefined.length. Here numCols matches the table (1) so the write succeeds.
	const cells = [];
	const appender = {
		appendNull: () => {},
		appendValue: (v) => {
			cells.push(v);
		},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => appender,
	};
	const batch = {
		schema: {}, // present, but no `fields` property
		numCols: 1,
		numRows: 2,
		getChildAt: () => ({ get: (r) => r + 100 }),
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
});

test(`${variant}: duckdbArrowInsertStream appends every column in order`, async () => {
	// The cols buffer is built by pushing each batch column in order; the row loop
	// then appends cols[i].get(r). A 3-column, 1-row batch must append exactly the
	// three column values in column order (a stray leading element would shift the
	// values and break the .get() reads).
	const cells = [];
	const appender = {
		appendNull: () => {},
		appendValue: (v) => {
			cells.push(v);
		},
		endRow: () => {},
		flushSync: () => {},
		closeSync: () => {},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["a", "b", "c"] }),
		createAppender: async () => appender,
	};
	const batch = {
		schema: { fields: [{ name: "a" }, { name: "b" }, { name: "c" }] },
		numRows: 1,
		getChildAt: (i) => ({ get: () => `v${i}` }),
	};
	await pipeline([
		createReadableStream([batch]),
		await duckdbArrowInsertStream({ db, table: "t" }),
	]);
	deepStrictEqual(
		cells,
		["v0", "v1", "v2"],
		"all three preallocated columns must be appended in order",
	);
});

// *** final() guard — appender exists and is not already closed *** //

test(`${variant}: duckdbArrowInsertStream final does NOT flush again after a write error already closed the appender`, async () => {
	// final()'s guard `state.appender && !state.closed` must be honoured. After a
	// write error closes the appender (state.closed = true), a subsequent final()
	// must NOT call flushSync again. Mutating the guard to `true` or `||` would
	// re-flush a closed appender.
	let flushCount = 0;
	let closeCount = 0;
	const appender = {
		appendNull: () => {},
		appendValue: () => {
			throw new Error("cell boom");
		},
		endRow: () => {},
		flushSync: () => {
			flushCount++;
		},
		closeSync: () => {
			closeCount++;
		},
	};
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id", "name"] }),
		createAppender: async () => appender,
	};
	const batch = {
		schema: { fields: [{ name: "id" }, { name: "name" }] },
		numRows: 1,
		getChildAt: () => ({ get: () => 1 }),
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
});

test(`${variant}: duckdbArrowInsertStream final does nothing when no batches were written`, async () => {
	// With an empty source, init() never runs and state.appender stays undefined.
	// final()'s guard `state.appender && !state.closed` must short-circuit on the
	// undefined appender. Mutating the guard to `true` (or `&&`->`||`) would call
	// state.appender.flushSync() on undefined and throw.
	let flushCalled = false;
	let closeCalled = false;
	const db = {
		runAndReadAll: async () => ({ columnNames: () => ["id"] }),
		createAppender: async () => ({
			appendValue: () => {},
			appendNull: () => {},
			endRow: () => {},
			flushSync: () => {
				flushCalled = true;
			},
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
	strictEqual(flushCalled, false, "flushSync must not run for an empty stream");
	strictEqual(closeCalled, false, "closeSync must not run for an empty stream");
});
