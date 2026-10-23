// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream } from "@datastream/core";

// DuckDB has no parameter binding for identifiers, so table/column names are
// interpolated into SQL. Double the embedded quotes and require a non-empty
// string so a crafted name can't break out of the quoted identifier.
export const quoteIdent = (name) => {
	if (typeof name !== "string" || name.length === 0) {
		throw new TypeError("duckdb: identifier must be a non-empty string");
	}
	return `"${name.replaceAll('"', '""')}"`;
};

// A failed existence probe should only be read as "table does not exist" when
// the error is a missing-table/catalog error. Any other failure (lock,
// permission, column read error, ...) must propagate so it is not masked by a
// confusing downstream "table already exists" from CREATE TABLE.
const isMissingTableError = (error) => {
	const message = String(error?.message ?? error).toLowerCase();
	return (
		message.includes("does not exist") ||
		message.includes("not found") ||
		message.includes("catalog error")
	);
};

// Arrow `Type` enum ids (apache-arrow is an optional peer, so not imported).
// Switch on typeId, never constructor.name: IPC/Parquet-decoded schemas use the
// generic Int_/Float/Date_/Timestamp_ classes rather than Int32/Float64/...
const ARROW_INT = 2;
const ARROW_FLOAT = 3;
const ARROW_BOOL = 6;
export const ARROW_DATE = 8;
export const ARROW_TIMESTAMP = 10;
const INT_SQL = { 8: "TINYINT", 16: "SMALLINT", 32: "INTEGER", 64: "BIGINT" };
// Indexed by Arrow TimeUnit: SECOND, MILLISECOND, MICROSECOND, NANOSECOND.
const TIMESTAMP_SQL = [
	"TIMESTAMP_S",
	"TIMESTAMP_MS",
	"TIMESTAMP",
	"TIMESTAMP_NS",
];

const arrowTypeToDuckDBSQL = (type) => {
	switch (type?.typeId) {
		case ARROW_BOOL:
			return "BOOLEAN";
		case ARROW_INT:
			return `${type.isSigned ? "" : "U"}${INT_SQL[type.bitWidth]}`;
		case ARROW_FLOAT:
			// Precision.DOUBLE is 2; HALF (0) and SINGLE (1) both fit in REAL.
			return type.precision === 2 ? "DOUBLE" : "REAL";
		case ARROW_DATE:
			return "DATE";
		case ARROW_TIMESTAMP:
			return TIMESTAMP_SQL[type.unit];
		default:
			return "VARCHAR";
	}
};

const tableExists = async (run, table) => {
	try {
		await run(`SELECT 1 FROM ${quoteIdent(table)} LIMIT 0`);
		return true;
	} catch (error) {
		if (isMissingTableError(error)) return false;
		throw error;
	}
};

const createTableFromArrowSchema = async (run, table, schema) => {
	const cols = schema.fields
		.map((f) => `${quoteIdent(f.name)} ${arrowTypeToDuckDBSQL(f.type)}`)
		.join(", ");
	await run(`CREATE TABLE ${quoteIdent(table)} (${cols})`);
};

// `run(sql)` executes a statement; `readColumnNames(sql)` runs a query and
// returns its column names. They wrap the node-api / duckdb-wasm connection.
export const ensureTableAndColumns = async (
	{ run, readColumnNames },
	table,
	schema,
) => {
	if (schema && !(await tableExists(run, table))) {
		await createTableFromArrowSchema(run, table, schema);
	}
	// Always derive the column order from the table's PHYSICAL layout. The node
	// appender appends positionally into physical columns, and the browser must
	// agree, so a provided schema whose field order differs from the physical
	// column order must not change which value lands in which column.
	return readColumnNames(`SELECT * FROM ${quoteIdent(table)} LIMIT 0`);
};

// Runs a write stream inside one transaction on the connection, so a failed or
// aborted pipeline commits nothing (node's appender auto-flushes every 204,800
// rows, which its clear() can't undo; the browser build inserts as it goes).
// BEGIN runs on the first write, before the table is created, and COMMIT after
// final(). Any failure releases the stream's handles, then rolls back. DuckDB
// has no nested transactions, so a connection that already has one open is
// refused (its failed BEGIN also aborts the caller's transaction).
// `run(sql)` executes a statement, `init()` runs once inside the transaction,
// and `release()` must be safe to call more than once.
export const createTransactionStream = (
	{ run, init, write, final, release = () => {} },
	streamOptions,
) => {
	let open = false;
	let ready;
	const begin = async () => {
		try {
			await run("BEGIN TRANSACTION");
		} catch (error) {
			throw new Error(
				"duckdb: could not begin the write stream's transaction; use a connection with no open transaction",
				{ cause: error },
			);
		}
		open = true;
		await init();
	};
	// Waits out a begin() still in flight, so an abort during it can't leave its
	// transaction open. A failed ROLLBACK is dropped: the stream is already
	// failing with the error that matters.
	const rollback = async () => {
		await ready?.catch(() => {});
		await release();
		if (!open) return;
		open = false;
		await run("ROLLBACK").catch(() => {});
	};
	const guard = (fn) => async (chunk) => {
		try {
			await fn(chunk);
		} catch (error) {
			await rollback();
			throw error;
		}
	};
	const { abort = () => {} } = streamOptions;
	return createWritableStream(
		guard(async (chunk) => {
			ready ??= begin();
			await ready;
			await write(chunk);
		}),
		guard(async () => {
			if (!open) return;
			await final();
			// DuckDB rolls a failed COMMIT back itself.
			open = false;
			await run("COMMIT");
		}),
		{
			...streamOptions,
			// An abort (upstream error, signal, destroy) skips write()/final(), so
			// roll back here too, before the caller's hook so a failure can't hide
			// behind it (runAbort swallows throws).
			abort: async (reason) => {
				await rollback();
				await abort(reason);
			},
		},
	);
};
