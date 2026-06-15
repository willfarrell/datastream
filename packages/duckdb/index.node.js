// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream, resolveLazy } from "@datastream/core";
import { DuckDBInstance } from "@duckdb/node-api";

export const duckdbConnect = async (path = ":memory:", options) => {
	// An empty path is a caller mistake (node-api would silently open an in-memory
	// database, masking the bug). Reject it so the ":memory:" sentinel must be used
	// explicitly for an in-memory database.
	if (typeof path !== "string" || path.length === 0) {
		throw new TypeError(
			'duckdb: path must be a non-empty string (use ":memory:" for in-memory)',
		);
	}
	const instance = await DuckDBInstance.create(path, options);
	return await instance.connect();
};

// DuckDB has no parameter binding for identifiers, so table/column names are
// interpolated into SQL. Double the embedded quotes and require a non-empty
// string so a crafted name can't break out of the quoted identifier.
const quoteIdent = (name) => {
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

const tableExists = async (db, table) => {
	try {
		await db.run(`SELECT 1 FROM ${quoteIdent(table)} LIMIT 0`);
		return true;
	} catch (error) {
		if (isMissingTableError(error)) return false;
		throw error;
	}
};

const fetchColumnNames = async (db, table) => {
	const reader = await db.runAndReadAll(
		`SELECT * FROM ${quoteIdent(table)} LIMIT 0`,
	);
	return reader.columnNames();
};

const arrowTypeToDuckDBSQL = (type) => {
	const name = type?.constructor?.name;
	switch (name) {
		case "Bool":
			return "BOOLEAN";
		case "Int8":
			return "TINYINT";
		case "Int16":
			return "SMALLINT";
		case "Int32":
			return "INTEGER";
		case "Int64":
			return "BIGINT";
		case "Uint8":
			return "UTINYINT";
		case "Uint16":
			return "USMALLINT";
		case "Uint32":
			return "UINTEGER";
		case "Uint64":
			return "UBIGINT";
		case "Float32":
			return "REAL";
		case "Float64":
			return "DOUBLE";
		case "Date_":
			return "DATE";
		case "TimestampSecond":
			return "TIMESTAMP_S";
		case "TimestampMillisecond":
			return "TIMESTAMP_MS";
		case "TimestampMicrosecond":
			return "TIMESTAMP";
		case "TimestampNanosecond":
			return "TIMESTAMP_NS";
		default:
			return "VARCHAR";
	}
};

const createTableFromArrowSchema = async (db, table, schema) => {
	const cols = schema.fields
		.map((f) => `${quoteIdent(f.name)} ${arrowTypeToDuckDBSQL(f.type)}`)
		.join(", ");
	await db.run(`CREATE TABLE ${quoteIdent(table)} (${cols})`);
};

const ensureTableAndColumns = async (db, table, schema) => {
	if (schema && !(await tableExists(db, table))) {
		await createTableFromArrowSchema(db, table, schema);
	}
	// Always derive the column order from the table's PHYSICAL layout. The
	// appender appends positionally into physical columns, so trusting the
	// provided schema.fields order would misalign (or hard-fail with a cast
	// error) whenever the schema order differs from the physical column order.
	return fetchColumnNames(db, table);
};

const appendCell = (appender, value) => {
	if (value === null || value === undefined) {
		appender.appendNull();
	} else {
		appender.appendValue(value);
	}
};

// Release a native appender handle exactly once. createWritableStream only runs
// final() on normal completion, so a thrown write() (or an aborted pipeline)
// would otherwise leak the underlying native appender.
const closeAppenderOnce = (state) => {
	if (!state.appender || state.closed) return;
	state.closed = true;
	try {
		state.appender.closeSync();
	} catch (_error) {
		// best-effort: the handle is being torn down on an error path.
	}
};

// createWritableStream (core) only wires write/final, so error/abort teardown
// never releases the native appender. Attach cleanup to the Writable's lifecycle
// (error/close) and to streamOptions.signal so an upstream error or an abort
// still closes the handle. closeAppenderOnce is idempotent, so the normal
// final()-then-close path stays a no-op here.
const wireAppenderCleanup = (stream, state, streamOptions) => {
	const cleanup = () => closeAppenderOnce(state);
	stream.once("error", cleanup);
	stream.once("close", cleanup);
	// streamOptions always defaults to {} at the public entry points, so it is
	// never nullish here.
	const signal = streamOptions.signal;
	if (signal) {
		// The appender is created lazily (during the first write), so it never
		// exists yet at wiring time; an already-aborted signal is therefore handled
		// by the stream's own abort teardown (which fires "error"/"close" above).
		// For a not-yet-aborted signal, release the handle when it aborts and stop
		// listening once the stream closes normally.
		signal.addEventListener("abort", cleanup, { once: true });
		stream.once("close", () => signal.removeEventListener("abort", cleanup));
	}
	return stream;
};

export const duckdbAppenderStream = async (
	{ db, table, schema },
	streamOptions = {},
) => {
	// state.appender starts undefined (set lazily in init) and state.closed starts
	// falsy; both are read with truthiness checks, so an empty object suffices.
	const state = {};
	let columnNames;

	const init = async () => {
		const resolvedSchema = resolveLazy(schema);
		columnNames = await ensureTableAndColumns(db, table, resolvedSchema);
		state.appender = await db.createAppender(table);
	};

	const write = async (row) => {
		if (!state.appender) await init();
		try {
			const isArray = Array.isArray(row);
			for (let i = 0, l = columnNames.length; i < l; i++) {
				const v = isArray ? row[i] : row[columnNames[i]];
				appendCell(state.appender, v);
			}
			state.appender.endRow();
		} catch (error) {
			closeAppenderOnce(state);
			throw error;
		}
	};
	const final = async () => {
		if (state.appender && !state.closed) {
			state.appender.flushSync();
			closeAppenderOnce(state);
		}
	};

	return wireAppenderCleanup(
		createWritableStream(write, final, streamOptions),
		state,
		streamOptions,
	);
};

export const duckdbArrowInsertStream = async (
	{ db, table, schema },
	streamOptions = {},
) => {
	// See duckdbAppenderStream: appender/closed are only read for truthiness.
	const state = {};
	let columnNames;

	const init = async () => {
		const resolvedSchema = resolveLazy(schema);
		columnNames = await ensureTableAndColumns(db, table, resolvedSchema);
		state.appender = await db.createAppender(table);
	};

	const write = async (batch) => {
		if (!state.appender) await init();
		try {
			const colCount = columnNames.length;
			// Validate the batch's column count against the target table so a
			// mismatch surfaces a clear schema error instead of a null-deref
			// (too few columns) or silent column drop (too many columns).
			const batchColCount = batch.schema?.fields?.length ?? batch.numCols;
			if (batchColCount !== colCount) {
				throw new Error(
					`duckdb: record batch column count (${batchColCount}) does not match table "${table}" column count (${colCount})`,
				);
			}
			const cols = [];
			for (let i = 0; i < colCount; i++) {
				const col = batch.getChildAt(i);
				if (col === null || col === undefined) {
					throw new Error(
						`duckdb: record batch is missing column ${i} for table "${table}"`,
					);
				}
				cols.push(col);
			}
			const rowCount = batch.numRows;
			for (let r = 0; r < rowCount; r++) {
				for (let i = 0; i < colCount; i++)
					appendCell(state.appender, cols[i].get(r));
				state.appender.endRow();
			}
		} catch (error) {
			closeAppenderOnce(state);
			throw error;
		}
	};
	const final = async () => {
		if (state.appender && !state.closed) {
			state.appender.flushSync();
			closeAppenderOnce(state);
		}
	};

	return wireAppenderCleanup(
		createWritableStream(write, final, streamOptions),
		state,
		streamOptions,
	);
};

export default {
	connect: duckdbConnect,
	appenderStream: duckdbAppenderStream,
	arrowInsertStream: duckdbArrowInsertStream,
};
