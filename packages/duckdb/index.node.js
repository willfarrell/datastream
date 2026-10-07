// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream, resolveLazy } from "@datastream/core";
import {
	DuckDBDateValue,
	DuckDBInstance,
	DuckDBTimestampValue,
	DuckDBTypeId,
} from "@duckdb/node-api";
import {
	ARROW_DATE,
	ARROW_TIMESTAMP,
	ensureTableAndColumns,
} from "./shared.js";

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

const MS_PER_DAY = 86400000;

// Target column types that take a wrapped DuckDB DATE/TIMESTAMP value (the
// appender casts between them). Any other target keeps arrow's raw epoch-ms
// number, e.g. a Timestamp into BIGINT stores the milliseconds.
const TEMPORAL_TARGETS = new Set([
	DuckDBTypeId.DATE,
	DuckDBTypeId.TIMESTAMP,
	DuckDBTypeId.TIMESTAMP_S,
	DuckDBTypeId.TIMESTAMP_MS,
	DuckDBTypeId.TIMESTAMP_NS,
	DuckDBTypeId.TIMESTAMP_TZ,
]);

// apache-arrow's get() returns Date and Timestamp values as epoch-millisecond
// numbers, which the appender would bind as BIGINT (no BIGINT -> DATE/TIMESTAMP
// cast). Wrap them in DuckDB values; the appender casts TIMESTAMP (µs) to the
// column's own unit. Sub-microsecond nanos are already lost by arrow's get().
// typeId is only set for a Date/Timestamp column whose target is temporal.
const toDuckDBValue = (typeId, value) => {
	if (typeId === ARROW_DATE) {
		return new DuckDBDateValue(Math.floor(value / MS_PER_DAY));
	}
	if (typeId === ARROW_TIMESTAMP) {
		return new DuckDBTimestampValue(BigInt(Math.round(value * 1000)));
	}
	return value;
};

const ensureTable = (db, table, schema) =>
	ensureTableAndColumns(
		{
			run: (sql) => db.run(sql),
			readColumnNames: async (sql) =>
				(await db.runAndReadAll(sql)).columnNames(),
		},
		table,
		schema,
	);

// typeId is an Arrow Date/Timestamp id to convert, else undefined (plain rows,
// non-temporal columns, or a non-temporal target column).
const appendCell = (appender, value, typeId) => {
	if (value === null || value === undefined) {
		appender.appendNull();
	} else {
		appender.appendValue(toDuckDBValue(typeId, value));
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
		columnNames = await ensureTable(db, table, resolvedSchema);
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
		columnNames = await ensureTable(db, table, resolvedSchema);
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
			// Look each table column up by NAME: the appender is positional over the
			// table's physical columns, but a batch's field order is arbitrary.
			// DuckDB identifiers are case-insensitive (table "ID" vs Arrow "id"), so
			// match case-insensitively. Table columns are unique ignoring case and
			// the counts match, so a batch with fields differing only by case always
			// leaves some table column unmatched and is rejected below.
			const fieldNames = new Map(
				(batch.schema?.fields ?? []).map((f) => [f.name.toLowerCase(), f.name]),
			);
			const cols = [];
			const typeIds = [];
			for (let i = 0; i < colCount; i++) {
				const name = columnNames[i];
				const col = batch.getChild(fieldNames.get(name.toLowerCase()) ?? name);
				if (col === null || col === undefined) {
					throw new Error(
						`duckdb: record batch is missing column "${name}" for table "${table}"`,
					);
				}
				cols.push(col);
				// Convert Date/Timestamp only when the TARGET column is temporal.
				const typeId = col.type?.typeId;
				typeIds.push(
					(typeId === ARROW_DATE || typeId === ARROW_TIMESTAMP) &&
						TEMPORAL_TARGETS.has(state.appender.columnType(i).typeId)
						? typeId
						: undefined,
				);
			}
			const rowCount = batch.numRows;
			for (let r = 0; r < rowCount; r++) {
				for (let i = 0; i < colCount; i++)
					appendCell(state.appender, cols[i].get(r), typeIds[i]);
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
