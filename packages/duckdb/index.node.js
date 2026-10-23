// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadableStream, resolveLazy } from "@datastream/core";
import {
	DuckDBDateValue,
	DuckDBInstance,
	DuckDBTimestampValue,
	DuckDBTypeId,
} from "@duckdb/node-api";
import {
	ARROW_DATE,
	ARROW_TIMESTAMP,
	createTransactionStream,
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

export const duckdbQueryStream = ({ db, sql, params }, streamOptions) => {
	let busy = false;
	const rows = async function* () {
		busy = true;
		try {
			const result = await db.stream(sql, params);
			for await (const chunk of result.yieldRowObjectJs()) {
				busy = false;
				yield* chunk;
				busy = true;
			}
		} finally {
			busy = false;
		}
	};
	const stream = createReadableStream(rows(), streamOptions);
	// node-api has no explicit result close (the native result is freed by its
	// finalizer once unreferenced). Destroying the stream returns the generator,
	// but that waits behind an in-flight stream()/fetchChunk(), so interrupt that
	// call first. interrupt() is connection-wide: it stops whatever the
	// connection is running, so it is skipped whenever this stream has nothing
	// in flight (an unread, paused, finished or failed stream).
	const destroy = stream._destroy.bind(stream);
	stream._destroy = (error, callback) => {
		if (busy) db.interrupt();
		destroy(error, callback);
	};
	return stream;
};

const MS_PER_DAY = 86400000;

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
const toDuckDBValue = {
	[ARROW_DATE]: (value) => new DuckDBDateValue(Math.floor(value / MS_PER_DAY)),
	[ARROW_TIMESTAMP]: (value) =>
		new DuckDBTimestampValue(BigInt(Math.round(value * 1000))),
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

const appendCell = (appender, value, typeId) => {
	if (value === null || value === undefined) {
		appender.appendNull();
	} else if (typeId !== undefined) {
		appender.appendValue(toDuckDBValue[typeId](value));
	} else if (typeof value === "string") {
		appender.appendVarchar(value);
	} else if (typeof value === "boolean") {
		appender.appendBoolean(value);
	} else if (typeof value !== "number") {
		appender.appendValue(value);
	} else if ((value | 0) === value) {
		appender.appendInteger(value);
	} else if (Number.isInteger(value)) {
		appender.appendValue(value);
	} else {
		appender.appendDouble(value);
	}
};

const closeAppenderOnce = (state) => {
	if (!state.appender || state.closed) return;
	state.closed = true;
	try {
		state.appender.closeSync();
	} catch (_error) {}
};

const appenderStream = ({ db, table, schema }, append, streamOptions) => {
	const state = {};
	return createTransactionStream(
		{
			run: (sql) => db.run(sql),
			init: async () => {
				state.columnNames = await ensureTable(db, table, resolveLazy(schema));
				state.appender = await db.createAppender(table);
			},
			write: (chunk) => append(state, chunk),
			final: () => {
				state.appender.flushSync();
				closeAppenderOnce(state);
			},
			release: () => closeAppenderOnce(state),
		},
		streamOptions,
	);
};

export const duckdbAppenderStream = async (options, streamOptions = {}) =>
	appenderStream(
		options,
		({ appender, columnNames }, row) => {
			const isArray = Array.isArray(row);
			for (let i = 0, l = columnNames.length; i < l; i++) {
				appendCell(appender, isArray ? row[i] : row[columnNames[i]]);
			}
			appender.endRow();
		},
		streamOptions,
	);

export const duckdbArrowInsertStream = async (options, streamOptions = {}) =>
	appenderStream(
		options,
		({ appender, columnNames }, batch) => {
			const { table } = options;
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
			// match case-insensitively.
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
				const typeId = col.type?.typeId;
				typeIds.push(
					(typeId === ARROW_DATE || typeId === ARROW_TIMESTAMP) &&
						TEMPORAL_TARGETS.has(appender.columnType(i).typeId)
						? typeId
						: undefined,
				);
			}
			const rowCount = batch.numRows;
			for (let r = 0; r < rowCount; r++) {
				for (let i = 0; i < colCount; i++)
					appendCell(appender, cols[i].get(r), typeIds[i]);
				appender.endRow();
			}
		},
		streamOptions,
	);
