// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadableStream, resolveLazy } from "@datastream/core";
import {
	createTransactionStream,
	ensureTableAndColumns,
	quoteIdent,
} from "./shared.js";

export const duckdbConnect = async (path = ":memory:", { bundles } = {}) => {
	const { AsyncDuckDB, ConsoleLogger, getJsDelivrBundles, selectBundle } =
		await import("@duckdb/duckdb-wasm");
	const bundle = await selectBundle(bundles ?? getJsDelivrBundles());
	const worker = new Worker(bundle.mainWorker);
	const db = new AsyncDuckDB(new ConsoleLogger(), worker);
	try {
		await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
		if (path !== ":memory:") await db.open({ path });
		const connection = await db.connect();
		connection.__duckdb = db;
		return connection;
	} catch (error) {
		await db.terminate();
		throw error;
	}
};

export const duckdbQueryStream = ({ db, sql, params }, streamOptions) => {
	let busy = false;
	const rows = (async function* () {
		let prepared;
		busy = true;
		try {
			if (params) prepared = await db.prepare(sql);
			const reader = prepared
				? await prepared.send(...params)
				: await db.send(sql, true);
			for await (const batch of reader) {
				busy = false;
				for (const row of batch.toArray()) yield row.toJSON();
				busy = true;
			}
		} finally {
			if (prepared) await prepared.close();
		}
	})();
	// core calls return() on cancel/abort, but a generator queues it behind an
	// in-flight next() (a query still running in the worker). Cancel that query
	// first so the wait ends, then let the generator run its finally.
	// cancelSent() is connection-wide, so it is skipped when this stream has
	// nothing in flight.
	const finish = rows.return;
	rows.return = async () => {
		if (busy) await db.cancelSent().catch(() => {});
		return finish.call(rows);
	};
	return createReadableStream(rows, streamOptions);
};

const ensureTable = (db, table, schema) =>
	ensureTableAndColumns(
		{
			run: (sql) => db.query(sql),
			readColumnNames: async (sql) =>
				(await db.query(sql)).schema.fields.map((f) => f.name),
		},
		table,
		schema,
	);

const buildInsertSQL = (table, columnNames) => {
	const cols = columnNames.map((n) => quoteIdent(n)).join(", ");
	const params = columnNames.map(() => "?").join(", ");
	return `INSERT INTO ${quoteIdent(table)} (${cols}) VALUES (${params})`;
};

export const duckdbAppenderStream = async (
	{ db, table, schema },
	streamOptions = {},
) => {
	let prepared;
	let columnNames;
	const close = async () => {
		const statement = prepared;
		prepared = undefined;
		await statement?.close();
	};

	return createTransactionStream(
		{
			run: (sql) => db.query(sql),
			init: async () => {
				columnNames = await ensureTable(db, table, resolveLazy(schema));
				prepared = await db.prepare(buildInsertSQL(table, columnNames));
			},
			write: (row) => {
				const isArray = Array.isArray(row);
				return prepared.send(
					...columnNames.map((name, i) => (isArray ? row[i] : row[name])),
				);
			},
			final: close,
			release: close,
		},
		streamOptions,
	);
};

export const duckdbArrowInsertStream = async (
	{ db, table, schema, batchRows = 100_000 },
	streamOptions = {},
) => {
	let columnNames;
	let pending = [];
	let pendingRows = 0;

	const { tableFromIPC, tableToIPC, Table } = await import("apache-arrow");

	// Build ONE Table from the pending RecordBatch objects. Serializing each
	// batch as its own complete IPC stream and byte-concatenating them would
	// produce multiple EOS markers; tableFromIPC stops at the first, silently
	// dropping every batch after the first.
	const flush = async () => {
		const ipc = tableToIPC(new Table(pending));
		pending = [];
		pendingRows = 0;
		await db.insertArrowTable(tableFromIPC(ipc), {
			name: table,
			create: false,
		});
	};

	// insertArrowTable({ create: false }) maps columns by POSITION, but a batch's
	// field order is arbitrary. Reorder each batch to the table's physical column
	// order, matching names case-insensitively (DuckDB identifiers are).
	const toTableOrder = (batch) => {
		const { fields } = batch.schema;
		const colCount = columnNames.length;
		if (fields.length !== colCount) {
			throw new Error(
				`duckdb: record batch column count (${fields.length}) does not match table "${table}" column count (${colCount})`,
			);
		}
		const fieldIndex = new Map(fields.map((f, i) => [f.name.toLowerCase(), i]));
		return batch.selectAt(
			columnNames.map((name) => {
				const index = fieldIndex.get(name.toLowerCase());
				if (index === undefined) {
					throw new Error(
						`duckdb: record batch is missing column "${name}" for table "${table}"`,
					);
				}
				return index;
			}),
		);
	};

	return createTransactionStream(
		{
			run: (sql) => db.query(sql),
			init: async () => {
				columnNames = await ensureTable(db, table, resolveLazy(schema));
			},
			write: async (batch) => {
				batch = toTableOrder(batch);
				pending.push(batch);
				pendingRows += batch.numRows;
				if (pendingRows >= batchRows) await flush();
			},
			final: async () => {
				if (pending.length) await flush();
			},
		},
		streamOptions,
	);
};
