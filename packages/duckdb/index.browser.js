// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream, resolveLazy } from "@datastream/core";
import { ensureTableAndColumns, quoteIdent } from "./shared.js";

// `path` is handed to AsyncDuckDB.open() (e.g. "opfs://file.duckdb");
// ":memory:" (the default) keeps the freshly instantiated in-memory database.
// `bundles` are duckdb-wasm bundles (e.g. self-hosted mainModule/mainWorker
// URLs). The default loads them from jsDelivr: that code is not integrity
// pinned, and browsers refuse `new Worker()` on a cross-origin URL, so
// production apps should self-host and pass their own bundles.
export const duckdbConnect = async (path = ":memory:", { bundles } = {}) => {
	const { AsyncDuckDB, ConsoleLogger, getJsDelivrBundles, selectBundle } =
		await import("@duckdb/duckdb-wasm");
	const bundle = await selectBundle(bundles ?? getJsDelivrBundles());
	const worker = new Worker(bundle.mainWorker);
	const db = new AsyncDuckDB(new ConsoleLogger(), worker);
	await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
	if (path !== ":memory:") await db.open({ path });
	const connection = await db.connect();
	connection.__duckdb = db;
	return connection;
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

	const init = async () => {
		const resolvedSchema = resolveLazy(schema);
		columnNames = await ensureTable(db, table, resolvedSchema);
		prepared = await db.prepare(buildInsertSQL(table, columnNames));
	};

	const write = async (row) => {
		if (!prepared) await init();
		const isArray = Array.isArray(row);
		try {
			await prepared.send(
				...columnNames.map((name, i) => (isArray ? row[i] : row[name])),
			);
		} catch (error) {
			// final() never runs once a write fails, so release the statement here.
			await prepared.close();
			throw error;
		}
	};
	const final = async () => {
		if (prepared) await prepared.close();
	};
	// An abort (upstream error / signal) skips both final() and write()'s catch,
	// so release the statement there too. Closed before the caller's hook so a
	// missing statement can't hide behind it (runAbort swallows throws).
	const { abort = () => {} } = streamOptions;

	return createWritableStream(write, final, {
		...streamOptions,
		abort: async (reason) => {
			if (prepared) await prepared.close();
			await abort(reason);
		},
	});
};

export const duckdbArrowInsertStream = async (
	{ db, table, schema, batchRows = 100_000 },
	streamOptions = {},
) => {
	let columnNames;
	let pending = [];
	let pendingRows = 0;

	const init = async () => {
		columnNames = await ensureTable(db, table, resolveLazy(schema));
	};

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

	const write = async (batch) => {
		if (!columnNames) await init();
		// Insert every `batchRows` rows instead of buffering the whole stream
		// until final() (which then made two more full copies).
		pending.push(batch);
		pendingRows += batch.numRows;
		if (pendingRows >= batchRows) await flush();
	};
	const final = async () => {
		if (pending.length) await flush();
	};

	return createWritableStream(write, final, streamOptions);
};
