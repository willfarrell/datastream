// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream, resolveLazy } from "@datastream/core";

export const duckdbConnect = async (..._args) => {
	const { AsyncDuckDB, ConsoleLogger, getJsDelivrBundles, selectBundle } =
		await import("@duckdb/duckdb-wasm");
	const bundle = await selectBundle(getJsDelivrBundles());
	const worker = new Worker(bundle.mainWorker);
	const db = new AsyncDuckDB(new ConsoleLogger(), worker);
	await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
	const connection = await db.connect();
	connection.__duckdb = db;
	return connection;
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
		await db.query(`SELECT 1 FROM ${quoteIdent(table)} LIMIT 0`);
		return true;
	} catch (error) {
		if (isMissingTableError(error)) return false;
		throw error;
	}
};

const fetchColumnNames = async (db, table) => {
	const result = await db.query(`SELECT * FROM ${quoteIdent(table)} LIMIT 0`);
	return result.schema.fields.map((f) => f.name);
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
	await db.query(`CREATE TABLE ${quoteIdent(table)} (${cols})`);
};

const ensureTableAndColumns = async (db, table, schema) => {
	if (schema && !(await tableExists(db, table))) {
		await createTableFromArrowSchema(db, table, schema);
	}
	// Always derive the column order from the table's PHYSICAL layout so node and
	// web agree: a provided schema whose field order differs from the physical
	// column order must not change which value lands in which column.
	return fetchColumnNames(db, table);
};

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
		columnNames = await ensureTableAndColumns(db, table, resolvedSchema);
		prepared = await db.prepare(buildInsertSQL(table, columnNames));
	};

	const write = async (row) => {
		if (!prepared) await init();
		const isArray = Array.isArray(row);
		const values = new Array(columnNames.length);
		for (let i = 0, l = columnNames.length; i < l; i++) {
			values[i] = isArray ? row[i] : row[columnNames[i]];
		}
		await prepared.send(...values);
	};
	const final = async () => {
		if (prepared) await prepared.close();
	};

	return createWritableStream(write, final, streamOptions);
};

export const duckdbArrowInsertStream = async (
	{ db, table, schema },
	streamOptions = {},
) => {
	let initialized = false;
	let columnNames;
	const batches = [];

	const init = async () => {
		const resolvedSchema = resolveLazy(schema);
		columnNames = await ensureTableAndColumns(db, table, resolvedSchema);
		initialized = true;
	};

	const { tableFromIPC, tableToIPC, Table } = await import("apache-arrow");

	const write = async (batch) => {
		if (!initialized) await init();
		// Accumulate the RecordBatch objects themselves. Serializing each batch as
		// its own complete IPC stream and byte-concatenating them would produce
		// multiple EOS markers; tableFromIPC stops at the first, silently dropping
		// every batch after the first. Build one Table from all batches instead.
		batches.push(batch);
	};
	const final = async () => {
		if (!batches.length) return;
		const ipc = tableToIPC(new Table(batches), "stream");
		const arrowTable = tableFromIPC(ipc);
		await db.insertArrowTable(arrowTable, { name: table, create: false });
		void columnNames;
	};

	return createWritableStream(write, final, streamOptions);
};

export default {
	connect: duckdbConnect,
	appenderStream: duckdbAppenderStream,
	arrowInsertStream: duckdbArrowInsertStream,
};
