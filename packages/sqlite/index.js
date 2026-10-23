// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadableStream, createWritableStream } from "@datastream/core";

// SQLite has no parameter binding for identifiers, so table/column names are
// interpolated into SQL. Double the embedded quotes and require a non-empty
// string so a crafted name can't break out of the quoted identifier.
const quoteIdent = (name) => {
	if (typeof name !== "string" || name.length === 0) {
		throw new TypeError("sqlite: identifier must be a non-empty string");
	}
	return `"${name.replaceAll('"', '""')}"`;
};

// `params` as an array binds positionally (?), as an object by name ($name).
// An array gets a leading {} in node:sqlite's named-parameters slot so a
// plain-object first value (e.g. untrusted JSON) fails to bind instead of
// being read as named params and shifting the rest. Readable.from calls the
// iterator's return() when the stream is destroyed early, which resets the
// statement and releases its read lock. null means no params: passed through,
// node:sqlite would bind it as a NULL value to ?1.
export const sqliteQueryStream = ({ db, sql, params }, streamOptions) => {
	params ??= [];
	return createReadableStream(
		db
			.prepare(sql)
			.iterate(...(Array.isArray(params) ? [{}, ...params] : [params])),
		streamOptions,
	);
};

const quoteColumns = (columns) => {
	if (columns.length === 0) {
		throw new TypeError("sqliteInsertStream: at least one column required");
	}
	return columns.map(quoteIdent).join(", ");
};

// All rows go into ONE transaction: BEGIN on the first write, COMMIT in final().
// A failed or aborted pipeline therefore commits nothing.
export const sqliteInsertStream = ({ db, table, columns }, streamOptions) => {
	const into = quoteIdent(table);
	// Validate given columns up front; derived ones are checked on first write.
	if (columns) quoteColumns(columns);
	// Set only for columns derived from the first row: an object key outside
	// them would otherwise be dropped silently. Given columns pick a subset.
	let derived;
	let insert;
	let open = false;

	const write = (row) => {
		if (!insert) {
			if (!columns && Array.isArray(row)) {
				throw new TypeError(
					"sqliteInsertStream: columns required for array rows",
				);
			}
			if (!columns) {
				columns = Object.keys(row);
				derived = new Set(columns);
			}
			const placeholders = columns.map(() => "?").join(", ");
			insert = db.prepare(
				`INSERT INTO ${into} (${quoteColumns(columns)}) VALUES (${placeholders})`,
			);
			// After prepare (a missing table opens nothing) and flagged only once
			// BEGIN succeeds, so a transaction someone else holds (the caller, or
			// another insert stream on this connection) is never rolled back.
			// SQLite has no nested BEGIN; fail with a clear message instead.
			if (db.isTransaction) {
				throw new Error(
					"sqlite: connection already has an open transaction; SQLite allows one writer per database file, so run insert streams on it one after another",
				);
			}
			db.exec("BEGIN");
			open = true;
		}
		let values;
		if (Array.isArray(row)) {
			// SQLite would NULL-pad a short row; a long one fails less clearly.
			if (row.length !== columns.length) {
				throw new TypeError(
					`sqliteInsertStream: array row has ${row.length} values, expected ${columns.length}`,
				);
			}
			values = row;
		} else {
			if (derived) {
				const unknown = Object.keys(row).filter((key) => !derived.has(key));
				if (unknown.length) {
					throw new TypeError(
						`sqliteInsertStream: row has keys not in columns: ${unknown.map((key) => JSON.stringify(key)).join(", ")}`,
					);
				}
			}
			// Own keys only, so "constructor"/"toString" columns don't read the
			// prototype. Missing and undefined values become NULL (node:sqlite
			// refuses to bind undefined).
			values = columns.map(
				(c) => (Object.hasOwn(row, c) ? row[c] : null) ?? null,
			);
		}
		// The leading {} fills node:sqlite's named-parameters slot: otherwise a
		// plain-object first value would be read as named params and the rest
		// would shift one column left.
		insert.run({}, ...values);
	};
	// `open` is cleared only after COMMIT succeeds: a failed COMMIT (deferred
	// foreign key, SQLITE_BUSY) leaves the transaction active for the rollback.
	const final = () => {
		if (open) {
			db.exec("COMMIT");
			open = false;
		}
	};

	const stream = createWritableStream(write, final, streamOptions);
	// createWritableStream only wires write/final; every other teardown (write
	// error, upstream error, destroy() without final, signal abort) ends in
	// "close", so roll back there. A no-op once committed. ROLLBACK can throw
	// when SQLite already rolled back on its own (e.g. a failed COMMIT); the
	// transaction is gone either way (or the caller closed the database).
	stream.once("close", () => {
		if (!open) return;
		try {
			db.exec("ROLLBACK");
		} catch {}
	});
	return stream;
};
