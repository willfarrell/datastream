// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadableStream, createWritableStream } from "@datastream/core";
import { openDB } from "idb";

export const indexedDBConnect = openDB;

export const indexedDBReadStream = async (
	{ db, store, index, key },
	streamOptions = {},
) => {
	const tx = db.transaction(store);
	// index and key are independent: key alone filters by primary key, index
	// alone walks the index. Only treat the index as absent when it is
	// null/undefined. IDB treats an undefined key range as "no range", so
	// iterate(undefined) walks everything and needs no special case.
	const target =
		index === null || index === undefined ? tx.store : tx.store.index(index);
	const source = target.iterate(key);
	// idb's async iterators yield IDBCursor objects, not the stored records.
	// Map each cursor to its `.value` before handing the source to the core
	// stream factory; otherwise consumers receive raw cursors instead of data.
	// An AbortSignal in streamOptions is honoured by createReadableStream, which
	// errors the stream and returns this iterator (ending the cursor walk).
	const records = (async function* () {
		for await (const cursor of source) yield cursor.value;
	})();
	return createReadableStream(records, streamOptions);
};

export const indexedDBWriteStream = async (
	{ db, store },
	streamOptions = {},
) => {
	// A single shared transaction auto-commits as soon as it goes idle between
	// async chunks, throwing TransactionInactiveError on the next write. Open a
	// fresh readwrite transaction per chunk so each add() runs inside an active
	// transaction, and await its completion to preserve ordering/backpressure.
	const write = async (chunk) => {
		const tx = db.transaction(store, "readwrite");
		await tx.store.add(chunk);
		await tx.done;
	};
	return createWritableStream(write, streamOptions);
};
