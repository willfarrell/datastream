// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadableStream, createWritableStream } from "@datastream/core";
import { openDB } from "idb";

export const indexedDBConnect = openDB;

const batchSize = 100;

export const indexedDBReadStream = async (
	{ db, store, index, key },
	streamOptions = {},
) => {
	// index and key are independent: key alone filters by primary key, index
	// alone walks the index. Only treat the index as absent when it is
	// null/undefined. IDB treats an undefined key range as "no range", so
	// openCursor(undefined) walks everything and needs no special case.
	const byStore = index === null || index === undefined;
	// A transaction auto-commits once it goes idle, so a cursor held open while
	// a slow consumer awaits other work throws TransactionInactiveError on its
	// next step. Read eagerly in batches, each in its own short transaction, and
	// resume after the last (key, primaryKey) position: index keys repeat, so
	// the primary key is what makes the position unique.
	// An AbortSignal in streamOptions is honoured by createReadableStream, which
	// errors the stream and returns this iterator (ending the reads).
	const records = (async function* () {
		let last;
		const compare = (cursor) =>
			indexedDB.cmp([cursor.key, cursor.primaryKey], last);
		while (true) {
			const tx = db.transaction(store);
			const target = byStore ? tx.store : tx.store.index(index);
			let cursor = await target.openCursor(key);
			if (last && cursor) {
				// Seek forward to the last position (a store cursor has no
				// continuePrimaryKey; its key is the primary key) ...
				if (compare(cursor) < 0) {
					cursor = await (byStore
						? cursor.continue(last[0])
						: cursor.continuePrimaryKey(...last));
				}
				// ... and step past it unless it was deleted meanwhile.
				if (cursor && compare(cursor) === 0) cursor = await cursor.continue();
			}
			const batch = [];
			while (cursor && batch.length < batchSize) {
				batch.push(cursor.value);
				last = [cursor.key, cursor.primaryKey];
				cursor = await cursor.continue();
			}
			yield* batch;
			if (!cursor) return;
		}
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
		// Await both together: a failed add() aborts the transaction, so tx.done
		// rejects as well and would otherwise be an unhandled rejection.
		await Promise.all([tx.store.add(chunk), tx.done]);
	};
	return createWritableStream(write, streamOptions);
};
