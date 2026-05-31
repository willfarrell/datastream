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
	let source = tx.store;
	// A falsy-but-valid index key (0 or "") must still select the index. Only
	// treat the index as absent when it is null/undefined, and only treat the
	// key as omitted when it is strictly undefined (null is a valid key range).
	if (index != null && key !== undefined) {
		source = source.index(index).iterate(key);
	} else {
		source = source.iterate();
	}
	const { signal } = streamOptions;
	// idb's async iterators yield IDBCursor objects, not the stored records.
	// Map each cursor to its `.value` before handing the source to the core
	// stream factory; otherwise consumers receive raw cursors instead of data.
	// Honor an AbortSignal by stopping iteration when it fires, and ALWAYS
	// remove the listener once iteration settles so a shared signal does not
	// accumulate one listener per stream (leak on normal completion).
	const records = (async function* () {
		let aborted = signal?.aborted ?? false;
		let onAbort;
		if (signal && !aborted) {
			onAbort = () => {
				aborted = true;
			};
			signal.addEventListener("abort", onAbort, { once: true });
		}
		try {
			for await (const cursor of source) {
				if (aborted) break;
				yield cursor.value;
			}
		} finally {
			if (signal && onAbort) {
				signal.removeEventListener("abort", onAbort);
				onAbort = undefined;
			}
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
		await tx.store.add(chunk);
		await tx.done;
	};
	return createWritableStream(write, streamOptions);
};

export default {
	connect: indexedDBConnect,
	readStream: indexedDBReadStream,
	writeStream: indexedDBWriteStream,
};
