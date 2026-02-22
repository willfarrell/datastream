// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadableStream, createWritableStream } from "@datastream/core";
import { openDB } from "idb";

export const indexedDBConnect = openDB;

export const indexedDBReadStream = async (
	{ db, store, index, key },
	streamOptions = {},
) => {
	const input = db.transaction(store).store;
	if (index && key) {
		input.index(index).iterate(key);
	}
	return createReadableStream(input, streamOptions);
};

export const indexedDBWriteStream = async (
	{ db, store },
	streamOptions = {},
) => {
	const tx = db.transaction(store, "readwrite");
	const write = async (chunk) => {
		await tx.store.add(chunk);
	};
	const final = async () => {
		await tx.done;
	};
	return createWritableStream(write, final, streamOptions);
};

export default {
	connect: indexedDBConnect,
	readStream: indexedDBReadStream,
	writeStream: indexedDBWriteStream,
};
