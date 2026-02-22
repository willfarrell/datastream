import { createReadableStream, createWriteStream } from "@datastream/core";
import { openDB } from "idb/with-async-ittr.js";

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
	return createWriteStream(write, final, streamOptions);
};

export default {
	connect: indexedDBConnect,
	readStream: indexedDBReadStream,
	writeStream: indexedDBWriteStream,
};
