// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { once } from "node:events";
import { addAbortSignal } from "node:stream";
import { finished } from "node:stream/promises";
import { createWritableStream } from "@datastream/core";
import { gcpReadableStream } from "./client.js";

const getFile = (method, bucket, name) => {
	if (!bucket) {
		throw new TypeError(`${method}: bucket required`);
	}
	if (typeof name !== "string" || name === "") {
		throw new TypeError(`${method}: name required`);
	}
	return bucket.file(name);
};

// The SDK's own byte stream: its request starts lazily on the first read, and
// destroying it (here also on abort) tears the download down. addAbortSignal
// drops its listener once the stream ends.
export const gcpStorageReadStream = (options, streamOptions = {}) => {
	const { bucket, name, ...readOptions } = options;
	const stream = getFile("gcpStorageReadStream", bucket, name).createReadStream(
		readOptions,
	);
	const { signal } = streamOptions;
	if (signal) addAbortSignal(signal, stream);
	return stream;
};

// File.createWriteStream() holds 'finish' until the object is committed and
// its checksum validated (a mismatch deletes the object and errors with
// FILE_NO_UPLOAD), so awaiting it is the commit. A destroy before end() never
// reaches the SDK's final(), so the last chunk is never sent and no object is
// committed.
export const gcpStorageWriteStream = (options, streamOptions = {}) => {
	const { bucket, name, ...writeOptions } = options;
	const upload = getFile(
		"gcpStorageWriteStream",
		bucket,
		name,
	).createWriteStream(writeOptions);
	const write = (chunk) => upload.write(chunk) || once(upload, "drain");
	const final = () => {
		upload.end();
		return finished(upload);
	};
	const stream = createWritableStream(write, final, {
		...streamOptions,
		abort: () => upload.destroy(),
	});
	// Errors the SDK raises on its own (e.g. creating the resumable session)
	// while the wrapper is idle.
	upload.on("error", (error) => stream.destroy(error));
	return stream;
};

// Yields each object's metadata (the API object resource: name, size,
// contentType, updated, ...), one getFiles page at a time. With `fields` the
// SDK returns the raw (partial) items instead of File objects, so those are
// yielded as they are.
export const gcpStorageListStream = (options, streamOptions = {}) => {
	const { bucket, ...listOptions } = options;
	if (!bucket) {
		throw new TypeError("gcpStorageListStream: bucket required");
	}
	async function* command(signal) {
		// getFiles takes no AbortSignal: race each page against the signal so a
		// destroy mid-request closes the stream instead of waiting on it. No
		// .catch: the first race (on the first read) already handles a rejection.
		const aborted = new Promise((_resolve, reject) => {
			signal.addEventListener("abort", () => reject(signal.reason));
		});
		let query = { ...listOptions, autoPaginate: false };
		while (query) {
			const [files, nextQuery] = await Promise.race([
				bucket.getFiles(query),
				aborted,
			]);
			yield* listOptions.fields ? files : files.map((file) => file.metadata);
			query = nextQuery;
		}
	}
	return gcpReadableStream(command, streamOptions);
};
