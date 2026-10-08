// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createPassThroughStream,
	createReadableStream,
} from "@datastream/core";

export const azureBlobDownloadStream = async (options, streamOptions = {}) => {
	const { client, offset, count, ...downloadOptions } = options;
	const { readableStreamBody: Body } = await client.download(offset, count, {
		...downloadOptions,
		abortSignal: streamOptions.signal,
	});
	if (!Body) {
		// Only the blob name: other options (customerProvidedKey) are secrets
		// that must not leak into logged error causes.
		throw new Error("Blob.download has no body", {
			cause: { name: client.name },
		});
	}
	const stream = createReadableStream(Body, streamOptions);
	// Tie the SDK body (live socket-backed readable) to the wrapper so a
	// consumer error/abort releases the HTTP connection.
	const teardownBody = () => {
		try {
			Body.destroy();
		} catch {}
	};
	stream.on("error", teardownBody);
	const { signal } = streamOptions;
	if (signal) {
		if (signal.aborted) {
			teardownBody();
		} else {
			signal.addEventListener("abort", teardownBody, { once: true });
			// Drop the listener once the stream is done so a long-lived signal shared
			// across many downloads does not accumulate listeners (each pinning a body).
			stream.once("close", () =>
				signal.removeEventListener("abort", teardownBody),
			);
		}
	}
	return stream;
};

export const azureBlobUploadStream = (options, streamOptions = {}) => {
	const { client, bufferSize, maxConcurrency, ...uploadOptions } = options;
	const stream = createPassThroughStream(() => {}, streamOptions);
	const result = client.uploadStream(stream, bufferSize, maxConcurrency, {
		...uploadOptions,
		abortSignal: streamOptions.signal,
	});
	// A failed upload stops draining the body; forward the SDK error so pipeline
	// rejects with it instead of hanging. result() still rethrows.
	result.catch((error) => stream.destroy(error));
	stream.result = async () => {
		await result;
		return {};
	};
	return stream;
};
