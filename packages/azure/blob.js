// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createPassThroughStream,
	createReadableStream,
} from "@datastream/core";
import { azureReadableStream } from "./client.js";

export const azureBlobDownloadStream = async (options, streamOptions = {}) => {
	const { client, offset, count, ...downloadOptions } = options;
	const { readableStreamBody: Body } = await client.download(offset, count, {
		...downloadOptions,
		abortSignal: streamOptions.signal,
	});
	if (!Body) {
		throw new Error("Blob.download has no body", {
			cause: { name: client.name },
		});
	}
	const stream = createReadableStream(Body, streamOptions);
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
			signal.addEventListener("abort", teardownBody);
			stream.once("close", () =>
				signal.removeEventListener("abort", teardownBody),
			);
		}
	}
	return stream;
};

export const azureBlobListStream = async (options, streamOptions = {}) => {
	const { client, ...listOptions } = options;
	if (!client) {
		throw new TypeError("azureBlobListStream: client required");
	}
	async function* command(signal) {
		yield* client.listBlobsFlat({ ...listOptions, abortSignal: signal });
	}
	return azureReadableStream(command, streamOptions);
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
