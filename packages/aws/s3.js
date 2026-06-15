// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global crypto */

import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import {
	createPassThroughStream,
	createReadableStream,
} from "@datastream/core";
import { awsClientDefaults } from "./client.js";

let defaultClient = new S3Client(awsClientDefaults);
export const awsS3SetClient = (s3Client) => {
	defaultClient = s3Client;
};

export const awsS3GetObjectStream = async (options, streamOptions = {}) => {
	const { client, ...params } = options;
	const { Body } = await (client ?? defaultClient).send(
		new GetObjectCommand(params),
		{ abortSignal: streamOptions.signal },
	);
	if (!Body) {
		throw new Error("S3.GetObject not found", { cause: params });
	}
	const stream = createReadableStream(Body, streamOptions);
	// Tie the SDK Body (live socket-backed readable) lifecycle to the returned
	// wrapper: if the consumer errors/aborts, tear down Body so the underlying
	// HTTP connection is not leaked.
	const teardownBody = () => {
		// The node SDK Body is a Readable (destroy). The try/catch swallows teardown
		// errors so releasing the socket cannot re-throw on an already-failed Body
		// (and tolerates a Body that does not expose destroy()).
		try {
			Body.destroy();
		} catch {}
	};
	// Node build: createReadableStream returns a node Readable; clean up on its
	// 'error' event (without an error argument, so releasing the socket does not
	// re-emit an unhandled 'error' on the already-failed Body).
	stream.on("error", teardownBody);
	// Any build given an abort signal also wires teardown to the abort signal so
	// socket teardown on consumer abort is consistent across builds.
	const { signal } = streamOptions;
	if (signal) {
		if (signal.aborted) {
			teardownBody();
		} else {
			signal.addEventListener("abort", teardownBody, { once: true });
		}
	}
	return stream;
};

export const awsS3PutObjectStream = (options, streamOptions = {}) => {
	const { onProgress, client, tags, partSize, queueSize, ...params } = options;
	const stream = createPassThroughStream(() => {}, streamOptions);
	// lib-storage defaults to a 5 MiB partSize and a 10,000-part ceiling
	// (~50 GiB max object). Expose partSize/queueSize so callers can raise the
	// ceiling for very large streamed objects.
	const upload = new Upload({
		client: client ?? defaultClient,
		params: {
			...params,
			Body: stream,
		},
		tags,
		partSize,
		queueSize,
	});
	if (onProgress) {
		stream.on("httpUploadProgress", onProgress);
	}
	const result = upload.done();

	stream.result = async () => {
		await result;
		return {};
	};
	return stream;
};

// This is designed to be used in the browser on a file that you want to upload via a presigned URL
// partSize; magic number, no 16MB mentioned in the docs
export const awsS3ChecksumStream = (
	{ ChecksumAlgorithm, partSize, resultKey } = {},
	streamOptions = {},
) => {
	ChecksumAlgorithm ??= "SHA256";
	partSize ??= 17_179_870; // ~16MB, just under S3 multipart minimum
	const algorithm = _algorithms[ChecksumAlgorithm];
	if (!algorithm)
		throw new Error(`Unsupported ChecksumAlgorithm: ${ChecksumAlgorithm}`);
	let checksums = [];
	const pending = [];
	let pendingLen = 0;
	const takePart = () => {
		const part = new Uint8Array(partSize);
		let filled = 0;
		while (filled < partSize) {
			const head = pending[0];
			const need = partSize - filled;
			if (head.byteLength <= need) {
				part.set(head, filled);
				filled += head.byteLength;
				pending.shift();
			} else {
				part.set(head.subarray(0, need), filled);
				pending[0] = head.subarray(need);
				filled += need;
			}
		}
		pendingLen -= partSize;
		return part;
	};
	const passThrough = async (chunk) => {
		if (typeof chunk === "string") {
			chunk = new TextEncoder().encode(chunk);
		} else if (!(chunk instanceof Uint8Array)) {
			// Match the old _concatBuffers handling of ArrayBuffer/Buffer inputs.
			chunk = new Uint8Array(chunk);
		}
		pending.push(chunk);
		pendingLen += chunk.byteLength;
		// Peel off every whole part the buffered bytes can supply; any trailing
		// partial part stays buffered for the next chunk (or the final flush).
		while (pendingLen >= partSize) {
			const checksum = await crypto.subtle.digest(algorithm, takePart());
			checksums.push(checksum);
		}
	};
	const flush = async () => {
		if (pendingLen > 0) {
			// Remainder is < partSize: a single concat of the leftover chunks.
			const checksum = await crypto.subtle.digest(
				algorithm,
				_concatBuffers(pending),
			);
			checksums.push(checksum);
		}
	};
	const stream = createPassThroughStream(passThrough, flush, streamOptions);
	let checksum;
	stream.result = async () => {
		if (!checksum) {
			if (checksums.length > 1) {
				checksum = await crypto.subtle.digest(
					algorithm,
					_concatBuffers(checksums),
				);
				checksum = `${_arrayBufferToBase64(checksum)}-${checksums.length}`;
			} else {
				// Single part -> its base64. Empty input leaves checksums empty, and
				// _arrayBufferToBase64(undefined) is the empty string, matching the
				// "no data digested" result without a dedicated branch.
				checksum = _arrayBufferToBase64(checksums[0]);
			}
			checksums = checksums.map(_arrayBufferToBase64);
		}
		return {
			key: resultKey ?? "s3",
			value: { checksum, checksums, partSize },
		};
	};
	return stream;
};

const _algorithms = {
	// AWS_NAME: NODE_NAME
	SHA1: "SHA-1",
	SHA256: "SHA-256",
	// CRC32: '',
	// CRC32C: '',
};
const _concatBuffers = (buffers) => {
	const tmp = new Uint8Array(
		buffers.reduce((byteLength, buffer) => byteLength + buffer.byteLength, 0),
	);
	let byteLength = 0;
	for (let i = 0, l = buffers.length; i < l; i++) {
		tmp.set(new Uint8Array(buffers[i]), byteLength);
		byteLength += buffers[i].byteLength;
	}
	return tmp.buffer;
};
const _arrayBufferToBase64 = (buffer) => {
	const bytes = new Uint8Array(buffer);
	return btoa(String.fromCharCode(...bytes));
};

export default {
	setClient: awsS3SetClient,
	getObjectStream: awsS3GetObjectStream,
	putObjectStream: awsS3PutObjectStream,
	checksumStream: awsS3ChecksumStream,
};
