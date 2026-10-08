// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import {
	createPassThroughStream,
	createReadableStream,
} from "@datastream/core";
import { awsClientDefaults } from "./client.js";

// Created on first use, so importing the module (or always passing a per-call
// client) never constructs an unused SDK client.
let defaultClient;
const getDefaultClient = () =>
	(defaultClient ??= new S3Client(awsClientDefaults));
export const awsS3SetClient = (s3Client) => {
	defaultClient = s3Client;
};

export const awsS3GetObjectStream = async (options, streamOptions = {}) => {
	const { client, ...params } = options;
	const { Body } = await (client ?? getDefaultClient()).send(
		new GetObjectCommand(params),
		{ abortSignal: streamOptions.signal },
	);
	if (!Body) {
		// Only Bucket/Key: other params (e.g. SSECustomerKey) are secrets that
		// must not leak into logged error causes.
		throw new Error("S3.GetObject not found", {
			cause: { Bucket: params.Bucket, Key: params.Key },
		});
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
			// Drop the listener once the stream is done so a long-lived signal shared
			// across many reads does not accumulate listeners (each pinning a Body).
			stream.once("close", () =>
				signal.removeEventListener("abort", teardownBody),
			);
		}
	}
	return stream;
};

export const awsS3PutObjectStream = (options, streamOptions = {}) => {
	const { onProgress, client, tags, partSize, queueSize, ...params } = options;
	const stream = createPassThroughStream(() => {}, streamOptions);
	// lib-storage return()s its Body iterator as soon as a request fails, which
	// destroys a node Readable with a generic AbortError before upload.done()
	// rejects. Hand it a detached view so `stream` stays alive and the real SDK
	// error can be forwarded to it below: a for-await (not `yield*`, which would
	// forward Readable.from's throw()) over a destroyOnReturn:false iterator only
	// ever return()s it without destroying `stream`.
	const body = Readable.from(_detach(stream));
	// lib-storage defaults to a 5 MiB partSize and a 10,000-part ceiling
	// (~50 GiB max object). Expose partSize/queueSize so callers can raise the
	// ceiling for very large streamed objects.
	const upload = new Upload({
		client: client ?? getDefaultClient(),
		params: {
			...params,
			Body: body,
		},
		tags,
		partSize,
		queueSize,
	});
	// lib-storage emits progress on the Upload instance, not the Body stream.
	if (onProgress) {
		upload.on("httpUploadProgress", onProgress);
	}
	const result = upload.done();
	// pipeline() only calls stream.result() on success, so handle the rejection
	// here: forwarding it to the returned stream surfaces the real SDK error (e.g.
	// AccessDenied) through pipeline instead of a generic AbortError when the
	// failed Upload stops draining the Body, and avoids an unhandled rejection
	// when an upstream failure aborts the upload. result() still awaits the
	// original promise and rethrows.
	result.catch((error) => stream.destroy(error));

	stream.result = async () => {
		await result;
		return {};
	};
	return stream;
};

// Computes the S3 multipart checksum of a file you want to upload via a
// presigned URL.
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
	const checksums = [];
	const pending = [];
	let pendingLen = 0;
	const digestPart = () => {
		const hash = createHash(algorithm);
		let filled = 0;
		while (filled < partSize) {
			const head = pending[0];
			// Hash as much of the head chunk as the part still needs. `rest` is
			// whatever is left of the head afterwards: drop the head once it is fully
			// consumed, otherwise keep the remainder at the front for the next part.
			const take = Math.min(head.byteLength, partSize - filled);
			hash.update(head.subarray(0, take));
			filled += take;
			const rest = head.subarray(take);
			if (rest.byteLength === 0) {
				pending.shift();
			} else {
				pending[0] = rest;
			}
		}
		pendingLen -= partSize;
		return hash.digest();
	};
	const passThrough = (chunk) => {
		// string -> UTF-8 bytes, ArrayBuffer -> view, Buffer/Uint8Array -> copy;
		// always a Buffer, so digestPart's subarray views are valid.
		chunk = Buffer.from(chunk);
		pending.push(chunk);
		pendingLen += chunk.byteLength;
		// Digest every whole part the buffered bytes can supply; any trailing
		// partial part (< partSize) stays buffered for the next chunk or the flush.
		const wholeParts = Math.floor(pendingLen / partSize);
		for (let part = 0; part < wholeParts; part++) {
			checksums.push(digestPart());
		}
	};
	const flush = () => {
		if (pendingLen > 0) {
			// Remainder is < partSize: a single concat of the leftover chunks.
			checksums.push(
				createHash(algorithm).update(Buffer.concat(pending)).digest(),
			);
		}
	};
	const stream = createPassThroughStream(passThrough, flush, streamOptions);
	// Pure over the collected part digests, so repeated calls return equal values.
	stream.result = async () => ({
		key: resultKey ?? "s3",
		value: {
			// Multipart: digest of the concatenated part digests, suffixed with the
			// part count. Single part: that part's digest. Empty input: concat of
			// no digests, i.e. the empty string.
			checksum:
				checksums.length > 1
					? `${createHash(algorithm).update(Buffer.concat(checksums)).digest("base64")}-${checksums.length}`
					: Buffer.concat(checksums).toString("base64"),
			checksums: checksums.map((checksum) => checksum.toString("base64")),
			partSize,
		},
	});
	return stream;
};

const _algorithms = {
	// AWS_NAME: NODE_NAME
	SHA1: "sha1",
	SHA256: "sha256",
	// CRC32: '',
	// CRC32C: '',
};

async function* _detach(stream) {
	for await (const chunk of stream.iterator({ destroyOnReturn: false })) {
		yield chunk;
	}
}
