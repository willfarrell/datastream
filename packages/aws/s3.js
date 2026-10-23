// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import {
	GetObjectCommand,
	ListObjectsV2Command,
	S3Client,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import {
	createPassThroughStream,
	createReadableStream,
} from "@datastream/core";
import { awsClientDefaults, awsReadableStream } from "./client.js";

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
			stream.once("close", () =>
				signal.removeEventListener("abort", teardownBody),
			);
		}
	}
	return stream;
};

export const awsS3ListObjectsStream = async (options, streamOptions = {}) => {
	const { client, ...listOptions } = options;
	async function* command(opts, signal) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new ListObjectsV2Command(opts),
				{ abortSignal: signal },
			);
			for (const item of response.Contents ?? []) {
				yield item;
			}
			opts.ContinuationToken = response.NextContinuationToken;
			expectMore = !!response.NextContinuationToken;
		}
	}
	return awsReadableStream(
		(signal) => command(listOptions, signal),
		streamOptions,
	);
};

// Shared default part size for awsS3PutObjectStream and awsS3ChecksumStream:
// a composite checksum depends on the part boundaries, so both defaults must
// match. Above S3's 5 MiB multipart minimum; 10,000 parts caps an object at
// ~160 GiB.
const DEFAULT_PART_SIZE = 17_179_870;

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
	const upload = new Upload({
		client: client ?? getDefaultClient(),
		params: {
			...params,
			Body: body,
		},
		tags,
		// ?? (not a destructuring default) so null also means default, as in
		// awsS3ChecksumStream; lib-storage would otherwise fall back to 5 MiB.
		partSize: partSize ?? DEFAULT_PART_SIZE,
		queueSize,
	});
	if (onProgress) {
		upload.on("httpUploadProgress", onProgress);
	}
	const result = upload.done();
	result.catch((error) => stream.destroy(error));

	stream.result = async () => {
		await result;
		return {};
	};
	return stream;
};

export const awsS3ChecksumStream = (
	{ ChecksumAlgorithm, partSize, resultKey } = {},
	streamOptions = {},
) => {
	ChecksumAlgorithm ??= "SHA256";
	partSize ??= DEFAULT_PART_SIZE;
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
		// Remainder is < partSize: a single concat of the leftover chunks. Empty
		// input still digests the (empty) remainder so the checksum is the
		// algorithm's empty-input digest, matching what S3 reports for 0 bytes.
		if (pendingLen > 0 || checksums.length === 0) {
			const hash = createHash(algorithm);
			for (const chunk of pending) {
				hash.update(chunk);
			}
			checksums.push(hash.digest());
		}
	};
	const stream = createPassThroughStream(passThrough, flush, streamOptions);
	stream.result = async () => ({
		key: resultKey ?? "s3",
		value: {
			// Multipart: digest of the concatenated part digests, suffixed with the
			// part count. Single part (including empty input): that part's digest.
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
