import { deepStrictEqual, rejects } from "node:assert";
import test from "node:test";
import {
	CreateMultipartUploadCommand,
	GetObjectCommand,
	PutObjectCommand,
	S3Client,
	UploadPartCommand,
} from "@aws-sdk/client-s3";
import s3Default, {
	awsS3ChecksumStream,
	awsS3GetObjectStream,
	awsS3PutObjectStream,
	awsS3SetClient,
} from "@datastream/aws/s3";
import {
	createReadableStream,
	pipeline,
	streamToString,
} from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

test(`${variant}: awsS3GetObjectStream should return chunks`, async (_t) => {
	const client = mockClient(S3Client);
	awsS3SetClient(client);
	client
		.on(GetObjectCommand, {
			Bucket: "bucket",
			Key: "file.ext",
		})
		.resolves({
			Body: createReadableStream("contents"),
		});

	const options = {
		Bucket: "bucket",
		Key: "file.ext",
	};
	const stream = await awsS3GetObjectStream(options);
	const output = await streamToString(stream);

	deepStrictEqual(output, "contents");
});

test(`${variant}: awsS3GetObjectStream should throw error when Body is null`, async (_t) => {
	const client = mockClient(S3Client);
	awsS3SetClient(client);
	client
		.on(GetObjectCommand, {
			Bucket: "bucket",
			Key: "file.ext",
		})
		.resolves({});

	const options = {
		Bucket: "bucket",
		Key: "file.ext",
	};

	try {
		await awsS3GetObjectStream(options);
		throw new Error("Expected error was not thrown");
	} catch (error) {
		deepStrictEqual(error.message, "S3.GetObject not found");
	}
});

test(`${variant}: awsS3PutObjectStream should put chunks`, async (_t) => {
	const client = mockClient(S3Client);

	// Hack to fix mock
	const defaultClient = new S3Client();
	client.config ??= {};
	client.config.requestChecksumCalculation ??=
		defaultClient.config.requestChecksumCalculation;

	awsS3SetClient(client);
	const input = "x".repeat(6 * 1024 * 1024);
	const options = {
		Bucket: "bucket",
		Key: "file.ext",
	};

	client
		.on(PutObjectCommand)
		.rejects()
		.on(CreateMultipartUploadCommand)
		.resolves({ UploadId: "1" })
		.on(UploadPartCommand)
		.resolves({ ETag: "1" });

	const stream = [createReadableStream(input), awsS3PutObjectStream(options)];
	const result = await pipeline(stream);

	deepStrictEqual(result, {});
});

test(`${variant}: awsS3PutObjectStream should put chunks with onProgress option`, async (_t) => {
	const client = mockClient(S3Client);

	// Hack to fix mock
	const defaultClient = new S3Client();
	client.config ??= {};
	client.config.requestChecksumCalculation ??=
		defaultClient.config.requestChecksumCalculation;

	awsS3SetClient(client);
	const input = "x".repeat(6 * 1024 * 1024);

	const options = {
		Bucket: "bucket",
		Key: "file.ext",
		onProgress: () => {},
	};

	client
		.on(PutObjectCommand)
		.rejects()
		.on(CreateMultipartUploadCommand)
		.resolves({ UploadId: "1" })
		.on(UploadPartCommand)
		.resolves({ ETag: "1" });

	const stream = [createReadableStream(input), awsS3PutObjectStream(options)];
	const result = await pipeline(stream);

	deepStrictEqual(result, {});
});

test(`${variant}: awsS3PutObjectStream should put chunks with tags`, async (_t) => {
	const client = mockClient(S3Client);

	// Hack to fix mock
	const defaultClient = new S3Client();
	client.config ??= {};
	client.config.requestChecksumCalculation ??=
		defaultClient.config.requestChecksumCalculation;

	awsS3SetClient(client);
	const input = "x".repeat(6 * 1024 * 1024);

	const options = {
		Bucket: "bucket",
		Key: "file.ext",
		tags: [{ Key: "env", Value: "test" }],
	};

	client
		.on(PutObjectCommand)
		.rejects()
		.on(CreateMultipartUploadCommand)
		.resolves({ UploadId: "1" })
		.on(UploadPartCommand)
		.resolves({ ETag: "1" });

	const stream = [createReadableStream(input), awsS3PutObjectStream(options)];
	const result = await pipeline(stream);

	deepStrictEqual(result, {});
});

test(`${variant}: awsS3PutObjectStream should use custom client option`, async (_t) => {
	const client = mockClient(S3Client);

	// Hack to fix mock
	const defaultClient = new S3Client();
	client.config ??= {};
	client.config.requestChecksumCalculation ??=
		defaultClient.config.requestChecksumCalculation;

	const input = "x".repeat(6 * 1024 * 1024);

	const options = {
		Bucket: "bucket",
		Key: "file.ext",
		client,
	};

	client
		.on(PutObjectCommand)
		.rejects()
		.on(CreateMultipartUploadCommand)
		.resolves({ UploadId: "1" })
		.on(UploadPartCommand)
		.resolves({ ETag: "1" });

	const stream = [createReadableStream(input), awsS3PutObjectStream(options)];
	const result = await pipeline(stream);

	deepStrictEqual(result, {});
});

test(`${variant}: awsS3ChecksumStream should make checksum of 16KB string (1 chunk)`, async (_t) => {
	const input = "x".repeat(1 * 16_384);
	const options = {
		ChecksumAlgorithm: "SHA256",
	};

	const stream = [createReadableStream(input), awsS3ChecksumStream(options)];
	const result = await pipeline(stream);

	deepStrictEqual(result, {
		s3: {
			checksum: "FTbEIsMcyYg0dZ1whc2jlKNRCgPXgYgkiYamsacgfQM=",
			checksums: ["FTbEIsMcyYg0dZ1whc2jlKNRCgPXgYgkiYamsacgfQM="],
			partSize: 17_179_870,
		},
	});
});

test(`${variant}: awsS3ChecksumStream should make checksum of 16KB string (2 chunk)`, async (_t) => {
	const input = "x".repeat(2 * 16_384);
	const options = {
		ChecksumAlgorithm: "SHA256",
	};

	const stream = [createReadableStream(input), awsS3ChecksumStream(options)];
	const result = await pipeline(stream);

	deepStrictEqual(result, {
		s3: {
			checksum: "Qnll9JqFcXTjCGWCJzJdvSP/Tsy+OZ1a1IF92j7Hn4c=",
			checksums: ["Qnll9JqFcXTjCGWCJzJdvSP/Tsy+OZ1a1IF92j7Hn4c="],
			partSize: 17_179_870,
		},
	});
});

test(`${variant}: awsS3ChecksumStream should make checksum of 16KB string with SHA1`, async (_t) => {
	const input = "x".repeat(1 * 16_384);
	const options = {
		ChecksumAlgorithm: "SHA1",
	};

	const stream = [createReadableStream(input), awsS3ChecksumStream(options)];
	const result = await pipeline(stream);

	deepStrictEqual(result, {
		s3: {
			checksum: "XhuZUWG9FmZw1UqD0xSn0ik7bD0=",
			checksums: ["XhuZUWG9FmZw1UqD0xSn0ik7bD0="],
			partSize: 17_179_870,
		},
	});
});

test(`${variant}: awsS3ChecksumStream should make multi-part checksum with small partSize`, async (_t) => {
	const input = "x".repeat(100);
	const options = {
		ChecksumAlgorithm: "SHA256",
		partSize: 50,
	};

	const stream = [createReadableStream(input), awsS3ChecksumStream(options)];
	const result = await pipeline(stream);

	deepStrictEqual(result.s3.checksums.length, 2);
	deepStrictEqual(result.s3.partSize, 50);
});

test(`${variant}: awsS3ChecksumStream should make checksum with custom resultKey`, async (_t) => {
	const input = "x".repeat(16_384);
	const options = {
		ChecksumAlgorithm: "SHA256",
		resultKey: "checksum",
	};

	const stream = [createReadableStream(input), awsS3ChecksumStream(options)];
	const result = await pipeline(stream);

	deepStrictEqual(
		result.checksum.checksum,
		"FTbEIsMcyYg0dZ1whc2jlKNRCgPXgYgkiYamsacgfQM=",
	);
});

test(`${variant}: awsS3ChecksumStream should handle Uint8Array input`, async (_t) => {
	const input = new TextEncoder().encode("x".repeat(100));
	const options = {
		ChecksumAlgorithm: "SHA256",
		partSize: 50,
	};

	const stream = [createReadableStream(input), awsS3ChecksumStream(options)];
	const result = await pipeline(stream);

	deepStrictEqual(result.s3.checksums.length, 2);
});

test(`${variant}: awsS3GetObjectStream should use custom client option`, async (_t) => {
	const client = mockClient(S3Client);
	client
		.on(GetObjectCommand, {
			Bucket: "bucket",
			Key: "file.ext",
		})
		.resolves({
			Body: createReadableStream("custom-client"),
		});

	const options = {
		Bucket: "bucket",
		Key: "file.ext",
		client,
	};
	const stream = await awsS3GetObjectStream(options);
	const output = await streamToString(stream);

	deepStrictEqual(output, "custom-client");
});

test(`${variant}: awsS3ChecksumStream should use default options`, async (_t) => {
	const input = "x".repeat(100);

	const stream = [createReadableStream(input), awsS3ChecksumStream()];
	const result = await pipeline(stream);

	deepStrictEqual(result.s3.partSize, 17_179_870);
	deepStrictEqual(result.s3.checksums.length, 1);
});

test(`${variant}: awsS3ChecksumStream should cache result on second call`, async (_t) => {
	const input = "x".repeat(100);
	const options = {
		ChecksumAlgorithm: "SHA256",
	};

	const checksumStream = awsS3ChecksumStream(options);
	const stream = [createReadableStream(input), checksumStream];
	await pipeline(stream);

	const result1 = await checksumStream.result();
	const result2 = await checksumStream.result();

	deepStrictEqual(result1, result2);
});

test(`${variant}: awsS3GetObjectStream should pass abort signal to client.send`, async (_t) => {
	const client = mockClient(S3Client);
	client.on(GetObjectCommand).resolves({
		Body: createReadableStream("data"),
	});

	const controller = new AbortController();
	await awsS3GetObjectStream(
		{ Bucket: "b", Key: "k", client },
		{ signal: controller.signal },
	);

	const calls = client.commandCalls(GetObjectCommand);
	deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
});

// setClient must STORE the passed client; a plain stub (prototype-mock-proof)
// proves the stored reference is used. A `setClient(){}` mutant leaves the prior
// client in place and the stub's send would never run.
test(`${variant}: awsS3SetClient stores the passed client reference`, async (_t) => {
	let calls = 0;
	const stub = {
		send: async () => {
			calls++;
			return { Body: createReadableStream("stub-data") };
		},
	};
	awsS3SetClient(stub);

	const stream = await awsS3GetObjectStream({ Bucket: "b", Key: "k" });
	const output = await streamToString(stream);

	deepStrictEqual(output, "stub-data");
	deepStrictEqual(calls, 1);
});

// The "not found" error carries the request params as its cause (kills the `{}`
// object-literal mutant on the error options).
test(`${variant}: awsS3GetObjectStream not-found error cause is the request params`, async (_t) => {
	const client = mockClient(S3Client);
	awsS3SetClient(client);
	client.on(GetObjectCommand).resolves({});

	const params = { Bucket: "bucket", Key: "missing.ext" };
	await rejects(
		() => awsS3GetObjectStream({ ...params }),
		(error) => {
			deepStrictEqual(error.message, "S3.GetObject not found");
			deepStrictEqual(error.cause, params);
			return true;
		},
	);
});

// On the node build the returned stream is a Readable; its 'error' event must
// tear down the SDK Body (a node Readable -> destroy()) so the socket is
// released. This pins the unconditional `stream.on("error", teardownBody)`
// wiring and the `Body.destroy()` call inside teardownBody.
test(`${variant}: awsS3GetObjectStream tears down the Body when the stream errors`, async (_t) => {
	let destroyed = 0;
	// An async-iterable Body that also exposes a destroy() spy (the node SDK Body
	// is a Readable).
	const body = {
		async *[Symbol.asyncIterator]() {
			yield "chunk";
		},
		destroy() {
			destroyed++;
		},
	};
	const stub = { send: async () => ({ Body: body }) };
	awsS3SetClient(stub);

	const stream = await awsS3GetObjectStream({ Bucket: "b", Key: "k" });
	// Emit an error on the returned node Readable -> the wired teardown runs.
	await new Promise((resolve) => {
		stream.on("error", () => resolve());
		stream.destroy(new Error("boom"));
	});
	// Allow the 'error' listener (teardownBody) to run.
	await new Promise((resolve) => setImmediate(resolve));

	deepStrictEqual(destroyed, 1);
});

// An abort signal that fires AFTER the stream is created must tear down the Body
// via the addEventListener("abort", ...) wiring. Pins `if (signal)`, the else
// branch, the "abort" event-name literal, and proves teardown is not run eagerly.
test(`${variant}: awsS3GetObjectStream tears down the Body when a later abort fires`, async (_t) => {
	let destroyed = 0;
	const body = {
		async *[Symbol.asyncIterator]() {
			yield "chunk";
		},
		destroy() {
			destroyed++;
		},
	};
	const stub = { send: async () => ({ Body: body }) };
	awsS3SetClient(stub);

	const controller = new AbortController();
	const stream = await awsS3GetObjectStream(
		{ Bucket: "b", Key: "k" },
		{ signal: controller.signal },
	);
	// Not aborted yet: teardown must NOT have run (kills `if (signal.aborted)` ->
	// true, which would tear down eagerly).
	deepStrictEqual(destroyed, 0);

	controller.abort();
	await new Promise((resolve) => setImmediate(resolve));
	// The "abort" listener fired teardownBody (the underlying Readable may also
	// surface the abort via its 'error' event, so teardown can run more than once;
	// it is idempotent). The key assertion is that it ran at all post-abort.
	deepStrictEqual(destroyed >= 1, true);

	// Cleanup so the test does not leak the open stream.
	stream.destroy();
});

// Pin the EXACT addEventListener wiring for the late-abort path. A non-aborted
// signal whose addEventListener is recorded proves the source registers
// teardownBody under the "abort" event name with `{ once: true }`. The source's
// listener is identified by being the one whose invocation tears down the Body
// (the node Readable's own internal "abort" listener does not touch Body). This
// kills: dropping the else block (no registration), the "" event-name mutant,
// the `{}` options mutant, and the `{ once: false }` mutant.
test(`${variant}: awsS3GetObjectStream registers the abort listener with the exact name and options`, async (_t) => {
	let destroyed = 0;
	const body = {
		async *[Symbol.asyncIterator]() {
			yield "chunk";
		},
		destroy() {
			destroyed++;
		},
	};
	const stub = { send: async () => ({ Body: body }) };
	awsS3SetClient(stub);

	const recorded = [];
	// A duck-typed (non-aborted) signal: createReadableStream and the source both
	// register on it; we record every addEventListener call.
	const signal = {
		aborted: false,
		addEventListener: (name, fn, options) => {
			recorded.push({ name, fn, options });
		},
		removeEventListener: () => {},
	};

	const stream = await awsS3GetObjectStream(
		{ Bucket: "b", Key: "k" },
		{ signal },
	);
	// teardown must NOT have run eagerly (signal is not aborted).
	deepStrictEqual(destroyed, 0);

	// Identify the source's teardown registration: it is the recorded entry whose
	// listener, when invoked, tears down the Body.
	let teardownEntry;
	for (const entry of recorded) {
		const before = destroyed;
		entry.fn();
		if (destroyed > before) {
			teardownEntry = entry;
			break;
		}
	}
	deepStrictEqual(teardownEntry?.name, "abort");
	deepStrictEqual(teardownEntry?.options, { once: true });

	stream.destroy();
});

// An already-aborted signal tears down the Body eagerly during creation. Pins
// `if (signal.aborted)` (a `false` mutant would skip the eager teardown).
test(`${variant}: awsS3GetObjectStream tears down the Body immediately for a pre-aborted signal`, async (_t) => {
	let destroyed = 0;
	const body = {
		async *[Symbol.asyncIterator]() {
			yield "chunk";
		},
		destroy() {
			destroyed++;
		},
	};
	const stub = { send: async () => ({ Body: body }) };
	awsS3SetClient(stub);

	const controller = new AbortController();
	controller.abort();
	const stream = await awsS3GetObjectStream(
		{ Bucket: "b", Key: "k" },
		{ signal: controller.signal },
	);
	// Eager teardown happened synchronously during the call.
	deepStrictEqual(destroyed, 1);
	stream.destroy();
});

// onProgress must be wired to the upload's 'httpUploadProgress' event. Pins the
// `if (onProgress)` branch and the "httpUploadProgress" event-name literal.
test(`${variant}: awsS3PutObjectStream forwards httpUploadProgress to onProgress`, async (_t) => {
	const client = mockClient(S3Client);
	const defaultClient = new S3Client();
	client.config ??= {};
	client.config.requestChecksumCalculation ??=
		defaultClient.config.requestChecksumCalculation;
	awsS3SetClient(client);

	client
		.on(PutObjectCommand)
		.rejects()
		.on(CreateMultipartUploadCommand)
		.resolves({ UploadId: "1" })
		.on(UploadPartCommand)
		.resolves({ ETag: "1" });

	let progressEvents = 0;
	const options = {
		Bucket: "bucket",
		Key: "file.ext",
		onProgress: () => {
			progressEvents++;
		},
	};

	const stream = awsS3PutObjectStream(options);
	// Emitting 'httpUploadProgress' must reach onProgress. A `""` event-name mutant
	// or a skipped `if (onProgress)` would never invoke the callback.
	stream.emit("httpUploadProgress", { loaded: 1, total: 1 });
	deepStrictEqual(progressEvents, 1);

	const input = "x".repeat(6 * 1024 * 1024);
	const result = await pipeline([createReadableStream(input), stream]);
	deepStrictEqual(result, {});
});

// An unsupported ChecksumAlgorithm throws with an informative message (pins the
// `if (!algorithm)` branch and the non-empty error template).
test(`${variant}: awsS3ChecksumStream throws for an unsupported ChecksumAlgorithm`, async (_t) => {
	let threw;
	try {
		awsS3ChecksumStream({ ChecksumAlgorithm: "NOPE" });
	} catch (error) {
		threw = error;
	}
	deepStrictEqual(threw?.message, "Unsupported ChecksumAlgorithm: NOPE");
});

// Empty input -> no parts digested -> checksum is the empty string and there are
// zero part checksums. Pins `if (bytes.byteLength)` (a `true` mutant would digest
// the empty buffer), the `else` empty-string branch and that string literal.
test(`${variant}: awsS3ChecksumStream returns empty checksum for empty input`, async (_t) => {
	const stream = [createReadableStream([]), awsS3ChecksumStream({})];
	const result = await pipeline(stream);
	deepStrictEqual(result.s3.checksum, "");
	deepStrictEqual(result.s3.checksums.length, 0);
});

// Single-part input -> exactly one part checksum and the result checksum is that
// single part's base64 (NOT the multi-part composite). Pins `checksums.length > 1`
// (false branch) and `checksums.length === 1`.
test(`${variant}: awsS3ChecksumStream single-part checksum equals the only part`, async (_t) => {
	const input = "x".repeat(16_384);
	const stream = [
		createReadableStream(input),
		awsS3ChecksumStream({ ChecksumAlgorithm: "SHA256" }),
	];
	const result = await pipeline(stream);
	deepStrictEqual(result.s3.checksums.length, 1);
	// For a single part, checksum === the lone part checksum (no `-N` suffix).
	deepStrictEqual(result.s3.checksum, result.s3.checksums[0]);
});

// Multi-part input -> composite checksum carries the `-<count>` suffix (kills the
// empty-string-literal mutant on the composite template) and differs from any
// single part. Also re-calling result() returns the identical cached value
// (pins `if (!checksum)` memoization: a `true` mutant would recompute over the
// already-base64'd checksums and produce a different value).
test(`${variant}: awsS3ChecksumStream multi-part composite checksum is suffixed and cached`, async (_t) => {
	const input = "x".repeat(100);
	const checksumStream = awsS3ChecksumStream({
		ChecksumAlgorithm: "SHA256",
		partSize: 50,
	});
	await pipeline([createReadableStream(input), checksumStream]);

	const result1 = await checksumStream.result();
	deepStrictEqual(result1.value.checksums.length, 2);
	// Pin the exact composite and per-part digests. A memoization mutant
	// (`if (true)`) recomputes on the second call over the already-base64'd
	// `checksums`, which the first call below would expose as a different value;
	// even the first call's value must be the real composite digest (not the
	// empty-input digest produced when the recompute path runs prematurely).
	deepStrictEqual(result1.value.checksums, [
		"d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=",
		"d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=",
	]);
	deepStrictEqual(
		result1.value.checksum,
		"//kFcRsCAXRHbjZsPUmCkRBx+J1hJiSiqKAF/q7oMi0=-2",
	);
	// Composite form: "<base64>-<count>".
	deepStrictEqual(result1.value.checksum.endsWith("-2"), true);

	const result2 = await checksumStream.result();
	deepStrictEqual(result1, result2);
	deepStrictEqual(result2.value.checksum, result1.value.checksum);
});

// Input exactly equal to partSize stays a single part (the trailing partial is
// digested by the flush, not split off).
test(`${variant}: awsS3ChecksumStream keeps input exactly equal to partSize as one part`, async (_t) => {
	const input = "x".repeat(50);
	const stream = [
		createReadableStream(input),
		awsS3ChecksumStream({ ChecksumAlgorithm: "SHA256", partSize: 50 }),
	];
	const result = await pipeline(stream);
	deepStrictEqual(result.s3.checksums.length, 1);
});

// Streaming part boundaries across MULTIPLE chunks pin the per-chunk peel loop:
// each 60-byte chunk completes exactly one 50-byte part and carries a sub-part
// remainder forward, so the stream yields parts [50, 50, 20]. This pins
// `Math.floor(bytes.byteLength / partSize)` (dropping the floor over-peels the
// carried remainder into extra short parts -> 4 parts), the `part < wholeParts`
// loop bound (a `<=` over-iterates) and the `bytes.slice(partSize)` advance.
test(`${variant}: awsS3ChecksumStream peels whole parts across chunks and carries the remainder`, async (_t) => {
	const stream = [
		createReadableStream(["x".repeat(60), "x".repeat(60)]),
		awsS3ChecksumStream({ ChecksumAlgorithm: "SHA256", partSize: 50 }),
	];
	const result = await pipeline(stream);
	deepStrictEqual(result.s3.checksums.length, 3);
	// Two complete 50-byte parts (identical bytes -> identical digest) and a
	// trailing 20-byte part.
	deepStrictEqual(result.s3.checksums, [
		"d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=",
		"d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=",
		"1PwdtmVEZQfcUbDJOS3ZZJKRWBv+G0jiQbKwgDKztkc=",
	]);
	deepStrictEqual(
		result.s3.checksum,
		"kE1tc6lRu6Azw5+k/yKQ/QDXDG236y62PebJpxEZQhQ=-3",
	);
});

test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(s3Default).sort(), [
		"checksumStream",
		"getObjectStream",
		"putObjectStream",
		"setClient",
	]);
});

// The teardownBody try/catch must swallow an error from Body.destroy() without
// re-throwing. Pins the empty `catch {}`.
test(`${variant}: awsS3GetObjectStream teardownBody swallows errors from destroy`, async (_t) => {
	const body = {
		async *[Symbol.asyncIterator]() {
			yield "chunk";
		},
		destroy() {
			throw new Error("destroy failed");
		},
	};
	const stub = { send: async () => ({ Body: body }) };
	awsS3SetClient(stub);

	const stream = await awsS3GetObjectStream({ Bucket: "b", Key: "k" });
	// Emit an error: teardownBody runs and both try/catch blocks execute. Neither
	// throw may propagate (a rethrow mutant would cause an unhandled rejection here).
	await new Promise((resolve) => {
		stream.on("error", () => resolve());
		stream.destroy(new Error("boom"));
	});
	await new Promise((resolve) => setImmediate(resolve));
	// If we reach here, the errors were swallowed correctly.
});
