import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import { createHash } from "node:crypto";
import { getEventListeners } from "node:events";
import test, { describe } from "node:test";
import {
	CreateMultipartUploadCommand,
	GetObjectCommand,
	ListObjectsV2Command,
	PutObjectCommand,
	S3Client,
	UploadPartCommand,
} from "@aws-sdk/client-s3";
import * as awsModule from "@datastream/aws/s3";
import {
	awsS3ChecksumStream,
	awsS3GetObjectStream,
	awsS3ListObjectsStream,
	awsS3PutObjectStream,
	awsS3SetClient,
} from "@datastream/aws/s3";
import {
	createReadableStream,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";
import { variant } from "../variant.js";

describe(`@datastream/aws/s3 (${variant})`, () => {
	test(`awsS3GetObjectStream should return chunks`, async (_t) => {
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

	test(`awsS3GetObjectStream should throw error when Body is null`, async (_t) => {
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

	test(`awsS3PutObjectStream should put chunks`, async (_t) => {
		const client = mockClient(S3Client);

		const defaultClient = new S3Client();
		client.config ??= {};
		client.config.requestChecksumCalculation ??=
			defaultClient.config.requestChecksumCalculation;

		awsS3SetClient(client);
		const input = "x".repeat(6 * 1024 * 1024);
		const options = {
			Bucket: "bucket",
			Key: "file.ext",
			partSize: 5 * 1024 * 1024,
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

	test(`awsS3PutObjectStream should put chunks with onProgress option`, async (_t) => {
		const client = mockClient(S3Client);

		const defaultClient = new S3Client();
		client.config ??= {};
		client.config.requestChecksumCalculation ??=
			defaultClient.config.requestChecksumCalculation;

		awsS3SetClient(client);
		const input = "x".repeat(6 * 1024 * 1024);

		const options = {
			Bucket: "bucket",
			Key: "file.ext",
			partSize: 5 * 1024 * 1024,
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

	test(`awsS3PutObjectStream should put chunks with tags`, async (_t) => {
		const client = mockClient(S3Client);

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
			partSize: 5 * 1024 * 1024,
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

	test(`awsS3PutObjectStream should use custom client option`, async (_t) => {
		const client = mockClient(S3Client);

		const defaultClient = new S3Client();
		client.config ??= {};
		client.config.requestChecksumCalculation ??=
			defaultClient.config.requestChecksumCalculation;

		const input = "x".repeat(6 * 1024 * 1024);

		const options = {
			Bucket: "bucket",
			Key: "file.ext",
			partSize: 5 * 1024 * 1024,
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

	test(`awsS3ChecksumStream should make checksum of 16KB string (1 chunk)`, async (_t) => {
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

	test(`awsS3ChecksumStream should make checksum of 16KB string (2 chunk)`, async (_t) => {
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

	test(`awsS3ChecksumStream should make checksum of 16KB string with SHA1`, async (_t) => {
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

	test(`awsS3ChecksumStream default partSize is the shared 17_179_870 bytes`, async (_t) => {
		const input = Buffer.alloc(17_179_870 + 1, "x");

		const stream = [createReadableStream([input]), awsS3ChecksumStream()];
		const result = await pipeline(stream);

		deepStrictEqual(result.s3.partSize, 17_179_870);
		deepStrictEqual(result.s3.checksums, [
			createHash("sha256")
				.update(input.subarray(0, 17_179_870))
				.digest("base64"),
			createHash("sha256").update(input.subarray(17_179_870)).digest("base64"),
		]);
	});

	test(`awsS3PutObjectStream default partSize is the shared 17_179_870 bytes`, async (_t) => {
		const client = mockClient(S3Client);
		const defaultClient = new S3Client();
		client.config ??= {};
		client.config.requestChecksumCalculation ??=
			defaultClient.config.requestChecksumCalculation;
		client
			.on(PutObjectCommand)
			.rejects()
			.on(CreateMultipartUploadCommand)
			.resolves({ UploadId: "1" })
			.on(UploadPartCommand)
			.resolves({ ETag: "1" });

		const input = Buffer.alloc(17_179_870 + 1, "x");
		const result = await pipeline([
			createReadableStream([input]),
			awsS3PutObjectStream({ Bucket: "bucket", Key: "file.ext", client }),
		]);

		deepStrictEqual(result, {});
		deepStrictEqual(
			client
				.commandCalls(UploadPartCommand)
				.map((call) => call.args[0].input.Body.byteLength),
			[17_179_870, 1],
		);
	});

	test(`awsS3PutObjectStream treats a null partSize as the shared default`, async (_t) => {
		const client = mockClient(S3Client);
		const defaultClient = new S3Client();
		client.config ??= {};
		client.config.requestChecksumCalculation ??=
			defaultClient.config.requestChecksumCalculation;
		client
			.on(PutObjectCommand)
			.rejects()
			.on(CreateMultipartUploadCommand)
			.resolves({ UploadId: "1" })
			.on(UploadPartCommand)
			.resolves({ ETag: "1" });

		const input = Buffer.alloc(17_179_870 + 1, "x");
		await pipeline([
			createReadableStream([input]),
			awsS3PutObjectStream({
				Bucket: "bucket",
				Key: "file.ext",
				client,
				partSize: null,
			}),
		]);

		deepStrictEqual(
			client
				.commandCalls(UploadPartCommand)
				.map((call) => call.args[0].input.Body.byteLength),
			[17_179_870, 1],
		);
	});

	test(`awsS3ChecksumStream should make multi-part checksum with small partSize`, async (_t) => {
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

	test(`awsS3ChecksumStream should emit a trailing partial part below partSize`, async (_t) => {
		const input = "x".repeat(130);
		const options = {
			ChecksumAlgorithm: "SHA256",
			partSize: 50,
		};

		const stream = [createReadableStream(input), awsS3ChecksumStream(options)];
		const result = await pipeline(stream);

		deepStrictEqual(result.s3.checksums.length, 3);
	});

	test(`awsS3ChecksumStream should assemble parts that span multiple chunks`, async (_t) => {
		const input = Array.from({ length: 4 }, () => new Uint8Array(30).fill(7));
		const options = {
			ChecksumAlgorithm: "SHA256",
			partSize: 50,
		};

		const stream = [createReadableStream(input), awsS3ChecksumStream(options)];
		const result = await pipeline(stream);

		deepStrictEqual(result.s3, {
			checksum: "51ikiYw8AUfLCvxucAGEQoRFEs4bOTPTFAADWHIM4A8=-3",
			checksums: [
				"RzijJRu2PwaWMYZcnxPIrnHdmuNdkxqv5B0qhFCgwtM=",
				"RzijJRu2PwaWMYZcnxPIrnHdmuNdkxqv5B0qhFCgwtM=",
				"Cx833lYKNrjROAIxe7FcGpjM7kL8iLVZz5IWVhtD+Ho=",
			],
			partSize: 50,
		});
	});

	test(`awsS3ChecksumStream should make checksum with custom resultKey`, async (_t) => {
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

	test(`awsS3ChecksumStream should handle Uint8Array input`, async (_t) => {
		const input = new TextEncoder().encode("x".repeat(100));
		const options = {
			ChecksumAlgorithm: "SHA256",
			partSize: 50,
		};

		const stream = [createReadableStream(input), awsS3ChecksumStream(options)];
		const result = await pipeline(stream);

		deepStrictEqual(result.s3, {
			checksum: "//kFcRsCAXRHbjZsPUmCkRBx+J1hJiSiqKAF/q7oMi0=-2",
			checksums: [
				"d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=",
				"d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=",
			],
			partSize: 50,
		});
	});

	test(`awsS3ChecksumStream should handle ArrayBuffer input`, async (_t) => {
		const input = [new TextEncoder().encode("x".repeat(100)).buffer];
		const options = {
			ChecksumAlgorithm: "SHA256",
			partSize: 50,
		};

		const stream = [createReadableStream(input), awsS3ChecksumStream(options)];
		const result = await pipeline(stream);

		deepStrictEqual(result.s3, {
			checksum: "//kFcRsCAXRHbjZsPUmCkRBx+J1hJiSiqKAF/q7oMi0=-2",
			checksums: [
				"d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=",
				"d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=",
			],
			partSize: 50,
		});
	});

	test(`awsS3GetObjectStream should use custom client option`, async (_t) => {
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

	test(`awsS3ChecksumStream should use default options`, async (_t) => {
		const input = "x".repeat(100);

		const stream = [createReadableStream(input), awsS3ChecksumStream()];
		const result = await pipeline(stream);

		deepStrictEqual(result.s3.partSize, 17_179_870);
		deepStrictEqual(result.s3.checksums.length, 1);
	});

	test(`awsS3ChecksumStream should cache result on second call`, async (_t) => {
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

	test(`awsS3GetObjectStream should pass abort signal to client.send`, async (_t) => {
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
	test(`awsS3SetClient stores the passed client reference`, async (_t) => {
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

	test(`awsS3GetObjectStream not-found error cause is only Bucket and Key`, async (_t) => {
		const client = mockClient(S3Client);
		awsS3SetClient(client);
		client.on(GetObjectCommand).resolves({});

		await rejects(
			() =>
				awsS3GetObjectStream({
					Bucket: "bucket",
					Key: "missing.ext",
					SSECustomerAlgorithm: "AES256",
					SSECustomerKey: "secret-key",
				}),
			(error) => {
				deepStrictEqual(error.message, "S3.GetObject not found");
				deepStrictEqual(error.cause, { Bucket: "bucket", Key: "missing.ext" });
				return true;
			},
		);
	});

	test(`awsS3GetObjectStream tears down the Body when the stream errors`, async (_t) => {
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

		const stream = await awsS3GetObjectStream({ Bucket: "b", Key: "k" });
		await new Promise((resolve) => {
			stream.on("error", () => resolve());
			stream.destroy(new Error("boom"));
		});
		await new Promise((resolve) => setImmediate(resolve));

		deepStrictEqual(destroyed, 1);
	});

	test(`awsS3GetObjectStream tears down the Body when a later abort fires`, async (_t) => {
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
		deepStrictEqual(destroyed, 0);

		controller.abort();
		await new Promise((resolve) => setImmediate(resolve));
		// The "abort" listener fired teardownBody (the underlying Readable may also
		// surface the abort via its 'error' event, so teardown can run more than once;
		// it is idempotent). The key assertion is that it ran at all post-abort.
		deepStrictEqual(destroyed >= 1, true);

		stream.destroy();
	});

	test(`awsS3GetObjectStream registers the abort listener with the exact name and options`, async (_t) => {
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
		deepStrictEqual(destroyed, 0);

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

	test(`awsS3GetObjectStream removes its abort listener once the stream closes`, async (_t) => {
		const stub = {
			send: async () => ({
				Body: {
					async *[Symbol.asyncIterator]() {
						yield "chunk";
					},
					destroy() {},
				},
			}),
		};
		awsS3SetClient(stub);

		const controller = new AbortController();
		for (let i = 0; i < 3; i++) {
			const stream = await awsS3GetObjectStream(
				{ Bucket: "b", Key: "k" },
				{ signal: controller.signal },
			);
			deepStrictEqual(await streamToString(stream), "chunk");
		}
		await new Promise((resolve) => setImmediate(resolve));
		deepStrictEqual(getEventListeners(controller.signal, "abort").length, 0);
	});

	test(`awsS3GetObjectStream tears down the Body immediately for a pre-aborted signal`, async (_t) => {
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
		deepStrictEqual(destroyed, 1);
		stream.destroy();
	});

	test(`awsS3PutObjectStream forwards httpUploadProgress to onProgress`, async (_t) => {
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

		const loaded = [];
		const options = {
			Bucket: "bucket",
			Key: "file.ext",
			partSize: 5 * 1024 * 1024,
			onProgress: (progress) => {
				loaded.push(progress.loaded);
			},
		};

		// lib-storage emits 'httpUploadProgress' on the Upload instance (not the
		// Body stream) once per uploaded part. A 6MiB body is two parts (5MiB +
		// 1MiB). A `""` event-name mutant or a skipped `if (onProgress)` would
		// never invoke the callback.
		const input = "x".repeat(6 * 1024 * 1024);
		const result = await pipeline([
			createReadableStream(input),
			awsS3PutObjectStream(options),
		]);
		deepStrictEqual(result, {});
		deepStrictEqual(
			loaded.sort((x, y) => x - y),
			[5 * 1024 * 1024, 6 * 1024 * 1024],
		);
	});

	test(`awsS3PutObjectStream upstream error does not cause an unhandled rejection`, async (_t) => {
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

		const unhandled = [];
		const onUnhandled = (reason) => {
			unhandled.push(reason);
		};
		process.on("unhandledRejection", onUnhandled);
		try {
			const source = createReadableStream(
				(async function* () {
					yield "x";
					throw new Error("upstream failed");
				})(),
			);
			const stream = awsS3PutObjectStream({
				Bucket: "bucket",
				Key: "file.ext",
			});
			await rejects(() => pipeline([source, stream]), {
				message: "upstream failed",
			});
			await rejects(() => stream.result());
			await new Promise((resolve) => setImmediate(resolve));
			deepStrictEqual(unhandled, []);
		} finally {
			process.off("unhandledRejection", onUnhandled);
		}
	});

	test(`awsS3PutObjectStream surfaces the SDK error from the upload`, async (_t) => {
		const client = mockClient(S3Client);
		const defaultClient = new S3Client();
		client.config ??= {};
		client.config.requestChecksumCalculation ??=
			defaultClient.config.requestChecksumCalculation;
		const accessDenied = new Error("Access Denied");
		accessDenied.name = "AccessDenied";
		client
			.on(PutObjectCommand)
			.rejects(accessDenied)
			.on(CreateMultipartUploadCommand)
			.rejects(accessDenied);

		// More input than one queued part: the failed Upload stops draining the
		// Body, so without forwarding its error the pipeline ends in AbortError.
		const chunk = "x".repeat(64 * 1024);
		async function* source() {
			for (let i = 0; i < 11 * 16; i++) yield chunk;
		}
		const stream = awsS3PutObjectStream({
			Bucket: "bucket",
			Key: "file.ext",
			client,
			queueSize: 1,
			partSize: 5 * 1024 * 1024,
		});
		await rejects(() => pipeline([createReadableStream(source()), stream]), {
			name: "AccessDenied",
			message: "Access Denied",
		});
	});

	test(`awsS3PutObjectStream destroys the stream with the upload error`, async (_t) => {
		const client = mockClient(S3Client);
		const defaultClient = new S3Client();
		client.config ??= {};
		client.config.requestChecksumCalculation ??=
			defaultClient.config.requestChecksumCalculation;
		const accessDenied = new Error("Access Denied");
		accessDenied.name = "AccessDenied";
		client.on(CreateMultipartUploadCommand).rejects(accessDenied);

		const stream = awsS3PutObjectStream({
			Bucket: "bucket",
			Key: "file.ext",
			client,
			// A single uploader: done() settles once it fails, without waiting on
			// idle uploaders for input that never comes.
			queueSize: 1,
			partSize: 5 * 1024 * 1024,
		});
		stream.on("error", () => {});
		// More than one 5MiB part, left open: forces CreateMultipartUpload.
		stream.write(Buffer.alloc(6 * 1024 * 1024));
		await rejects(() => stream.result(), accessDenied);
		strictEqual(stream.errored, accessDenied);
	});

	test(`awsS3ChecksumStream throws for an unsupported ChecksumAlgorithm`, async (_t) => {
		let threw;
		try {
			awsS3ChecksumStream({ ChecksumAlgorithm: "NOPE" });
		} catch (error) {
			threw = error;
		}
		deepStrictEqual(threw?.message, "Unsupported ChecksumAlgorithm: NOPE");
	});

	test(`awsS3ChecksumStream returns the empty-input SHA256 digest for empty input`, async (_t) => {
		const stream = [createReadableStream([]), awsS3ChecksumStream({})];
		const result = await pipeline(stream);
		deepStrictEqual(
			result.s3.checksum,
			"47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
		);
		deepStrictEqual(result.s3.checksums, [
			"47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
		]);
	});

	test(`awsS3ChecksumStream returns the empty-input SHA1 digest for empty input`, async (_t) => {
		const stream = [
			createReadableStream([]),
			awsS3ChecksumStream({ ChecksumAlgorithm: "SHA1" }),
		];
		const result = await pipeline(stream);
		deepStrictEqual(result.s3.checksum, "2jmj7l5rSw0yVb/vlWAYkK/YBwk=");
	});

	test(`awsS3ChecksumStream single-part checksum equals the only part`, async (_t) => {
		const input = "x".repeat(16_384);
		const stream = [
			createReadableStream(input),
			awsS3ChecksumStream({ ChecksumAlgorithm: "SHA256" }),
		];
		const result = await pipeline(stream);
		deepStrictEqual(result.s3.checksums.length, 1);
		deepStrictEqual(result.s3.checksum, result.s3.checksums[0]);
	});

	test(`awsS3ChecksumStream multi-part composite checksum is suffixed and cached`, async (_t) => {
		const input = "x".repeat(100);
		const checksumStream = awsS3ChecksumStream({
			ChecksumAlgorithm: "SHA256",
			partSize: 50,
		});
		await pipeline([createReadableStream(input), checksumStream]);

		const result1 = await checksumStream.result();
		deepStrictEqual(result1.value.checksums.length, 2);
		deepStrictEqual(result1.value.checksums, [
			"d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=",
			"d88SBg1HGD6oxANF5ziefgXLB1PKs3Sl50+TKYFbTLU=",
		]);
		deepStrictEqual(
			result1.value.checksum,
			"//kFcRsCAXRHbjZsPUmCkRBx+J1hJiSiqKAF/q7oMi0=-2",
		);
		deepStrictEqual(result1.value.checksum.endsWith("-2"), true);

		const result2 = await checksumStream.result();
		deepStrictEqual(result1, result2);
		deepStrictEqual(result2.value.checksum, result1.value.checksum);
	});

	test(`awsS3ChecksumStream keeps input exactly equal to partSize as one part`, async (_t) => {
		const input = "x".repeat(50);
		const stream = [
			createReadableStream(input),
			awsS3ChecksumStream({ ChecksumAlgorithm: "SHA256", partSize: 50 }),
		];
		const result = await pipeline(stream);
		deepStrictEqual(result.s3.checksums.length, 1);
	});

	test(`awsS3ChecksumStream peels whole parts across chunks and carries the remainder`, async (_t) => {
		const stream = [
			createReadableStream(["x".repeat(60), "x".repeat(60)]),
			awsS3ChecksumStream({ ChecksumAlgorithm: "SHA256", partSize: 50 }),
		];
		const result = await pipeline(stream);
		deepStrictEqual(result.s3.checksums.length, 3);
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

	test(`awsS3ListObjectsStream yields Contents across ContinuationToken pages`, async (_t) => {
		const pages = [
			{
				Contents: [{ Key: "a" }, { Key: "b" }],
				IsTruncated: true,
				NextContinuationToken: "t1",
			},
			{ Contents: [{ Key: "c" }], IsTruncated: false },
		];
		const inputs = [];
		awsS3SetClient({
			send: async (command) => {
				strictEqual(command instanceof ListObjectsV2Command, true);
				inputs.push({ ...command.input });
				const page = pages.shift();
				if (!page) throw new Error("listed past the last page");
				return page;
			},
		});

		const options = { Bucket: "bucket", Prefix: "p/" };
		const optionsCopy = { ...options };
		const stream = await awsS3ListObjectsStream(options);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ Key: "a" }, { Key: "b" }, { Key: "c" }]);
		deepStrictEqual(inputs, [
			{ Bucket: "bucket", Prefix: "p/" },
			{ Bucket: "bucket", Prefix: "p/", ContinuationToken: "t1" },
		]);
		deepStrictEqual(options, optionsCopy);
	});

	test(`awsS3ListObjectsStream handles a page without Contents`, async (_t) => {
		const client = mockClient(S3Client);
		awsS3SetClient(client);
		client
			.on(ListObjectsV2Command)
			.resolvesOnce({ IsTruncated: true, NextContinuationToken: "t1" })
			.resolvesOnce({ KeyCount: 0 })
			.rejects(new Error("listed past the last page"));

		const stream = await awsS3ListObjectsStream({ Bucket: "bucket" });

		deepStrictEqual(await streamToArray(stream), []);
		deepStrictEqual(client.commandCalls(ListObjectsV2Command).length, 2);
	});

	test(`awsS3ListObjectsStream uses the per-call client option`, async (_t) => {
		awsS3SetClient({
			send: async () => {
				throw new Error("module client used");
			},
		});
		const inputs = [];
		const client = {
			send: async (command) => {
				inputs.push({ ...command.input });
				return { Contents: [{ Key: "a" }] };
			},
		};

		const stream = await awsS3ListObjectsStream({ Bucket: "bucket", client });

		deepStrictEqual(await streamToArray(stream), [{ Key: "a" }]);
		deepStrictEqual(inputs, [{ Bucket: "bucket" }]);
	});

	test(`awsS3ListObjectsStream forwards abortSignal to client.send`, async (_t) => {
		const client = mockClient(S3Client);
		awsS3SetClient(client);
		client.on(ListObjectsV2Command).resolves({ Contents: [{ Key: "a" }] });

		const controller = new AbortController();
		const stream = await awsS3ListObjectsStream(
			{ Bucket: "bucket" },
			{ signal: controller.signal },
		);
		await streamToArray(stream);

		const sent =
			client.commandCalls(ListObjectsV2Command)[0].args[1]?.abortSignal;
		strictEqual(sent.aborted, false);
		controller.abort();
		strictEqual(sent.aborted, true);
	});

	test(`awsS3ListObjectsStream destroy aborts the in-flight request`, async (_t) => {
		let sent;
		awsS3SetClient({
			send: (_command, { abortSignal }) => {
				sent = abortSignal;
				return new Promise((_resolve, reject) => {
					abortSignal.addEventListener("abort", () =>
						reject(abortSignal.reason),
					);
				});
			},
		});
		const stream = await awsS3ListObjectsStream({ Bucket: "bucket" });
		stream.resume();
		await new Promise((resolve) => setImmediate(resolve));
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(sent.aborted, true);
	});

	test(`s3 has no default export`, (_t) => {
		strictEqual(Object.hasOwn(awsModule, "default"), false);
	});

	test(`awsS3GetObjectStream teardownBody swallows errors from destroy`, async (_t) => {
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
		await new Promise((resolve) => {
			stream.on("error", () => resolve());
			stream.destroy(new Error("boom"));
		});
		await new Promise((resolve) => setImmediate(resolve));
	});
});
