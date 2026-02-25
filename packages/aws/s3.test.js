import { deepStrictEqual } from "node:assert";
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

test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(s3Default).sort(), [
		"checksumStream",
		"getObjectStream",
		"putObjectStream",
		"setClient",
	]);
});
