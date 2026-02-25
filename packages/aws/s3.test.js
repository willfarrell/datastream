import { deepStrictEqual } from "node:assert";
import test from "node:test";
import {
	CreateMultipartUploadCommand,
	GetObjectCommand,
	PutObjectCommand,
	S3Client,
	UploadPartCommand,
} from "@aws-sdk/client-s3";
import {
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
// import sinon from 'sinon'
import { mockClient } from "aws-sdk-client-mock";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

if (variant === "node") {
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
} else {
	console.info(
		"awsS3PutObjectStream doesn't work with webstreams at this time",
	);
}

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

// test(`${variant}: awsS3ChecksumStream should make checksum of 8MB string (0.5 block)`, async (_t) => {
//   const input = 'x'.repeat(8 * 1024 * 1024)
//   const options = {
//     ChecksumAlgorithm: 'SHA256'
//   }

//   const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
//   const result = await pipeline(stream)

//   deepStrictEqual(result, {
//     s3: {
//       checksum: 'DHe8CgeVqTYS1FJWiXRW0PyyTxUcRMFQ0H7NA/TvUWg=',
//       checksums:['DHe8CgeVqTYS1FJWiXRW0PyyTxUcRMFQ0H7NA/TvUWg='],
//       partSize: 17_179_870
//     }
//   })
// })
// test(`${variant}: awsS3ChecksumStream should make checksum of 16 MB string (1 block)`, async (_t) => {
//   const input = 'x'.repeat(17_179_870)
//   const options = {
//     ChecksumAlgorithm: 'SHA256'
//   }

//   const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
//   const result = await pipeline(stream)

//   deepStrictEqual(result, {
//     s3: {
//       checksum: 'WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o=',
//       checksums:['WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o='],
//       partSize: 17_179_870
//     }
//   })
// })
// test(`${variant}: awsS3ChecksumStream should make checksum of 24MB string (1.5 blocks)`, async (_t) => {
//   const input = 'x'.repeat(24 * 1024 * 1024)
//   const options = {
//     ChecksumAlgorithm: 'SHA256'
//   }

//   const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
//   const result = await pipeline(stream)

//   deepStrictEqual(result, {
//     s3: {
//       checksum: 'eWQzGj3USSV0NvWbhxtpmbkHgNReYxUzwVBXAU86X/4=-2',
//       checksums:[

//               'WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o=',
//               'HLZQKZLvENstyfk2WtaEZGcol2s/v4xvkPX30aqd0XY='],
//       partSize: 17_179_870
//     }
//   })
// })
// test(`${variant}: awsS3ChecksumStream should make checksum of file 32 MB string (2 blocks)`, async (_t) => {
//   const input = 'x'.repeat(17179870 * 2)
//   const options = {
//     ChecksumAlgorithm: 'SHA256'
//   }

//   const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
//   const result = await pipeline(stream)

//   deepStrictEqual(result, {
//     s3: {
//       checksum: '65/QvEoh9MiBIPeSgTqKTptI3Vnf+vaJ1om/MYYMpBU=-2',
//       checksums:[
//               'WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o=',
//               'WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o='],
//       partSize: 17_179_870
//     }
//   })
// })

// BUG? createReadableStream not chunking Uint8Array properly?
// test(`${variant}: awsS3ChecksumStream should make checksum of 16KB ArrayBuffer (1 chunk)`, async (_t) => {
//   const input = new TextEncoder('utf-8').encode('x'.repeat(1*16_384))
//   const options = {
//     ChecksumAlgorithm: 'SHA256'
//   }

//   const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
//   const result = await pipeline(stream)

//   deepStrictEqual(result, {
//     s3: {
//       checksum: 'FTbEIsMcyYg0dZ1whc2jlKNRCgPXgYgkiYamsacgfQM=',
//       checksums:['FTbEIsMcyYg0dZ1whc2jlKNRCgPXgYgkiYamsacgfQM='],
//       partSize: 17_179_870
//     }
//   })
// })

// test(`${variant}: awsS3ChecksumStream should make checksum of 16KB ArrayBuffer (2 chunk)`, async (_t) => {
//   const input = new TextEncoder('utf-8').encode('x'.repeat(2*16_384))
//   const options = {
//     ChecksumAlgorithm: 'SHA256'
//   }

//   const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
//   const result = await pipeline(stream)

//   deepStrictEqual(result, {
//     s3: {
//       checksum: 'Qnll9JqFcXTjCGWCJzJdvSP/Tsy+OZ1a1IF92j7Hn4c=',
//       checksums:['Qnll9JqFcXTjCGWCJzJdvSP/Tsy+OZ1a1IF92j7Hn4c='],
//       partSize: 17_179_870
//     }
//   })
// })

//   test(`${variant}: awsS3ChecksumStream should make checksum of 8MB ArrayBuffer (0.5 block)`, async (_t) => {
//   const input = new TextEncoder('utf-8').encode('x'.repeat(8 * 1024 * 1024))
//   const options = {
//     ChecksumAlgorithm: 'SHA256'
//   }

//   const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
//   const result = await pipeline(stream)

//   deepStrictEqual(result, {
//     s3: {
//       checksum: 'DHe8CgeVqTYS1FJWiXRW0PyyTxUcRMFQ0H7NA/TvUWg=',
//       checksums:['DHe8CgeVqTYS1FJWiXRW0PyyTxUcRMFQ0H7NA/TvUWg='],
//       partSize: 17_179_870
//     }
//   })
// })
// test(`${variant}: awsS3ChecksumStream should make checksum of 16 MB ArrayBuffer (1 block)`, async (_t) => {
//   const input = new TextEncoder('utf-8').encode('x'.repeat(17179870))
//   const options = {
//     ChecksumAlgorithm: 'SHA256'
//   }

//   const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
//   const result = await pipeline(stream)

//   deepStrictEqual(result, {
//     s3: {
//       checksum: 'WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o=',
//       checksums:['WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o='],
//       partSize: 17_179_870
//     }
//   })
// })
//   test(`${variant}: awsS3ChecksumStream should make checksum of 24MB ArrayBuffer (1.5 blocks)`, async (_t) => {
//     const input = new TextEncoder('utf-8').encode('x'.repeat(24 * 1024 * 1024))
//     const options = {
//       ChecksumAlgorithm: 'SHA256'
//     }
//
//     const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
//     const result = await pipeline(stream)
//
//     deepStrictEqual(result, {
//   s3: {
//     checksum: 'eWQzGj3USSV0NvWbhxtpmbkHgNReYxUzwVBXAU86X/4=-2',
//     checksums:[

//             'WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o=',
//             'HLZQKZLvENstyfk2WtaEZGcol2s/v4xvkPX30aqd0XY='],
//     partSize: 17_179_870
//   }
// })
//   })
//   test(`${variant}: awsS3ChecksumStream should make checksum of file 32 MB ArrayBuffer (2 blocks)`, async (_t) => {
//     const input = new TextEncoder('utf-8').encode('x'.repeat(17179870 * 2))
//     const options = {
//       ChecksumAlgorithm: 'SHA256'
//     }
//
//     const stream = [createReadableStream(input), awsS3ChecksumStream(options)]
//     const result = await pipeline(stream)
//
//     deepStrictEqual(result, {
//   s3: {
//     checksum: '65/QvEoh9MiBIPeSgTqKTptI3Vnf+vaJ1om/MYYMpBU=-2',
//     checksums:[
//             'WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o=',
//             'WN4WZJbH8owC673D8TAJBGXF4n7cIY7lDhbZmvIOX5o='],
//     partSize: 17_179_870
//   }
// })
//   })
