import { Readable } from "node:stream";
import { pipeline as nodeStreamPipeline } from "node:stream/promises";
import test from "node:test";
import {
	brotliCompressStream,
	brotliDecompressStream,
	deflateCompressStream,
	deflateDecompressStream,
	gzipCompressStream,
	gzipDecompressStream,
} from "@datastream/compress";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

const collectStream = async (stream) => {
	const chunks = [];
	for await (const chunk of stream) {
		chunks.push(chunk);
	}
	return Buffer.concat(chunks);
};

// *** gzip roundtrip *** //
test("fuzz gzip roundtrip compress -> decompress", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.string({ minLength: 1, maxLength: 10000 }),
			async (input) => {
				try {
					const compress = gzipCompressStream();
					const decompress = gzipDecompressStream();
					const source = Readable.from([input]);
					await nodeStreamPipeline(source, compress, decompress);
					const result = await collectStream(decompress);
					const output = result.toString();
					if (output !== input) {
						throw new Error("Gzip roundtrip failed");
					}
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 100,
			verbose: 2,
			examples: [],
		},
	);
});

// *** deflate roundtrip *** //
test("fuzz deflate roundtrip compress -> decompress", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.string({ minLength: 1, maxLength: 10000 }),
			async (input) => {
				try {
					const compress = deflateCompressStream();
					const decompress = deflateDecompressStream();
					const source = Readable.from([input]);
					await nodeStreamPipeline(source, compress, decompress);
					const result = await collectStream(decompress);
					const output = result.toString();
					if (output !== input) {
						throw new Error("Deflate roundtrip failed");
					}
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 100,
			verbose: 2,
			examples: [],
		},
	);
});

// *** brotli roundtrip *** //
test("fuzz brotli roundtrip compress -> decompress", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.string({ minLength: 1, maxLength: 10000 }),
			async (input) => {
				try {
					const compress = brotliCompressStream();
					const decompress = brotliDecompressStream();
					const source = Readable.from([input]);
					await nodeStreamPipeline(source, compress, decompress);
					const result = await collectStream(decompress);
					const output = result.toString();
					if (output !== input) {
						throw new Error("Brotli roundtrip failed");
					}
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 100,
			verbose: 2,
			examples: [],
		},
	);
});

// *** gzip w/ quality *** //
test("fuzz gzip compress w/ quality", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.integer({ min: -1, max: 9 }),
			fc.string({ minLength: 1, maxLength: 1000 }),
			async (quality, input) => {
				try {
					const compress = gzipCompressStream({ quality });
					const decompress = gzipDecompressStream();
					const source = Readable.from([input]);
					await nodeStreamPipeline(source, compress, decompress);
					await collectStream(decompress);
				} catch (e) {
					catchError({ quality, input }, e);
				}
			},
		),
		{
			numRuns: 100,
			verbose: 2,
			examples: [],
		},
	);
});

// *** brotli w/ quality *** //
test("fuzz brotli compress w/ quality", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.integer({ min: 0, max: 11 }),
			fc.string({ minLength: 1, maxLength: 1000 }),
			async (quality, input) => {
				try {
					const compress = brotliCompressStream({ quality });
					const decompress = brotliDecompressStream();
					const source = Readable.from([input]);
					await nodeStreamPipeline(source, compress, decompress);
					await collectStream(decompress);
				} catch (e) {
					catchError({ quality, input }, e);
				}
			},
		),
		{
			numRuns: 100,
			verbose: 2,
			examples: [],
		},
	);
});
