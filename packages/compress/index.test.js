import { strictEqual } from "node:assert";
import test from "node:test";
import {
	brotliCompressSync,
	deflateSync,
	gzipSync,
	zstdCompressSync,
	zstdDecompressSync,
} from "node:zlib";
import {
	brotliCompressStream,
	brotliDecompressStream,
} from "@datastream/compress/brotli";
import {
	deflateCompressStream,
	deflateDecompressStream,
} from "@datastream/compress/deflate";
import {
	gzipCompressStream,
	gzipDecompressStream,
} from "@datastream/compress/gzip";
import {
	createReadableStream,
	pipejoin,
	streamToBuffer,
	streamToString,
} from "@datastream/core";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

const compressibleBody = JSON.stringify(new Array(1024).fill(0));

if (variant === "node") {
	const { zstdCompressStream, zstdDecompressStream } = await import(
		"@datastream/compress/zstd"
	);

	// *** zstd *** //
	test(`${variant}: zstdCompressStream should compress`, async (_t) => {
		const input = compressibleBody;
		const streams = [createReadableStream(input), zstdCompressStream()];
		const output = await streamToBuffer(pipejoin(streams));
		// strictEqual(output, zstdCompressSync(compressibleBody)) // fails, see https://github.com/nodejs/node/issues/58392
		strictEqual(zstdDecompressSync(output).toString(), compressibleBody);
	});

	test(`${variant}: zstdDecompressStream should decompress`, async (_t) => {
		const input = zstdCompressSync(compressibleBody);
		const streams = [createReadableStream(input), zstdDecompressStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});
}

// *** brotli *** //
test(`${variant}: brotliCompressStream should compress`, async (_t) => {
	const input = compressibleBody;
	const streams = [createReadableStream(input), brotliCompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, brotliCompressSync(compressibleBody).toString());
});

test(`${variant}: brotliDecompressStream should decompress`, async (_t) => {
	const input = brotliCompressSync(compressibleBody);
	const streams = [createReadableStream(input), brotliDecompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, compressibleBody);
});

// *** gzip *** //
test(`${variant}: gzipCompressStream should compress`, async (_t) => {
	const input = compressibleBody;
	const streams = [createReadableStream(input), gzipCompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, gzipSync(compressibleBody).toString());
});

test(`${variant}: gzipDecompressStream should decompress`, async (_t) => {
	const input = gzipSync(compressibleBody);
	const streams = [createReadableStream(input), gzipDecompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, compressibleBody);
});

// *** deflate *** //
test(`${variant}: deflateCompressStream should compress`, async (_t) => {
	const input = compressibleBody;
	const streams = [createReadableStream(input), deflateCompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, deflateSync(compressibleBody).toString());
});

test(`${variant}: deflateDecompressStream should decompress`, async (_t) => {
	const input = deflateSync(compressibleBody);
	const streams = [createReadableStream(input), deflateDecompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, compressibleBody);
});
