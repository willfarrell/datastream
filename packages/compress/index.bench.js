import { bench, suite } from "node:bench";
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
} from "@datastream/core";

// -- Data generators --

const OPS = 1;
const options = { warmup: 1, samples: 30 };

// Compressible data (~1MB of repeated JSON-like content)
const compressibleBody = JSON.stringify(
	Array.from({ length: 10_000 }, (_, i) => ({
		id: i,
		name: `item_${i}`,
		value: Math.random(),
	})),
);

// -- Tests --

suite("gzipCompressStream", () => {
	bench(`${compressibleBody.length} chars`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				gzipCompressStream(),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});

	bench(`${compressibleBody.length} chars, quality 1`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				gzipCompressStream({ quality: 1 }),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});

	bench(`${compressibleBody.length} chars, quality 9`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				gzipCompressStream({ quality: 9 }),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("gzip roundtrip", () => {
	bench(`${compressibleBody.length} chars`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				gzipCompressStream(),
				gzipDecompressStream(),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("deflateCompressStream", () => {
	bench(`${compressibleBody.length} chars`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				deflateCompressStream(),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});

	bench(`${compressibleBody.length} chars, quality 1`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				deflateCompressStream({ quality: 1 }),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});

	bench(`${compressibleBody.length} chars, quality 9`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				deflateCompressStream({ quality: 9 }),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("deflate roundtrip", () => {
	bench(`${compressibleBody.length} chars`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				deflateCompressStream(),
				deflateDecompressStream(),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("brotliCompressStream", () => {
	bench(`${compressibleBody.length} chars`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				brotliCompressStream(),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});

	bench(`${compressibleBody.length} chars, quality 1`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				brotliCompressStream({ quality: 1 }),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});

	bench(`${compressibleBody.length} chars, quality 11`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				brotliCompressStream({ quality: 11 }),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("brotli roundtrip", () => {
	bench(`${compressibleBody.length} chars`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(compressibleBody),
				brotliCompressStream(),
				brotliDecompressStream(),
			];
			await streamToBuffer(pipejoin(streams));
		}
		b.end(OPS);
	});
});

// zstd requires Node.js with --conditions=node, test separately
