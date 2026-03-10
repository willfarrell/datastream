import test from "node:test";
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
import { Bench } from "tinybench";

// -- Data generators --

const time = 5_000;

// Compressible data (~1MB of repeated JSON-like content)
const compressibleBody = JSON.stringify(
	Array.from({ length: 10_000 }, (_, i) => ({
		id: i,
		name: `item_${i}`,
		value: Math.random(),
	})),
);

// -- Tests --

test("perf: gzipCompressStream", async () => {
	const bench = new Bench({ name: "gzipCompressStream", time });

	bench.add(`${compressibleBody.length} chars`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			gzipCompressStream(),
		];
		await streamToBuffer(pipejoin(streams));
	});

	bench.add(`${compressibleBody.length} chars, quality 1`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			gzipCompressStream({ quality: 1 }),
		];
		await streamToBuffer(pipejoin(streams));
	});

	bench.add(`${compressibleBody.length} chars, quality 9`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			gzipCompressStream({ quality: 9 }),
		];
		await streamToBuffer(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: gzip roundtrip (compress → decompress)", async () => {
	const bench = new Bench({ name: "gzip roundtrip", time });

	bench.add(`${compressibleBody.length} chars`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			gzipCompressStream(),
			gzipDecompressStream(),
		];
		await streamToBuffer(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: deflateCompressStream", async () => {
	const bench = new Bench({ name: "deflateCompressStream", time });

	bench.add(`${compressibleBody.length} chars`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			deflateCompressStream(),
		];
		await streamToBuffer(pipejoin(streams));
	});

	bench.add(`${compressibleBody.length} chars, quality 1`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			deflateCompressStream({ quality: 1 }),
		];
		await streamToBuffer(pipejoin(streams));
	});

	bench.add(`${compressibleBody.length} chars, quality 9`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			deflateCompressStream({ quality: 9 }),
		];
		await streamToBuffer(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: deflate roundtrip (compress → decompress)", async () => {
	const bench = new Bench({ name: "deflate roundtrip", time });

	bench.add(`${compressibleBody.length} chars`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			deflateCompressStream(),
			deflateDecompressStream(),
		];
		await streamToBuffer(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: brotliCompressStream", async () => {
	const bench = new Bench({ name: "brotliCompressStream", time });

	bench.add(`${compressibleBody.length} chars`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			brotliCompressStream(),
		];
		await streamToBuffer(pipejoin(streams));
	});

	bench.add(`${compressibleBody.length} chars, quality 1`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			brotliCompressStream({ quality: 1 }),
		];
		await streamToBuffer(pipejoin(streams));
	});

	bench.add(`${compressibleBody.length} chars, quality 11`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			brotliCompressStream({ quality: 11 }),
		];
		await streamToBuffer(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: brotli roundtrip (compress → decompress)", async () => {
	const bench = new Bench({ name: "brotli roundtrip", time });

	bench.add(`${compressibleBody.length} chars`, async () => {
		const streams = [
			createReadableStream(compressibleBody),
			brotliCompressStream(),
			brotliDecompressStream(),
		];
		await streamToBuffer(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

// zstd requires Node.js with --conditions=node, test separately
