/// <reference lib="dom" />
/// <reference types="node" />

import type * as compressNode from "@datastream/compress";
import {
	brotliCompressStream,
	brotliDecompressStream,
	deflateCompressStream,
	deflateDecompressStream,
	gzipCompressStream,
	gzipDecompressStream,
	zstdCompressStream,
	zstdDecompressStream,
} from "@datastream/compress";
import * as brotliBrowser from "@datastream/compress/brotli/browser";
import * as compressBrowser from "@datastream/compress/browser";
import * as deflateBrowser from "@datastream/compress/deflate/browser";
import * as gzipBrowser from "@datastream/compress/gzip/browser";
import { describe, expect, test } from "tstyche";

describe("gzip", () => {
	test("gzipCompressStream returns a stream", () => {
		expect(gzipCompressStream()).type.not.toBeAssignableTo<never>();
	});

	test("gzipDecompressStream returns a stream", () => {
		expect(gzipDecompressStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("deflate", () => {
	test("deflateCompressStream returns a stream", () => {
		expect(deflateCompressStream()).type.not.toBeAssignableTo<never>();
	});

	// deflate.node.js and gzip.node.js both read `level ?? quality`.
	test("deflateCompressStream and gzipCompressStream accept level", () => {
		expect(deflateCompressStream).type.toBeCallableWith({ level: 9 });
		expect(gzipCompressStream).type.toBeCallableWith({ level: 9 });
	});

	test("deflateDecompressStream returns a stream", () => {
		expect(deflateDecompressStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("brotli", () => {
	test("brotliCompressStream accepts quality", () => {
		expect(
			brotliCompressStream({ quality: 5 }),
		).type.not.toBeAssignableTo<never>();
	});

	test("brotliDecompressStream returns a stream", () => {
		expect(brotliDecompressStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("zstd", () => {
	test("zstdCompressStream returns a stream", () => {
		expect(zstdCompressStream()).type.not.toBeAssignableTo<never>();
	});

	test("zstdDecompressStream returns a stream", () => {
		expect(zstdDecompressStream()).type.not.toBeAssignableTo<never>();
	});
});

// maxOutputSize: null disables the limit (including the 256 MiB decompress default).
describe("maxOutputSize", () => {
	test("gzipCompressStream accepts a number or null", () => {
		expect(gzipCompressStream).type.toBeCallableWith({ maxOutputSize: 1024 });
		expect(gzipCompressStream).type.toBeCallableWith({ maxOutputSize: null });
	});
	test("gzipDecompressStream accepts a number or null", () => {
		expect(gzipDecompressStream).type.toBeCallableWith({ maxOutputSize: 1024 });
		expect(gzipDecompressStream).type.toBeCallableWith({ maxOutputSize: null });
	});
	test("deflateCompressStream accepts a number or null", () => {
		expect(deflateCompressStream).type.toBeCallableWith({
			maxOutputSize: 1024,
		});
		expect(deflateCompressStream).type.toBeCallableWith({
			maxOutputSize: null,
		});
	});
	test("deflateDecompressStream accepts a number or null", () => {
		expect(deflateDecompressStream).type.toBeCallableWith({
			maxOutputSize: 1024,
		});
		expect(deflateDecompressStream).type.toBeCallableWith({
			maxOutputSize: null,
		});
	});
	test("brotliCompressStream accepts a number or null", () => {
		expect(brotliCompressStream).type.toBeCallableWith({ maxOutputSize: 1024 });
		expect(brotliCompressStream).type.toBeCallableWith({ maxOutputSize: null });
	});
	test("brotliDecompressStream accepts a number or null", () => {
		expect(brotliDecompressStream).type.toBeCallableWith({
			maxOutputSize: 1024,
		});
		expect(brotliDecompressStream).type.toBeCallableWith({
			maxOutputSize: null,
		});
	});
	test("zstdCompressStream accepts a number or null", () => {
		expect(zstdCompressStream).type.toBeCallableWith({ maxOutputSize: 1024 });
		expect(zstdCompressStream).type.toBeCallableWith({ maxOutputSize: null });
	});
	test("zstdDecompressStream accepts a number or null", () => {
		expect(zstdDecompressStream).type.toBeCallableWith({ maxOutputSize: 1024 });
		expect(zstdDecompressStream).type.toBeCallableWith({ maxOutputSize: null });
	});
});

describe("params (Node.js only)", () => {
	test("zstd and brotliDecompressStream accept zlib params", () => {
		expect(zstdCompressStream).type.toBeCallableWith({ params: { 100: 3 } });
		expect(zstdDecompressStream).type.toBeCallableWith({
			params: { 100: 27 },
		});
		expect(brotliDecompressStream).type.toBeCallableWith({
			params: { 0: true },
		});
	});
	test("brotliCompressStream does not accept params", () => {
		expect(brotliCompressStream).type.not.toBeCallableWith({
			params: { 1: 5 },
		});
	});
});

describe("browser build", () => {
	test("does not export zstd (Node.js only)", () => {
		expect<typeof compressBrowser>().type.not.toHaveProperty(
			"zstdCompressStream",
		);
		expect<typeof compressBrowser>().type.not.toHaveProperty(
			"zstdDecompressStream",
		);
		expect<typeof compressNode>().type.toHaveProperty("zstdCompressStream");
	});

	test("exports brotli, deflate and gzip as web TransformStreams", () => {
		expect(compressBrowser.brotliCompressStream()).type.toBe<
			TransformStream<unknown, unknown>
		>();
		expect(compressBrowser.deflateDecompressStream()).type.toBe<
			TransformStream<unknown, unknown>
		>();
		expect(compressBrowser.gzipCompressStream()).type.toBe<
			TransformStream<unknown, unknown>
		>();
	});

	test("codec /browser subpaths are web TransformStreams", () => {
		expect(brotliBrowser.brotliDecompressStream()).type.toBe<
			TransformStream<unknown, unknown>
		>();
		expect(deflateBrowser.deflateCompressStream()).type.toBe<
			TransformStream<unknown, unknown>
		>();
		expect(gzipBrowser.gzipDecompressStream()).type.toBe<
			TransformStream<unknown, unknown>
		>();
	});

	test("shares the option types with the node build", () => {
		expect(compressBrowser.brotliDecompressStream).type.toBeCallableWith({
			maxOutputSize: null,
			params: { 0: true },
		});
		expect(compressBrowser.deflateCompressStream).type.toBeCallableWith({
			level: 9,
		});
		expect(compressBrowser.gzipCompressStream).type.toBeCallableWith({
			level: 9,
		});
	});
});
