/// <reference lib="dom" />
/// <reference types="node" />
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
