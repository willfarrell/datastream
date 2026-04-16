/// <reference lib="dom" />
/// <reference types="node" />
import {
	brotliCompressStream,
	brotliDecompressStream,
	deflateCompressStream,
	deflateDecompressStream,
	gzipCompressStream,
	gzipDecompressStream,
	protobufDeserializeStream,
	protobufSerializeStream,
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

describe("protobuf", () => {
	test("protobufSerializeStream accepts Type", () => {
		expect(
			protobufSerializeStream({
				Type: {} as import("@datastream/compress/protobuf").ProtobufType,
			}),
		).type.not.toBeAssignableTo<never>();
	});

	test("protobufDeserializeStream accepts Type", () => {
		expect(
			protobufDeserializeStream({
				Type: {} as import("@datastream/compress/protobuf").ProtobufType,
			}),
		).type.not.toBeAssignableTo<never>();
	});
});
