/// <reference lib="dom" />
/// <reference types="node" />
import {
	stringCountStream,
	stringLengthStream,
	stringMinimumChunkSizeStream,
	stringMinimumFirstChunkSizeStream,
	stringReplaceStream,
	stringSplitStream,
} from "@datastream/string";
import { describe, expect, test } from "tstyche";

describe("stringLengthStream", () => {
	test("returns stream with result method", () => {
		const stream = stringLengthStream();
		expect(stream.result).type.not.toBeAssignableTo<never>();
		expect(stream.result()).type.not.toBeAssignableTo<never>();
	});

	test("accepts resultKey option", () => {
		expect(
			stringLengthStream({ resultKey: "len" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("stringCountStream", () => {
	test("returns stream with result method", () => {
		const stream = stringCountStream({ substr: "x" });
		expect(stream.result()).type.not.toBeAssignableTo<never>();
	});
});

describe("stringMinimumFirstChunkSizeStream", () => {
	test("accepts chunkSize option", () => {
		expect(
			stringMinimumFirstChunkSizeStream({ chunkSize: 2048 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("stringMinimumChunkSizeStream", () => {
	test("accepts chunkSize option", () => {
		expect(
			stringMinimumChunkSizeStream({ chunkSize: 2048 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("stringReplaceStream", () => {
	test("requires pattern and replacement", () => {
		expect(
			stringReplaceStream({ pattern: /a/, replacement: "b" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts a function replacement and match options", () => {
		expect(
			stringReplaceStream({
				pattern: /a(b)/g,
				replacement: (match: string, p1: string, offset: number) =>
					`${match}${p1}${offset}`,
				maxMatchLength: 2,
				lookbehind: 4,
			}),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects a non-string, non-function replacement", () => {
		expect(stringReplaceStream).type.not.toBeCallableWith({
			pattern: "a",
			replacement: 1,
		});
	});
});

describe("stringSplitStream", () => {
	test("requires separator", () => {
		expect(
			stringSplitStream({ separator: "\n" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts maxBufferSize number or null", () => {
		expect(
			stringSplitStream({ separator: "\n", maxBufferSize: null }),
		).type.not.toBeAssignableTo<never>();
		expect(
			stringReplaceStream({ pattern: "a", replacement: "b", maxBufferSize: 1 }),
		).type.not.toBeAssignableTo<never>();
	});
});
