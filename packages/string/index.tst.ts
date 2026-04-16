import {
	stringCountStream,
	stringLengthStream,
	stringMinimumChunkSize,
	stringMinimumFirstChunkSize,
	stringReadableStream,
	stringReplaceStream,
	stringSplitStream,
} from "@datastream/string";
import { describe, expect, test } from "tstyche";

describe("stringReadableStream", () => {
	test("accepts string input", () => {
		expect(stringReadableStream("hello")).type.not.toBeAssignableTo<never>();
	});

	test("accepts array input", () => {
		expect(stringReadableStream(["a", "b"])).type.not.toBeAssignableTo<never>();
	});
});

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

describe("stringMinimumFirstChunkSize", () => {
	test("accepts chunkSize option", () => {
		expect(
			stringMinimumFirstChunkSize({ chunkSize: 2048 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("stringMinimumChunkSize", () => {
	test("accepts chunkSize option", () => {
		expect(
			stringMinimumChunkSize({ chunkSize: 2048 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("stringReplaceStream", () => {
	test("requires pattern and replacement", () => {
		expect(
			stringReplaceStream({ pattern: /a/, replacement: "b" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("stringSplitStream", () => {
	test("requires separator", () => {
		expect(
			stringSplitStream({ separator: "\n" }),
		).type.not.toBeAssignableTo<never>();
	});
});
