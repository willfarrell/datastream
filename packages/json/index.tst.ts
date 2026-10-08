/// <reference lib="dom" />
/// <reference types="node" />
import type { JsonError } from "@datastream/json";
import {
	jsonFormatStream,
	jsonParseStream,
	ndjsonFormatStream,
	ndjsonParseStream,
} from "@datastream/json";
import { describe, expect, test } from "tstyche";

// Writable-side chunk type of the web TransformStream member.
type Input<T> = T extends TransformStream<infer I, unknown> ? I : never;

describe("JsonError", () => {
	test("has required fields", () => {
		expect<JsonError>().type.toBeAssignableTo<{
			id: string;
			message: string;
			idx: number[];
			count: number;
		}>();
	});
});

describe("ndjsonParseStream", () => {
	test("returns stream with result", () => {
		const stream = ndjsonParseStream();
		expect(stream.result).type.not.toBeAssignableTo<never>();
	});

	test("accepts string and byte chunks", () => {
		expect<Input<ReturnType<typeof ndjsonParseStream>>>().type.toBe<
			string | Uint8Array
		>();
	});

	test("accepts maxBufferSize", () => {
		expect(
			ndjsonParseStream({ maxBufferSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts null limits and maxErrorRows", () => {
		expect(
			ndjsonParseStream({ maxBufferSize: null, maxErrorRows: null }),
		).type.not.toBeAssignableTo<never>();
		expect(
			jsonParseStream({
				maxBufferSize: null,
				maxValueSize: null,
				maxErrorRows: 10,
			}),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts resultKey", () => {
		expect(
			ndjsonParseStream({ resultKey: "myErrors" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("ndjsonFormatStream", () => {
	test("accepts no options", () => {
		expect(ndjsonFormatStream()).type.not.toBeAssignableTo<never>();
	});

	test("accepts space option", () => {
		expect(ndjsonFormatStream({ space: 2 })).type.not.toBeAssignableTo<never>();
	});
});

describe("jsonParseStream", () => {
	test("returns stream with result", () => {
		const stream = jsonParseStream();
		expect(stream.result).type.not.toBeAssignableTo<never>();
	});

	test("accepts string and byte chunks", () => {
		expect<Input<ReturnType<typeof jsonParseStream>>>().type.toBe<
			string | Uint8Array
		>();
	});

	test("accepts maxBufferSize and maxValueSize", () => {
		expect(
			jsonParseStream({ maxBufferSize: 1024, maxValueSize: 512 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("jsonFormatStream", () => {
	test("accepts no options", () => {
		expect(jsonFormatStream()).type.not.toBeAssignableTo<never>();
	});

	test("accepts space option", () => {
		expect(jsonFormatStream({ space: 2 })).type.not.toBeAssignableTo<never>();
	});
});
