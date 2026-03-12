import type { StreamOptions, StreamResult } from "@datastream/core";
import {
	createReadableStream,
	createTransformStream,
	createWritableStream,
	isReadable,
	isWritable,
	makeOptions,
	pipeline,
	result,
	streamToArray,
	streamToBuffer,
	streamToObject,
	streamToString,
	timeout,
} from "@datastream/core";
import { describe, expect, test } from "tstyche";

describe("StreamOptions", () => {
	test("accepts valid options", () => {
		expect<StreamOptions>().type.toBeAssignableTo<{
			highWaterMark?: number;
			chunkSize?: number;
			signal?: AbortSignal;
		}>();
	});
});

describe("StreamResult", () => {
	test("has key and value", () => {
		expect<StreamResult<number>>().type.toBe<{
			key: string;
			value: number;
		}>();
	});
});

describe("pipeline", () => {
	test("returns Promise of record", () => {
		expect(pipeline([])).type.toBe<Promise<Record<string, unknown>>>();
	});

	test("accepts stream options", () => {
		expect(pipeline([], { highWaterMark: 16 })).type.toBe<
			Promise<Record<string, unknown>>
		>();
	});
});

describe("result", () => {
	test("returns Promise of record", () => {
		expect(result([])).type.toBe<Promise<Record<string, unknown>>>();
	});
});

describe("streamToArray", () => {
	test("returns Promise of array", () => {
		expect(streamToArray({})).type.toBe<Promise<unknown[]>>();
	});

	test("accepts generic type", () => {
		expect(streamToArray<string>({})).type.toBe<Promise<string[]>>();
	});
});

describe("streamToObject", () => {
	test("returns Promise of object", () => {
		expect(streamToObject({})).type.toBe<Promise<Record<string, unknown>>>();
	});
});

describe("streamToString", () => {
	test("returns Promise of string", () => {
		expect(streamToString({})).type.toBe<Promise<string>>();
	});
});

describe("streamToBuffer", () => {
	test("returns Promise of Buffer", () => {
		expect(streamToBuffer({})).type.toBe<Promise<Buffer>>();
	});
});

describe("type guards", () => {
	test("isReadable returns boolean", () => {
		expect(isReadable({})).type.toBe<boolean>();
	});

	test("isWritable returns boolean", () => {
		expect(isWritable({})).type.toBe<boolean>();
	});
});

describe("makeOptions", () => {
	test("returns record", () => {
		expect(makeOptions()).type.toBe<Record<string, unknown>>();
	});

	test("accepts stream options", () => {
		expect(makeOptions({ highWaterMark: 16 })).type.toBe<
			Record<string, unknown>
		>();
	});
});

describe("createReadableStream", () => {
	test("accepts string input", () => {
		expect(createReadableStream("hello")).type.not.toBeAssignableTo<never>();
	});

	test("accepts array input", () => {
		expect(createReadableStream([1, 2, 3])).type.not.toBeAssignableTo<never>();
	});

	test("accepts no input", () => {
		expect(createReadableStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("createTransformStream", () => {
	test("accepts transform function", () => {
		expect(
			createTransformStream((chunk: string, enqueue: (c: string) => void) => {
				enqueue(chunk);
			}),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("createWritableStream", () => {
	test("accepts write function", () => {
		expect(
			createWritableStream((chunk: unknown) => {}),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts no arguments", () => {
		expect(createWritableStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("timeout", () => {
	test("returns Promise<void>", () => {
		expect(timeout(100)).type.toBe<Promise<void>>();
	});

	test("accepts signal option", () => {
		expect(timeout(100, { signal: new AbortController().signal })).type.toBe<
			Promise<void>
		>();
	});
});
