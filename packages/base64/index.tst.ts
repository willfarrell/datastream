/// <reference lib="dom" />
/// <reference types="node" />
import type * as base64 from "@datastream/base64";
import { base64DecodeStream, base64EncodeStream } from "@datastream/base64";
import { describe, expect, test } from "tstyche";

describe("base64EncodeStream", () => {
	test("returns a stream", () => {
		expect(base64EncodeStream()).type.not.toBeAssignableTo<never>();
	});

	test("accepts options", () => {
		expect(
			base64EncodeStream({}, { highWaterMark: 16 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("base64DecodeStream", () => {
	test("returns a stream", () => {
		expect(base64DecodeStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("default export", () => {
	test("is removed", () => {
		expect<typeof base64>().type.not.toHaveProperty("default");
	});
});
