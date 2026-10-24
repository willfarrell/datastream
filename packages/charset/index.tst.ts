/// <reference lib="dom" />
/// <reference types="node" />

import type * as charset from "@datastream/charset";
import {
	charsetDecodeStream,
	charsetDetectStream,
	charsetEncodeStream,
} from "@datastream/charset";
import type * as charsetDetect from "@datastream/charset/detect";
import type { StreamResult } from "@datastream/core";
import { describe, expect, test } from "tstyche";

describe("charsetDecodeStream", () => {
	test("accepts charset option", () => {
		expect(
			charsetDecodeStream({ charset: "utf-8" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts no options", () => {
		expect(charsetDecodeStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("charsetDetectStream", () => {
	test("returns stream with result", () => {
		const stream = charsetDetectStream();
		expect(stream.result).type.not.toBeAssignableTo<never>();
	});

	test("result charset may be undefined (empty input)", () => {
		expect<
			() => StreamResult<{ charset: undefined; confidence: number }>
		>().type.toBeAssignableTo<
			ReturnType<typeof charsetDetectStream>["result"]
		>();
	});

	test("accepts resultKey", () => {
		expect(
			charsetDetectStream({ resultKey: "encoding" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("charsetEncodeStream", () => {
	test("accepts charset option", () => {
		expect(
			charsetEncodeStream({ charset: "utf-8" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts no options", () => {
		expect(charsetEncodeStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("getSupportedEncoding", () => {
	test("is internal (not exported)", () => {
		expect<typeof charset>().type.not.toHaveProperty("getSupportedEncoding");
		expect<typeof charsetDetect>().type.not.toHaveProperty(
			"getSupportedEncoding",
		);
	});
});
