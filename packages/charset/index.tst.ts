import {
	charsetDecodeStream,
	charsetDetectStream,
	charsetEncodeStream,
} from "@datastream/charset";
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
