import _default, {
	base64DecodeStream,
	base64EncodeStream,
} from "@datastream/base64";
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
	test("has encodeStream", () => {
		expect(_default.encodeStream).type.toBe<typeof base64EncodeStream>();
	});

	test("has decodeStream", () => {
		expect(_default.decodeStream).type.toBe<typeof base64DecodeStream>();
	});
});
