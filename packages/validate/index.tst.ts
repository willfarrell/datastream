/// <reference lib="dom" />
/// <reference types="node" />
import type { ValidateError } from "@datastream/validate";
import { transpileSchema, validateStream } from "@datastream/validate";
import { describe, expect, test } from "tstyche";

describe("ValidateError", () => {
	test("has required properties", () => {
		expect<ValidateError>().type.toBeAssignableTo<{
			id: string;
			keys: string[];
			message: string;
			idx: number[];
		}>();
	});
});

describe("transpileSchema", () => {
	test("returns validator function", () => {
		const validate = transpileSchema({ type: "object" });
		expect(validate).type.toBe<(data: unknown) => boolean>();
	});
});

describe("validateStream", () => {
	test("accepts schema object", () => {
		expect(
			validateStream({ schema: { type: "object" } }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts compiled schema function", () => {
		expect(
			validateStream({ schema: (_data: unknown) => true }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts all options", () => {
		expect(
			validateStream({
				schema: { type: "object" },
				idxStart: 0,
				onErrorEnqueue: true,
				allowCoerceTypes: false,
				resultKey: "errors",
			}),
		).type.not.toBeAssignableTo<never>();
	});
});
