import type { DigestAlgorithm } from "@datastream/digest";
import { digestStream } from "@datastream/digest";
import { describe, expect, test } from "tstyche";

describe("DigestAlgorithm", () => {
	test("accepts valid algorithms", () => {
		expect<"SHA2-256">().type.toBeAssignableTo<DigestAlgorithm>();
		expect<"SHA2-384">().type.toBeAssignableTo<DigestAlgorithm>();
		expect<"SHA2-512">().type.toBeAssignableTo<DigestAlgorithm>();
		expect<"SHA3-256">().type.toBeAssignableTo<DigestAlgorithm>();
		expect<"SHA3-384">().type.toBeAssignableTo<DigestAlgorithm>();
		expect<"SHA3-512">().type.toBeAssignableTo<DigestAlgorithm>();
	});

	test("rejects invalid algorithms", () => {
		expect<"MD5">().type.not.toBeAssignableTo<DigestAlgorithm>();
	});
});

describe("digestStream", () => {
	test("returns a promise", () => {
		const result = digestStream({ algorithm: "SHA2-256" });
		expect(result).type.toBeAssignableTo<Promise<unknown>>();
	});

	test("accepts resultKey", () => {
		expect(
			digestStream({ algorithm: "SHA3-512", resultKey: "hash" }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});
});
