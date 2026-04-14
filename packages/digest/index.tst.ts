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
	test("returns a stream or promise of stream", () => {
		const result = digestStream({ algorithm: "SHA2-256" });
		expect(result).type.not.toBeAssignableTo<never>();
	});

	test("accepts resultKey", () => {
		expect(
			digestStream({ algorithm: "SHA3-512", resultKey: "hash" }),
		).type.not.toBeAssignableTo<never>();
	});
});
