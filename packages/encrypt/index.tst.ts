import _default, {
	decryptStream,
	encryptStream,
	generateEncryptionKey,
} from "@datastream/encrypt";
import { describe, expect, test } from "tstyche";

describe("encryptStream", () => {
	test("returns a stream or promise", () => {
		const key = new Uint8Array(32);
		expect(encryptStream({ key })).type.not.toBeAssignableTo<never>();
	});

	test("accepts algorithm option", () => {
		const key = new Uint8Array(32);
		expect(
			encryptStream({ key, algorithm: "AES-256-CTR" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts maxInputSize option", () => {
		const key = new Uint8Array(32);
		expect(
			encryptStream({ key, maxInputSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("decryptStream", () => {
	test("returns a stream or promise", () => {
		const key = new Uint8Array(32);
		const iv = new Uint8Array(12);
		expect(decryptStream({ key, iv })).type.not.toBeAssignableTo<never>();
	});

	test("accepts maxOutputSize option", () => {
		const key = new Uint8Array(32);
		const iv = new Uint8Array(12);
		expect(
			decryptStream({ key, iv, maxOutputSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("generateEncryptionKey", () => {
	test("returns Uint8Array", () => {
		expect(generateEncryptionKey()).type.toBeAssignableTo<Uint8Array>();
	});

	test("accepts bits option", () => {
		expect(
			generateEncryptionKey({ bits: 128 }),
		).type.toBeAssignableTo<Uint8Array>();
	});
});

describe("default export", () => {
	test("has encryptStream", () => {
		expect(_default.encryptStream).type.toBe<typeof encryptStream>();
	});

	test("has decryptStream", () => {
		expect(_default.decryptStream).type.toBe<typeof decryptStream>();
	});

	test("has generateEncryptionKey", () => {
		expect(_default.generateEncryptionKey).type.toBe<
			typeof generateEncryptionKey
		>();
	});
});
