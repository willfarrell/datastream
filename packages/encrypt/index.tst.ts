/// <reference lib="dom" />
/// <reference types="node" />
import {
	decryptStream,
	encryptStream,
	generateEncryptionKey,
} from "@datastream/encrypt";
import { describe, expect, test } from "tstyche";

describe("encryptStream", () => {
	test("returns a promise", () => {
		const key = new Uint8Array(32);
		expect(encryptStream({ key })).type.toBeAssignableTo<Promise<unknown>>();
	});

	test("accepts null limits", () => {
		expect(encryptStream).type.toBeCallableWith({
			key: new Uint8Array(32),
			maxInputSize: null,
			aad: null,
		});
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
	test("returns a promise", () => {
		const key = new Uint8Array(32);
		const iv = new Uint8Array(12);
		expect(decryptStream({ key, iv })).type.toBeAssignableTo<
			Promise<unknown>
		>();
	});

	test("accepts null limits", () => {
		expect(decryptStream).type.toBeCallableWith({
			key: new Uint8Array(32),
			iv: new Uint8Array(12),
			maxInputSize: null,
			maxOutputSize: null,
		});
	});

	test("accepts maxOutputSize option", () => {
		const key = new Uint8Array(32);
		const iv = new Uint8Array(12);
		expect(
			decryptStream({ key, iv, maxOutputSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts maxInputSize option", () => {
		expect(decryptStream).type.toBeCallableWith({
			key: new Uint8Array(32),
			iv: new Uint8Array(12),
			maxInputSize: 1024,
		});
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
