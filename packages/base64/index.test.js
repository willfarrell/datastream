import { deepStrictEqual } from "node:assert";
import { Buffer } from "node:buffer";
import test from "node:test";
import base64Default, {
	base64DecodeStream,
	base64EncodeStream,
} from "@datastream/base64";
import {
	createReadableStream,
	pipejoin,
	streamToBuffer,
	streamToString,
} from "@datastream/core";

const decoder = new TextDecoder();

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** base64EncodeStream *** //
test(`${variant}: base64EncodeStream should encode`, async (_t) => {
	const input = "encode";
	const streams = [createReadableStream(input), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));

	deepStrictEqual(output, btoa(input));
});

// *** base64DecodeStream *** //
test(`${variant}: base64DecodeStream should decode`, async (_t) => {
	const input = "decode";
	const streams = [createReadableStream(btoa(input)), base64DecodeStream()];
	const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

	deepStrictEqual(output, input);
});

// *** Misc *** //
test(`${variant}: base64Stream should encode/decode`, async (_t) => {
	for (let i = 1; i <= 16; i++) {
		const input = "x".repeat(i);
		const streams = [
			createReadableStream(input),
			base64EncodeStream(),
			base64DecodeStream(),
		];
		const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

		deepStrictEqual(output, input);
	}
});

// Test encoding with multiple chunks that have remainder bytes
test(`${variant}: base64EncodeStream should handle multiple chunks with remainders`, async (_t) => {
	const input = [Buffer.from("aaaa"), Buffer.from("bbbb"), Buffer.from("cccc")]; // 4 bytes each, 4 % 3 = 1 remainder
	const streams = [createReadableStream(input), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));

	deepStrictEqual(output, btoa("aaaabbbbcccc"));
});

// Test decoding with multiple chunks that have remainder characters
test(`${variant}: base64DecodeStream should handle multiple chunks with remainders`, async (_t) => {
	const input = btoa("aaaabbbbcccc");
	const chunks = [input.slice(0, 4), input.slice(4, 8), input.slice(8)]; // 4 chars each, 4 % 4 = 0, but test chunking
	const streams = [createReadableStream(chunks), base64DecodeStream()];
	const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

	deepStrictEqual(output, "aaaabbbbcccc");
});

// Test encoding single byte (1 % 3 = 1 remainder)
test(`${variant}: base64EncodeStream should encode single byte`, async (_t) => {
	const input = "a";
	const streams = [createReadableStream(input), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));

	deepStrictEqual(output, btoa("a"));
});

// Test encoding two bytes (2 % 3 = 2 remainder)
test(`${variant}: base64EncodeStream should encode two bytes`, async (_t) => {
	const input = "ab";
	const streams = [createReadableStream(input), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));

	deepStrictEqual(output, btoa("ab"));
});

// Test decode with non-multiple of 4 characters in chunk
test(`${variant}: base64DecodeStream should decode partial base64`, async (_t) => {
	const input = btoa("hello");
	const chunks = [input.slice(0, 2), input.slice(2)]; // 2 and rest
	const streams = [createReadableStream(chunks), base64DecodeStream()];
	const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

	deepStrictEqual(output, "hello");
});

// Test decode flush with remaining characters
test(`${variant}: base64DecodeStream should flush remaining characters`, async (_t) => {
	const input = btoa("ab"); // "YWI="
	const chunks = [input.slice(0, 2)]; // Only send "YW"
	const streams = [createReadableStream(chunks), base64DecodeStream()];
	const output = await streamToBuffer(pipejoin(streams));

	deepStrictEqual(output.byteLength, 1);
});

// *** Binary correctness *** //
test(`${variant}: base64EncodeStream should encode all 256 byte values`, async (_t) => {
	const bytes = new Uint8Array(256);
	for (let i = 0; i < 256; i++) bytes[i] = i;
	const streams = [createReadableStream([bytes]), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));
	deepStrictEqual(output, Buffer.from(bytes).toString("base64"));
});

test(`${variant}: base64DecodeStream should round-trip all 256 byte values`, async (_t) => {
	const bytes = new Uint8Array(256);
	for (let i = 0; i < 256; i++) bytes[i] = i;
	const b64 = Buffer.from(bytes).toString("base64");
	const streams = [createReadableStream([b64]), base64DecodeStream()];
	const output = await streamToBuffer(pipejoin(streams));
	deepStrictEqual(Uint8Array.from(output), bytes);
});

test(`${variant}: base64EncodeStream should encode Uint8Array chunks across boundary`, async (_t) => {
	// 4 bytes per chunk, 4%3 = 1 remainder per chunk
	const chunks = [
		new Uint8Array([1, 2, 3, 4]),
		new Uint8Array([5, 6, 7, 8]),
		new Uint8Array([9, 10, 11, 12]),
	];
	const streams = [createReadableStream(chunks), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));
	const flat = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
	deepStrictEqual(output, Buffer.from(flat).toString("base64"));
});

// *** default export *** //
test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(base64Default).sort(), [
		"decodeStream",
		"encodeStream",
	]);
});
