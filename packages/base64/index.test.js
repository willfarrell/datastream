import { deepStrictEqual } from "node:assert";
import test from "node:test";
import base64Default, {
	base64DecodeStream,
	base64EncodeStream,
} from "@datastream/base64";
import {
	createReadableStream,
	pipejoin,
	streamToString,
} from "@datastream/core";

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
	const output = await streamToString(pipejoin(streams));

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
		const output = await streamToString(pipejoin(streams));

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
	const output = await streamToString(pipejoin(streams));

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
	const output = await streamToString(pipejoin(streams));

	deepStrictEqual(output, "hello");
});

// Test decode flush with remaining characters
test(`${variant}: base64DecodeStream should flush remaining characters`, async (_t) => {
	// btoa("a") = "YQ==" (4 chars), btoa("ab") = "YWI=" (4 chars)
	// Split so last chunk has 2 chars (not multiple of 4)
	const input = btoa("ab"); // "YWI="
	const chunks = [input.slice(0, 2)]; // Only send "YW" - leaves 2 chars in extra
	// The stream should end with extra = "I=" and flush should decode it
	const streams = [createReadableStream(chunks), base64DecodeStream()];
	const output = await streamToString(pipejoin(streams));

	// "YW" + "I=" would decode to "ab", but since we only sent "YW",
	// extra becomes "YW" (2 % 4 = 2), then flush decodes it
	// Actually "YW" is incomplete base64 - let's think again
	// When chunk = "YW", remaining = 2 % 4 = 2, extra = "YW" (last 2 chars)
	// chunk becomes "" (first 0 chars), so nothing enqueued in transform
	// Then flush is called with extra = "YW"
	// Buffer.from("YW", "base64") decodes to "a" (partial decode)
	deepStrictEqual(output.length, 1);
});

// *** default export *** //
test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(base64Default).sort(), [
		"decodeStream",
		"encodeStream",
	]);
});
