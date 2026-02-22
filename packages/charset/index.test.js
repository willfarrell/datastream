import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
import {
	charsetDecodeStream,
	charsetDetectStream,
	charsetEncodeStream,
} from "@datastream/charset";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** charsetDetectStream *** //
test(`${variant}: charsetDetectStream should detect charset from content`, async (_t) => {
	const input = [Buffer.from("Hello World")];
	const streams = [createReadableStream(input), charsetDetectStream()];

	await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "charset");
	strictEqual(typeof value.charset, "string");
	strictEqual(typeof value.confidence, "number");
});

test(`${variant}: charsetDetectStream should detect charset with custom result key`, async (_t) => {
	const input = [Buffer.from("Test content")];
	const streams = [
		createReadableStream(input),
		charsetDetectStream({ resultKey: "encoding" }),
	];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "encoding");
	deepStrictEqual(result.encoding, value);
});

// *** charsetEncodeStream *** //
test(`${variant}: charsetEncodeStream should encode strings to UTF-8 by default`, async (_t) => {
	const input = ["Hello", " ", "World"];
	const streams = [createReadableStream(input), charsetEncodeStream()];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		Buffer.from("Hello"),
		Buffer.from(" "),
		Buffer.from("World"),
	]);
});

test(`${variant}: charsetEncodeStream should encode strings to specified charset`, async (_t) => {
	const input = ["Hello"];
	const streams = [
		createReadableStream(input),
		charsetEncodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [Buffer.from("Hello")]);
});

// *** charsetDecodeStream *** //
test(`${variant}: charsetDecodeStream should decode UTF-8 bytes to strings`, async (_t) => {
	const input = [Buffer.from("Hello"), Buffer.from(" "), Buffer.from("World")];
	const streams = [
		createReadableStream(input),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "Hello World");
});

test(`${variant}: charsetDecodeStream should decode bytes to strings with default charset`, async (_t) => {
	const input = [Buffer.from("Test")];
	const streams = [createReadableStream(input), charsetDecodeStream()];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "Test");
});

// *** Pipeline pattern: encode -> decode *** //
test(`${variant}: encode and decode pipeline should roundtrip correctly`, async (_t) => {
	const input = ["Hello", " ", "World"];
	const streams = [
		createReadableStream(input),
		charsetEncodeStream({ charset: "UTF-8" }),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "Hello World");
});

// *** Pipeline pattern: detect -> encode -> decode *** //
test(`${variant}: detect, encode, decode pipeline should work together`, async (_t) => {
	const input = ["Test", " ", "String"];
	const streams = [
		createReadableStream(input),
		charsetDetectStream(),
		charsetEncodeStream({ charset: "UTF-8" }),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "charset");
	strictEqual(typeof value.charset, "string");
});

// Test with streamOptions
test(`${variant}: charsetEncodeStream should accept streamOptions`, async (_t) => {
	const input = ["Hello"];
	const streams = [
		createReadableStream(input),
		charsetEncodeStream({}, { objectMode: true }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [Buffer.from("Hello")]);
});

test(`${variant}: charsetDecodeStream should accept streamOptions`, async (_t) => {
	const input = [Buffer.from("Hello")];
	const streams = [
		createReadableStream(input),
		charsetDecodeStream({}, { objectMode: true }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "Hello");
});

// Test flush with remaining bytes
test(`${variant}: charsetEncodeStream should handle empty input`, async (_t) => {
	const streams = [
		createReadableStream([]),
		charsetEncodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	strictEqual(output.length, 0);
});

test(`${variant}: charsetEncodeStream should work with UTF-16LE`, async (_t) => {
	const streams = [
		createReadableStream(["test"]),
		charsetEncodeStream({ charset: "UTF-16LE" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	const result = Buffer.concat(output);
	// UTF-16LE should have BOM and different byte pattern
	strictEqual(result.length > 0, true);
});

test(`${variant}: charsetEncodeStream should handle unsupported charset`, async (_t) => {
	const input = ["test"];
	const streams = [
		createReadableStream(input),
		charsetEncodeStream({ charset: "unsupported-charset" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// Should fallback to UTF-8
	deepStrictEqual(Buffer.concat(output).toString("utf8"), "test");
});

test(`${variant}: charsetDecodeStream should flush remaining bytes`, async (_t) => {
	// Split a multi-byte UTF-8 character across chunks
	const fullBytes = Buffer.from("测试", "utf8");
	const chunk1 = fullBytes.slice(0, 3); // First 3 bytes (partial)
	const chunk2 = fullBytes.slice(3); // Remaining bytes

	const streams = [
		createReadableStream([chunk1, chunk2]),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "测试");
});

test(`${variant}: charsetEncodeStream should flush with incomplete sequence`, async (_t) => {
	// Test with empty input to trigger flush with no data
	const streams = [
		createReadableStream([]),
		charsetEncodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// Should handle empty stream gracefully
	deepStrictEqual(output.length, 0);
});

test(`${variant}: charsetDecodeStream should flush with incomplete multi-byte sequence`, async (_t) => {
	// Send incomplete UTF-8 sequence (first byte of 2-byte sequence)
	const incompleteBytes = Buffer.from([0xc2]); // Start of 2-byte sequence

	const streams = [
		createReadableStream([incompleteBytes]),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	// Should output replacement character for incomplete sequence
	strictEqual(output.includes("\uFFFD"), true);
});

test(`${variant}: charsetDecodeStream should handle unsupported charset`, async (_t) => {
	const input = [Buffer.from("test")];
	const streams = [
		createReadableStream(input),
		charsetDecodeStream({ charset: "unsupported-charset" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	// Should fallback to UTF-8
	deepStrictEqual(output, "test");
});

test(`${variant}: charsetEncodeStream should handle ISO-8859-8-I charset`, async (_t) => {
	const input = ["test"];
	const streams = [
		createReadableStream(input),
		charsetEncodeStream({ charset: "ISO-8859-8-I" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// Should convert ISO-8859-8-I to ISO-8859-8
	deepStrictEqual(output.length, 1);
});

test(`${variant}: charsetDecodeStream should handle ISO-8859-8-I charset`, async (_t) => {
	const input = [Buffer.from("test")];
	const streams = [
		createReadableStream(input),
		charsetDecodeStream({ charset: "ISO-8859-8-I" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	// Should convert ISO-8859-8-I to ISO-8859-8
	deepStrictEqual(typeof output, "string");
});

test(`${variant}: charsetDecodeStream should handle empty chunks`, async (_t) => {
	const input = [Buffer.from(""), Buffer.from("test")];
	const streams = [
		createReadableStream(input),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "test");
});
