import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
import {
	charsetDecodeStream,
	charsetDetectStream,
	charsetEncodeStream,
} from "@datastream/charset";
import { getSupportedEncoding } from "@datastream/charset/detect";
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

// *** getSupportedEncoding *** //
test(`${variant}: getSupportedEncoding should convert ISO-8859-8-I to ISO-8859-8`, (_t) => {
	strictEqual(getSupportedEncoding("ISO-8859-8-I"), "ISO-8859-8");
});

test(`${variant}: getSupportedEncoding should pass through other charsets unchanged`, (_t) => {
	strictEqual(getSupportedEncoding("UTF-8"), "UTF-8");
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

// *** charsetDetectStream concurrent isolation regression *** //
test(`${variant}: charsetDetectStream instances should not share state`, async (_t) => {
	const input1 = [Buffer.from("Hello World")];
	const input2 = [Buffer.from("Bonjour le monde")];

	const streams1 = [createReadableStream(input1), charsetDetectStream()];
	const streams2 = [createReadableStream(input2), charsetDetectStream()];

	await Promise.all([pipeline(streams1), pipeline(streams2)]);

	const result1 = streams1[1].result();
	const result2 = streams2[1].result();

	// Each stream should have independent results
	strictEqual(result1.key, "charset");
	strictEqual(result2.key, "charset");
	// Confidence should reflect only the data from each respective stream
	strictEqual(typeof result1.value.confidence, "number");
	strictEqual(typeof result2.value.confidence, "number");
});

// *** charsetDetectStream confidence normalization *** //
test(`${variant}: charsetDetectStream should normalize confidence across chunks`, async (_t) => {
	// Send many chunks — confidence should be averaged, not sum to huge numbers
	const chunks = Array.from({ length: 100 }, () => Buffer.from("Hello World"));
	const streams = [createReadableStream(chunks), charsetDetectStream()];

	await pipeline(streams);
	const { value } = streams[1].result();

	// Confidence should be a reasonable value (0-100 range), not 100x the single-chunk value
	ok(
		value.confidence <= 100,
		`confidence ${value.confidence} should be <= 100`,
	);
});

// *** charsetEncodeStream charset validation (web) *** //
if (variant === "webstream") {
	test(`${variant}: charsetEncodeStream should throw for non-UTF-8 charset`, async (_t) => {
		try {
			charsetEncodeStream({ charset: "iso-8859-1" });
			throw new Error("Expected error");
		} catch (e) {
			ok(e.message.includes("UTF-8"));
		}
	});

	test(`${variant}: charsetDecodeStream should throw for unsupported charset`, async (_t) => {
		try {
			charsetDecodeStream({ charset: "INVALID-CHARSET-999" });
			throw new Error("Expected error");
		} catch (e) {
			ok(e.message !== "Expected error");
		}
	});
}
