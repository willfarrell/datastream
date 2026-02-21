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
