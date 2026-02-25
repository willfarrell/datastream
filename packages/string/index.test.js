import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
// import sinon from 'sinon'
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

import stringDefault, {
	stringCountStream,
	stringLengthStream,
	stringReadableStream,
	stringReplaceStream,
	stringSkipConsecutiveDuplicates,
	stringSplitStream,
} from "@datastream/string";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** stringReadableStream *** //
test(`${variant}: stringReadableStream should read in initial chunks`, async (_t) => {
	const input = "abc";
	const streams = [stringReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [input]);
});

// *** stringLengthStream *** //
test(`${variant}: stringLengthStream should count length of chunks`, async (_t) => {
	const input = ["1", "2", "3"];
	const streams = [createReadableStream(input), stringLengthStream()];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "length");
	strictEqual(result.length, 3);
	strictEqual(value, 3);
});

test(`${variant}: stringSizeStream should count length of chunks with custom key`, async (_t) => {
	const input = ["1", "2", "3"];
	const streams = [
		createReadableStream(input),
		stringLengthStream({ resultKey: "string" }),
	];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "string");
	strictEqual(result.string, 3);
	strictEqual(value, 3);
});

// *** stringSkipConsecutiveDuplicates *** //
test(`${variant}: stringSkipConsecutiveDuplicates should skip consecutive duplicates`, async (_t) => {
	const input = ["1", "2", "2", "3"];
	const streams = [
		createReadableStream(input),
		stringSkipConsecutiveDuplicates(),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["1", "2", "3"]);
});

// *** stringSplitStream *** //
test(`${variant}: stringSplitStream should split into empty strings`, async (_t) => {
	const input = [",,", ",,"];
	const streams = [
		createReadableStream(input),
		stringSplitStream({ separator: "," }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["", "", "", "", ""]);
});

test(`${variant}: stringSplitStream should split across chunk boundaries`, async (_t) => {
	const input = ["a,b", "c,d"];
	const streams = [
		createReadableStream(input),
		stringSplitStream({ separator: "," }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["a", "bc", "d"]);
});

// *** stringCountStream *** //
test(`${variant}: stringCountStream should count occurrences of substring`, async (_t) => {
	const input = ["hello world", "hello universe"];
	const streams = [
		createReadableStream(input),
		stringCountStream({ substr: "hello" }),
	];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "count");
	strictEqual(result.count, 2);
	strictEqual(value, 2);
});

test(`${variant}: stringCountStream should count multiple occurrences in single chunk`, async (_t) => {
	const input = ["aaa aaa aaa"];
	const streams = [
		createReadableStream(input),
		stringCountStream({ substr: "a" }),
	];

	await pipeline(streams);
	const { value } = streams[1].result();

	strictEqual(value, 9);
});

test(`${variant}: stringCountStream should use custom result key`, async (_t) => {
	const input = ["test test"];
	const streams = [
		createReadableStream(input),
		stringCountStream({ substr: "test", resultKey: "matches" }),
	];

	const result = await pipeline(streams);
	const { key } = streams[1].result();

	strictEqual(key, "matches");
	strictEqual(result.matches, 2);
});

// *** stringReplaceStream *** //
test(`${variant}: stringReplaceStream should replace pattern across chunks`, async (_t) => {
	const input = ["hello world", "hello universe"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({ pattern: /hello/g, replacement: "hi" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// First chunk: previousChunk="", outputs "", previousChunk becomes "hi world"
	// Second chunk: previousChunk="hi world", newChunk = "hi worldhello universe".replace() = "hi worldhi universe"
	// outputs "hi world" (substring 0, 8), previousChunk becomes "hi universe"
	// flush outputs "hi universe"
	deepStrictEqual(output, ["", "hi world", "hi universe"]);
});

test(`${variant}: stringReplaceStream should handle pattern spanning chunks`, async (_t) => {
	const input = ["hel", "lo world"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({ pattern: /hello/g, replacement: "hi" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// Implementation buffers and matches across chunks:
	// Chunk 1: "hel" -> enqueue "", previousChunk="hel"
	// Chunk 2: "hel"+"lo world" = "hello world" -> replace = "hi world" -> enqueue "hi ", previousChunk="world"
	// Flush: enqueue "world"
	deepStrictEqual(output, ["", "hi ", "world"]);
});

test(`${variant}: stringReplaceStream should replace with string pattern`, async (_t) => {
	const input = ["hello world"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({ pattern: "hello", replacement: "hi" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// Output includes empty string from first chunk (previousChunk.length was 0)
	deepStrictEqual(output, ["", "hi world"]);
});

// *** default export *** //
test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(stringDefault).sort(), [
		"countStream",
		"lengthStream",
		"readableStream",
		"replaceStream",
		"skipConsecutiveDuplicates",
		"splitStream",
	]);
});
