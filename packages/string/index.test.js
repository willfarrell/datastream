import { deepStrictEqual, ok, strictEqual } from "node:assert";
import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

import stringDefault, {
	stringCountStream,
	stringLengthStream,
	stringMinimumChunkSize,
	stringMinimumFirstChunkSize,
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

// *** stringMinimumFirstChunkSize *** //
test(`${variant}: stringMinimumFirstChunkSize should buffer until chunkSize reached`, async (_t) => {
	const input = ["ab", "cd", "ef"];
	const streams = [
		createReadableStream(input),
		stringMinimumFirstChunkSize({ chunkSize: 4 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["abcd", "ef"]);
});

test(`${variant}: stringMinimumFirstChunkSize should flush small input`, async (_t) => {
	const input = ["ab"];
	const streams = [
		createReadableStream(input),
		stringMinimumFirstChunkSize({ chunkSize: 100 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["ab"]);
});

test(`${variant}: stringMinimumFirstChunkSize should pass through after first chunk met`, async (_t) => {
	const input = ["abcdef", "gh", "ij"];
	const streams = [
		createReadableStream(input),
		stringMinimumFirstChunkSize({ chunkSize: 4 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["abcdef", "gh", "ij"]);
});

// *** stringMinimumChunkSize *** //
test(`${variant}: stringMinimumChunkSize should buffer until chunkSize reached`, async (_t) => {
	const input = ["ab", "cd", "ef"];
	const streams = [
		createReadableStream(input),
		stringMinimumChunkSize({ chunkSize: 4 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["abcd", "ef"]);
});

test(`${variant}: stringMinimumChunkSize should flush small input`, async (_t) => {
	const input = ["ab"];
	const streams = [
		createReadableStream(input),
		stringMinimumChunkSize({ chunkSize: 100 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["ab"]);
});

test(`${variant}: stringMinimumChunkSize should buffer all chunks to minimum size`, async (_t) => {
	const input = ["abcdef", "gh", "ij"];
	const streams = [
		createReadableStream(input),
		stringMinimumChunkSize({ chunkSize: 4 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["abcdef", "ghij"]);
});

// *** stringReplaceStream buffer limit regression *** //
test(`${variant}: stringReplaceStream should throw when buffer exceeds maxBufferSize`, async (_t) => {
	const input = ["aaaa", "bbbb", "cccc"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({
			pattern: "zzz",
			replacement: "yyy",
			maxBufferSize: 3,
		}),
	];
	try {
		await pipeline(streams);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxBufferSize"));
	}
});

// *** stringSplitStream buffer limit regression *** //
test(`${variant}: stringSplitStream should throw when buffer exceeds maxBufferSize`, async (_t) => {
	const input = ["aaaaaa", "bbbbbb", "cccccc"];
	const streams = [
		createReadableStream(input),
		stringSplitStream({ separator: "zzz", maxBufferSize: 10 }),
	];
	try {
		await pipeline(streams);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxBufferSize"));
	}
});

// *** stringSplitStream empty separator guard *** //
test(`${variant}: stringSplitStream should throw on empty separator at construction`, (_t) => {
	let threw = false;
	try {
		stringSplitStream({ separator: "" });
	} catch (e) {
		threw = true;
		ok(
			e.message.includes("non-empty separator"),
			`expected non-empty separator in error, got: ${e.message}`,
		);
	}
	ok(threw, "stringSplitStream({ separator: '' }) should throw");
});

// *** stringCountStream cross-chunk boundary *** //
test(`${variant}: stringCountStream should count occurrences spanning chunk boundaries`, async (_t) => {
	const input = ["hel", "lo"];
	const streams = [
		createReadableStream(input),
		stringCountStream({ substr: "hello" }),
	];

	await pipeline(streams);
	const { value } = streams[1].result();

	strictEqual(value, 1);
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

// *** stringCountStream guard: typeof check *** //
test(`${variant}: stringCountStream should throw when substr is not a string (number)`, (_t) => {
	let threw = false;
	try {
		stringCountStream({ substr: 42 });
	} catch (e) {
		threw = true;
		strictEqual(e.message, "stringCountStream requires a non-empty substr");
	}
	ok(threw, "stringCountStream({ substr: 42 }) should throw");
});

test(`${variant}: stringCountStream should throw when substr is not a string (undefined)`, (_t) => {
	let threw = false;
	try {
		stringCountStream({});
	} catch (e) {
		threw = true;
		strictEqual(e.message, "stringCountStream requires a non-empty substr");
	}
	ok(threw, "stringCountStream({}) should throw");
});

test(`${variant}: stringCountStream should throw on empty substr`, (_t) => {
	let threw = false;
	try {
		stringCountStream({ substr: "" });
	} catch (e) {
		threw = true;
		strictEqual(e.message, "stringCountStream requires a non-empty substr");
	}
	ok(threw, "stringCountStream({ substr: '' }) should throw");
});

// *** stringCountStream: cursor boundary (< vs <=) *** //
test(`${variant}: stringCountStream should count match at the very end of combined string`, async (_t) => {
	// substr "ab" with combined exactly ending in "ab" — exercises cursor at length boundary
	const input = ["xab"];
	const streams = [
		createReadableStream(input),
		stringCountStream({ substr: "ab" }),
	];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value, 1);
});

// *** stringCountStream: carry arithmetic (substr.length - 1 vs + 1) *** //
test(`${variant}: stringCountStream should NOT double-count across chunk boundary when carry is exact`, async (_t) => {
	// substr = "ab", chunk1 = "a", chunk2 = "b" => carry="a", combined="ab" => 1 match
	// if carry used -(length+1) it would incorrectly carry "xa" or extra chars, changing count
	const input = ["a", "b", "ab"];
	const streams = [
		createReadableStream(input),
		stringCountStream({ substr: "ab" }),
	];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value, 2);
});

test(`${variant}: stringCountStream carry should not include extra characters that cause false positives`, async (_t) => {
	// substr = "ab" (length 2), carry should be 1 char (length-1)
	// chunk1="xxa", chunk2="b" => combined="xxab", carry after chunk1 = "a" (1 char)
	// if carry were 2 chars ("xa"), combined in chunk2 = "xab", still finds "ab" once
	// but with "aab" boundary: chunk1="aab", chunk2="c" => should count 1 not 2
	const input = ["aab", "c"];
	const streams = [
		createReadableStream(input),
		stringCountStream({ substr: "ab" }),
	];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value, 1);
});

test(`${variant}: stringCountStream carry boundary with 3-char substr`, async (_t) => {
	// substr = "abc" (length 3), carry should be 2 chars (length-1=2)
	// chunk1="xab", chunk2="cd" => carry="ab" (2 chars), combined="abcd", finds "abc" once total
	// if carry were 3 chars "xab", combined="xabcd", still finds "abc" once - same
	// Better: chunk1="abc" chunk2="abc" => 2 matches; carry from chunk1 = "bc" (2 chars)
	// combined in chunk2 = "bcabc" => finds "abc" at pos 2 => total = 2
	const input = ["abc", "abc"];
	const streams = [
		createReadableStream(input),
		stringCountStream({ substr: "abc" }),
	];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value, 2);
});

// *** stringMinimumFirstChunkSize: buffer reset after emit *** //
test(`${variant}: stringMinimumFirstChunkSize should output exact chunk size when met and not carry leftovers`, async (_t) => {
	// chunkSize=4, input ["ab","cd","ef"]
	// After emitting "abcd", buffer must be "" so next chunk "ef" is passed through as-is
	// If buffer="" mutant used "Stryker was here!", subsequent chunk would be wrong
	const input = ["ab", "cd", "ef"];
	const streams = [
		createReadableStream(input),
		stringMinimumFirstChunkSize({ chunkSize: 4 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, ["abcd", "ef"]);
});

test(`${variant}: stringMinimumFirstChunkSize should not emit in flush when done=true and buffer empty`, async (_t) => {
	// When chunkSize is met exactly, done=true and buffer="" => flush should emit nothing
	const input = ["abcd"];
	const streams = [
		createReadableStream(input),
		stringMinimumFirstChunkSize({ chunkSize: 4 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	// "abcd" meets chunkSize=4 exactly, emitted in transform; flush emits nothing (buffer empty, done=true)
	deepStrictEqual(output, ["abcd"]);
});

test(`${variant}: stringMinimumFirstChunkSize should not flush empty buffer when done=false`, async (_t) => {
	// Input is empty => done=false, buffer="" => flush should NOT emit empty string
	const input = [""];
	const streams = [
		createReadableStream(input),
		stringMinimumFirstChunkSize({ chunkSize: 4 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	// buffer is "" — flush condition `buffer.length > 0` must prevent enqueue
	deepStrictEqual(output, []);
});

// *** stringReplaceStream: RegExp guard *** //
test(`${variant}: stringReplaceStream should throw on RegExp without g or y flag`, (_t) => {
	let threw = false;
	try {
		stringReplaceStream({ pattern: /hello/, replacement: "hi" });
	} catch (e) {
		threw = true;
		strictEqual(
			e.message,
			"RegExp pattern must include the global (g) or sticky (y) flag",
		);
	}
	ok(threw, "stringReplaceStream with /hello/ (no flags) should throw");
});

test(`${variant}: stringReplaceStream should NOT throw on RegExp with g flag`, async (_t) => {
	const input = ["hello"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({ pattern: /hello/g, replacement: "hi" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, ["", "hi"]);
});

test(`${variant}: stringReplaceStream should NOT throw on RegExp with y flag`, async (_t) => {
	const input = ["hello"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({ pattern: /hello/y, replacement: "hi" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, ["", "hi"]);
});

test(`${variant}: stringReplaceStream should NOT throw on RegExp with both g and y flags`, async (_t) => {
	const input = ["hello"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({ pattern: /hello/gy, replacement: "hi" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, ["", "hi"]);
});

// *** stringReplaceStream: useReplaceAll (string vs RegExp path) *** //
test(`${variant}: stringReplaceStream should replace ALL occurrences with string pattern (replaceAll path)`, async (_t) => {
	// String pattern uses replaceAll, regex uses replace
	// With string "a", input "aaa" => all 3 replaced
	const input = ["aaa"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({ pattern: "a", replacement: "b" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, ["", "bbb"]);
});

test(`${variant}: stringReplaceStream with regex /a/g should replace all occurrences (replace path with global)`, async (_t) => {
	const input = ["aaa"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({ pattern: /a/g, replacement: "b" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, ["", "bbb"]);
});

// *** stringReplaceStream: maxBufferSize exact boundary (> vs >=) *** //
test(`${variant}: stringReplaceStream should NOT throw when buffer equals maxBufferSize exactly`, async (_t) => {
	// previousChunk.length === maxBufferSize should NOT throw (only > maxBufferSize should)
	// Use pattern that never matches so buffer grows; first chunk "aaaa" (4 chars) with maxBufferSize=4
	// After first chunk: combined="aaaa", enqueue substring(0,0)="", previousChunk="aaaa" (4 chars)
	// 4 > 4 is false => should NOT throw
	const input = ["aaaa"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({
			pattern: "zzz",
			replacement: "yyy",
			maxBufferSize: 4,
		}),
	];
	// Should not throw for 4-char buffer with maxBufferSize=4
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, ["", "aaaa"]);
});

test(`${variant}: stringReplaceStream should throw when buffer exceeds maxBufferSize by 1`, async (_t) => {
	// previousChunk.length = 5 > maxBufferSize = 4 => throws
	const input = ["aaaaa"];
	const streams = [
		createReadableStream(input),
		stringReplaceStream({
			pattern: "zzz",
			replacement: "yyy",
			maxBufferSize: 4,
		}),
	];
	try {
		await pipeline(streams);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxBufferSize"));
	}
});

// *** stringSplitStream: typeof separator guard *** //
test(`${variant}: stringSplitStream should throw when separator is not a string (number)`, (_t) => {
	let threw = false;
	try {
		stringSplitStream({ separator: 42 });
	} catch (e) {
		threw = true;
		ok(
			e.message.includes("non-empty separator"),
			`expected non-empty separator error, got: ${e.message}`,
		);
	}
	ok(threw, "stringSplitStream({ separator: 42 }) should throw");
});

test(`${variant}: stringSplitStream should throw when separator is undefined`, (_t) => {
	let threw = false;
	try {
		stringSplitStream({});
	} catch (e) {
		threw = true;
		ok(
			e.message.includes("non-empty separator"),
			`expected non-empty separator error, got: ${e.message}`,
		);
	}
	ok(threw, "stringSplitStream({}) should throw");
});

// *** stringSplitStream: maxBufferSize exact boundary (> vs >=) *** //
test(`${variant}: stringSplitStream should NOT throw when buffer equals maxBufferSize exactly`, async (_t) => {
	// separator "zzz" won't be found; chunk "aaaa" (4 chars), maxBufferSize=4
	// previousChunk.length=4, 4 > 4 is false => should NOT throw
	const input = ["aaaa"];
	const streams = [
		createReadableStream(input),
		stringSplitStream({ separator: "zzz", maxBufferSize: 4 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, ["aaaa"]);
});

test(`${variant}: stringSplitStream should throw when buffer exceeds maxBufferSize by 1`, async (_t) => {
	// chunk "aaaaa" (5 chars), maxBufferSize=4; 5 > 4 => throws
	const input = ["aaaaa"];
	const streams = [
		createReadableStream(input),
		stringSplitStream({ separator: "zzz", maxBufferSize: 4 }),
	];
	try {
		await pipeline(streams);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxBufferSize"));
	}
});
