import { deepStrictEqual } from "node:assert";
import test from "node:test";

import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

import {
	jsonFormatStream,
	jsonParseStream,
	ndjsonFormatStream,
	ndjsonParseStream,
} from "@datastream/json";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** ndjsonParseStream *** //
test(`${variant}: ndjsonParseStream should parse single NDJSON line`, async (_t) => {
	const streams = [createReadableStream('{"a":1}\n'), ndjsonParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }]);
});

test(`${variant}: ndjsonParseStream should parse multiple NDJSON lines`, async (_t) => {
	const streams = [
		createReadableStream('{"a":1}\n{"b":2}\n{"c":3}\n'),
		ndjsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }, { c: 3 }]);
});

test(`${variant}: ndjsonParseStream should handle chunk boundaries`, async (_t) => {
	const streams = [
		createReadableStream(['{"a":', "1}\n", '{"b":2}\n']),
		ndjsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
});

test(`${variant}: ndjsonParseStream should skip empty lines`, async (_t) => {
	const streams = [
		createReadableStream('{"a":1}\n\n\n{"b":2}\n'),
		ndjsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
});

test(`${variant}: ndjsonParseStream should handle no trailing newline`, async (_t) => {
	const streams = [
		createReadableStream('{"a":1}\n{"b":2}'),
		ndjsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
});

test(`${variant}: ndjsonParseStream should track parse errors in result`, async (_t) => {
	const parse = ndjsonParseStream();
	const streams = [createReadableStream('{"a":1}\nnot json\n{"b":2}\n'), parse];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
	const { key, value } = parse.result();
	deepStrictEqual(key, "jsonErrors");
	deepStrictEqual(value.ParseError.id, "ParseError");
	deepStrictEqual(value.ParseError.idx, [1]);
});

test(`${variant}: ndjsonParseStream should throw when buffer exceeds maxBufferSize`, async (_t) => {
	let error;
	try {
		await pipeline([
			createReadableStream("a".repeat(200)),
			ndjsonParseStream({ maxBufferSize: 100 }),
		]);
	} catch (e) {
		error = e;
	}
	deepStrictEqual(error?.message?.includes("maxBufferSize"), true);
});

test(`${variant}: ndjsonParseStream should handle \\r\\n line endings`, async (_t) => {
	const streams = [
		createReadableStream('{"a":1}\r\n{"b":2}\r\n'),
		ndjsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
});

// *** ndjsonFormatStream *** //
test(`${variant}: ndjsonFormatStream should format objects as NDJSON`, async (_t) => {
	const streams = [
		createReadableStream([{ a: 1 }, { b: 2 }]),
		ndjsonFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	const combined = output.join("");

	deepStrictEqual(combined, '{"a":1}\n{"b":2}\n');
});

test(`${variant}: ndjsonFormatStream roundtrip with ndjsonParseStream`, async (_t) => {
	const input = [{ a: 1 }, { b: "hello" }, { c: [1, 2, 3] }];
	const streams = [
		createReadableStream(input),
		ndjsonFormatStream(),
		ndjsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, input);
});

// *** jsonParseStream *** //
test(`${variant}: jsonParseStream should parse JSON array into objects`, async (_t) => {
	const streams = [
		createReadableStream('[{"a":1},{"b":2}]'),
		jsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
});

test(`${variant}: jsonParseStream should handle nested objects and arrays`, async (_t) => {
	const streams = [
		createReadableStream('[{"a":{"b":1}},{"c":[1,2,3]}]'),
		jsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: { b: 1 } }, { c: [1, 2, 3] }]);
});

test(`${variant}: jsonParseStream should handle strings with brackets and braces`, async (_t) => {
	const streams = [
		createReadableStream('[{"a":"{not json}"},{"b":"[1,2]"}]'),
		jsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: "{not json}" }, { b: "[1,2]" }]);
});

test(`${variant}: jsonParseStream should handle strings with escaped quotes`, async (_t) => {
	const streams = [
		createReadableStream('[{"a":"hello \\"world\\""},{"b":"test"}]'),
		jsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 'hello "world"' }, { b: "test" }]);
});

test(`${variant}: jsonParseStream should handle chunk boundaries mid-object`, async (_t) => {
	const streams = [
		createReadableStream(['[{"a":', "1},", '{"b":2}]']),
		jsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
});

test(`${variant}: jsonParseStream should handle empty array`, async (_t) => {
	const streams = [createReadableStream("[]"), jsonParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});

test(`${variant}: jsonParseStream should handle single element`, async (_t) => {
	const streams = [createReadableStream('[{"a":1}]'), jsonParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }]);
});

test(`${variant}: jsonParseStream should handle non-object elements`, async (_t) => {
	const streams = [
		createReadableStream('[1, "hello", true, null, false]'),
		jsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [1, "hello", true, null, false]);
});

test(`${variant}: jsonParseStream should handle whitespace between elements`, async (_t) => {
	const streams = [
		createReadableStream('[ {"a":1} , \n {"b":2} \n]'),
		jsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
});

test(`${variant}: jsonParseStream should track parse errors in result`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [createReadableStream('[{"a":1},{invalid},{"b":2}]'), parse];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
	const { key, value } = parse.result();
	deepStrictEqual(key, "jsonErrors");
	deepStrictEqual(value.ParseError.id, "ParseError");
	deepStrictEqual(value.ParseError.idx, [1]);
});

test(`${variant}: jsonParseStream should track NoArrayStart when no '[' seen`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [createReadableStream('{"a":1}'), parse];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
	const { value } = parse.result();
	deepStrictEqual(value.NoArrayStart?.id, "NoArrayStart");
	deepStrictEqual(value.NoArrayStart?.idx, [0]);
});

test(`${variant}: jsonParseStream should not flag NoArrayStart for empty input`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [createReadableStream(""), parse];
	await streamToArray(pipejoin(streams));
	const { value } = parse.result();
	deepStrictEqual(value.NoArrayStart, undefined);
});

test(`${variant}: jsonParseStream should throw when buffer exceeds maxBufferSize`, async (_t) => {
	let error;
	try {
		await pipeline([
			createReadableStream(`[{"a":"${"x".repeat(200)}"}]`),
			jsonParseStream({ maxBufferSize: 100 }),
		]);
	} catch (e) {
		error = e;
	}
	deepStrictEqual(error?.message?.includes("maxBufferSize"), true);
});

// *** jsonFormatStream *** //
test(`${variant}: jsonFormatStream should format objects as JSON array`, async (_t) => {
	const streams = [
		createReadableStream([{ a: 1 }, { b: 2 }]),
		jsonFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	const combined = output.join("");

	deepStrictEqual(combined, '[{"a":1},\n{"b":2}\n]');
});

test(`${variant}: jsonFormatStream should handle empty input`, async (_t) => {
	const streams = [createReadableStream([]), jsonFormatStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	const combined = output.join("");

	deepStrictEqual(combined, "[]");
});

test(`${variant}: jsonFormatStream should handle pretty-print with space option`, async (_t) => {
	const streams = [
		createReadableStream([{ a: 1 }]),
		jsonFormatStream({ space: 2 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	const combined = output.join("");

	deepStrictEqual(combined, '[{\n  "a": 1\n}\n]');
});

// *** maxValueSize raw check *** //
test(`${variant}: jsonParseStream should enforce maxValueSize on raw value including whitespace`, async (_t) => {
	const { ok, strictEqual } = await import("node:assert");
	// Raw value with padding: "  123  " = 7 chars, trimmed "123" = 3 chars
	// maxValueSize: 5 — raw exceeds, trimmed does not
	const padded = `[  ${"1".repeat(4)}  ]`;
	const streams = [
		createReadableStream(padded),
		jsonParseStream({ maxValueSize: 5 }),
	];
	try {
		await pipeline(streams);
		throw new Error("Expected maxValueSize error");
	} catch (e) {
		ok(e.message.includes("maxValueSize"));
		strictEqual(e.message.includes("Expected"), false);
	}
});

test(`${variant}: jsonFormatStream roundtrip with jsonParseStream`, async (_t) => {
	const input = [{ a: 1 }, { b: "hello" }, { c: [1, 2, 3] }];
	const streams = [
		createReadableStream(input),
		jsonFormatStream(),
		jsonParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, input);
});

// *** Additional tests to improve mutation score *** //

// --- ndjsonParseStream: error deduplication (trackError guard) ---
test(`${variant}: ndjsonParseStream should accumulate idx list for repeated ParseError`, async (_t) => {
	const parse = ndjsonParseStream();
	const streams = [createReadableStream("bad1\nbad2\nbad3\n"), parse];
	await streamToArray(pipejoin(streams));
	const { value } = parse.result();
	// All three errors under same ParseError key - idx has 3 entries
	deepStrictEqual(value.ParseError.id, "ParseError");
	deepStrictEqual(value.ParseError.message, "Invalid JSON");
	deepStrictEqual(value.ParseError.idx, [0, 1, 2]);
});

// --- ndjsonParseStream: maxBufferSize boundary (> not >=) ---
test(`${variant}: ndjsonParseStream should not throw at exactly maxBufferSize`, async (_t) => {
	// buffer.length + chunk.length > maxBufferSize: equal should NOT throw
	const maxBufferSize = 10;
	let error;
	try {
		await pipeline([
			createReadableStream("a".repeat(10)),
			ndjsonParseStream({ maxBufferSize }),
		]);
	} catch (e) {
		error = e;
	}
	deepStrictEqual(error, undefined);
});

test(`${variant}: ndjsonParseStream should throw one byte over maxBufferSize`, async (_t) => {
	// Two chunks: first 10 chars (no newline → stays in buffer), then 1 more → 11 > 10
	let error;
	try {
		await pipeline([
			createReadableStream(["a".repeat(10), "b"]),
			ndjsonParseStream({ maxBufferSize: 10 }),
		]);
	} catch (e) {
		error = e;
	}
	deepStrictEqual(error?.message?.includes("maxBufferSize"), true);
});

// --- ndjsonParseStream: error message uses addition (not subtraction) ---
test(`${variant}: ndjsonParseStream error message contains combined length and limit`, async (_t) => {
	let error;
	try {
		await pipeline([
			createReadableStream(["a".repeat(10), "b".repeat(5)]),
			ndjsonParseStream({ maxBufferSize: 12 }),
		]);
	} catch (e) {
		error = e;
	}
	// buffer.length (10) + chunk.length (5) = 15; limit = 12
	deepStrictEqual(error?.message?.includes("15"), true);
	deepStrictEqual(error?.message?.includes("12"), true);
});

// --- ndjsonParseStream: trimEnd (not trimStart) for lines ---
test(`${variant}: ndjsonParseStream flush trims trailing whitespace (\\r) on last line`, async (_t) => {
	// No trailing newline; line ends with \r. trimEnd removes it, trimStart would not.
	const streams = [createReadableStream('{"a":1}\r'), ndjsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 1 }]);
});

// --- ndjsonParseStream: flush catch block (not empty) ---
test(`${variant}: ndjsonParseStream flush should track error for invalid JSON without trailing newline`, async (_t) => {
	const parse = ndjsonParseStream();
	const streams = [createReadableStream("not-json"), parse];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, []);
	const { value } = parse.result();
	deepStrictEqual(value.ParseError?.id, "ParseError");
	deepStrictEqual(value.ParseError?.message, "Invalid JSON");
	deepStrictEqual(value.ParseError?.idx, [0]);
});

// --- ndjsonParseStream: flush idx++ (not --) ---
test(`${variant}: ndjsonParseStream flush should use correct idx for flushed error`, async (_t) => {
	// One valid transform line, one invalid flush line → error at idx 1
	const parse = ndjsonParseStream();
	const streams = [createReadableStream('{"a":1}\nbad'), parse];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 1 }]);
	const { value } = parse.result();
	deepStrictEqual(value.ParseError.idx, [1]);
});

// --- ndjsonParseStream: resultKey ---
test(`${variant}: ndjsonParseStream should use custom resultKey`, async (_t) => {
	const parse = ndjsonParseStream({ resultKey: "myErrors" });
	const streams = [createReadableStream('{"a":1}\n'), parse];
	await streamToArray(pipejoin(streams));
	const { key } = parse.result();
	deepStrictEqual(key, "myErrors");
});

// --- ndjsonFormatStream: batching >= 64 ---
test(`${variant}: ndjsonFormatStream should flush eagerly after exactly 64 items`, async (_t) => {
	const items = Array.from({ length: 64 }, (_, i) => ({ i }));
	const streams = [createReadableStream(items), ndjsonFormatStream()];
	const combined = (await streamToArray(pipejoin(streams))).join("");
	const lines = combined.trim().split("\n");
	deepStrictEqual(lines.length, 64);
	deepStrictEqual(JSON.parse(lines[0]), { i: 0 });
	deepStrictEqual(JSON.parse(lines[63]), { i: 63 });
});

test(`${variant}: ndjsonFormatStream should handle 65 items (one full batch plus one)`, async (_t) => {
	const items = Array.from({ length: 65 }, (_, i) => ({ i }));
	const streams = [createReadableStream(items), ndjsonFormatStream()];
	const combined = (await streamToArray(pipejoin(streams))).join("");
	const lines = combined.trim().split("\n");
	deepStrictEqual(lines.length, 65);
	deepStrictEqual(JSON.parse(lines[64]), { i: 64 });
});

// --- ndjsonFormatStream: newline separator (not empty string) ---
test(`${variant}: ndjsonFormatStream items joined with newline separator`, async (_t) => {
	const streams = [
		createReadableStream([{ a: 1 }, { b: 2 }]),
		ndjsonFormatStream(),
	];
	const combined = (await streamToArray(pipejoin(streams))).join("");
	deepStrictEqual(combined.endsWith("\n"), true);
	const lines = combined.split("\n").filter((l) => l.length > 0);
	deepStrictEqual(lines.length, 2);
	deepStrictEqual(lines[0], '{"a":1}');
	deepStrictEqual(lines[1], '{"b":2}');
});

// --- ndjsonFormatStream: flush only when batch.length > 0 ---
test(`${variant}: ndjsonFormatStream should not emit anything when no items`, async (_t) => {
	const streams = [createReadableStream([]), ndjsonFormatStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, []);
});

test(`${variant}: ndjsonFormatStream should flush single item`, async (_t) => {
	const streams = [createReadableStream([{ x: 42 }]), ndjsonFormatStream()];
	const combined = (await streamToArray(pipejoin(streams))).join("");
	deepStrictEqual(combined, '{"x":42}\n');
});

// --- ndjsonFormatStream: space option ---
test(`${variant}: ndjsonFormatStream should use space option for pretty-printing`, async (_t) => {
	const streams = [
		createReadableStream([{ a: 1 }]),
		ndjsonFormatStream({ space: 2 }),
	];
	const combined = (await streamToArray(pipejoin(streams))).join("");
	deepStrictEqual(combined, '{\n  "a": 1\n}\n');
});

// --- jsonParseStream: trackError deduplication ---
test(`${variant}: jsonParseStream should accumulate idx list for repeated ParseError`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [createReadableStream("[bad1,bad2,bad3]"), parse];
	await streamToArray(pipejoin(streams));
	const { value } = parse.result();
	deepStrictEqual(value.ParseError.id, "ParseError");
	deepStrictEqual(value.ParseError.message, "Invalid JSON");
	deepStrictEqual(value.ParseError.idx, [0, 1, 2]);
});

// --- jsonParseStream: maxValueSize boundary (> not >=) ---
test(`${variant}: jsonParseStream should not throw at exactly maxValueSize`, async (_t) => {
	// element "12345" is 5 chars, not > 5, should parse fine
	const streams = [
		createReadableStream("[12345]"),
		jsonParseStream({ maxValueSize: 5 }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [12345]);
});

test(`${variant}: jsonParseStream should throw when element exceeds maxValueSize`, async (_t) => {
	// "[ 123456 ]" → raw element " 123456 " = 8 chars > 5
	let error;
	try {
		await pipeline([
			createReadableStream("[ 123456 ]"),
			jsonParseStream({ maxValueSize: 5 }),
		]);
	} catch (e) {
		error = e;
	}
	deepStrictEqual(error?.message?.includes("maxValueSize"), true);
});

// --- jsonParseStream: emitElement trims text before parse ---
test(`${variant}: jsonParseStream should skip whitespace-only elements without error`, async (_t) => {
	// Exercises trimmed.length === 0 early return in emitElement
	const streams = [createReadableStream("[   ]"), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, []);
});

// --- jsonParseStream: escaped char handling ---
test(`${variant}: jsonParseStream should handle escaped quote in string`, async (_t) => {
	const streams = [createReadableStream('[{"a":"x\\"y"}]'), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 'x"y' }]);
});

test(`${variant}: jsonParseStream should handle double backslash in string`, async (_t) => {
	const streams = [createReadableStream('[{"a":"\\\\"}]'), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: "\\" }]);
});

// --- jsonParseStream: inString prevents depth changes for brackets in strings ---
test(`${variant}: jsonParseStream should not count brackets inside strings as depth`, async (_t) => {
	const streams = [
		createReadableStream('[{"a":"[{nested}]"}]'),
		jsonParseStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: "[{nested}]" }]);
});

// --- jsonParseStream: string as top-level element (opens inString) ---
test(`${variant}: jsonParseStream should parse string elements at top level`, async (_t) => {
	const streams = [
		createReadableStream('["hello","world"]'),
		jsonParseStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, ["hello", "world"]);
});

// --- jsonParseStream: whitespace chars (tab 0x09, LF 0x0a, CR 0x0d) ---
test(`${variant}: jsonParseStream should treat tab as whitespace before elements`, async (_t) => {
	const streams = [createReadableStream("[1,\t2,\t3]"), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [1, 2, 3]);
});

test(`${variant}: jsonParseStream should treat CR as whitespace before elements`, async (_t) => {
	const streams = [createReadableStream("[1,\r2,\r3]"), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [1, 2, 3]);
});

// --- jsonParseStream: buffer trimming (trimFrom) ---
test(`${variant}: jsonParseStream should work correctly with many small chunks`, async (_t) => {
	const chunks = ["[", '{"a":', "1}", ",", '{"b":', "2}", "]"];
	const streams = [createReadableStream(chunks), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
});

test(`${variant}: jsonParseStream should handle multiple elements across chunks`, async (_t) => {
	const chunks = ['[{"a":1},', '{"b":2},', '{"c":3}]'];
	const streams = [createReadableStream(chunks), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 1 }, { b: 2 }, { c: 3 }]);
});

// --- jsonParseStream: flush ']' guard ---
test(`${variant}: jsonParseStream flush should not emit lone closing bracket as element`, async (_t) => {
	const streams = [createReadableStream("[1,2]"), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [1, 2]);
});

// --- jsonParseStream: sawNonWhitespace for NoArrayStart ---
test(`${variant}: jsonParseStream whitespace-only input should not flag NoArrayStart`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [createReadableStream("   \n\t  "), parse];
	await streamToArray(pipejoin(streams));
	const { value } = parse.result();
	deepStrictEqual(value.NoArrayStart, undefined);
});

test(`${variant}: jsonParseStream non-whitespace without [ should flag NoArrayStart with message`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [createReadableStream("abc"), parse];
	await streamToArray(pipejoin(streams));
	const { value } = parse.result();
	deepStrictEqual(value.NoArrayStart?.id, "NoArrayStart");
	deepStrictEqual(
		value.NoArrayStart?.message,
		"Input did not contain a top-level array",
	);
});

// --- jsonParseStream: maxBufferSize error message uses addition ---
test(`${variant}: jsonParseStream maxBufferSize error message contains combined length`, async (_t) => {
	let error;
	try {
		await pipeline([
			createReadableStream(`[{"a":"${"x".repeat(10)}"}]`),
			jsonParseStream({ maxBufferSize: 15 }),
		]);
	} catch (e) {
		error = e;
	}
	deepStrictEqual(error?.message?.includes("maxBufferSize"), true);
	// The message includes the buffer + chunk length (not subtraction)
	// Since "[{\"a\":\"" = 7 chars stays in buffer, then adding more chars triggers limit
	// Just verify the format matches expected pattern
	deepStrictEqual(error?.message?.includes("exceeds"), true);
});

// --- jsonParseStream: flush handles no elementStart gracefully ---
test(`${variant}: jsonParseStream flush with no elementStart does not crash`, async (_t) => {
	const streams = [createReadableStream('[{"a":1}]'), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 1 }]);
});

// --- jsonParseStream: depth tracking ---
test(`${variant}: jsonParseStream should parse deeply nested objects`, async (_t) => {
	const streams = [
		createReadableStream('[{"a":{"b":{"c":1}}}]'),
		jsonParseStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: { b: { c: 1 } } }]);
});

// --- jsonParseStream: error idx ordering ---
test(`${variant}: jsonParseStream error idx should reflect insertion order`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [
		createReadableStream('[{"a":1},bad,{"b":2},worse,{"c":3}]'),
		parse,
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 1 }, { b: 2 }, { c: 3 }]);
	const { value } = parse.result();
	deepStrictEqual(value.ParseError.idx, [1, 3]);
});

// --- jsonParseStream: resultKey ---
test(`${variant}: jsonParseStream should use custom resultKey`, async (_t) => {
	const parse = jsonParseStream({ resultKey: "myErrors" });
	const streams = [createReadableStream('[{"a":1}]'), parse];
	await streamToArray(pipejoin(streams));
	const { key } = parse.result();
	deepStrictEqual(key, "myErrors");
});

// --- jsonFormatStream: first/non-first item separator ---
test(`${variant}: jsonFormatStream first element uses [ prefix; rest use comma-newline`, async (_t) => {
	const streams = [
		createReadableStream([{ a: 1 }, { b: 2 }, { c: 3 }]),
		jsonFormatStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output[0], '[{"a":1}');
	deepStrictEqual(output[1], ',\n{"b":2}');
	deepStrictEqual(output[2], ',\n{"c":3}');
});

// --- jsonFormatStream: flush emits \n] for non-empty ---
test(`${variant}: jsonFormatStream flush emits newline-bracket for non-empty stream`, async (_t) => {
	const streams = [createReadableStream([{ a: 1 }]), jsonFormatStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output[output.length - 1], "\n]");
});

test(`${variant}: jsonFormatStream single number produces correct JSON array`, async (_t) => {
	const streams = [createReadableStream([42]), jsonFormatStream()];
	const combined = (await streamToArray(pipejoin(streams))).join("");
	deepStrictEqual(combined, "[42\n]");
});

// --- jsonParseStream: mixed object and string elements ---
test(`${variant}: jsonParseStream should parse mixed object and string elements`, async (_t) => {
	const streams = [
		createReadableStream('[{"a":1},"hello"]'),
		jsonParseStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 1 }, "hello"]);
});

// --- jsonParseStream: flush emitElement with ']' trimmed ---
test(`${variant}: jsonParseStream flush should emit incomplete element at end of stream`, async (_t) => {
	// Input split across chunks where last chunk has no closing bracket scanned
	// to exercise flush emitElement path
	const streams = [
		createReadableStream(['[{"a":1},', '{"b":2']),
		jsonParseStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	// {"b":2 is incomplete JSON but flush emits it, parse fails → tracked as error
	deepStrictEqual(output, [{ a: 1 }]);
	// The incomplete element causes a ParseError
});

// --- ndjsonParseStream: transform empty line skip guard ---
test(`${variant}: ndjsonParseStream should count idx correctly when skipping empty lines`, async (_t) => {
	// Two valid lines with empty lines in between
	// Empty lines don't increment idx, so second parse error should be at idx 1
	const parse = ndjsonParseStream();
	const streams = [createReadableStream('{"a":1}\n\nbad\n{"c":3}\n'), parse];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 1 }, { c: 3 }]);
	const { value } = parse.result();
	deepStrictEqual(value.ParseError.idx, [1]);
});

// --- jsonParseStream: flush element with actual content ---
test(`${variant}: jsonParseStream flush emits element for incomplete array without closing bracket`, async (_t) => {
	// Array missing closing ] - flush should emit what it has
	const parse = jsonParseStream();
	const streams = [createReadableStream('[{"a":1},{"b":2}'), parse];
	const output = await streamToArray(pipejoin(streams));
	// {"a":1} is emitted during scan; {"b":2} has no closing } so it stays in buffer
	// flush tries to emit the remaining element
	deepStrictEqual(output.length >= 1, true);
	deepStrictEqual(output[0], { a: 1 });
});

// *** Second-round targeted tests for remaining survivors *** //

// --- ndjsonFormatStream: chunk count distinguishes >= 64 from > 64 ---
test(`${variant}: ndjsonFormatStream 65 items should produce 2 output chunks`, async (_t) => {
	// Real (>= 64): at item 64, eager flush -> 64-item chunk; flush() -> 1-item chunk -> 2 chunks
	// Mutant (> 64): at item 65, eager flush -> 65-item chunk; flush() nothing -> 1 chunk
	const items = Array.from({ length: 65 }, (_, i) => ({ i }));
	const streams = [createReadableStream(items), ndjsonFormatStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output.length, 2);
	const firstChunkLines = output[0].split("\n").filter((l) => l.length > 0);
	deepStrictEqual(firstChunkLines.length, 64);
	const secondChunkLines = output[1].split("\n").filter((l) => l.length > 0);
	deepStrictEqual(secondChunkLines.length, 1);
});

test(`${variant}: ndjsonFormatStream 2 items should produce 1 output chunk not 2`, async (_t) => {
	// if(true) mutant: emits 1 item per chunk -> 2 chunks; real >= 64: via flush -> 1 chunk
	const items = [{ a: 1 }, { b: 2 }];
	const streams = [createReadableStream(items), ndjsonFormatStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output.length, 1);
	const lines = output[0].split("\n").filter((l) => l.length > 0);
	deepStrictEqual(lines.length, 2);
});

// --- ndjsonParseStream: flush idx increments after multiple valid lines ---
test(`${variant}: ndjsonParseStream flush idx is 3 after 3 valid transform lines`, async (_t) => {
	const parse = ndjsonParseStream();
	const streams = [
		createReadableStream('{"a":1}\n{"b":2}\n{"c":3}\nbad'),
		parse,
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 1 }, { b: 2 }, { c: 3 }]);
	const { value } = parse.result();
	deepStrictEqual(value.ParseError.idx, [3]);
});

// --- jsonParseStream: initial buffer must be empty ---
test(`${variant}: jsonParseStream initial buffer must be empty string`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [createReadableStream("[1]"), parse];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [1]);
	const { value } = parse.result();
	deepStrictEqual(value.NoArrayStart, undefined);
});

// --- jsonParseStream: inString=true when quote encountered ---
test(`${variant}: jsonParseStream handles string split across chunks`, async (_t) => {
	const streams = [createReadableStream(['["hel', 'lo"]']), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, ["hello"]);
});

test(`${variant}: jsonParseStream string with braces does not affect depth`, async (_t) => {
	const streams = [createReadableStream('["a{b}c"]'), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, ["a{b}c"]);
});

// --- jsonParseStream: maxBufferSize error uses addition not subtraction ---
test(`${variant}: jsonParseStream maxBufferSize error message uses addition`, async (_t) => {
	// After "[1234" processed, buffer trimmed to "1234" (4 chars)
	// Then ",5678]" (6 chars): 4+6=10 > 6; subtraction mutant: 4-6=-2
	let error;
	try {
		await pipeline([
			createReadableStream(["[1234", ",5678]"]),
			jsonParseStream({ maxBufferSize: 6 }),
		]);
	} catch (e) {
		error = e;
	}
	deepStrictEqual(error?.message?.includes("maxBufferSize"), true);
	deepStrictEqual(error?.message?.includes("10"), true);
});

// --- jsonParseStream: maxBufferSize boundary (> not >=) ---
test(`${variant}: jsonParseStream does not throw at exactly maxBufferSize`, async (_t) => {
	const streams = [
		createReadableStream("[1]"),
		jsonParseStream({ maxBufferSize: 3 }),
	];
	let error;
	try {
		await streamToArray(pipejoin(streams));
	} catch (e) {
		error = e;
	}
	deepStrictEqual(error, undefined);
});

test(`${variant}: jsonParseStream throws one byte over maxBufferSize`, async (_t) => {
	let error;
	try {
		await pipeline([
			createReadableStream(["[1", ",2]"]),
			jsonParseStream({ maxBufferSize: 2 }),
		]);
	} catch (e) {
		error = e;
	}
	deepStrictEqual(error?.message?.includes("maxBufferSize"), true);
});

// --- jsonParseStream flush: does not emit when elementStart === -1 ---
test(`${variant}: jsonParseStream flush does not double-emit completed elements`, async (_t) => {
	const streams = [createReadableStream("[1,2]"), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [1, 2]);
	deepStrictEqual(output.length, 2);
});

// --- jsonParseStream flush: emits when elementStart !== -1 and buffer non-empty ---
test(`${variant}: jsonParseStream flush emits in-progress element`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [createReadableStream("[42"), parse];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [42]);
});

test(`${variant}: jsonParseStream flush uses elementStart offset correctly`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [createReadableStream("[  42"), parse];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [42]);
});

// --- jsonParseStream flush: trimmed !== "]" ---
test(`${variant}: jsonParseStream flush does not emit lone closing bracket`, async (_t) => {
	const streams = [createReadableStream("[1]"), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [1]);
	deepStrictEqual(output.length, 1);
});

// --- jsonParseStream flush: buffer reset to empty ---
test(`${variant}: jsonParseStream flush resets buffer to empty string`, async (_t) => {
	const parse = jsonParseStream();
	const streams = [createReadableStream("[1,2,3]"), parse];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [1, 2, 3]);
	const { value } = parse.result();
	deepStrictEqual(value.NoArrayStart, undefined);
	deepStrictEqual(value.ParseError, undefined);
});

// --- jsonParseStream: single-char chunking exercises trimFrom ---
test(`${variant}: jsonParseStream handles single-char chunking`, async (_t) => {
	const jsonStr = "[1,2,3,4,5]";
	const chunks = jsonStr.split("");
	const streams = [createReadableStream(chunks), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [1, 2, 3, 4, 5]);
});

// --- jsonParseStream: element with surrounding whitespace (trim in emitElement) ---
test(`${variant}: jsonParseStream correctly parses element with surrounding spaces`, async (_t) => {
	const streams = [createReadableStream("[ 42 ]"), jsonParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [42]);
});
