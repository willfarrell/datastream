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
