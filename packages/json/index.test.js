import { deepStrictEqual, rejects } from "node:assert";
import test, { describe } from "node:test";
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
import { variant } from "../variant.js";

describe(`@datastream/json (${variant})`, () => {
	test(`ndjsonParseStream should parse single NDJSON line`, async (_t) => {
		const streams = [createReadableStream('{"a":1}\n'), ndjsonParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }]);
	});

	test(`ndjsonParseStream should parse multiple NDJSON lines`, async (_t) => {
		const streams = [
			createReadableStream('{"a":1}\n{"b":2}\n{"c":3}\n'),
			ndjsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { b: 2 }, { c: 3 }]);
	});

	test(`ndjsonParseStream should handle chunk boundaries`, async (_t) => {
		const streams = [
			createReadableStream(['{"a":', "1}\n", '{"b":2}\n']),
			ndjsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
	});

	test(`ndjsonParseStream should skip empty lines`, async (_t) => {
		const streams = [
			createReadableStream('{"a":1}\n\n\n{"b":2}\n'),
			ndjsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
	});

	test(`ndjsonParseStream should handle no trailing newline`, async (_t) => {
		const streams = [
			createReadableStream('{"a":1}\n{"b":2}'),
			ndjsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
	});

	test(`ndjsonParseStream should track parse errors in result`, async (_t) => {
		const parse = ndjsonParseStream();
		const streams = [
			createReadableStream('{"a":1}\nnot json\n{"b":2}\n'),
			parse,
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
		const { key, value } = parse.result();
		deepStrictEqual(key, "ndjsonErrors");
		deepStrictEqual(value.ParseError.id, "ParseError");
		deepStrictEqual(value.ParseError.idx, [1]);
	});

	test(`ndjsonParseStream should throw when buffer exceeds maxBufferSize`, async (_t) => {
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

	test(`ndjsonParseStream should handle \\r\\n line endings`, async (_t) => {
		const streams = [
			createReadableStream('{"a":1}\r\n{"b":2}\r\n'),
			ndjsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
	});

	test(`ndjsonFormatStream should format objects as NDJSON`, async (_t) => {
		const streams = [
			createReadableStream([{ a: 1 }, { b: 2 }]),
			ndjsonFormatStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		const combined = output.join("");

		deepStrictEqual(combined, '{"a":1}\n{"b":2}\n');
	});

	test(`ndjsonFormatStream roundtrip with ndjsonParseStream`, async (_t) => {
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

	test(`jsonParseStream should parse JSON array into objects`, async (_t) => {
		const streams = [
			createReadableStream('[{"a":1},{"b":2}]'),
			jsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
	});

	test(`jsonParseStream should handle nested objects and arrays`, async (_t) => {
		const streams = [
			createReadableStream('[{"a":{"b":1}},{"c":[1,2,3]}]'),
			jsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: { b: 1 } }, { c: [1, 2, 3] }]);
	});

	test(`jsonParseStream should handle strings with brackets and braces`, async (_t) => {
		const streams = [
			createReadableStream('[{"a":"{not json}"},{"b":"[1,2]"}]'),
			jsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: "{not json}" }, { b: "[1,2]" }]);
	});

	test(`jsonParseStream should handle strings with escaped quotes`, async (_t) => {
		const streams = [
			createReadableStream('[{"a":"hello \\"world\\""},{"b":"test"}]'),
			jsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 'hello "world"' }, { b: "test" }]);
	});

	test(`jsonParseStream should handle chunk boundaries mid-object`, async (_t) => {
		const streams = [
			createReadableStream(['[{"a":', "1},", '{"b":2}]']),
			jsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
	});

	test(`jsonParseStream should handle empty array`, async (_t) => {
		const streams = [createReadableStream("[]"), jsonParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, []);
	});

	test(`jsonParseStream should handle single element`, async (_t) => {
		const streams = [createReadableStream('[{"a":1}]'), jsonParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }]);
	});

	test(`jsonParseStream should handle non-object elements`, async (_t) => {
		const streams = [
			createReadableStream('[1, "hello", true, null, false]'),
			jsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [1, "hello", true, null, false]);
	});

	test(`jsonParseStream should handle whitespace between elements`, async (_t) => {
		const streams = [
			createReadableStream('[ {"a":1} , \n {"b":2} \n]'),
			jsonParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
	});

	test(`jsonParseStream should track parse errors in result`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [
			createReadableStream('[{"a":1},{invalid},{"b":2}]'),
			parse,
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
		const { key, value } = parse.result();
		deepStrictEqual(key, "jsonErrors");
		deepStrictEqual(value.ParseError.id, "ParseError");
		deepStrictEqual(value.ParseError.idx, [1]);
	});

	test(`jsonParseStream should track NoArrayStart when no '[' seen`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream('{"a":1}'), parse];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, []);
		const { value } = parse.result();
		deepStrictEqual(value.NoArrayStart?.id, "NoArrayStart");
		deepStrictEqual(value.NoArrayStart?.idx, [0]);
	});

	test(`jsonParseStream should not flag NoArrayStart for empty input`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream(""), parse];
		await streamToArray(pipejoin(streams));
		const { value } = parse.result();
		deepStrictEqual(value.NoArrayStart, undefined);
	});

	test(`jsonParseStream should throw when buffer exceeds maxBufferSize`, async (_t) => {
		let error;
		try {
			await pipeline([
				createReadableStream(`[{"a":"${"x".repeat(200)}`),
				jsonParseStream({ maxBufferSize: 100 }),
			]);
		} catch (e) {
			error = e;
		}
		deepStrictEqual(error?.message?.includes("maxBufferSize"), true);
	});

	test(`jsonFormatStream should format objects as JSON array`, async (_t) => {
		const streams = [
			createReadableStream([{ a: 1 }, { b: 2 }]),
			jsonFormatStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		const combined = output.join("");

		deepStrictEqual(combined, '[{"a":1},\n{"b":2}\n]');
	});

	test(`jsonFormatStream should handle empty input`, async (_t) => {
		const streams = [createReadableStream([]), jsonFormatStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		const combined = output.join("");

		deepStrictEqual(combined, "[]");
	});

	test(`jsonFormatStream should handle pretty-print with space option`, async (_t) => {
		const streams = [
			createReadableStream([{ a: 1 }]),
			jsonFormatStream({ space: 2 }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		const combined = output.join("");

		deepStrictEqual(combined, '[{\n  "a": 1\n}\n]');
	});

	test(`jsonParseStream should enforce maxValueSize on raw value including whitespace`, async (_t) => {
		const { ok, strictEqual } = await import("node:assert");
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

	test(`jsonFormatStream roundtrip with jsonParseStream`, async (_t) => {
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

	test(`ndjsonParseStream should accumulate idx list for repeated ParseError`, async (_t) => {
		const parse = ndjsonParseStream();
		const streams = [createReadableStream("bad1\nbad2\nbad3\n"), parse];
		await streamToArray(pipejoin(streams));
		const { value } = parse.result();
		deepStrictEqual(value.ParseError.id, "ParseError");
		deepStrictEqual(value.ParseError.message, "Invalid JSON");
		deepStrictEqual(value.ParseError.idx, [0, 1, 2]);
	});

	test(`ndjsonParseStream should not throw at exactly maxBufferSize`, async (_t) => {
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

	test(`ndjsonParseStream should throw one byte over maxBufferSize`, async (_t) => {
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

	test(`ndjsonParseStream error message contains combined length and limit`, async (_t) => {
		let error;
		try {
			await pipeline([
				createReadableStream(["a".repeat(10), "b".repeat(5)]),
				ndjsonParseStream({ maxBufferSize: 12 }),
			]);
		} catch (e) {
			error = e;
		}
		deepStrictEqual(error?.message?.includes("15"), true);
		deepStrictEqual(error?.message?.includes("12"), true);
	});

	test(`ndjsonParseStream flush trims trailing whitespace (\\r) on last line`, async (_t) => {
		const streams = [createReadableStream('{"a":1}\r'), ndjsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 1 }]);
	});

	test(`ndjsonParseStream flush should track error for invalid JSON without trailing newline`, async (_t) => {
		const parse = ndjsonParseStream();
		const streams = [createReadableStream("not-json"), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, []);
		const { value } = parse.result();
		deepStrictEqual(value.ParseError?.id, "ParseError");
		deepStrictEqual(value.ParseError?.message, "Invalid JSON");
		deepStrictEqual(value.ParseError?.idx, [0]);
	});

	test(`ndjsonParseStream flush should use correct idx for flushed error`, async (_t) => {
		const parse = ndjsonParseStream();
		const streams = [createReadableStream('{"a":1}\nbad'), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 1 }]);
		const { value } = parse.result();
		deepStrictEqual(value.ParseError.idx, [1]);
	});

	test(`ndjsonParseStream should use custom resultKey`, async (_t) => {
		const parse = ndjsonParseStream({ resultKey: "myErrors" });
		const streams = [createReadableStream('{"a":1}\n'), parse];
		await streamToArray(pipejoin(streams));
		const { key } = parse.result();
		deepStrictEqual(key, "myErrors");
	});

	test(`ndjsonFormatStream should flush eagerly after exactly 64 items`, async (_t) => {
		const items = Array.from({ length: 64 }, (_, i) => ({ i }));
		const streams = [createReadableStream(items), ndjsonFormatStream()];
		const combined = (await streamToArray(pipejoin(streams))).join("");
		const lines = combined.trim().split("\n");
		deepStrictEqual(lines.length, 64);
		deepStrictEqual(JSON.parse(lines[0]), { i: 0 });
		deepStrictEqual(JSON.parse(lines[63]), { i: 63 });
	});

	test(`ndjsonFormatStream should handle 65 items (one full batch plus one)`, async (_t) => {
		const items = Array.from({ length: 65 }, (_, i) => ({ i }));
		const streams = [createReadableStream(items), ndjsonFormatStream()];
		const combined = (await streamToArray(pipejoin(streams))).join("");
		const lines = combined.trim().split("\n");
		deepStrictEqual(lines.length, 65);
		deepStrictEqual(JSON.parse(lines[64]), { i: 64 });
	});

	test(`ndjsonFormatStream items joined with newline separator`, async (_t) => {
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

	test(`ndjsonFormatStream should not emit anything when no items`, async (_t) => {
		const streams = [createReadableStream([]), ndjsonFormatStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, []);
	});

	test(`ndjsonFormatStream should flush single item`, async (_t) => {
		const streams = [createReadableStream([{ x: 42 }]), ndjsonFormatStream()];
		const combined = (await streamToArray(pipejoin(streams))).join("");
		deepStrictEqual(combined, '{"x":42}\n');
	});

	test(`ndjsonFormatStream ignores space and keeps one record per line`, async (_t) => {
		const streams = [
			createReadableStream([{ a: 1 }, { b: [2] }]),
			ndjsonFormatStream({ space: 2 }),
		];
		const combined = (await streamToArray(pipejoin(streams))).join("");
		deepStrictEqual(combined, '{"a":1}\n{"b":[2]}\n');
	});

	test(`jsonParseStream should accumulate idx list for repeated ParseError`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream("[bad1,bad2,bad3]"), parse];
		await streamToArray(pipejoin(streams));
		const { value } = parse.result();
		deepStrictEqual(value.ParseError.id, "ParseError");
		deepStrictEqual(value.ParseError.message, "Invalid JSON");
		deepStrictEqual(value.ParseError.idx, [0, 1, 2]);
	});

	test(`jsonParseStream should not throw at exactly maxValueSize`, async (_t) => {
		const streams = [
			createReadableStream("[12345]"),
			jsonParseStream({ maxValueSize: 5 }),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [12345]);
	});

	test(`jsonParseStream should throw when element exceeds maxValueSize`, async (_t) => {
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

	test(`jsonParseStream should skip whitespace-only elements without error`, async (_t) => {
		const streams = [createReadableStream("[   ]"), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, []);
	});

	test(`jsonParseStream should handle escaped quote in string`, async (_t) => {
		const streams = [
			createReadableStream('[{"a":"x\\"y"}]'),
			jsonParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 'x"y' }]);
	});

	test(`jsonParseStream should handle double backslash in string`, async (_t) => {
		const streams = [createReadableStream('[{"a":"\\\\"}]'), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "\\" }]);
	});

	test(`jsonParseStream should not count brackets inside strings as depth`, async (_t) => {
		const streams = [
			createReadableStream('[{"a":"[{nested}]"}]'),
			jsonParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "[{nested}]" }]);
	});

	test(`jsonParseStream should parse string elements at top level`, async (_t) => {
		const streams = [
			createReadableStream('["hello","world"]'),
			jsonParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, ["hello", "world"]);
	});

	test(`jsonParseStream should treat tab as whitespace before elements`, async (_t) => {
		const streams = [createReadableStream("[1,\t2,\t3]"), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [1, 2, 3]);
	});

	test(`jsonParseStream should treat CR as whitespace before elements`, async (_t) => {
		const streams = [createReadableStream("[1,\r2,\r3]"), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [1, 2, 3]);
	});

	test(`jsonParseStream should work correctly with many small chunks`, async (_t) => {
		const chunks = ["[", '{"a":', "1}", ",", '{"b":', "2}", "]"];
		const streams = [createReadableStream(chunks), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
	});

	test(`jsonParseStream should handle multiple elements across chunks`, async (_t) => {
		const chunks = ['[{"a":1},', '{"b":2},', '{"c":3}]'];
		const streams = [createReadableStream(chunks), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 1 }, { b: 2 }, { c: 3 }]);
	});

	test(`jsonParseStream flush should not emit lone closing bracket as element`, async (_t) => {
		const streams = [createReadableStream("[1,2]"), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [1, 2]);
	});

	test(`jsonParseStream whitespace-only input should not flag NoArrayStart`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream("   \n\t  "), parse];
		await streamToArray(pipejoin(streams));
		const { value } = parse.result();
		deepStrictEqual(value.NoArrayStart, undefined);
	});

	test(`jsonParseStream non-whitespace without [ should flag NoArrayStart with message`, async (_t) => {
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

	test(`jsonParseStream maxBufferSize error message contains combined length`, async (_t) => {
		let error;
		try {
			await pipeline([
				createReadableStream(`[{"a":"${"x".repeat(10)}`),
				jsonParseStream({ maxBufferSize: 15 }),
			]);
		} catch (e) {
			error = e;
		}
		deepStrictEqual(
			error?.message,
			"jsonParseStream buffer (16) exceeds maxBufferSize (15)",
		);
	});

	test(`jsonParseStream flush with no elementStart does not crash`, async (_t) => {
		const streams = [createReadableStream('[{"a":1}]'), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 1 }]);
	});

	test(`jsonParseStream should parse deeply nested objects`, async (_t) => {
		const streams = [
			createReadableStream('[{"a":{"b":{"c":1}}}]'),
			jsonParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: { b: { c: 1 } } }]);
	});

	test(`jsonParseStream error idx should reflect insertion order`, async (_t) => {
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

	test(`jsonParseStream should use custom resultKey`, async (_t) => {
		const parse = jsonParseStream({ resultKey: "myErrors" });
		const streams = [createReadableStream('[{"a":1}]'), parse];
		await streamToArray(pipejoin(streams));
		const { key } = parse.result();
		deepStrictEqual(key, "myErrors");
	});

	test(`jsonFormatStream first element uses [ prefix; rest use comma-newline`, async (_t) => {
		const streams = [
			createReadableStream([{ a: 1 }, { b: 2 }, { c: 3 }]),
			jsonFormatStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output[0], '[{"a":1}');
		deepStrictEqual(output[1], ',\n{"b":2}');
		deepStrictEqual(output[2], ',\n{"c":3}');
	});

	test(`jsonFormatStream flush emits newline-bracket for non-empty stream`, async (_t) => {
		const streams = [createReadableStream([{ a: 1 }]), jsonFormatStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output[output.length - 1], "\n]");
	});

	test(`jsonFormatStream single number produces correct JSON array`, async (_t) => {
		const streams = [createReadableStream([42]), jsonFormatStream()];
		const combined = (await streamToArray(pipejoin(streams))).join("");
		deepStrictEqual(combined, "[42\n]");
	});

	test(`jsonParseStream should parse mixed object and string elements`, async (_t) => {
		const streams = [
			createReadableStream('[{"a":1},"hello"]'),
			jsonParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 1 }, "hello"]);
	});

	test(`jsonParseStream flush should emit incomplete element at end of stream`, async (_t) => {
		const streams = [
			createReadableStream(['[{"a":1},', '{"b":2']),
			jsonParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 1 }]);
	});

	test(`ndjsonParseStream should count idx correctly when skipping empty lines`, async (_t) => {
		const parse = ndjsonParseStream();
		const streams = [createReadableStream('{"a":1}\n\nbad\n{"c":3}\n'), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 1 }, { c: 3 }]);
		const { value } = parse.result();
		deepStrictEqual(value.ParseError.idx, [1]);
	});

	test(`jsonParseStream flush emits element for incomplete array without closing bracket`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream('[{"a":1},{"b":2}'), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output.length >= 1, true);
		deepStrictEqual(output[0], { a: 1 });
	});

	test(`ndjsonFormatStream 65 items should produce 2 output chunks`, async (_t) => {
		const items = Array.from({ length: 65 }, (_, i) => ({ i }));
		const streams = [createReadableStream(items), ndjsonFormatStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output.length, 2);
		const firstChunkLines = output[0].split("\n").filter((l) => l.length > 0);
		deepStrictEqual(firstChunkLines.length, 64);
		const secondChunkLines = output[1].split("\n").filter((l) => l.length > 0);
		deepStrictEqual(secondChunkLines.length, 1);
	});

	test(`ndjsonFormatStream 2 items should produce 1 output chunk not 2`, async (_t) => {
		const items = [{ a: 1 }, { b: 2 }];
		const streams = [createReadableStream(items), ndjsonFormatStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output.length, 1);
		const lines = output[0].split("\n").filter((l) => l.length > 0);
		deepStrictEqual(lines.length, 2);
	});

	test(`ndjsonParseStream flush idx is 3 after 3 valid transform lines`, async (_t) => {
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

	test(`jsonParseStream initial buffer must be empty string`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream("[1]"), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [1]);
		const { value } = parse.result();
		deepStrictEqual(value.NoArrayStart, undefined);
	});

	test(`jsonParseStream handles string split across chunks`, async (_t) => {
		const streams = [
			createReadableStream(['["hel', 'lo"]']),
			jsonParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, ["hello"]);
	});

	test(`jsonParseStream string with braces does not affect depth`, async (_t) => {
		const streams = [createReadableStream('["a{b}c"]'), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, ["a{b}c"]);
	});

	test(`jsonParseStream maxBufferSize error reports the pending element length`, async (_t) => {
		let error;
		try {
			await pipeline([
				createReadableStream(["[1234", "5678"]),
				jsonParseStream({ maxBufferSize: 6 }),
			]);
		} catch (e) {
			error = e;
		}
		deepStrictEqual(
			error?.message,
			"jsonParseStream buffer (8) exceeds maxBufferSize (6)",
		);
	});

	test(`jsonParseStream does not throw at exactly maxBufferSize`, async (_t) => {
		const streams = [
			createReadableStream(["[12", "]"]),
			jsonParseStream({ maxBufferSize: 2 }),
		];
		let error;
		try {
			await streamToArray(pipejoin(streams));
		} catch (e) {
			error = e;
		}
		deepStrictEqual(error, undefined);
	});

	test(`jsonParseStream throws one byte over maxBufferSize`, async (_t) => {
		let error;
		try {
			await pipeline([
				createReadableStream(["[12", "3"]),
				jsonParseStream({ maxBufferSize: 2 }),
			]);
		} catch (e) {
			error = e;
		}
		deepStrictEqual(error?.message?.includes("maxBufferSize"), true);
	});

	test(`jsonParseStream flush does not double-emit completed elements`, async (_t) => {
		const streams = [createReadableStream("[1,2]"), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [1, 2]);
		deepStrictEqual(output.length, 2);
	});

	test(`jsonParseStream flush emits in-progress element`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream("[42"), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [42]);
	});

	test(`jsonParseStream flush uses elementStart offset correctly`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream("[  42"), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [42]);
	});

	test(`jsonParseStream flush does not emit lone closing bracket`, async (_t) => {
		const streams = [createReadableStream("[1]"), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [1]);
		deepStrictEqual(output.length, 1);
	});

	test(`jsonParseStream flush resets buffer to empty string`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream("[1,2,3]"), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [1, 2, 3]);
		const { value } = parse.result();
		deepStrictEqual(value.NoArrayStart, undefined);
		deepStrictEqual(value.ParseError, undefined);
	});

	test(`jsonParseStream handles single-char chunking`, async (_t) => {
		const jsonStr = "[1,2,3,4,5]";
		const chunks = jsonStr.split("");
		const streams = [createReadableStream(chunks), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [1, 2, 3, 4, 5]);
	});

	test(`jsonParseStream correctly parses element with surrounding spaces`, async (_t) => {
		const streams = [createReadableStream("[ 42 ]"), jsonParseStream()];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [42]);
	});

	test(`jsonParseStream splits adjacent objects with no comma between them`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream('[{"a":1}{"b":2}]'), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 1 }, { b: 2 }]);
		const { value } = parse.result();
		deepStrictEqual(value.ParseError, undefined);
	});

	test(`jsonParseStream splits adjacent arrays with no comma between them`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream("[[1,2][3,4]]"), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [
			[1, 2],
			[3, 4],
		]);
		const { value } = parse.result();
		deepStrictEqual(value.ParseError, undefined);
	});

	const whitespaceLeadCases = {
		space: "[    123]",
		tab: "[\t\t\t\t123]",
		"line feed": "[\n\n\n\n123]",
		"carriage return": "[\r\r\r\r123]",
	};
	for (const [name, input] of Object.entries(whitespaceLeadCases)) {
		test(`jsonParseStream skips leading ${name} so raw value stays within maxValueSize`, async (_t) => {
			const streams = [
				createReadableStream(input),
				jsonParseStream({ maxValueSize: 5 }),
			];
			const output = await streamToArray(pipejoin(streams));
			deepStrictEqual(output, [123]);
		});
	}

	test(`jsonParseStream trims consumed buffer so many chunks fit a small maxBufferSize`, async (_t) => {
		const chunks = ["["];
		for (let i = 0; i < 50; i++) chunks.push("1,");
		chunks.push("1]");
		const parse = jsonParseStream({ maxBufferSize: 10 });
		const output = await streamToArray(
			pipejoin([createReadableStream(chunks), parse]),
		);
		deepStrictEqual(output.length, 51);
		deepStrictEqual(output[0], 1);
		const { value } = parse.result();
		deepStrictEqual(value.ParseError, undefined);
	});

	test(`jsonParseStream preserves an in-progress element across a chunk boundary`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream(['[{"abc":', "123}]"]), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ abc: 123 }]);
		const { value } = parse.result();
		deepStrictEqual(value.ParseError, undefined);
	});

	test(`jsonParseStream keeps elementStart unset across a comma-aligned chunk boundary`, async (_t) => {
		const streams = [
			createReadableStream(["[1,", "   123]"]),
			jsonParseStream({ maxValueSize: 4 }),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [1, 123]);
	});

	test(`jsonParseStream flush ignores a trailing lone closing bracket`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream(["[", "1", "]"]), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [1]);
		const { value } = parse.result();
		deepStrictEqual(value.ParseError, undefined);
	});

	const splitBytes = (text, char) => {
		const bytes = new TextEncoder().encode(text);
		const at = new TextEncoder().encode(text.split(char)[0]).length + 1;
		return [bytes.subarray(0, at), bytes.subarray(at)];
	};

	test(`ndjsonParseStream decodes a multibyte char split across byte chunks`, async (_t) => {
		const streams = [
			createReadableStream(splitBytes('{"a":"é"}\n{"b":1}', "é")),
			ndjsonParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "é" }, { b: 1 }]);
	});

	test(`ndjsonParseStream flags a truncated trailing multibyte sequence`, async (_t) => {
		const parse = ndjsonParseStream();
		const streams = [
			createReadableStream([new Uint8Array([0x31, 0xc3])]),
			parse,
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, []);
		deepStrictEqual(parse.result().value.ParseError?.idx, [0]);
	});

	test(`jsonParseStream decodes a multibyte char split across byte chunks`, async (_t) => {
		const streams = [
			createReadableStream(splitBytes('[{"a":"é"},"é"]', "é")),
			jsonParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "é" }, "é"]);
	});

	test(`jsonParseStream flags a truncated trailing multibyte sequence`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [
			createReadableStream([new Uint8Array([0x5b, 0x31, 0xc3])]),
			parse,
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, []);
		deepStrictEqual(parse.result().value.ParseError?.idx, [0]);
	});

	test(`jsonParseStream does not flag NoArrayStart for whitespace-only bytes`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [
			createReadableStream([new Uint8Array([0x20, 0x0a])]),
			parse,
		];
		deepStrictEqual(await streamToArray(pipejoin(streams)), []);
		deepStrictEqual(parse.result().value, {});
	});

	test(`jsonParseStream rejects input whose first non-whitespace char is not [`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream(' {"a":[1,2]}'), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, []);
		deepStrictEqual(parse.result().value, {
			NoArrayStart: {
				id: "NoArrayStart",
				message: "Input did not contain a top-level array",
				idx: [0],
				count: 1,
			},
		});
	});

	test(`jsonParseStream ignores chunks after rejecting a non-array start`, async (_t) => {
		const parse = jsonParseStream({ maxBufferSize: 2 });
		const streams = [createReadableStream(["x", "[1]"]), parse];
		deepStrictEqual(await streamToArray(pipejoin(streams)), []);
		deepStrictEqual(parse.result().value.NoArrayStart?.idx, [0]);
	});

	test(`jsonParseStream rejects a lone truncated multibyte sequence as NoArrayStart`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream([new Uint8Array([0xc3])]), parse];
		deepStrictEqual(await streamToArray(pipejoin(streams)), []);
		deepStrictEqual(Object.keys(parse.result().value), ["NoArrayStart"]);
	});

	for (const [name, parse] of [
		["jsonParseStream", () => jsonParseStream()],
		["ndjsonParseStream", () => ndjsonParseStream()],
	]) {
		const input = name === "jsonParseStream" ? "[1,2]" : "1\n2\n";
		test(`${name} strips a leading BOM from string input`, async (_t) => {
			const stream = parse();
			const streams = [createReadableStream([`\uFEFF${input}`]), stream];
			deepStrictEqual(await streamToArray(pipejoin(streams)), [1, 2]);
			deepStrictEqual(stream.result().value, {});
		});

		test(`${name} empty first chunks do not consume the BOM check`, async (_t) => {
			const stream = parse();
			const streams = [createReadableStream(["", `\uFEFF${input}`]), stream];
			deepStrictEqual(await streamToArray(pipejoin(streams)), [1, 2]);
			deepStrictEqual(stream.result().value, {});
		});

		test(`${name} strips a BOM from byte input exactly once`, async (_t) => {
			const bytes = new TextEncoder().encode(`\uFEFF${input}`);
			const streams = [createReadableStream([bytes]), parse()];
			deepStrictEqual(await streamToArray(pipejoin(streams)), [1, 2]);
		});
	}

	test(`jsonParseStream keeps a U+FEFF at the start of a later chunk`, async (_t) => {
		const streams = [
			createReadableStream(['["a', '\uFEFFb"]']),
			jsonParseStream(),
		];
		deepStrictEqual(await streamToArray(pipejoin(streams)), ["a\uFEFFb"]);
	});

	test(`ndjsonParseStream keeps a U+FEFF at the start of a later chunk`, async (_t) => {
		const streams = [
			createReadableStream(['"a', '\uFEFFb"\n']),
			ndjsonParseStream(),
		];
		deepStrictEqual(await streamToArray(pipejoin(streams)), ["a\uFEFFb"]);
	});

	test(`json parsers exceed limits with a RangeError`, async (_t) => {
		const cases = [
			[
				ndjsonParseStream({ maxBufferSize: 2 }),
				"[1]",
				"ndjsonParseStream buffer (3) exceeds maxBufferSize (2)",
			],
			[
				jsonParseStream({ maxBufferSize: 2 }),
				"[123",
				"jsonParseStream buffer (3) exceeds maxBufferSize (2)",
			],
			[
				jsonParseStream({ maxValueSize: 2 }),
				"[123]",
				"jsonParseStream value size (3) exceeds maxValueSize (2)",
			],
		];
		for (const [parse, input, message] of cases) {
			await rejects(pipeline([createReadableStream(input), parse]), {
				name: "RangeError",
				message,
			});
		}
	});

	test(`json parser limits accept null as unlimited`, async (_t) => {
		const big = `[${"1".repeat(40)}]`;
		deepStrictEqual(
			await streamToArray(
				pipejoin([
					createReadableStream(big, { chunkSize: 4 }),
					jsonParseStream({ maxBufferSize: null, maxValueSize: null }),
				]),
			),
			[Number("1".repeat(40))],
		);
		deepStrictEqual(
			await streamToArray(
				pipejoin([
					createReadableStream(`${"1".repeat(40)}\n`),
					ndjsonParseStream({ maxBufferSize: null }),
				]),
			),
			[Number("1".repeat(40))],
		);
	});

	for (const [name, factory, wrap] of [
		["ndjsonParseStream", ndjsonParseStream, (rows) => `${rows.join("\n")}\n`],
		["jsonParseStream", jsonParseStream, (rows) => `[${rows.join(",")}]`],
	]) {
		test(`${name} caps ParseError idx at maxErrorRows and keeps the true count`, async (_t) => {
			const parse = factory({ maxErrorRows: 2 });
			await pipeline([createReadableStream(wrap(["x", "x", "x"])), parse]);
			deepStrictEqual(parse.result().value, {
				ParseError: {
					id: "ParseError",
					message: "Invalid JSON",
					idx: [0, 1],
					count: 3,
				},
			});
		});

		test(`${name} maxErrorRows defaults to 1000 and null is unlimited`, async (_t) => {
			const rows = Array.from({ length: 1001 }, () => "x");
			const capped = factory();
			await pipeline([createReadableStream(wrap(rows)), capped]);
			const { idx, count } = capped.result().value.ParseError;
			deepStrictEqual([idx.length, idx.at(-1), count], [1000, 999, 1001]);
			const unlimited = factory({ maxErrorRows: null });
			await pipeline([createReadableStream(wrap(rows)), unlimited]);
			deepStrictEqual(unlimited.result().value.ParseError.idx.length, 1001);
		});
	}

	test(`ndjsonParseStream accepts one large chunk of many short lines`, async (_t) => {
		const input = '{"a":1}\n'.repeat(50);
		const output = await streamToArray(
			pipejoin([
				createReadableStream(input),
				ndjsonParseStream({ maxBufferSize: 10 }),
			]),
		);
		deepStrictEqual(output.length, 50);
	});

	test(`ndjsonParseStream maxBufferSize counts only the trailing partial line`, async (_t) => {
		await rejects(
			pipeline([
				createReadableStream(`${"1".repeat(19)}\n${"2".repeat(11)}`),
				ndjsonParseStream({ maxBufferSize: 10 }),
			]),
			{
				name: "RangeError",
				message: "ndjsonParseStream buffer (11) exceeds maxBufferSize (10)",
			},
		);
	});

	test(`jsonParseStream accepts one large chunk of many small elements`, async (_t) => {
		const input = `[${Array.from({ length: 50 }, () => '{"a":1}').join(",")}]`;
		const output = await streamToArray(
			pipejoin([
				createReadableStream(input),
				jsonParseStream({ maxBufferSize: 10 }),
			]),
		);
		deepStrictEqual(output.length, 50);
	});

	test(`jsonParseStream does not apply maxBufferSize to rejected or trailing input`, async (_t) => {
		const rejected = jsonParseStream({ maxBufferSize: 2 });
		await pipeline([createReadableStream('{"a":"xxxxxxxx"}'), rejected]);
		deepStrictEqual(Object.keys(rejected.result().value), ["NoArrayStart"]);

		const trailing = jsonParseStream({ maxBufferSize: 2 });
		await pipeline([createReadableStream('[1]{"a":"xxxxxxxx"}'), trailing]);
		deepStrictEqual(Object.keys(trailing.result().value), ["TrailingData"]);
	});

	test(`jsonParseStream flags content after the top-level array`, async (_t) => {
		const parse = jsonParseStream();
		const output = await streamToArray(
			pipejoin([createReadableStream('[1,2]3,{"x":1}'), parse]),
		);
		deepStrictEqual(output, [1, 2]);
		deepStrictEqual(parse.result().value, {
			TrailingData: {
				id: "TrailingData",
				message: "Unexpected content after top-level array",
				idx: [2],
				count: 1,
			},
		});
	});

	test(`jsonParseStream flags a second top-level array`, async (_t) => {
		const parse = jsonParseStream();
		const output = await streamToArray(
			pipejoin([createReadableStream("[1][2]"), parse]),
		);
		deepStrictEqual(output, [1]);
		deepStrictEqual(parse.result().value.TrailingData.idx, [1]);
		deepStrictEqual(parse.result().value.UnexpectedEnd, undefined);
	});

	test(`jsonParseStream records trailing content once and ignores later chunks`, async (_t) => {
		const parse = jsonParseStream();
		const output = await streamToArray(
			pipejoin([createReadableStream(["[1] ", " 2", "[3]", "4"]), parse]),
		);
		deepStrictEqual(output, [1]);
		deepStrictEqual(parse.result().value.TrailingData.count, 1);
	});

	test(`jsonParseStream allows whitespace after the top-level array`, async (_t) => {
		const parse = jsonParseStream();
		const output = await streamToArray(
			pipejoin([createReadableStream(["[1]", " \t\r\n", "\n"]), parse]),
		);
		deepStrictEqual(output, [1]);
		deepStrictEqual(parse.result().value, {});
	});

	test(`jsonParseStream flags a truncated multibyte sequence after the array`, async (_t) => {
		const parse = jsonParseStream();
		const bytes = new Uint8Array([0x5b, 0x31, 0x5d, 0xe2, 0x82]);
		const output = await streamToArray(
			pipejoin([createReadableStream([bytes]), parse]),
		);
		deepStrictEqual(output, [1]);
		deepStrictEqual(Object.keys(parse.result().value), ["TrailingData"]);
	});

	test(`jsonParseStream flags a truncated array`, async (_t) => {
		const parse = jsonParseStream();
		const output = await streamToArray(
			pipejoin([createReadableStream("[1,2"), parse]),
		);
		deepStrictEqual(output, [1, 2]);
		deepStrictEqual(parse.result().value, {
			UnexpectedEnd: {
				id: "UnexpectedEnd",
				message: "Input ended before the top-level array closed",
				idx: [2],
				count: 1,
			},
		});
	});

	test(`jsonParseStream flags a truncated array with no pending element`, async (_t) => {
		for (const input of ["[", "[1,", "[1, \n"]) {
			const parse = jsonParseStream();
			await streamToArray(pipejoin([createReadableStream(input), parse]));
			deepStrictEqual(Object.keys(parse.result().value), ["UnexpectedEnd"]);
		}
	});

	test(`jsonParseStream does not flag UnexpectedEnd for empty or complete input`, async (_t) => {
		for (const input of ["", "  ", "[]", "[1]", "[1] "]) {
			const parse = jsonParseStream();
			await streamToArray(pipejoin([createReadableStream(input), parse]));
			deepStrictEqual(parse.result().value, {});
		}
	});

	test(`ndjsonFormatStream writes null for values with no JSON form`, async (_t) => {
		const combined = (
			await streamToArray(
				pipejoin([
					createReadableStream([() => 1, { a: 1 }, Symbol("s")]),
					ndjsonFormatStream(),
				]),
			)
		).join("");
		deepStrictEqual(combined, 'null\n{"a":1}\nnull\n');
	});

	test(`jsonFormatStream writes null for values with no JSON form`, async (_t) => {
		for (const space of [undefined, 2]) {
			const combined = (
				await streamToArray(
					pipejoin([
						createReadableStream([1, () => 1, 2]),
						jsonFormatStream({ space }),
					]),
				)
			).join("");
			deepStrictEqual(combined, "[1,\nnull,\n2\n]");
			deepStrictEqual(JSON.parse(combined), [1, null, 2]);
		}
	});
	test(`jsonParseStream drops consumed input once the top-level array closes`, async (_t) => {
		const parse = jsonParseStream({ maxBufferSize: 10 });
		const output = await streamToArray(
			pipejoin([
				createReadableStream(["[1]", " ".repeat(8), " ".repeat(8)]),
				parse,
			]),
		);
		deepStrictEqual(output, [1]);
		deepStrictEqual(parse.result().value, {});
	});

	test(`jsonParseStream should handle a chunk boundary right before a closing quote`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream(['[{"a":"x', '"}]']), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "x" }]);
		deepStrictEqual(parse.result().value, {});
	});

	test(`jsonParseStream should handle a chunk boundary right after an escape`, async (_t) => {
		const parse = jsonParseStream();
		const streams = [createReadableStream(['[{"a":"x\\', '"y"}]']), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 'x"y' }]);
		deepStrictEqual(parse.result().value, {});
	});
});
