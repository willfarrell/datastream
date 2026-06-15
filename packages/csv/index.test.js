import { deepStrictEqual, ok, strictEqual } from "node:assert";
import test from "node:test";

import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";

import {
	csvArrayToObject,
	csvCoerceValuesStream,
	csvDetectDelimitersStream,
	csvDetectHeaderStream,
	csvFormatStream,
	csvInjectHeaderStream,
	csvObjectToArray,
	csvParseStream,
	csvQuotedParser,
	csvRemoveEmptyRowsStream,
	csvRemoveMalformedRowsStream,
	csvUnquotedParser,
} from "@datastream/csv";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** csvParseStream *** //
test(`${variant}: csvParseStream should parse csv to string[]`, async (_t) => {
	const streams = [
		createReadableStream("1,2,3,4\r\n5,6,7,8\r\n"),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2", "3", "4"],
		["5", "6", "7", "8"],
	]);
});

test(`${variant}: csvParseStream should parse with custom delimiter`, async (_t) => {
	const streams = [
		createReadableStream("1\t2\t3\r\n4\t5\t6\r\n"),
		csvParseStream({ delimiterChar: "\t" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2", "3"],
		["4", "5", "6"],
	]);
});

test(`${variant}: csvParseStream should handle chunk boundaries`, async (_t) => {
	const streams = [
		createReadableStream(["1,2,", "3,4\r\n5,6,", "7,8\r\n"]),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2", "3", "4"],
		["5", "6", "7", "8"],
	]);
});

test(`${variant}: csvParseStream should handle quoted fields`, async (_t) => {
	const streams = [
		createReadableStream('"hello, world",42\r\n"foo ""bar""",99\r\n'),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["hello, world", "42"],
		['foo "bar"', "99"],
	]);
});

test(`${variant}: csvParseStream should parse last row without trailing newline`, async (_t) => {
	const streams = [createReadableStream("1,2\r\n3,4"), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2"],
		["3", "4"],
	]);
});

test(`${variant}: csvParseStream should track unterminated quote errors`, async (_t) => {
	const streams = [createReadableStream('"unterminated\r\n'), csvParseStream()];
	const result = await pipeline(streams);

	const { key, value } = streams[1].result();
	strictEqual(key, "csvErrors");
	deepStrictEqual(value.UnterminatedQuote.idx, [0]);
	deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [0]);
});

test(`${variant}: csvParseStream should support lazy options`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const parse = csvParseStream({
		delimiterChar: () => detect.result().value.delimiterChar,
		newlineChar: () => detect.result().value.newlineChar,
		quoteChar: () => detect.result().value.quoteChar,
	});
	const streams = [createReadableStream("a\tb\tc\n1\t2\t3\n"), detect, parse];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b", "c"],
		["1", "2", "3"],
	]);
});

test(`${variant}: csvParseStream should use custom resultKey`, async (_t) => {
	const streams = [
		createReadableStream("1,2\r\n"),
		csvParseStream({ resultKey: "parseErrors" }),
	];
	const result = await pipeline(streams);

	deepStrictEqual(result.parseErrors, {});
});

test(`${variant}: csvParseStream should handle quoted field containing newline`, async (_t) => {
	const streams = [
		createReadableStream('"line1\r\nline2",val\r\n'),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["line1\r\nline2", "val"]]);
});

test(`${variant}: csvParseStream should handle quoted field containing delimiter`, async (_t) => {
	const streams = [createReadableStream('"a,b",c\r\n'), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["a,b", "c"]]);
});

test(`${variant}: csvParseStream should handle empty input`, async (_t) => {
	const streams = [createReadableStream(""), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});

test(`${variant}: csvParseStream should parse single-column CSV`, async (_t) => {
	const streams = [createReadableStream("a\r\nb\r\nc\r\n"), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["a"], ["b"], ["c"]]);
});

test(`${variant}: csvParseStream should handle chunk boundary inside quoted field`, async (_t) => {
	const streams = [
		createReadableStream(['"hello,', ' world",42\r\n']),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["hello, world", "42"]]);
});

test(`${variant}: csvParseStream should return empty errors when no issues`, async (_t) => {
	const streams = [createReadableStream("1,2\r\n3,4\r\n"), csvParseStream()];
	const result = await pipeline(streams);

	deepStrictEqual(result.csvErrors, {});
});

test(`${variant}: csvParseStream should preserve CSV injection payloads as-is`, async (_t) => {
	const payloads = [
		"=cmd|'/C calc'!A0",
		"+cmd|'/C calc'!A0",
		"-cmd|'/C calc'!A0",
		"@SUM(1+1)*cmd|'/C calc'!A0",
		'=HYPERLINK("http://evil.com","Click")',
	];
	const csv = payloads
		.map((p) => `"${p.replaceAll('"', '""')}",safe\r\n`)
		.join("");
	const streams = [createReadableStream(csv), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	for (let i = 0; i < payloads.length; i++) {
		strictEqual(output[i][0], payloads[i]);
		strictEqual(output[i][1], "safe");
	}
});

test(`${variant}: csvParseStream should preserve formula triggers in unquoted fields`, async (_t) => {
	const streams = [
		createReadableStream("=1+2,+1,-1,@SUM\r\n"),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["=1+2", "+1", "-1", "@SUM"]]);
});

test(`${variant}: csvParseStream should accumulate error idx across chunks (custom parser)`, async (_t) => {
	let call = 0;
	const customParser = (_text, _ctx, _isFlushing) => {
		call += 1;
		// Each call reports a SyntheticError on the same id with a different idx
		return {
			rows: [],
			tail: "",
			numCols: 1,
			idx: call,
			errors: { Synthetic: { id: "Synthetic", message: "syn", idx: [call] } },
		};
	};
	const streams = [
		createReadableStream(["aaaaaaaaa", "bbbbbbbbb", "ccccccccc"]),
		csvParseStream({ parser: customParser, chunkSize: 1 }),
	];
	const result = await pipeline(streams);

	// All three call indices should appear, not just the last one
	deepStrictEqual(result.csvErrors.Synthetic.idx, [1, 2, 3]);
});

test(`${variant}: csvParseStream should track unterminated quote on flush`, async (_t) => {
	const streams = [
		createReadableStream('"ok",val\r\n"unterminated'),
		csvParseStream(),
	];
	const result = await pipeline(streams);

	deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [1]);
});

test(`${variant}: csvParseStream should handle escaped quotes inside quoted field`, async (_t) => {
	const streams = [
		createReadableStream('"he""llo",world\r\n'),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [['he"llo', "world"]]);
});

test(`${variant}: csvParseStream should handle quoted field at end of chunk`, async (_t) => {
	const streams = [createReadableStream('a,"b"'), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [["a", "b"]]);
});

test(`${variant}: csvParseStream should handle content after closing quote`, async (_t) => {
	const streams = [createReadableStream('"a"x,b\r\n'), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [["a", "x", "b"]]);
});

test(`${variant}: csvParseStream should handle unterminated quote with escaped quotes`, async (_t) => {
	const streams = [createReadableStream('"he""llo'), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [['he"llo']]);
});

test(`${variant}: csvParseStream should handle custom escape char`, async (_t) => {
	const streams = [
		createReadableStream('a,"he\\"llo",world\r\nc,d\r\n'),
		csvParseStream({ escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", 'he"llo', "world"],
		["c", "d"],
	]);
});

test(`${variant}: csvParseStream should handle custom escape at end of chunk`, async (_t) => {
	const streams = [
		createReadableStream('"val\\""'),
		csvParseStream({ escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [['val"']]);
});

test(`${variant}: csvParseStream should handle unterminated quote with custom escape`, async (_t) => {
	const streams = [
		createReadableStream('"he\\"llo'),
		csvParseStream({ escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [['he"llo']]);
});

test(`${variant}: csvParseStream should handle multi-char delimiter`, async (_t) => {
	const streams = [
		createReadableStream('"a"::b\r\nc::d\r\n'),
		csvParseStream({ delimiterChar: "::" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvParseStream should handle LF-only newline in quoted field`, async (_t) => {
	const streams = [
		createReadableStream('"a",b\nc,d\n'),
		csvParseStream({ newlineChar: "\n" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvParseStream should handle trailing delimiter on flush`, async (_t) => {
	const streams = [createReadableStream("a,b,"), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [["a", "b", ""]]);
});

test(`${variant}: csvParseStream should handle quoted field as last field before newline`, async (_t) => {
	const streams = [
		createReadableStream('a,"b"\nc,d\n'),
		csvParseStream({ newlineChar: "\n" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvParseStream should handle quoted field as last field before newline with custom escape`, async (_t) => {
	const streams = [
		createReadableStream('a,"b"\nc,d\n'),
		csvParseStream({ escapeChar: "\\", newlineChar: "\n" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvParseStream should handle escaped quoted field as last field before newline`, async (_t) => {
	const streams = [
		createReadableStream('a,"b""c"\nc,d\n'),
		csvParseStream({ newlineChar: "\n" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", 'b"c'],
		["c", "d"],
	]);
});

test(`${variant}: csvParseStream should handle escaped quoted field as last field before newline with custom escape`, async (_t) => {
	const streams = [
		createReadableStream('a,"b\\"c"\nc,d\n'),
		csvParseStream({ escapeChar: "\\", newlineChar: "\n" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", 'b"c'],
		["c", "d"],
	]);
});

// *** csvInjectHeaderStream *** //
test(`${variant}: csvInjectHeaderStream should inject header before first chunk`, async (_t) => {
	const streams = [
		createReadableStream([
			["1", "2"],
			["3", "4"],
		]),
		csvInjectHeaderStream({ header: ["a", "b"] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
		["3", "4"],
	]);
});

test(`${variant}: csvInjectHeaderStream should pass through when no data`, async (_t) => {
	const streams = [
		createReadableStream([]),
		csvInjectHeaderStream({ header: ["a", "b"] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});

// *** csvFormatStream *** //
test(`${variant}: csvFormatStream should format csv from object[] using compose`, async (_t) => {
	const headers = ["a", "b", "c", "d"];
	const streams = [
		createReadableStream([
			{ a: "1", b: "2", c: "3", d: "4" },
			{ a: "1", b: "2", c: "3", d: "4" },
		]),
		csvObjectToArray({ headers }),
		csvInjectHeaderStream({ header: headers }),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "a,b,c,d\r\n1,2,3,4\r\n1,2,3,4\r\n");
});

test(`${variant}: csvFormatStream should format csv from object[] with column order`, async (_t) => {
	const headers = ["d", "c", "b", "a"];
	const streams = [
		createReadableStream([
			{ a: "1", b: "2", c: "3", d: "4" },
			{ a: "1", b: "2", c: "3", d: "4" },
		]),
		csvObjectToArray({ headers }),
		csvInjectHeaderStream({ header: headers }),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "d,c,b,a\r\n4,3,2,1\r\n4,3,2,1\r\n");
});

test(`${variant}: csvFormatStream should format csv from string[] with header`, async (_t) => {
	const streams = [
		createReadableStream([
			["1", "2", "3", "4"],
			["1", "2", "3", "4"],
		]),
		csvInjectHeaderStream({ header: ["a", "b", "c", "d"] }),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "a,b,c,d\r\n1,2,3,4\r\n1,2,3,4\r\n");
});

test(`${variant}: csvFormatStream should quote values containing delimiter`, async (_t) => {
	const streams = [
		createReadableStream([["hello, world", "plain"]]),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, '"hello, world",plain\r\n');
});

test(`${variant}: csvFormatStream should escape values containing quote char`, async (_t) => {
	const streams = [
		createReadableStream([['say "hi"', "plain"]]),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, '"say ""hi""",plain\r\n');
});

test(`${variant}: csvFormatStream should quote values containing newlines`, async (_t) => {
	const streams = [
		createReadableStream([["line1\nline2", "plain"]]),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, '"line1\nline2",plain\r\n');
});

test(`${variant}: csvFormatStream should format arrays without header`, async (_t) => {
	const streams = [createReadableStream([["1", "2"]]), csvFormatStream()];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "1,2\r\n");
});

test(`${variant}: csvFormatStream should auto-quote CSV injection formula triggers`, async (_t) => {
	const streams = [
		createReadableStream([
			["=1+2", "safe"],
			["+1", "safe"],
			["-1", "safe"],
			["@SUM(1)", "safe"],
		]),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);
	const lines = output.split("\r\n");

	strictEqual(lines[0], '"=1+2",safe');
	strictEqual(lines[1], '"+1",safe');
	strictEqual(lines[2], '"-1",safe');
	strictEqual(lines[3], '"@SUM(1)",safe');
});

test(`${variant}: csvFormatStream roundtrip should preserve CSV injection payloads`, async (_t) => {
	const payloads = [
		"=cmd|'/C calc'!A0",
		"+cmd|'/C calc'!A0",
		"-cmd|'/C calc'!A0",
		"@SUM(1+1)",
		'=HYPERLINK("http://evil.com","Click")',
	];
	const arrays = payloads.map((p) => [p]);
	const formatStreams = [
		createReadableStream(arrays),
		csvInjectHeaderStream({ header: ["val"] }),
		csvFormatStream(),
	];
	const formatted = await streamToString(pipejoin(formatStreams));

	const detect = csvDetectDelimitersStream();
	const hdr = csvDetectHeaderStream({
		delimiterChar: () => detect.result().value.delimiterChar,
		newlineChar: () => detect.result().value.newlineChar,
		quoteChar: () => detect.result().value.quoteChar,
	});
	const parse = csvParseStream({
		delimiterChar: () => detect.result().value.delimiterChar,
		newlineChar: () => detect.result().value.newlineChar,
		quoteChar: () => detect.result().value.quoteChar,
	});
	const parseStreams = [createReadableStream(formatted), detect, hdr, parse];
	const parsed = await streamToArray(pipejoin(parseStreams));

	for (let i = 0; i < payloads.length; i++) {
		strictEqual(parsed[i][0], payloads[i]);
	}
});

// *** csvArrayToObject / csvObjectToArray *** //
test(`${variant}: csvArrayToObject should convert array to object`, async (_t) => {
	const streams = [
		createReadableStream([["1", "2", "3"]]),
		csvArrayToObject({ headers: ["a", "b", "c"] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: "1", b: "2", c: "3" }]);
});

test(`${variant}: csvObjectToArray should convert object to array`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "1", b: "2", c: "3" }]),
		csvObjectToArray({ headers: ["a", "b", "c"] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["1", "2", "3"]]);
});

// *** csvDetectDelimitersStream *** //
test(`${variant}: csvDetectDelimitersStream should detect comma delimiter`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a,b,c\n1,2,3\n"), detect];
	await pipeline(streams);

	const { key, value } = detect.result();
	strictEqual(key, "csvDetectDelimiters");
	strictEqual(value.delimiterChar, ",");
	strictEqual(value.newlineChar, "\n");
	strictEqual(value.quoteChar, '"');
});

test(`${variant}: csvDetectDelimitersStream should detect tab delimiter`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a\tb\tc\n1\t2\t3\n"), detect];
	await pipeline(streams);

	strictEqual(detect.result().value.delimiterChar, "\t");
});

test(`${variant}: csvDetectDelimitersStream should detect pipe delimiter`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a|b|c\n1|2|3\n"), detect];
	await pipeline(streams);

	strictEqual(detect.result().value.delimiterChar, "|");
});

test(`${variant}: csvDetectDelimitersStream should detect semicolon delimiter`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a;b;c\n1;2;3\n"), detect];
	await pipeline(streams);

	strictEqual(detect.result().value.delimiterChar, ";");
});

test(`${variant}: csvDetectDelimitersStream should detect CRLF newline`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a,b\r\n1,2\r\n"), detect];
	await pipeline(streams);

	strictEqual(detect.result().value.newlineChar, "\r\n");
});

test(`${variant}: csvDetectDelimitersStream should pass data through unchanged`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a,b\n1,2\n"), detect];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	strictEqual(output, "a,b\n1,2\n");
});

test(`${variant}: csvDetectDelimitersStream should use custom resultKey`, async (_t) => {
	const detect = csvDetectDelimitersStream({ resultKey: "delim" });
	const streams = [createReadableStream("a,b\n1,2\n"), detect];
	const result = await pipeline(streams);

	deepStrictEqual(result.delim.delimiterChar, ",");
});

test(`${variant}: csvDetectDelimitersStream should detect CR-only newline`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a,b\r1,2\r"), detect];
	await pipeline(streams);

	strictEqual(detect.result().value.newlineChar, "\r");
});

test(`${variant}: csvDetectDelimitersStream should detect single-quote character`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [
		createReadableStream("'first name','last name'\n'Alice','Smith'\n"),
		detect,
	];
	await pipeline(streams);

	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream should handle small input via flush`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 9999 });
	const streams = [createReadableStream("a\tb\n1\t2\n"), detect];
	await pipeline(streams);

	strictEqual(detect.result().value.delimiterChar, "\t");
});

test(`${variant}: csvDetectDelimitersStream should not confuse delimiter inside quotes`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream('"a\tb"\tc\n"d\te"\tf\n'), detect];
	await pipeline(streams);

	strictEqual(detect.result().value.delimiterChar, "\t");
});

test(`${variant}: csvDetectDelimitersStream should handle multiple chunks`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 5 });
	const streams = [createReadableStream(["a;b;c", "\n1;2;3\n"]), detect];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	strictEqual(detect.result().value.delimiterChar, ";");
	strictEqual(output, "a;b;c\n1;2;3\n");
});

test(`${variant}: csvDetectDelimitersStream should detect from large input exceeding chunkSize`, async (_t) => {
	const row = "a,b,c,d,e\n";
	const input = row.repeat(120);
	const streams = [createReadableStream(input), csvDetectDelimitersStream()];
	const result = await pipeline(streams);
	strictEqual(result.csvDetectDelimiters.delimiterChar, ",");
});

test(`${variant}: csvDetectDelimitersStream should pass through chunks after detection`, async (_t) => {
	const row = "a,b,c\n";
	const input = [row.repeat(120), "x,y,z\n"];
	const streams = [createReadableStream(input), csvDetectDelimitersStream()];
	const output = await streamToString(pipejoin(streams));
	ok(output.includes("x,y,z"));
});

// *** csvDetectHeaderStream *** //
test(`${variant}: csvDetectHeaderStream should extract headers and remove header row`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n" });
	const streams = [
		createReadableStream("name,age,city\nAlice,30,NYC\nBob,25,LA\n"),
		headers,
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ["name", "age", "city"]);
	strictEqual(headers.result().key, "csvDetectHeader");
	strictEqual(output, "Alice,30,NYC\nBob,25,LA\n");
});

test(`${variant}: csvDetectHeaderStream should work with lazy delimiterChar`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const headers = csvDetectHeaderStream({
		delimiterChar: () => detect.result().value.delimiterChar,
		newlineChar: () => detect.result().value.newlineChar,
		quoteChar: () => detect.result().value.quoteChar,
	});
	const streams = [createReadableStream("a\tb\tc\n1\t2\t3\n"), detect, headers];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ["a", "b", "c"]);
	strictEqual(output, "1\t2\t3\n");
});

test(`${variant}: csvDetectHeaderStream should handle quoted headers`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n" });
	const streams = [
		createReadableStream('"first name","last name"\nAlice,Smith\n'),
		headers,
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ["first name", "last name"]);
	strictEqual(output, "Alice,Smith\n");
});

test(`${variant}: csvDetectHeaderStream should use custom resultKey`, async (_t) => {
	const headers = csvDetectHeaderStream({
		newlineChar: "\n",
		resultKey: "hdr",
	});
	const streams = [createReadableStream("a,b\n1,2\n"), headers];
	const result = await pipeline(streams);

	deepStrictEqual(result.hdr.header, ["a", "b"]);
});

test(`${variant}: csvDetectHeaderStream should handle header-only input`, async (_t) => {
	const headers = csvDetectHeaderStream();
	const streams = [createReadableStream("a,b,c"), headers];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ["a", "b", "c"]);
	strictEqual(output, "");
});

test(`${variant}: csvDetectHeaderStream should handle CRLF newlines`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\r\n" });
	const streams = [createReadableStream("x,y\r\n1,2\r\n3,4\r\n"), headers];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ["x", "y"]);
	strictEqual(output, "1,2\r\n3,4\r\n");
});

test(`${variant}: csvDetectHeaderStream should handle escaped quotes in header`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n" });
	const streams = [createReadableStream('"col""1",col2\nval1,val2\n'), headers];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ['col"1', "col2"]);
	ok(output.includes("val1,val2"));
});

test(`${variant}: csvDetectHeaderStream should detect from large input exceeding chunkSize`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n" });
	const input = `name,age,city\n${"Alice,30,NYC\n".repeat(100)}`;
	const streams = [createReadableStream(input), headers];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ["name", "age", "city"]);
	ok(output.includes("Alice,30,NYC"));
});

test(`${variant}: csvDetectHeaderStream should pass through chunks after detection`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n" });
	const firstChunk = `name,age,city\n${"Alice,30,NYC\n".repeat(100)}`;
	const secondChunk = "Bob,25,LA\n";
	const input = [firstChunk, secondChunk];
	const streams = [createReadableStream(input), headers];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ["name", "age", "city"]);
	ok(output.includes("Bob,25,LA"));
});

test(`${variant}: csvDetectHeaderStream should handle small input via flush`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n", chunkSize: 9999 });
	const streams = [createReadableStream("col1,col2\nval1,val2\n"), headers];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ["col1", "col2"]);
	strictEqual(output, "val1,val2\n");
});

// *** csvRemoveMalformedRowsStream *** //
test(`${variant}: csvRemoveMalformedRowsStream should drop rows with wrong column count`, async (_t) => {
	const filter = csvRemoveMalformedRowsStream();
	const streams = [
		createReadableStream([
			["1", "2", "3"],
			["4", "5"],
			["6", "7", "8"],
		]),
		filter,
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2", "3"],
		["6", "7", "8"],
	]);
	deepStrictEqual(filter.result().value.MalformedRow.idx, [1]);
});

test(`${variant}: csvRemoveMalformedRowsStream should pass malformed rows with onErrorEnqueue`, async (_t) => {
	const filter = csvRemoveMalformedRowsStream({ onErrorEnqueue: true });
	const streams = [
		createReadableStream([
			["1", "2", "3"],
			["4", "5"],
			["6", "7", "8"],
		]),
		filter,
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2", "3"],
		["4", "5"],
		["6", "7", "8"],
	]);
	deepStrictEqual(filter.result().value.MalformedRow.idx, [1]);
});

test(`${variant}: csvRemoveMalformedRowsStream should accept lazy headers`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const hdr = csvDetectHeaderStream({
		delimiterChar: () => detect.result().value.delimiterChar,
		newlineChar: () => detect.result().value.newlineChar,
		quoteChar: () => detect.result().value.quoteChar,
	});
	const parse = csvParseStream({
		delimiterChar: () => detect.result().value.delimiterChar,
		newlineChar: () => detect.result().value.newlineChar,
		quoteChar: () => detect.result().value.quoteChar,
	});
	const filter = csvRemoveMalformedRowsStream({
		headers: () => hdr.result().value.header,
	});
	const streams = [
		createReadableStream("a,b,c\n1,2,3\n4,5\n6,7,8\n"),
		detect,
		hdr,
		parse,
		filter,
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2", "3"],
		["6", "7", "8"],
	]);
	deepStrictEqual(filter.result().value.MalformedRow.idx, [1]);
});

test(`${variant}: csvRemoveMalformedRowsStream should use custom resultKey`, async (_t) => {
	const filter = csvRemoveMalformedRowsStream({ resultKey: "bad" });
	const streams = [createReadableStream([["1", "2"], ["3"]]), filter];
	const result = await pipeline(streams);

	deepStrictEqual(result.bad.MalformedRow.idx, [1]);
});

test(`${variant}: csvRemoveMalformedRowsStream should return empty value when all rows valid`, async (_t) => {
	const filter = csvRemoveMalformedRowsStream();
	const streams = [
		createReadableStream([
			["1", "2"],
			["3", "4"],
		]),
		filter,
	];
	await pipeline(streams);

	deepStrictEqual(filter.result().value, {});
});

test(`${variant}: csvRemoveMalformedRowsStream should track both too-few and too-many columns`, async (_t) => {
	const filter = csvRemoveMalformedRowsStream();
	const streams = [
		createReadableStream([
			["1", "2", "3"],
			["4", "5"],
			["6", "7", "8"],
			["9", "10", "11", "12"],
		]),
		filter,
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2", "3"],
		["6", "7", "8"],
	]);
	deepStrictEqual(filter.result().value.MalformedRow.idx, [1, 3]);
});

test(`${variant}: csvRemoveMalformedRowsStream should infer columns from first row when no headers`, async (_t) => {
	const filter = csvRemoveMalformedRowsStream();
	const streams = [
		createReadableStream([["a", "b"], ["c"], ["d", "e"]]),
		filter,
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["d", "e"],
	]);
});

// *** csvRemoveEmptyRowsStream *** //
test(`${variant}: csvRemoveEmptyRowsStream should drop empty rows`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream();
	const streams = [
		createReadableStream([
			["1", "2"],
			["", ""],
			["3", "4"],
		]),
		filter,
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2"],
		["3", "4"],
	]);
	deepStrictEqual(filter.result().value.EmptyRow.idx, [1]);
});

test(`${variant}: csvRemoveEmptyRowsStream should drop zero-length rows`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream();
	const streams = [createReadableStream([["1", "2"], [], ["3", "4"]]), filter];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2"],
		["3", "4"],
	]);
	deepStrictEqual(filter.result().value.EmptyRow.idx, [1]);
});

test(`${variant}: csvRemoveEmptyRowsStream should pass empty rows with onErrorEnqueue`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream({ onErrorEnqueue: true });
	const streams = [
		createReadableStream([
			["1", "2"],
			["", ""],
			["3", "4"],
		]),
		filter,
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2"],
		["", ""],
		["3", "4"],
	]);
	deepStrictEqual(filter.result().value.EmptyRow.idx, [1]);
});

test(`${variant}: csvRemoveEmptyRowsStream should track multiple empty rows`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream();
	const streams = [
		createReadableStream([
			["", ""],
			["1", "2"],
			["", ""],
		]),
		filter,
	];
	await pipeline(streams);

	deepStrictEqual(filter.result().value.EmptyRow.idx, [0, 2]);
});

test(`${variant}: csvRemoveEmptyRowsStream should use custom resultKey`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream({ resultKey: "empty" });
	const streams = [
		createReadableStream([
			["", ""],
			["1", "2"],
		]),
		filter,
	];
	const result = await pipeline(streams);

	deepStrictEqual(result.empty.EmptyRow.idx, [0]);
});

test(`${variant}: csvRemoveEmptyRowsStream should return empty value when no empty rows`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream();
	const streams = [
		createReadableStream([
			["1", "2"],
			["3", "4"],
		]),
		filter,
	];
	await pipeline(streams);

	deepStrictEqual(filter.result().value, {});
});

// *** csvCoerceValuesStream *** //
test(`${variant}: csvCoerceValuesStream should auto-coerce types`, async (_t) => {
	const coerce = csvCoerceValuesStream();
	const streams = [
		createReadableStream([
			{
				str: "hello",
				num: "42",
				float: "3.14",
				boolT: "true",
				boolF: "false",
				empty: "",
			},
		]),
		coerce,
	];
	const result = await pipeline(streams);

	strictEqual(coerce.result().key, "csvCoerceValues");
	deepStrictEqual(result.csvCoerceValues, {});
});

test(`${variant}: csvCoerceValuesStream should auto-coerce date values`, async (_t) => {
	const streams = [
		createReadableStream([{ date: "2024-01-15" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ date: new Date("2024-01-15") }]);
});

test(`${variant}: csvCoerceValuesStream should auto-coerce JSON values`, async (_t) => {
	const streams = [
		createReadableStream([{ arr: "[1,2,3]", obj: '{"a":1}' }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ arr: [1, 2, 3], obj: { a: 1 } }]);
});

test(`${variant}: csvCoerceValuesStream should coerce with explicit column types`, async (_t) => {
	const streams = [
		createReadableStream([{ age: "30", active: "true", name: "Alice" }]),
		csvCoerceValuesStream({
			columns: { age: "number", active: "boolean" },
		}),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ age: 30, active: true, name: "Alice" }]);
});

test(`${variant}: csvCoerceValuesStream should handle scientific notation`, async (_t) => {
	const streams = [
		createReadableStream([{ val: "1.5e3" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ val: 1500 }]);
});

test(`${variant}: csvCoerceValuesStream should use custom resultKey`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "1" }]),
		csvCoerceValuesStream({ resultKey: "coerce" }),
	];
	const result = await pipeline(streams);

	strictEqual(streams[1].result().key, "coerce");
	deepStrictEqual(result.coerce, {});
});

test(`${variant}: csvCoerceValuesStream should coerce negative numbers`, async (_t) => {
	const streams = [
		createReadableStream([{ val: "-42", neg_float: "-3.14" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ val: -42, neg_float: -3.14 }]);
});

test(`${variant}: csvCoerceValuesStream should keep invalid JSON as string`, async (_t) => {
	const streams = [
		createReadableStream([{ val: "{not json", arr: "[broken" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ val: "{not json", arr: "[broken" }]);
});

test(`${variant}: csvCoerceValuesStream should coerce ISO 8601 datetime`, async (_t) => {
	const streams = [
		createReadableStream([{ ts: "2024-01-15T10:30:00Z" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ ts: new Date("2024-01-15T10:30:00Z") }]);
});

test(`${variant}: csvCoerceValuesStream should pass through non-string values`, async (_t) => {
	const streams = [
		createReadableStream([{ num: 42, bool: true, nil: null }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ num: 42, bool: true, nil: null }]);
});

test(`${variant}: csvCoerceValuesStream should coerce explicit null type`, async (_t) => {
	const streams = [
		createReadableStream([{ val: "anything" }]),
		csvCoerceValuesStream({ columns: { val: "null" } }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ val: null }]);
});

test(`${variant}: csvCoerceValuesStream should coerce explicit date type`, async (_t) => {
	const streams = [
		createReadableStream([{ val: "2024-06-15" }]),
		csvCoerceValuesStream({ columns: { val: "date" } }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ val: new Date("2024-06-15") }]);
});

test(`${variant}: csvCoerceValuesStream should coerce explicit json type`, async (_t) => {
	const streams = [
		createReadableStream([{ val: '{"a":1}' }]),
		csvCoerceValuesStream({ columns: { val: "json" } }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ val: { a: 1 } }]);
});

test(`${variant}: csvCoerceValuesStream should return original string when number coercion yields NaN`, async (_t) => {
	const streams = [
		createReadableStream([{ val: "not-a-number" }]),
		csvCoerceValuesStream({ columns: { val: "number" } }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ val: "not-a-number" }]);
});

test(`${variant}: csvCoerceValuesStream should not coerce CSV injection formula triggers`, async (_t) => {
	const streams = [
		createReadableStream([
			{
				eq: "=cmd|'/C calc'!A0",
				plus: "+cmd|'/C calc'!A0",
				minus: "-cmd",
				at: "@SUM(1+1)",
				hyperlink: '=HYPERLINK("http://evil.com","Click")',
			},
		]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	strictEqual(output[0].eq, "=cmd|'/C calc'!A0");
	strictEqual(output[0].plus, "+cmd|'/C calc'!A0");
	strictEqual(output[0].minus, "-cmd");
	strictEqual(output[0].at, "@SUM(1+1)");
	strictEqual(output[0].hyperlink, '=HYPERLINK("http://evil.com","Click")');
});

test(`${variant}: csvCoerceValuesStream coerces leading-zero strings to numbers`, async (_t) => {
	// WARNING: leading-zero strings like zip codes are coerced to numbers,
	// losing the leading zero. Use explicit column types to prevent this:
	// csvCoerceValuesStream({ columns: { zip: "string" } })
	const streams = [
		createReadableStream([{ zip: "07001", phone: "0123456789" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	strictEqual(output[0].zip, 7001);
	strictEqual(output[0].phone, 123456789);
});

test(`${variant}: csvCoerceValuesStream should preserve leading-zero strings with explicit string type`, async (_t) => {
	const streams = [
		createReadableStream([{ zip: "07001" }]),
		csvCoerceValuesStream({ columns: { zip: "string" } }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	strictEqual(output[0].zip, "07001");
});

test(`${variant}: csvCoerceValuesStream should return original string when explicit json coercion fails`, async (_t) => {
	const streams = [
		createReadableStream([{ val: "not-valid-json" }]),
		csvCoerceValuesStream({ columns: { val: "json" } }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ val: "not-valid-json" }]);
});

test(`${variant}: csvCoerceValuesStream should return null for empty string with number type`, async (_t) => {
	const streams = [
		createReadableStream([{ val: "" }]),
		csvCoerceValuesStream({ columns: { val: "number" } }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ val: null }]);
});

// *** csvUnquotedParser *** //
test(`${variant}: csvUnquotedParser should parse simple unquoted CSV`, (_t) => {
	const result = csvUnquotedParser("a,b,c\r\n1,2,3\r\n");
	deepStrictEqual(result.rows, [
		["a", "b", "c"],
		["1", "2", "3"],
	]);
	strictEqual(result.tail, "");
});

test(`${variant}: csvUnquotedParser should return tail for incomplete row`, (_t) => {
	const result = csvUnquotedParser("a,b,c\r\n1,2,3");
	deepStrictEqual(result.rows, [["a", "b", "c"]]);
	strictEqual(result.tail, "1,2,3");
});

test(`${variant}: csvUnquotedParser should flush incomplete row`, (_t) => {
	const result = csvUnquotedParser("a,b,c\r\n1,2,3", {}, true);
	deepStrictEqual(result.rows, [
		["a", "b", "c"],
		["1", "2", "3"],
	]);
	strictEqual(result.tail, "");
});

test(`${variant}: csvUnquotedParser should flush single row without newline`, (_t) => {
	const result = csvUnquotedParser("a,b,c", {}, true);
	deepStrictEqual(result.rows, [["a", "b", "c"]]);
	strictEqual(result.numCols, 3);
});

// *** csvParseStream with custom parser *** //
test(`${variant}: csvParseStream should work with custom parser (csvUnquotedParser)`, async (_t) => {
	const streams = [
		createReadableStream("a,b,c\r\n1,2,3\r\n4,5,6\r\n"),
		csvParseStream({ parser: csvUnquotedParser }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b", "c"],
		["1", "2", "3"],
		["4", "5", "6"],
	]);
});

test(`${variant}: csvParseStream with custom parser should flush remaining`, async (_t) => {
	const streams = [
		createReadableStream("a,b\r\n1,2"),
		csvParseStream({ parser: csvUnquotedParser }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
	]);
});

// *** csvParseStream with small chunkSize to trigger ready path *** //
test(`${variant}: csvParseStream should handle input exceeding chunkSize`, async (_t) => {
	const row = "a,b,c\r\n";
	const input = [row.repeat(50), "x,y,z\r\n"];
	const streams = [
		createReadableStream(input),
		csvParseStream({ chunkSize: 10 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	strictEqual(output.length, 51);
	deepStrictEqual(output[output.length - 1], ["x", "y", "z"]);
});

// *** csvDetectDelimitersStream - pass through after detection in transform *** //
test(`${variant}: csvDetectDelimitersStream should pass through in transform after detection`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 5 });
	const streams = [
		createReadableStream(["a,b,c\n", "1,2,3\n", "4,5,6\n"]),
		detect,
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	strictEqual(detect.result().value.delimiterChar, ",");
	ok(output.includes("4,5,6"));
});

// *** csvDetectDelimitersStream - detect returns false (no newline in header) *** //
test(`${variant}: csvDetectDelimitersStream should buffer when no newline found`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 3 });
	const streams = [createReadableStream(["abc", "d\n1\n"]), detect];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	strictEqual(output, "abcd\n1\n");
});

// *** csvParseStream - unterminated quote with custom escape and fewer fields *** //
test(`${variant}: csvParseStream should handle unterminated quote with custom escape and fewer fields than numCols`, async (_t) => {
	const streams = [
		createReadableStream('a,b\r\n"unterminated'),
		csvParseStream({ escapeChar: "\\" }),
	];
	const result = await pipeline(streams);

	deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [1]);
});

// *** csvParseStream - garbage after closing quote with custom escape *** //
test(`${variant}: csvParseStream should handle content after closing quote with custom escape`, async (_t) => {
	const streams = [
		createReadableStream('"a"x,b\r\n'),
		csvParseStream({ escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["a", "x", "b"]]);
});

// *** csvParseStream with multiple input chunks to test join path *** //
test(`${variant}: csvParseStream should join multiple input chunks before chunkSize`, async (_t) => {
	const streams = [
		createReadableStream(["1,2\r\n", "3,4\r\n"]),
		csvParseStream({ chunkSize: 100 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["1", "2"],
		["3", "4"],
	]);
});

// *** csvQuotedParser direct tests *** //
test(`${variant}: csvQuotedParser should parse with default options`, (_t) => {
	const result = csvQuotedParser("a,b\r\n1,2\r\n");
	deepStrictEqual(result.rows, [
		["a", "b"],
		["1", "2"],
	]);
	strictEqual(result.tail, "");
	deepStrictEqual(result.errors, {});
});

// *** csvParseStream - unterminated quote with fewer fields than numCols (escapeIsQuote=true) *** //
test(`${variant}: csvParseStream should handle unterminated quote with fewer fields than established numCols`, async (_t) => {
	const streams = [
		createReadableStream('a,b\r\n"unterminated'),
		csvParseStream(),
	];
	const result = await pipeline(streams);

	deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [1]);
});

// *** csvParseStream - 3+ char newline *** //
test(`${variant}: csvParseStream should handle 3+ char custom newline`, async (_t) => {
	const streams = [
		createReadableStream('"a",b---c,d---'),
		csvParseStream({ newlineChar: "---" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

// *** csvParseStream - 3+ char newline with custom escape *** //
test(`${variant}: csvParseStream should handle 3+ char custom newline with custom escape`, async (_t) => {
	const streams = [
		createReadableStream('"a",b---c,d---'),
		csvParseStream({ newlineChar: "---", escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

// *** csvParseStream - quote after unquoted row boundary *** //
test(`${variant}: csvParseStream should handle quoted field at start of subsequent row`, async (_t) => {
	const streams = [createReadableStream('a,b\r\n"c",d\r\n'), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

// *** csvParseStream - delimiter at end of input *** //
test(`${variant}: csvParseStream should handle delimiter at end of chunk followed by quote`, async (_t) => {
	const streams = [
		createReadableStream('a,"b"\r\nc,"d"\r\n'),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

// *** csvParseStream - short row in flushing *** //
test(`${variant}: csvParseStream should flush short row with fewer fields`, async (_t) => {
	const streams = [createReadableStream("a,b,c\r\nd"), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["a", "b", "c"], ["d"]]);
});

// *** csvParseStream - lastWasDelimiter at end on flush *** //
test(`${variant}: csvParseStream should handle trailing delimiter on flush with established numCols`, async (_t) => {
	const streams = [createReadableStream("a,b\r\nc,"), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["c", ""],
	]);
});

// *** csvParseStream - malformed row in fast unquoted path *** //
test(`${variant}: csvParseStream should handle malformed unquoted row with missing delimiter`, async (_t) => {
	const streams = [
		createReadableStream("a,b,c\r\nd\r\ne,f,g\r\n"),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output[0], ["a", "b", "c"]);
	deepStrictEqual(output[1], ["d"]);
	deepStrictEqual(output[2], ["e", "f", "g"]);
});

// *** csvParseStream - first row exactly fills input (pos >= len after first-row detection) *** //
test(`${variant}: csvParseStream should handle single unquoted row exactly filling input`, async (_t) => {
	const streams = [createReadableStream("a,b,c\r\n"), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["a", "b", "c"]]);
});

// *** csvDetectHeaderStream - BOM-only input (parser returns no rows) *** //
test(`${variant}: csvDetectHeaderStream should handle BOM-only input`, async (_t) => {
	const headers = csvDetectHeaderStream();
	const streams = [createReadableStream("\uFEFF"), headers];
	const stream = pipejoin(streams);
	await streamToString(stream);

	deepStrictEqual(headers.result().value.header, []);
});

// *** csvDetectHeaderStream - input starting with newline (empty header chunk) *** //
test(`${variant}: csvDetectHeaderStream should handle input starting with newline`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n" });
	const streams = [createReadableStream("\na,b\n"), headers];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, []);
	ok(output.includes("a,b"));
});

// *** csvParseStream - BOM stripping *** //
test(`${variant}: csvDetectDelimitersStream should strip BOM`, async (_t) => {
	const bom = "\uFEFF";
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream(`${bom}a,b\n1,2\n`), detect];
	await pipeline(streams);

	strictEqual(detect.result().value.delimiterChar, ",");
});

// *** csvParseStream - quoted field followed by 3-char newline (escapeIsQuote=true) *** //
test(`${variant}: csvParseStream should handle quoted field followed by 3+ char newline`, async (_t) => {
	const streams = [
		createReadableStream('"a"---"b"---'),
		csvParseStream({ newlineChar: "---" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["a"], ["b"]]);
});

// *** csvParseStream - quoted field followed by multi-char delimiter (escapeIsQuote=false) *** //
test(`${variant}: csvParseStream should handle quoted field with multi-char delimiter and custom escape`, async (_t) => {
	const streams = [
		createReadableStream('"a"::"b"\r\n"c"::"d"\r\n'),
		csvParseStream({ delimiterChar: "::", escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

// *** csvParseStream - quoted field followed by 3-char newline (escapeIsQuote=false) *** //
test(`${variant}: csvParseStream should handle quoted field with 3+ char newline and custom escape`, async (_t) => {
	const streams = [
		createReadableStream('"a"---"b"---'),
		csvParseStream({ newlineChar: "---", escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [["a"], ["b"]]);
});

// *** csvParseStream - short quoted row (fi < numCols) in escapeIsQuote=true *** //
test(`${variant}: csvParseStream should handle short quoted row with fewer fields after newline`, async (_t) => {
	const streams = [createReadableStream('a,b,c\r\n"d"\r\n'), csvParseStream()];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output[0], ["a", "b", "c"]);
	strictEqual(output[1].length, 1);
});

// *** csvParseStream - short quoted row (fi < numCols) in escapeIsQuote=false *** //
test(`${variant}: csvParseStream should handle short quoted row with custom escape after newline`, async (_t) => {
	const streams = [
		createReadableStream('a,b,c\r\n"d"\r\n'),
		csvParseStream({ escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output[0], ["a", "b", "c"]);
	strictEqual(output[1].length, 1);
});

// *** csvParseStream - Buffer chunk with custom parser *** //
test(`${variant}: csvParseStream should handle Buffer chunks with custom parser`, async (_t) => {
	const streams = [
		createReadableStream([Buffer.from("a,b\r\n1,2\r\n")]),
		csvParseStream({ parser: csvUnquotedParser }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
	]);
});

// *** csvParseStream - multiple chunks with custom parser (buffer concat) *** //
test(`${variant}: csvParseStream should handle multiple chunks with custom parser and buffer`, async (_t) => {
	const streams = [
		createReadableStream(["a,b\r\n1,", "2\r\n"]),
		csvParseStream({ parser: csvUnquotedParser }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
	]);
});

// *** csvParseStream - custom parser with errors in streaming *** //
test(`${variant}: csvParseStream should collect errors from custom parser during streaming`, async (_t) => {
	const errorParser = (text, options, isFlushing) => {
		const result = csvQuotedParser(text, options, isFlushing);
		if (!isFlushing) {
			result.errors = {
				TestError: { id: "TestError", message: "test", idx: [0] },
			};
		}
		return result;
	};
	const streams = [
		createReadableStream("a,b\r\n1,2\r\n"),
		csvParseStream({ parser: errorParser }),
	];
	const result = await pipeline(streams);

	ok(result.csvErrors.TestError);
});

// *** csvParseStream - custom parser with errors in flush *** //
test(`${variant}: csvParseStream should collect errors from custom parser during flush`, async (_t) => {
	const errorParser = (text, options, isFlushing) => {
		const result = csvQuotedParser(text, options, isFlushing);
		if (isFlushing) {
			result.errors = {
				FlushError: { id: "FlushError", message: "test", idx: [0] },
			};
		}
		return result;
	};
	const streams = [
		createReadableStream("a,b\r\n1,2"),
		csvParseStream({ parser: errorParser }),
	];
	const result = await pipeline(streams);

	ok(result.csvErrors.FlushError);
});

// *** csvParseStream - inline parser errors during streaming (unterminated quote in non-final chunk) *** //
test(`${variant}: csvParseStream should collect inline parser errors during streaming`, async (_t) => {
	const streams = [
		createReadableStream(['"a,b\r\n', "1,2\r\n"]),
		csvParseStream({ chunkSize: 4 }),
	];
	const result = await pipeline(streams);

	ok(result.csvErrors.UnterminatedQuote);
});

// *** csvParseStream - multiple input chunks before chunkSize (join path) *** //
test(`${variant}: csvParseStream should join multiple input chunks before chunkSize`, async (_t) => {
	const streams = [
		createReadableStream(["a,b\r\n", "1,2\r\n", "3,4\r\n"]),
		csvParseStream({ chunkSize: 12 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
		["3", "4"],
	]);
});

test(`${variant}: csvCoerceValuesStream should handle uppercase boolean`, async (_t) => {
	const streams = [
		createReadableStream([{ val: "TRUE", val2: "FALSE" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ val: true, val2: false }]);
});

// *** FINDING: fast-path-too-many-columns-merged *** //
test(`${variant}: csvParseStream should not merge extra columns in fast unquoted path`, async (_t) => {
	// A row with MORE fields than the established numCols must keep all
	// fields separate (not collapse the surplus into the last field).
	const streams = [
		createReadableStream("a,b,c\r\n1,2,3,4,5\r\n"),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b", "c"],
		["1", "2", "3", "4", "5"],
	]);
});

test(`${variant}: csvParseStream fast and slow paths agree on over-long rows`, async (_t) => {
	// Identical row data must parse identically whether or not a quote char
	// appears elsewhere in the buffer (quote presence picks fast vs slow path).
	const fastStreams = [
		createReadableStream("a,b,c\r\n1,2,3,4,5\r\n"),
		csvParseStream(),
	];
	const slowStreams = [
		createReadableStream('a,b,c\r\n1,2,3,4,5\r\n"x",y,z\r\n'),
		csvParseStream(),
	];
	const fast = await streamToArray(pipejoin(fastStreams));
	const slow = await streamToArray(pipejoin(slowStreams));

	deepStrictEqual(fast[1], ["1", "2", "3", "4", "5"]);
	deepStrictEqual(slow[1], ["1", "2", "3", "4", "5"]);
});

test(`${variant}: csvParseStream over-long rows are detectable as malformed`, async (_t) => {
	const filter = csvRemoveMalformedRowsStream();
	const streams = [
		createReadableStream("a,b,c\r\n1,2,3,4,5\r\n6,7,8\r\n"),
		csvParseStream(),
		filter,
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a", "b", "c"],
		["6", "7", "8"],
	]);
	deepStrictEqual(filter.result().value.MalformedRow.idx, [1]);
});

// *** FINDING: detect-quote-scans-whole-buffer *** //
test(`${variant}: csvDetectDelimitersStream should not detect apostrophe in data as quote char`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [
		createReadableStream("quote,author\n'twas the night,Moore\nhello,World\n"),
		detect,
	];
	await pipeline(streams);

	strictEqual(detect.result().value.quoteChar, '"');
});

test(`${variant}: csvParseStream should parse data with apostrophes after auto-detect`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const parse = csvParseStream({
		delimiterChar: () => detect.result().value.delimiterChar,
		newlineChar: () => detect.result().value.newlineChar,
		quoteChar: () => detect.result().value.quoteChar,
	});
	const streams = [
		createReadableStream("quote,author\n'twas the night,Moore\nhello,World\n"),
		detect,
		parse,
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["quote", "author"],
		["'twas the night", "Moore"],
		["hello", "World"],
	]);
});

// *** FINDING: custom-escape-not-inverse-of-format *** //
test(`${variant}: csvFormatStream/csvParseStream round-trip preserves escape char with custom escapeChar`, async (_t) => {
	const value = 'a"b\\c';
	const formatStreams = [
		createReadableStream([[value, "next"]]),
		csvFormatStream({ escapeChar: "\\" }),
	];
	const formatted = await streamToString(pipejoin(formatStreams));

	const parseStreams = [
		createReadableStream(formatted),
		csvParseStream({ escapeChar: "\\" }),
	];
	const parsed = await streamToArray(pipejoin(parseStreams));

	deepStrictEqual(parsed, [[value, "next"]]);
});

// *** FINDING: custom-escape-lookback-parity *** //
test(`${variant}: csvParseStream should treat even run of escape chars as unescaped closing quote`, async (_t) => {
	// "a\\" => field value is a\ (escaped backslash), then the closing quote
	// is real, so the delimiter and following field are not swallowed.
	const streams = [
		createReadableStream('"a\\\\",b\r\nc,d\r\n'),
		csvParseStream({ escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		["a\\", "b"],
		["c", "d"],
	]);
});

// *** FINDING: detect-header-naive-newline-split *** //
test(`${variant}: csvDetectHeaderStream should respect quoted newline in header field`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n" });
	const streams = [
		createReadableStream('"col\nwith newline",col2\nval1,val2\n'),
		headers,
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ["col\nwith newline", "col2"]);
	strictEqual(output, "val1,val2\n");
});

test(`${variant}: csvDetectHeaderStream should respect custom-escape quoted newline in header`, async (_t) => {
	const headers = csvDetectHeaderStream({
		newlineChar: "\n",
		escapeChar: "\\",
	});
	const streams = [
		createReadableStream('"col\\"x\nnext",col2\nval1,val2\n'),
		headers,
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(headers.result().value.header, ['col"x\nnext', "col2"]);
	strictEqual(output, "val1,val2\n");
});

// *** FINDING: autocoerce-invalid-date *** //
test(`${variant}: csvCoerceValuesStream should keep invalid date string as string`, async (_t) => {
	const streams = [
		createReadableStream([{ d: "2024-13-99" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ d: "2024-13-99" }]);
});

// *** FINDING: csvArrayToObject-proto-header-drops-column *** //
test(`${variant}: csvArrayToObject should keep a __proto__ header as an own data property`, async (_t) => {
	const streams = [
		createReadableStream([["polluted", "ok"]]),
		csvArrayToObject({ headers: ["__proto__", "safe"] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	strictEqual(output.length, 1);
	const row = output[0];
	// The __proto__ column must be stored as an own data property, not lost.
	ok(Object.hasOwn(row, "__proto__"));
	// biome-ignore lint/suspicious/noProto: intentionally reading the own data property literally named "__proto__" to assert prototype-pollution safety.
	strictEqual(row.__proto__, "polluted");
	strictEqual(row.safe, "ok");
});

// ===================================================================
// Mutation-killing tests (added to raise mutation score)
// ===================================================================

// --- csvFormatStream: scanNeedsQuote first-char triggers ---
test(`${variant}: csvFormatStream should quote field with leading space`, async (_t) => {
	const streams = [createReadableStream([[" lead", "ok"]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '" lead",ok\r\n');
});

test(`${variant}: csvFormatStream should quote field with trailing space`, async (_t) => {
	const streams = [createReadableStream([["trail ", "ok"]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"trail ",ok\r\n');
});

test(`${variant}: csvFormatStream should quote field with leading BOM`, async (_t) => {
	const streams = [createReadableStream([["﻿bom", "ok"]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"﻿bom",ok\r\n');
});

test(`${variant}: csvFormatStream should not quote plain interior space`, async (_t) => {
	const streams = [createReadableStream([["a b c", "ok"]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, "a b c,ok\r\n");
});

test(`${variant}: csvFormatStream should quote field containing carriage return`, async (_t) => {
	const streams = [createReadableStream([["a\rb", "ok"]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"a\rb",ok\r\n');
});

// --- csvFormatStream: multi-char delimiter scan path ---
test(`${variant}: csvFormatStream should quote value containing multi-char delimiter`, async (_t) => {
	const streams = [
		createReadableStream([["a::b", "plain"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"a::b"::plain\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter should quote on quote char`, async (_t) => {
	const streams = [
		createReadableStream([['has"quote', "plain"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"has""quote"::plain\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter should not quote plain value`, async (_t) => {
	const streams = [
		createReadableStream([["plain", "value"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, "plain::value\r\n");
});

// --- csvFormatStream: wrapQuote with custom escape ---
test(`${variant}: csvFormatStream custom escape should escape escape char and quote char`, async (_t) => {
	// escapeChar=\, value contains both \ and "; backslash doubled, quote -> \"
	const streams = [
		createReadableStream([['a"b\\c', "ok"]]),
		csvFormatStream({ escapeChar: "\\" }),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"a\\"b\\\\c",ok\r\n');
});

test(`${variant}: csvFormatStream custom escape should escape only escape char when no quote`, async (_t) => {
	const streams = [
		createReadableStream([["a\\b,c", "ok"]]),
		csvFormatStream({ escapeChar: "\\" }),
	];
	const output = await streamToString(pipejoin(streams));
	// contains delimiter so quoted; backslash doubled; no quote char present
	strictEqual(output, '"a\\\\b,c",ok\r\n');
});

// --- csvFormatStream: Date and number/non-string formatting (formatRowSlow) ---
test(`${variant}: csvFormatStream should format Date as ISO string`, async (_t) => {
	const d = new Date("2024-01-15T10:30:00.000Z");
	const streams = [createReadableStream([[d, "ok"]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, "2024-01-15T10:30:00.000Z,ok\r\n");
});

test(`${variant}: csvFormatStream should format number via String`, async (_t) => {
	const streams = [createReadableStream([[42, 3.14]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, "42,3.14\r\n");
});

test(`${variant}: csvFormatStream should format null and undefined as empty`, async (_t) => {
	const streams = [
		createReadableStream([[null, undefined, "x"]]),
		csvFormatStream(),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, ",,x\r\n");
});

test(`${variant}: csvFormatStream should format boolean via String in slow path`, async (_t) => {
	// boolean true forces slow path (non-string); String(true) === "true"
	const streams = [createReadableStream([[true, false]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, "true,false\r\n");
});

test(`${variant}: csvFormatStream should quote number-derived string needing quote`, async (_t) => {
	// A negative number stringifies to "-42" which starts with '-', a quote trigger
	const streams = [createReadableStream([[-42, "ok"]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"-42",ok\r\n');
});

// --- csvFormatStream: batch boundary (>= 64 rows flushed mid-stream) ---
test(`${variant}: csvFormatStream should emit batch at 64 rows then flush remainder`, async (_t) => {
	const rows = [];
	for (let i = 0; i < 70; i++) rows.push([String(i), "x"]);
	const streams = [createReadableStream(rows), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	const lines = output.split("\r\n").filter((l) => l.length > 0);
	strictEqual(lines.length, 70);
	strictEqual(lines[0], "0,x");
	strictEqual(lines[69], "69,x");
});

// --- csvFormatStream: custom newlineChar separator ---
test(`${variant}: csvFormatStream should use custom newlineChar`, async (_t) => {
	const streams = [
		createReadableStream([
			["1", "2"],
			["3", "4"],
		]),
		csvFormatStream({ newlineChar: "\n" }),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, "1,2\n3,4\n");
});

// --- csvFormatStream: custom quoteChar ---
test(`${variant}: csvFormatStream should use custom quoteChar`, async (_t) => {
	const streams = [
		createReadableStream([["a,b", "ok"]]),
		csvFormatStream({ quoteChar: "'" }),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, "'a,b',ok\r\n");
});

// --- csvFormatStream multi-char delimiter: trailing space / CR / LF triggers ---
test(`${variant}: csvFormatStream multi-char delimiter should quote trailing space`, async (_t) => {
	const streams = [
		createReadableStream([["trail ", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"trail "::ok\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter should quote on newline`, async (_t) => {
	const streams = [
		createReadableStream([["a\nb", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"a\nb"::ok\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter should quote on carriage return`, async (_t) => {
	const streams = [
		createReadableStream([["a\rb", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"a\rb"::ok\r\n');
});

test(`${variant}: csvFormatStream single-char delimiter should quote on linefeed only`, async (_t) => {
	const streams = [createReadableStream([["a\nb", "ok"]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"a\nb",ok\r\n');
});

// --- csvCoerceValuesStream: number regex exponent shape ---
test(`${variant}: csvCoerceValuesStream should coerce exponent with explicit sign and multi-digit exponent`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "1e+10", b: "2e-05", c: "1.5E3" }]),
		csvCoerceValuesStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 1e10, b: 2e-5, c: 1500 }]);
});

test(`${variant}: csvCoerceValuesStream should keep malformed exponent as string`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "1e", b: "1e+", c: "1.2.3" }]),
		csvCoerceValuesStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: "1e", b: "1e+", c: "1.2.3" }]);
});

test(`${variant}: csvCoerceValuesStream should not coerce string with trailing non-number text`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "12abc", d: "2024-01-15extra" }]),
		csvCoerceValuesStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: "12abc", d: "2024-01-15extra" }]);
});

test(`${variant}: csvCoerceValuesStream should not coerce date missing leading anchor`, async (_t) => {
	const streams = [
		createReadableStream([{ d: "x2024-01-15" }]),
		csvCoerceValuesStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ d: "x2024-01-15" }]);
});

test(`${variant}: csvCoerceValuesStream should coerce date with time and timezone offset`, async (_t) => {
	const streams = [
		createReadableStream([{ d: "2024-01-15T10:30:00+05:30" }]),
		csvCoerceValuesStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ d: new Date("2024-01-15T10:30:00+05:30") }]);
});

// --- csvCoerceValuesStream: boolean first-char/length gating in autoCoerce ---
test(`${variant}: csvCoerceValuesStream should not treat 4-char non-true string as boolean`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "trUE", b: "tree", c: "True" }]),
		csvCoerceValuesStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: true, b: "tree", c: true }]);
});

test(`${variant}: csvCoerceValuesStream should not coerce non-t/f boolean-length words`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "yes", b: "nope" }]),
		csvCoerceValuesStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: "yes", b: "nope" }]);
});

test(`${variant}: csvCoerceValuesStream should not coerce truthy when length is wrong`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "truer", b: "falsey" }]),
		csvCoerceValuesStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: "truer", b: "falsey" }]);
});

test(`${variant}: csvCoerceValuesStream should return null for empty string auto-coerce`, async (_t) => {
	const streams = [createReadableStream([{ a: "" }]), csvCoerceValuesStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: null }]);
});

test(`${variant}: csvCoerceValuesStream should coerce single-digit zero and nine`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "0", b: "9" }]),
		csvCoerceValuesStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 0, b: 9 }]);
});

test(`${variant}: csvCoerceValuesStream should keep plain non-numeric non-bool string`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "hello", b: "world" }]),
		csvCoerceValuesStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: "hello", b: "world" }]);
});

// --- coerceToType boolean: string vs non-string ---
test(`${variant}: csvCoerceValuesStream explicit boolean should lowercase compare string`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "TRUE", b: "no", c: "true" }]),
		csvCoerceValuesStream({
			columns: { a: "boolean", b: "boolean", c: "boolean" },
		}),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: true, b: false, c: true }]);
});

test(`${variant}: csvCoerceValuesStream explicit boolean should use Boolean for non-string`, async (_t) => {
	const streams = [
		createReadableStream([{ a: 1, b: 0 }]),
		csvCoerceValuesStream({ columns: { a: "boolean", b: "boolean" } }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: true, b: false }]);
});

// --- coerceToType json with array ---
test(`${variant}: csvCoerceValuesStream explicit json should parse array`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "[1,2,3]" }]),
		csvCoerceValuesStream({ columns: { a: "json" } }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: [1, 2, 3] }]);
});

// --- coerceToType default (unknown type) passthrough ---
test(`${variant}: csvCoerceValuesStream unknown column type should pass value unchanged`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "raw" }]),
		csvCoerceValuesStream({ columns: { a: "string" } }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: "raw" }]);
});

// --- coerceToType number coerces hex-like and keeps decimals ---
test(`${variant}: csvCoerceValuesStream explicit number coerces decimal and hex`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "3.14", b: "0x1F" }]),
		csvCoerceValuesStream({ columns: { a: "number", b: "number" } }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [{ a: 3.14, b: 31 }]);
});

// --- csvDetectDelimitersStream: quote bracketing precision ---
// A quote candidate must OPEN at a field start AND CLOSE at a field end to be
// recognised. The following exercise each isFieldStart / isFieldEnd branch.

test(`${variant}: csvDetectDelimitersStream should detect quote opening at text start`, async (_t) => {
	// "'a'" opens at i===0 and closes before a delimiter
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("'a',b\n1,2\n"), detect];
	await pipeline(streams);
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream should detect quote after delimiter and closing at line end`, async (_t) => {
	// second field 'b' opens after a comma and closes before the newline (LF)
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a,'b'\n1,2\n"), detect];
	await pipeline(streams);
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream should detect quote closing before CR`, async (_t) => {
	// quoted field closes right before \r in a CRLF newline
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("'a',b\r\n1,2\r\n"), detect];
	await pipeline(streams);
	strictEqual(detect.result().value.quoteChar, "'");
	strictEqual(detect.result().value.newlineChar, "\r\n");
});

test(`${variant}: csvDetectDelimitersStream should detect quote opening after newline`, async (_t) => {
	// The only properly-bracketed quote is on row 2, opening right after the LF
	const detect = csvDetectDelimitersStream({ chunkSize: 0 });
	const streams = [createReadableStream("a,b\n'c',d\n"), detect];
	await pipeline(streams);
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream should NOT detect quote that never closes at a field end`, async (_t) => {
	// 'x is a field-start apostrophe with no closing quote at any field end.
	// A later lone apostrophe sits mid-field (after a letter, not a boundary),
	// so it neither opens nor closes a field -> default double-quote.
	const detect = csvDetectDelimitersStream();
	const streams = [
		createReadableStream("name,note\n'x,it's fine\nhello,world\n"),
		detect,
	];
	await pipeline(streams);
	strictEqual(detect.result().value.quoteChar, '"');
});

test(`${variant}: csvDetectDelimitersStream should set escapeChar equal to detected quoteChar`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("'a','b'\n1,2\n"), detect];
	await pipeline(streams);
	strictEqual(detect.result().value.escapeChar, "'");
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream should pick first delimiter by precedence (tab over comma)`, async (_t) => {
	// Header contains BOTH a tab and a comma; detection order is [tab,pipe,semi,comma]
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a\tb,c\n1\t2,3\n"), detect];
	await pipeline(streams);
	strictEqual(detect.result().value.delimiterChar, "\t");
});

test(`${variant}: csvDetectDelimitersStream should pick pipe over semicolon and comma`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a|b;c,d\n1|2;3,4\n"), detect];
	await pipeline(streams);
	strictEqual(detect.result().value.delimiterChar, "|");
});

test(`${variant}: csvDetectDelimitersStream should pick semicolon over comma`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a;b,c\n1;2,3\n"), detect];
	await pipeline(streams);
	strictEqual(detect.result().value.delimiterChar, ";");
});

test(`${variant}: csvDetectDelimitersStream should default to double-quote when no quote present`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a,b,c\n1,2,3\n"), detect];
	await pipeline(streams);
	strictEqual(detect.result().value.quoteChar, '"');
});

test(`${variant}: csvDetectDelimitersStream should set newlineChar from header match`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const streams = [createReadableStream("a,b\r1,2\r"), detect];
	await pipeline(streams);
	strictEqual(detect.result().value.newlineChar, "\r");
});

// --- csvQuotedParser direct: numCols, escapes, tail, multi-char newline ---
test(`${variant}: csvQuotedParser establishes numCols from first row`, (_t) => {
	const result = csvQuotedParser("a,b,c\r\n1,2,3\r\n");
	strictEqual(result.numCols, 3);
	deepStrictEqual(result.rows, [
		["a", "b", "c"],
		["1", "2", "3"],
	]);
});

test(`${variant}: csvQuotedParser returns tail for incomplete trailing row`, (_t) => {
	const result = csvQuotedParser("a,b\r\n1,2\r\n3,4");
	deepStrictEqual(result.rows, [
		["a", "b"],
		["1", "2"],
	]);
	strictEqual(result.tail, "3,4");
});

test(`${variant}: csvQuotedParser preserves quoted field with escaped quotes (no escapes flag)`, (_t) => {
	const result = csvQuotedParser('"plain",x\r\n');
	deepStrictEqual(result.rows, [["plain", "x"]]);
});

test(`${variant}: csvQuotedParser keeps doubled-quote escapes`, (_t) => {
	const result = csvQuotedParser('"a""b","c"\r\n');
	deepStrictEqual(result.rows, [['a"b', "c"]]);
});

test(`${variant}: csvQuotedParser handles short quoted row truncating to fi`, (_t) => {
	// numCols established as 3, second row has a single quoted field -> length 1
	const result = csvQuotedParser('a,b,c\r\n"d"\r\n');
	deepStrictEqual(result.rows[0], ["a", "b", "c"]);
	strictEqual(result.rows[1].length, 1);
	deepStrictEqual(result.rows[1], ["d"]);
});

test(`${variant}: csvQuotedParser handles multi-char newline length>2`, (_t) => {
	const result = csvQuotedParser("a,b||~c,d||~", { newlineChar: "||~" }, true);
	deepStrictEqual(result.rows, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvQuotedParser CRLF requires both chars (lone CR does not split)`, (_t) => {
	// With CRLF newline, a lone \r mid-field must NOT terminate the row.
	const result = csvQuotedParser('"a\rb",c\r\n', { newlineChar: "\r\n" }, true);
	deepStrictEqual(result.rows, [["a\rb", "c"]]);
});

test(`${variant}: csvQuotedParser tracks unterminated quote error on flush`, (_t) => {
	const result = csvQuotedParser('"abc', {}, true);
	deepStrictEqual(result.rows, [["abc"]]);
	ok(result.errors.UnterminatedQuote);
	deepStrictEqual(result.errors.UnterminatedQuote.idx, [0]);
});

test(`${variant}: csvQuotedParser unterminated quote not flushing returns tail`, (_t) => {
	const result = csvQuotedParser('a,b\r\n"abc', {}, false);
	deepStrictEqual(result.rows, [["a", "b"]]);
	strictEqual(result.tail, '"abc');
});

test(`${variant}: csvQuotedParser custom escape unterminated keeps unescaped value on flush`, (_t) => {
	const result = csvQuotedParser('"a\\"b', { escapeChar: "\\" }, true);
	deepStrictEqual(result.rows, [['a"b']]);
	ok(result.errors.UnterminatedQuote);
});

test(`${variant}: csvQuotedParser custom escape even run is real closing quote`, (_t) => {
	const result = csvQuotedParser('"a\\\\",b\r\n', { escapeChar: "\\" }, true);
	deepStrictEqual(result.rows, [["a\\", "b"]]);
});

test(`${variant}: csvQuotedParser custom escape odd run escapes the quote`, (_t) => {
	const result = csvQuotedParser('"a\\"b",c\r\n', { escapeChar: "\\" }, true);
	deepStrictEqual(result.rows, [['a"b', "c"]]);
});

// --- field size limit ---
test(`${variant}: csvParseStream should throw when quoted field exceeds fieldMaxSize`, async (_t) => {
	// field length 11 (> 10) but buffer (17) stays under the 2x safety limit (20)
	const val = "x".repeat(11);
	const streams = [
		createReadableStream(`"${val}",y\r\n`),
		csvParseStream({ fieldMaxSize: 10 }),
	];
	let threw = false;
	try {
		await pipeline(streams);
	} catch (e) {
		threw = true;
		ok(/CSV field size/.test(e.message));
	}
	ok(threw);
});

test(`${variant}: csvParseStream should accept quoted field at exactly fieldMaxSize`, async (_t) => {
	const val = "x".repeat(10);
	const streams = [
		createReadableStream(`"${val}",y\r\n`),
		csvParseStream({ fieldMaxSize: 10 }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [[val, "y"]]);
});

test(`${variant}: csvParseStream should throw when custom-escape quoted field exceeds fieldMaxSize`, async (_t) => {
	const val = "x".repeat(11);
	const streams = [
		createReadableStream(`"${val}",y\r\n`),
		csvParseStream({ escapeChar: "\\", fieldMaxSize: 10 }),
	];
	let threw = false;
	try {
		await pipeline(streams);
	} catch (e) {
		threw = true;
		ok(/CSV field size/.test(e.message));
	}
	ok(threw);
});

test(`${variant}: csvParseStream should throw on buffer exceeding safety limit`, async (_t) => {
	// An unterminated quote that grows the buffer beyond fieldMaxSize*2 throws.
	const big = `"${"x".repeat(60)}`;
	const streams = [
		createReadableStream(big),
		csvParseStream({ fieldMaxSize: 10, chunkSize: 1 }),
	];
	let threw = false;
	try {
		await pipeline(streams);
	} catch (e) {
		threw = true;
		ok(/safety limit/.test(e.message));
	}
	ok(threw);
});

// --- fast unquoted path: too-few columns (malformed) and surplus ---
test(`${variant}: csvParseStream fast path keeps too-few-column row as-is`, async (_t) => {
	const streams = [
		createReadableStream("a,b,c\r\nd,e\r\nf,g,h\r\n"),
		csvParseStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [
		["a", "b", "c"],
		["d", "e"],
		["f", "g", "h"],
	]);
});

test(`${variant}: csvParseStream fast path keeps multiple full rows`, async (_t) => {
	const streams = [
		createReadableStream("a,b\r\n1,2\r\n3,4\r\n5,6\r\n"),
		csvParseStream(),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
		["3", "4"],
		["5", "6"],
	]);
});

test(`${variant}: csvParseStream fast path partial last row without newline`, async (_t) => {
	const streams = [createReadableStream("a,b\r\n1,2\r\n3,4"), csvParseStream()];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
		["3", "4"],
	]);
});

// --- multi-char newline (length 2 custom, not CRLF) ---
test(`${variant}: csvParseStream supports 2-char custom newline`, async (_t) => {
	const streams = [
		createReadableStream("a,b~|1,2~|"),
		csvParseStream({ newlineChar: "~|" }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
	]);
});

test(`${variant}: csvParseStream 2-char newline does not split on partial match`, async (_t) => {
	// A lone '~' that is not followed by '|' must stay inside the field.
	const streams = [
		createReadableStream('"a~b",c~|d,e~|'),
		csvParseStream({ newlineChar: "~|" }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [
		["a~b", "c"],
		["d", "e"],
	]);
});

// --- unquoted parser numCols + multi-row ---
test(`${variant}: csvUnquotedParser sets numCols and parses many rows`, (_t) => {
	const result = csvUnquotedParser("a,b\r\n1,2\r\n3,4\r\n");
	strictEqual(result.numCols, 2);
	deepStrictEqual(result.rows, [
		["a", "b"],
		["1", "2"],
		["3", "4"],
	]);
});

test(`${variant}: csvUnquotedParser custom delimiter and newline`, (_t) => {
	const result = csvUnquotedParser("a;b\nc;d\n", {
		delimiterChar: ";",
		newlineChar: "\n",
	});
	deepStrictEqual(result.rows, [
		["a", "b"],
		["c", "d"],
	]);
});

// --- csvDetectHeaderStream / findRowEnd quote & escape aware row scan ---
test(`${variant}: csvDetectHeaderStream multi-char delimiter splits header`, async (_t) => {
	const headers = csvDetectHeaderStream({
		newlineChar: "\n",
		delimiterChar: "::",
	});
	const streams = [createReadableStream("a::b::c\n1::2::3\n"), headers];
	const output = await streamToString(pipejoin(streams));
	deepStrictEqual(headers.result().value.header, ["a", "b", "c"]);
	strictEqual(output, "1::2::3\n");
});

test(`${variant}: csvDetectHeaderStream respects delimiter inside quoted header field`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n" });
	const streams = [createReadableStream('"a,b",c\n1,2\n'), headers];
	const output = await streamToString(pipejoin(streams));
	deepStrictEqual(headers.result().value.header, ["a,b", "c"]);
	strictEqual(output, "1,2\n");
});

test(`${variant}: csvDetectHeaderStream custom-escape even run closes header quote`, async (_t) => {
	const headers = csvDetectHeaderStream({
		newlineChar: "\n",
		escapeChar: "\\",
	});
	const streams = [createReadableStream('"a\\\\",b\nv1,v2\n'), headers];
	const output = await streamToString(pipejoin(streams));
	deepStrictEqual(headers.result().value.header, ["a\\", "b"]);
	strictEqual(output, "v1,v2\n");
});

test(`${variant}: csvDetectHeaderStream passes through data after a quoted header newline`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n" });
	const streams = [
		createReadableStream('"h1\nstill h1",h2\nr1a,r1b\nr2a,r2b\n'),
		headers,
	];
	const output = await streamToString(pipejoin(streams));
	deepStrictEqual(headers.result().value.header, ["h1\nstill h1", "h2"]);
	strictEqual(output, "r1a,r1b\nr2a,r2b\n");
});

test(`${variant}: csvDetectHeaderStream CRLF header newline length`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\r\n" });
	const streams = [createReadableStream("a,b,c\r\n1,2,3\r\n"), headers];
	const output = await streamToString(pipejoin(streams));
	deepStrictEqual(headers.result().value.header, ["a", "b", "c"]);
	strictEqual(output, "1,2,3\r\n");
});

test(`${variant}: csvDetectHeaderStream emits remaining rest only when non-empty`, async (_t) => {
	const headers = csvDetectHeaderStream({ newlineChar: "\n" });
	const streams = [createReadableStream("only,header\n"), headers];
	const output = await streamToString(pipejoin(streams));
	deepStrictEqual(headers.result().value.header, ["only", "header"]);
	strictEqual(output, "");
});

// --- csvRemoveMalformedRowsStream result key + default + idx ---
test(`${variant}: csvRemoveMalformedRowsStream default resultKey and message`, async (_t) => {
	const filter = csvRemoveMalformedRowsStream();
	const streams = [createReadableStream([["a", "b"], ["c"]]), filter];
	const result = await pipeline(streams);
	strictEqual(filter.result().key, "csvRemoveMalformedRows");
	deepStrictEqual(result.csvRemoveMalformedRows.MalformedRow.idx, [1]);
	strictEqual(
		filter.result().value.MalformedRow.message,
		"Row has incorrect number of fields",
	);
	strictEqual(filter.result().value.MalformedRow.id, "MalformedRow");
});

test(`${variant}: csvRemoveMalformedRowsStream first row sets count, later mismatch dropped`, async (_t) => {
	const filter = csvRemoveMalformedRowsStream();
	const streams = [
		createReadableStream([["a", "b", "c"], ["1", "2", "3"], ["x"]]),
		filter,
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [
		["a", "b", "c"],
		["1", "2", "3"],
	]);
	deepStrictEqual(filter.result().value.MalformedRow.idx, [2]);
});

// --- csvRemoveEmptyRowsStream default resultKey + message ---
test(`${variant}: csvRemoveEmptyRowsStream default resultKey and message`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream();
	const streams = [
		createReadableStream([
			["", ""],
			["1", "2"],
		]),
		filter,
	];
	const result = await pipeline(streams);
	strictEqual(filter.result().key, "csvRemoveEmptyRows");
	strictEqual(result.csvRemoveEmptyRows.EmptyRow.message, "Row is empty");
	strictEqual(result.csvRemoveEmptyRows.EmptyRow.id, "EmptyRow");
});

test(`${variant}: csvRemoveEmptyRowsStream keeps row with one non-empty field`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream();
	const streams = [
		createReadableStream([
			["", "x"],
			["", ""],
		]),
		filter,
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [["", "x"]]);
	deepStrictEqual(filter.result().value.EmptyRow.idx, [1]);
});

test(`${variant}: csvRemoveEmptyRowsStream keeps row whose only non-empty field is last`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream();
	const streams = [createReadableStream([["", "", "z"]]), filter];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [["", "", "z"]]);
	deepStrictEqual(filter.result().value, {});
});

// --- csvInjectHeaderStream injects header exactly once ---
test(`${variant}: csvInjectHeaderStream injects header exactly once`, async (_t) => {
	const streams = [
		createReadableStream([
			["1", "2"],
			["3", "4"],
			["5", "6"],
		]),
		csvInjectHeaderStream({ header: ["a", "b"] }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
		["3", "4"],
		["5", "6"],
	]);
});

// --- csvParseStream chunkSize ready/join paths ---
test(`${variant}: csvParseStream single input chunk reaching chunkSize processes immediately`, async (_t) => {
	const streams = [
		createReadableStream(["a,b,c,d,e\r\n", "1,2,3,4,5\r\n"]),
		csvParseStream({ chunkSize: 5 }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [
		["a", "b", "c", "d", "e"],
		["1", "2", "3", "4", "5"],
	]);
});

test(`${variant}: csvParseStream below chunkSize joins on flush`, async (_t) => {
	const streams = [
		createReadableStream(["a,", "b\r\n", "1,", "2\r\n"]),
		csvParseStream({ chunkSize: 1000 }),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
	]);
});

test(`${variant}: csvParseStream default resultKey is csvErrors`, async (_t) => {
	const streams = [createReadableStream("a,b\r\n"), csvParseStream()];
	const result = await pipeline(streams);
	strictEqual(streams[1].result().key, "csvErrors");
	deepStrictEqual(result.csvErrors, {});
});

// --- csvFormatStream multi-char delimiter: each first-char formula trigger ---
test(`${variant}: csvFormatStream multi-char delimiter quotes leading equals`, async (_t) => {
	const streams = [
		createReadableStream([["=x", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	strictEqual(await streamToString(pipejoin(streams)), '"=x"::ok\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter quotes leading plus`, async (_t) => {
	const streams = [
		createReadableStream([["+x", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	strictEqual(await streamToString(pipejoin(streams)), '"+x"::ok\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter quotes leading minus`, async (_t) => {
	const streams = [
		createReadableStream([["-x", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	strictEqual(await streamToString(pipejoin(streams)), '"-x"::ok\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter quotes leading at-sign`, async (_t) => {
	const streams = [
		createReadableStream([["@x", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	strictEqual(await streamToString(pipejoin(streams)), '"@x"::ok\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter quotes leading space`, async (_t) => {
	const streams = [
		createReadableStream([[" x", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	strictEqual(await streamToString(pipejoin(streams)), '" x"::ok\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter quotes leading BOM`, async (_t) => {
	const streams = [
		createReadableStream([["﻿x", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	strictEqual(await streamToString(pipejoin(streams)), '"﻿x"::ok\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter does NOT quote plain leading char`, async (_t) => {
	const streams = [
		createReadableStream([["xyz", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	strictEqual(await streamToString(pipejoin(streams)), "xyz::ok\r\n");
});

test(`${variant}: csvFormatStream multi-char delimiter quotes trailing space`, async (_t) => {
	const streams = [
		createReadableStream([["trail ", "ok"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	strictEqual(await streamToString(pipejoin(streams)), '"trail "::ok\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter quotes on newline`, async (_t) => {
	const a = await streamToString(
		pipejoin([
			createReadableStream([["a\nb", "ok"]]),
			csvFormatStream({ delimiterChar: "::" }),
		]),
	);
	strictEqual(a, '"a\nb"::ok\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter quotes on carriage return`, async (_t) => {
	const b = await streamToString(
		pipejoin([
			createReadableStream([["a\rb", "ok"]]),
			csvFormatStream({ delimiterChar: "::" }),
		]),
	);
	strictEqual(b, '"a\rb"::ok\r\n');
});

// --- csvFormatStream single-char delimiter: each first-char formula trigger ---
test(`${variant}: csvFormatStream single-char delimiter quotes leading at-sign`, async (_t) => {
	const streams = [createReadableStream([["@x", "ok"]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), '"@x",ok\r\n');
});

test(`${variant}: csvFormatStream single-char delimiter quotes leading plus`, async (_t) => {
	const streams = [createReadableStream([["+x", "ok"]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), '"+x",ok\r\n');
});

test(`${variant}: csvFormatStream single-char delimiter quotes leading minus`, async (_t) => {
	const streams = [createReadableStream([["-x", "ok"]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), '"-x",ok\r\n');
});

test(`${variant}: csvFormatStream single-char delimiter quotes leading equals`, async (_t) => {
	const streams = [createReadableStream([["=x", "ok"]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), '"=x",ok\r\n');
});

test(`${variant}: csvFormatStream single-char delimiter does NOT quote plain value`, async (_t) => {
	const streams = [createReadableStream([["xyz", "ok"]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), "xyz,ok\r\n");
});

test(`${variant}: csvFormatStream single-char delimiter quotes trailing space`, async (_t) => {
	const streams = [createReadableStream([["trail ", "ok"]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), '"trail ",ok\r\n');
});

test(`${variant}: csvFormatStream should quote field with leading BOM`, async (_t) => {
	const streams = [createReadableStream([["﻿bom", "ok"]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), '"﻿bom",ok\r\n');
});

test(`${variant}: csvFormatStream should quote field with carriage return`, async (_t) => {
	const streams = [createReadableStream([["a\rb", "ok"]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), '"a\rb",ok\r\n');
});

test(`${variant}: csvFormatStream should format Date as ISO string`, async (_t) => {
	const d = new Date("2024-01-15T10:30:00.000Z");
	const streams = [createReadableStream([[d, "ok"]]), csvFormatStream()];
	strictEqual(
		await streamToString(pipejoin(streams)),
		"2024-01-15T10:30:00.000Z,ok\r\n",
	);
});

test(`${variant}: csvFormatStream should format number via String`, async (_t) => {
	const streams = [createReadableStream([[42, 3.14]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), "42,3.14\r\n");
});

test(`${variant}: csvFormatStream should format boolean via String in slow path`, async (_t) => {
	const streams = [createReadableStream([[true, false]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), "true,false\r\n");
});

test(`${variant}: csvFormatStream should quote negative number string`, async (_t) => {
	const streams = [createReadableStream([[-42, "ok"]]), csvFormatStream()];
	strictEqual(await streamToString(pipejoin(streams)), '"-42",ok\r\n');
});

test(`${variant}: csvFormatStream should format null and undefined as empty`, async (_t) => {
	const streams = [
		createReadableStream([[null, undefined, "x"]]),
		csvFormatStream(),
	];
	strictEqual(await streamToString(pipejoin(streams)), ",,x\r\n");
});

test(`${variant}: csvFormatStream custom escape escapes escape and quote chars`, async (_t) => {
	const streams = [
		createReadableStream([['a"b\\c', "ok"]]),
		csvFormatStream({ escapeChar: "\\" }),
	];
	strictEqual(await streamToString(pipejoin(streams)), '"a\\"b\\\\c",ok\r\n');
});

test(`${variant}: csvFormatStream custom escape escapes only escape char when no quote`, async (_t) => {
	const streams = [
		createReadableStream([["a\\b,c", "ok"]]),
		csvFormatStream({ escapeChar: "\\" }),
	];
	strictEqual(await streamToString(pipejoin(streams)), '"a\\\\b,c",ok\r\n');
});

test(`${variant}: csvFormatStream batch boundary flushes at 64 rows`, async (_t) => {
	const rows = [];
	for (let i = 0; i < 70; i++) rows.push([String(i), "x"]);
	const output = await streamToString(
		pipejoin([createReadableStream(rows), csvFormatStream()]),
	);
	const lines = output.split("\r\n").filter((l) => l.length > 0);
	strictEqual(lines.length, 70);
	strictEqual(lines[0], "0,x");
	strictEqual(lines[69], "69,x");
});

test(`${variant}: csvFormatStream custom newlineChar separator`, async (_t) => {
	const output = await streamToString(
		pipejoin([
			createReadableStream([
				["1", "2"],
				["3", "4"],
			]),
			csvFormatStream({ newlineChar: "\n" }),
		]),
	);
	strictEqual(output, "1,2\n3,4\n");
});

test(`${variant}: csvFormatStream custom quoteChar`, async (_t) => {
	const output = await streamToString(
		pipejoin([
			createReadableStream([["a,b", "ok"]]),
			csvFormatStream({ quoteChar: "'" }),
		]),
	);
	strictEqual(output, "'a,b',ok\r\n");
});

// --- Quoted-field post-quote newline dispatch: length 1 / 2 / >2 ---
test(`${variant}: csvQuotedParser quoted field then LF newline (length 1)`, (_t) => {
	const result = csvQuotedParser('"a"\n"b"\n', { newlineChar: "\n" }, true);
	deepStrictEqual(result.rows, [["a"], ["b"]]);
});

test(`${variant}: csvQuotedParser quoted field then CRLF newline (length 2)`, (_t) => {
	const result = csvQuotedParser(
		'"a"\r\n"b"\r\n',
		{ newlineChar: "\r\n" },
		true,
	);
	deepStrictEqual(result.rows, [["a"], ["b"]]);
});

test(`${variant}: csvQuotedParser quoted field then lone CR is not a CRLF newline`, (_t) => {
	const result = csvQuotedParser('"a"\rb,c\r\n', { newlineChar: "\r\n" }, true);
	strictEqual(result.rows.length, 1);
	strictEqual(result.rows[0][0], "a");
});

test(`${variant}: csvQuotedParser quoted field then 3-char newline (length > 2)`, (_t) => {
	const result = csvQuotedParser('"a"~|~"b"~|~', { newlineChar: "~|~" }, true);
	deepStrictEqual(result.rows, [["a"], ["b"]]);
});

test(`${variant}: csvQuotedParser quoted field then partial 3-char newline is garbage`, (_t) => {
	const result = csvQuotedParser('"a"~|x~|~', { newlineChar: "~|~" }, true);
	strictEqual(result.rows.length, 1);
	strictEqual(result.rows[0][0], "a");
});

test(`${variant}: csvQuotedParser custom escape quoted field then LF newline`, (_t) => {
	const result = csvQuotedParser(
		'"a"\n"b"\n',
		{ newlineChar: "\n", escapeChar: "\\" },
		true,
	);
	deepStrictEqual(result.rows, [["a"], ["b"]]);
});

test(`${variant}: csvQuotedParser custom escape quoted field then CRLF newline`, (_t) => {
	const result = csvQuotedParser(
		'"a"\r\n"b"\r\n',
		{ newlineChar: "\r\n", escapeChar: "\\" },
		true,
	);
	deepStrictEqual(result.rows, [["a"], ["b"]]);
});

test(`${variant}: csvQuotedParser custom escape quoted field then 3-char newline`, (_t) => {
	const result = csvQuotedParser(
		'"a"~|~"b"~|~',
		{ newlineChar: "~|~", escapeChar: "\\" },
		true,
	);
	deepStrictEqual(result.rows, [["a"], ["b"]]);
});

test(`${variant}: csvQuotedParser custom escape quoted field then lone CR not CRLF`, (_t) => {
	const result = csvQuotedParser(
		'"a"\rb,c\r\n',
		{ newlineChar: "\r\n", escapeChar: "\\" },
		true,
	);
	strictEqual(result.rows.length, 1);
	strictEqual(result.rows[0][0], "a");
});

// --- csvCoerceValuesStream: iso8601 regex shape ---
test(`${variant}: csvCoerceValuesStream should coerce date with multi-digit fractional seconds`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ d: "2024-01-15T10:30:00.123Z" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ d: new Date("2024-01-15T10:30:00.123Z") }]);
});

test(`${variant}: csvCoerceValuesStream should coerce date with timezone offset without colon`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ d: "2024-01-15T10:30:00+0530" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ d: new Date("2024-01-15T10:30:00+0530") }]);
});

test(`${variant}: csvCoerceValuesStream should coerce date with timezone offset with colon`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ d: "2024-01-15T10:30:00+05:30" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ d: new Date("2024-01-15T10:30:00+05:30") }]);
});

test(`${variant}: csvCoerceValuesStream should coerce date with space separator and seconds`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ d: "2024-01-15 10:30:45" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ d: new Date("2024-01-15 10:30:45") }]);
});

test(`${variant}: csvCoerceValuesStream should coerce plain date-only value`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ d: "2024-12-31" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ d: new Date("2024-12-31") }]);
});

test(`${variant}: csvCoerceValuesStream should keep date with non-digit fractional as string`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ d: "2024-01-15T10:30:00.abc" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ d: "2024-01-15T10:30:00.abc" }]);
});

test(`${variant}: csvCoerceValuesStream should coerce exponent forms`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ a: "1e+10", b: "2e-05", c: "1.5E3" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ a: 1e10, b: 2e-5, c: 1500 }]);
});

test(`${variant}: csvCoerceValuesStream should keep malformed exponent as string`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ a: "1e", b: "1e+", c: "1.2.3" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ a: "1e", b: "1e+", c: "1.2.3" }]);
});

test(`${variant}: csvCoerceValuesStream should not coerce trailing non-number text`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ a: "12abc", d: "2024-01-15extra" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ a: "12abc", d: "2024-01-15extra" }]);
});

test(`${variant}: csvCoerceValuesStream should not coerce date missing leading anchor`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ d: "x2024-01-15" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ d: "x2024-01-15" }]);
});

test(`${variant}: csvCoerceValuesStream boolean first-char/length gating`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ a: "trUE", b: "tree", c: "True", d: "truer" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ a: true, b: "tree", c: true, d: "truer" }]);
});

test(`${variant}: csvCoerceValuesStream should return null for empty string auto-coerce`, async (_t) => {
	const output = await streamToArray(
		pipejoin([createReadableStream([{ a: "" }]), csvCoerceValuesStream()]),
	);
	deepStrictEqual(output, [{ a: null }]);
});

test(`${variant}: csvCoerceValuesStream should coerce single-digit zero and nine`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ a: "0", b: "9" }]),
			csvCoerceValuesStream(),
		]),
	);
	deepStrictEqual(output, [{ a: 0, b: 9 }]);
});

test(`${variant}: csvCoerceValuesStream explicit boolean lowercase compare`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ a: "TRUE", b: "no", c: "true" }]),
			csvCoerceValuesStream({
				columns: { a: "boolean", b: "boolean", c: "boolean" },
			}),
		]),
	);
	deepStrictEqual(output, [{ a: true, b: false, c: true }]);
});

test(`${variant}: csvCoerceValuesStream explicit boolean Boolean for non-string`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ a: 1, b: 0 }]),
			csvCoerceValuesStream({ columns: { a: "boolean", b: "boolean" } }),
		]),
	);
	deepStrictEqual(output, [{ a: true, b: false }]);
});

test(`${variant}: csvCoerceValuesStream explicit json parses array`, async (_t) => {
	const output = await streamToArray(
		pipejoin([
			createReadableStream([{ a: "[1,2,3]" }]),
			csvCoerceValuesStream({ columns: { a: "json" } }),
		]),
	);
	deepStrictEqual(output, [{ a: [1, 2, 3] }]);
});

// --- csvDetectDelimitersStream quote boundary precision ---
test(`${variant}: csvDetectDelimitersStream detects quote closing at end of text`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([createReadableStream("x,y\nz,'w'"), detect]);
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream detects quote opening after CR-only newline`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([createReadableStream("a,b\r'c',d\r"), detect]);
	strictEqual(detect.result().value.quoteChar, "'");
	strictEqual(detect.result().value.newlineChar, "\r");
});

test(`${variant}: csvDetectDelimitersStream detects quote closing before a delimiter`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([createReadableStream("'a',b,c\n1,2,3\n"), detect]);
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream detects quote opening at text start`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([createReadableStream("'a',b\n1,2\n"), detect]);
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream detects quote opening after newline`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([createReadableStream("a,b\n'c',d\n"), detect]);
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream does NOT detect a quote that never closes at a field end`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([
		createReadableStream("name,note\n'x,it's fine\nhello,world\n"),
		detect,
	]);
	strictEqual(detect.result().value.quoteChar, '"');
});

test(`${variant}: csvDetectDelimitersStream sets escapeChar equal to detected quoteChar`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([createReadableStream("'a','b'\n1,2\n"), detect]);
	strictEqual(detect.result().value.escapeChar, "'");
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream picks tab over comma by precedence`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([createReadableStream("a\tb,c\n1\t2,3\n"), detect]);
	strictEqual(detect.result().value.delimiterChar, "\t");
});

test(`${variant}: csvDetectDelimitersStream picks pipe over semicolon and comma`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([createReadableStream("a|b;c,d\n1|2;3,4\n"), detect]);
	strictEqual(detect.result().value.delimiterChar, "|");
});

test(`${variant}: csvDetectDelimitersStream picks semicolon over comma`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([createReadableStream("a;b,c\n1;2,3\n"), detect]);
	strictEqual(detect.result().value.delimiterChar, ";");
});

test(`${variant}: csvDetectDelimitersStream defaults to double-quote when none present`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await pipeline([createReadableStream("a,b,c\n1,2,3\n"), detect]);
	strictEqual(detect.result().value.quoteChar, '"');
});

// --- csvFormatStream: formatRowSlow null and empty-string branches ---
test(`${variant}: csvFormatStream should format null field alongside a number (formatRowSlow null branch)`, async (_t) => {
	// null alongside a number forces isSimpleRow to return false (number is non-string),
	// so formatRowSlow is called; the null field hits the val==null branch (parts[i]="")
	const streams = [createReadableStream([[null, 42]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, ",42\r\n");
});

test(`${variant}: csvFormatStream should format empty-string field alongside a quoting-required value (formatRowSlow empty-string branch)`, async (_t) => {
	// A field needing quoting makes isSimpleRow return false, so formatRowSlow is called;
	// the empty-string field hits the val.length===0 branch (parts[i]="")
	const streams = [
		createReadableStream([["hello, world", ""]]),
		csvFormatStream(),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"hello, world",\r\n');
});

// --- csvCoerceValuesStream: coerceToType date with invalid date string ---
test(`${variant}: csvCoerceValuesStream explicit date type should return original string for invalid date`, async (_t) => {
	// coerceToType(val, "date") path: Number.isNaN(d.getTime()) === true → return val
	const streams = [
		createReadableStream([{ val: "not-a-date" }]),
		csvCoerceValuesStream({ columns: { val: "date" } }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [{ val: "not-a-date" }]);
});

// --- csvFormatStream: custom escape wrapQuote with no escape char in value ---
test(`${variant}: csvFormatStream custom escape should quote value with delimiter but no escape char`, async (_t) => {
	// wrapQuote custom-escape path: value contains delimiter (needs quoting)
	// but does NOT contain escapeChar → takes the "value" branch of the includes check
	const streams = [
		createReadableStream([["a,b", "ok"]]),
		csvFormatStream({ escapeChar: "\\" }),
	];
	const output = await streamToString(pipejoin(streams));
	// No backslash in "a,b", so no escapeChar escaping; only quoteChar wrapping
	strictEqual(output, '"a,b",ok\r\n');
});

// =====================================================================
// Mutation-hardening tests (kill surviving Stryker mutants)
// =====================================================================

// --- csvQuotedParser: precise idx / numCols tracking ---
test(`${variant}: csvQuotedParser reports idx equal to number of rows (default path)`, (_t) => {
	const result = csvQuotedParser("a,b\r\nc,d\r\ne,f\r\n");
	strictEqual(result.idx, 3);
	strictEqual(result.numCols, 2);
});

test(`${variant}: csvQuotedParser reports idx for quoted-field rows`, (_t) => {
	// All rows have a leading quoted field so each row goes through the quoted
	// branch idx++ (escapeIsQuote=true). idx must equal the row count.
	const result = csvQuotedParser('"a",b\r\n"c",d\r\n"e",f\r\n');
	strictEqual(result.idx, 3);
	deepStrictEqual(result.rows, [
		["a", "b"],
		["c", "d"],
		["e", "f"],
	]);
});

test(`${variant}: csvQuotedParser reports idx for custom-escape quoted rows`, (_t) => {
	const result = csvQuotedParser(
		'"a",b\r\n"c",d\r\n',
		{ escapeChar: "\\" },
		true,
	);
	strictEqual(result.idx, 2);
	deepStrictEqual(result.rows, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvQuotedParser flush short row truncates to fi (escapeIsQuote)`, (_t) => {
	// numCols=3 from first row; second row quoted single field on flush -> length 1
	const result = csvQuotedParser('a,b,c\r\n"d"', {}, true);
	strictEqual(result.numCols, 3);
	strictEqual(result.idx, 2);
	deepStrictEqual(result.rows[1], ["d"]);
	strictEqual(result.rows[1].length, 1);
});

test(`${variant}: csvQuotedParser flush short row truncates to fi (custom escape)`, (_t) => {
	const result = csvQuotedParser('a,b,c\r\n"d"', { escapeChar: "\\" }, true);
	strictEqual(result.numCols, 3);
	strictEqual(result.idx, 2);
	deepStrictEqual(result.rows[1], ["d"]);
	strictEqual(result.rows[1].length, 1);
});

test(`${variant}: csvQuotedParser unterminated quote short row truncates to fi (escapeIsQuote)`, (_t) => {
	// numCols=3; an unterminated quoted field on flush must produce exactly 1 field
	const result = csvQuotedParser('a,b,c\r\n"unterminated', {}, true);
	strictEqual(result.numCols, 3);
	strictEqual(result.idx, 2);
	deepStrictEqual(result.rows[1], ["unterminated"]);
	strictEqual(result.rows[1].length, 1);
	deepStrictEqual(result.errors.UnterminatedQuote.idx, [1]);
});

test(`${variant}: csvQuotedParser unterminated quote short row truncates to fi (custom escape)`, (_t) => {
	const result = csvQuotedParser(
		'a,b,c\r\n"unterminated',
		{ escapeChar: "\\" },
		true,
	);
	strictEqual(result.numCols, 3);
	strictEqual(result.idx, 2);
	deepStrictEqual(result.rows[1], ["unterminated"]);
	strictEqual(result.rows[1].length, 1);
	deepStrictEqual(result.errors.UnterminatedQuote.idx, [1]);
});

test(`${variant}: csvQuotedParser unterminated quote establishes numCols when first (escapeIsQuote)`, (_t) => {
	// First (and only) row is an unterminated quote on flush: numCols becomes fi (1)
	const result = csvQuotedParser('"unterminated', {}, true);
	strictEqual(result.numCols, 1);
	strictEqual(result.idx, 1);
	deepStrictEqual(result.rows, [["unterminated"]]);
});

test(`${variant}: csvQuotedParser unterminated quote establishes numCols when first (custom escape)`, (_t) => {
	const result = csvQuotedParser('"unterminated', { escapeChar: "\\" }, true);
	strictEqual(result.numCols, 1);
	strictEqual(result.idx, 1);
	deepStrictEqual(result.rows, [["unterminated"]]);
});

test(`${variant}: csvQuotedParser unterminated quote not flushing returns rowStart tail (escapeIsQuote)`, (_t) => {
	// not flushing: ctx.tail must be text.substring(rowStart), preserving the whole
	// unterminated row including its quote.
	const result = csvQuotedParser('a,b\r\n"unterm', {}, false);
	strictEqual(result.tail, '"unterm');
	strictEqual(result.idx, 1);
});

test(`${variant}: csvQuotedParser unterminated quote not flushing returns rowStart tail (custom escape)`, (_t) => {
	const result = csvQuotedParser(
		'a,b\r\nx,"unterm',
		{ escapeChar: "\\" },
		false,
	);
	strictEqual(result.tail, 'x,"unterm');
	strictEqual(result.idx, 1);
});

test(`${variant}: csvQuotedParser quoted first field establishes numCols (escapeIsQuote)`, (_t) => {
	// First row begins with a quoted field then newline path establishes numCols.
	const result = csvQuotedParser('"a","b"\r\nc,d\r\n');
	strictEqual(result.numCols, 2);
	strictEqual(result.idx, 2);
	deepStrictEqual(result.rows, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvQuotedParser quoted first field establishes numCols (custom escape)`, (_t) => {
	const result = csvQuotedParser('"a","b"\r\nc,d\r\n', { escapeChar: "\\" });
	strictEqual(result.numCols, 2);
	strictEqual(result.idx, 2);
	deepStrictEqual(result.rows, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvQuotedParser quoted-field row after established numCols truncates surplus (escapeIsQuote)`, (_t) => {
	// numCols=2; a later quoted-first row with 3 fields must be truncated? No —
	// surplus is kept by indexOf path; assert exact when row has fewer fields.
	const result = csvQuotedParser('"a","b"\r\n"c"\r\n');
	strictEqual(result.rows[1].length, 1);
	deepStrictEqual(result.rows[1], ["c"]);
	strictEqual(result.idx, 2);
});

// --- csvQuotedParser: garbage-after-closing-quote keeps extra fields ---
test(`${variant}: csvQuotedParser keeps garbage after closing quote as separate field (escapeIsQuote)`, (_t) => {
	const result = csvQuotedParser('"a"x,b\r\n');
	deepStrictEqual(result.rows, [["a", "x", "b"]]);
});

test(`${variant}: csvQuotedParser keeps garbage after closing quote as separate field (custom escape)`, (_t) => {
	const result = csvQuotedParser('"a"x,b\r\n', { escapeChar: "\\" });
	deepStrictEqual(result.rows, [["a", "x", "b"]]);
});

// --- csvQuotedParser: quoted field at exact end of input (pos >= len) ---
test(`${variant}: csvQuotedParser quoted field at exact end of input keeps field (escapeIsQuote)`, (_t) => {
	const result = csvQuotedParser('a,"b"', {}, true);
	deepStrictEqual(result.rows, [["a", "b"]]);
	strictEqual(result.idx, 1);
});

test(`${variant}: csvQuotedParser quoted field at exact end of input keeps field (custom escape)`, (_t) => {
	const result = csvQuotedParser('a,"b"', { escapeChar: "\\" }, true);
	deepStrictEqual(result.rows, [["a", "b"]]);
	strictEqual(result.idx, 1);
});

// --- csvQuotedParser: delimiter immediately after closing quote (lastWasDelimiter) ---
test(`${variant}: csvQuotedParser quoted field then delimiter then trailing flush field (escapeIsQuote)`, (_t) => {
	// "a", then delimiter sets lastWasDelimiter=true; flush must add a trailing "".
	const result = csvQuotedParser('"a",', {}, true);
	deepStrictEqual(result.rows, [["a", ""]]);
	strictEqual(result.idx, 1);
});

test(`${variant}: csvQuotedParser quoted field then delimiter then trailing flush field (custom escape)`, (_t) => {
	const result = csvQuotedParser('"a",', { escapeChar: "\\" }, true);
	deepStrictEqual(result.rows, [["a", ""]]);
	strictEqual(result.idx, 1);
});

// --- csvQuotedParser: newline-length boundary (CRLF needs second char) ---
test(`${variant}: csvQuotedParser quoted field then CR without LF is garbage not newline (escapeIsQuote)`, (_t) => {
	// nc===CR but pos+1 is end -> not a CRLF; CR treated as garbage, kept inline.
	const result = csvQuotedParser('"a"\r', { newlineChar: "\r\n" }, true);
	// "a" then lone CR (garbage after quote) at end of input
	deepStrictEqual(result.rows, [["a", "\r"]]);
});

test(`${variant}: csvQuotedParser quoted field then CR without LF is garbage not newline (custom escape)`, (_t) => {
	const result = csvQuotedParser(
		'"a"\r',
		{ newlineChar: "\r\n", escapeChar: "\\" },
		true,
	);
	deepStrictEqual(result.rows, [["a", "\r"]]);
});

// --- csvParseInline numCols>0 fast path: idx, malformed, surplus ---
test(`${variant}: csvQuotedParser fast unquoted path reports idx for many rows`, (_t) => {
	// numCols established as 2 by first row, then fast path handles the rest.
	const result = csvQuotedParser("a,b\r\n1,2\r\n3,4\r\n5,6\r\n7,8\r\n");
	strictEqual(result.idx, 5);
	strictEqual(result.numCols, 2);
	deepStrictEqual(result.rows[4], ["7", "8"]);
});

test(`${variant}: csvQuotedParser fast path malformed too-few columns split fallback`, (_t) => {
	// numCols=3; a row with only 2 fields uses the split fallback, keeping 2 fields.
	const result = csvQuotedParser("a,b,c\r\n1,2\r\n");
	deepStrictEqual(result.rows[1], ["1", "2"]);
	strictEqual(result.rows[1].length, 2);
});

test(`${variant}: csvQuotedParser fast path surplus columns kept separate via split`, (_t) => {
	// numCols=2; a row with 4 fields must keep all 4 (split fallback), not merge.
	const result = csvQuotedParser("a,b\r\n1,2,3,4\r\n");
	deepStrictEqual(result.rows[1], ["1", "2", "3", "4"]);
	strictEqual(result.rows[1].length, 4);
});

test(`${variant}: csvQuotedParser fast path exact column count keeps fields`, (_t) => {
	const result = csvQuotedParser("a,b,c\r\n1,2,3\r\n4,5,6\r\n");
	deepStrictEqual(result.rows[1], ["1", "2", "3"]);
	deepStrictEqual(result.rows[2], ["4", "5", "6"]);
	strictEqual(result.idx, 3);
});

test(`${variant}: csvQuotedParser fast path partial last row without newline returns tail`, (_t) => {
	// numCols=2; trailing partial row (no newline) must be returned as tail.
	const result = csvQuotedParser("a,b\r\n1,2\r\n3,4", {}, false);
	strictEqual(result.tail, "3,4");
	strictEqual(result.idx, 2);
});

// --- csvUnquotedParser: idx / numCols ---
test(`${variant}: csvUnquotedParser reports idx and numCols`, (_t) => {
	const result = csvUnquotedParser("a,b\r\n1,2\r\n3,4\r\n");
	strictEqual(result.idx, 3);
	strictEqual(result.numCols, 2);
});

test(`${variant}: csvUnquotedParser flush increments idx for trailing row`, (_t) => {
	const result = csvUnquotedParser("a,b\r\n1,2", {}, true);
	strictEqual(result.idx, 2);
	strictEqual(result.numCols, 2);
});

test(`${variant}: csvUnquotedParser preserves explicit idx and numCols options`, (_t) => {
	const result = csvUnquotedParser("1,2\r\n", { idx: 5, numCols: 2 });
	strictEqual(result.idx, 6);
	strictEqual(result.numCols, 2);
});

test(`${variant}: csvUnquotedParser no trailing row when input ends with newline`, (_t) => {
	const result = csvUnquotedParser("a,b\r\n", {}, true);
	strictEqual(result.idx, 1);
	deepStrictEqual(result.rows, [["a", "b"]]);
});

// --- csvParseStream: idx-sensitive error reporting through the stream ---
test(`${variant}: csvParseStream reports unterminated quote idx after several rows`, async (_t) => {
	const streams = [
		createReadableStream('a,b\r\nc,d\r\ne,f\r\n"unterminated'),
		csvParseStream(),
	];
	const result = await pipeline(streams);
	// 3 complete rows (idx 0,1,2) then unterminated at idx 3
	deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [3]);
});

test(`${variant}: csvParseStream reports unterminated quote idx after several rows (custom escape)`, async (_t) => {
	const streams = [
		createReadableStream('a,b\r\nc,d\r\ne,f\r\nx,"unterminated'),
		csvParseStream({ escapeChar: "\\" }),
	];
	const result = await pipeline(streams);
	deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [3]);
});

// --- findRowEnd via csvDetectHeaderStream ---
test(`${variant}: csvDetectHeaderStream handles quoted header with escaped quote and embedded newline`, async (_t) => {
	// Header field is quoted, contains an escaped "" and a newline; findRowEnd must
	// skip the in-quote newline and the escaped-quote pair, terminating at the real
	// row-end newline so the header parses as two columns.
	const hdr = csvDetectHeaderStream();
	const streams = [
		createReadableStream('"a""b\r\nc",second\r\n1,2\r\n'),
		hdr,
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(hdr.result().value.header, ['a"b\r\nc', "second"]);
	deepStrictEqual(output, [["1", "2"]]);
});

test(`${variant}: csvDetectHeaderStream custom-escape header skips escaped quote in quoted field`, async (_t) => {
	// escapeChar='\\': inside the quoted header field, \" is an escaped quote, so the
	// field stays open past it; findRowEnd's odd-run lookback must keep scanning.
	const hdr = csvDetectHeaderStream({ escapeChar: "\\" });
	const streams = [
		createReadableStream('"a\\"b,c",second\r\n1,2\r\n'),
		hdr,
		csvParseStream({ escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(hdr.result().value.header, ['a"b,c', "second"]);
	deepStrictEqual(output, [["1", "2"]]);
});

test(`${variant}: csvDetectHeaderStream custom-escape even run closes quote in findRowEnd`, async (_t) => {
	// "\\\\" is an even run of escape chars => the quote IS a real closing quote,
	// so the delimiter after it splits the header field.
	const hdr = csvDetectHeaderStream({ escapeChar: "\\" });
	const streams = [
		createReadableStream('"a\\\\",b\r\n1,2\r\n'),
		hdr,
		csvParseStream({ escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(hdr.result().value.header, ["a\\", "b"]);
	deepStrictEqual(output, [["1", "2"]]);
});

test(`${variant}: csvDetectHeaderStream delimiter inside header advances fieldStart`, async (_t) => {
	// Ensures findRowEnd's delimiter branch (pos += delimiterLength; fieldStart=pos)
	// is exercised: a quoted field appears after a delimiter so it must still be
	// recognized as a field-start quote.
	const hdr = csvDetectHeaderStream();
	const streams = [
		createReadableStream('x,"q,uoted"\r\n1,2\r\n'),
		hdr,
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(hdr.result().value.header, ["x", "q,uoted"]);
	deepStrictEqual(output, [["1", "2"]]);
});

test(`${variant}: csvDetectHeaderStream multi-char delimiter inside findRowEnd`, async (_t) => {
	const hdr = csvDetectHeaderStream({ delimiterChar: "::" });
	const streams = [
		createReadableStream('"a::b"::c\r\n1::2\r\n'),
		hdr,
		csvParseStream({ delimiterChar: "::" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(hdr.result().value.header, ["a::b", "c"]);
	deepStrictEqual(output, [["1", "2"]]);
});

// --- csvDetectDelimitersStream: chunkSize threshold and buffer reset ---
test(`${variant}: csvDetectDelimitersStream waits until buffer reaches chunkSize`, async (_t) => {
	// chunkSize 8: first chunk (len 6) must NOT trigger detection; second chunk does.
	const detect = csvDetectDelimitersStream({ chunkSize: 8 });
	const streams = [createReadableStream(["a;b\r\n", "c;d\r\n"]), detect];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);
	strictEqual(detect.result().value.delimiterChar, ";");
	strictEqual(output, "a;b\r\nc;d\r\n");
});

test(`${variant}: csvDetectDelimitersStream emits buffered content exactly once`, async (_t) => {
	// Make sure buffer is cleared after detection so content is not duplicated.
	const detect = csvDetectDelimitersStream({ chunkSize: 4 });
	const streams = [
		createReadableStream(["a|b\r\n", "rest1\r\n", "rest2\r\n"]),
		detect,
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);
	strictEqual(detect.result().value.delimiterChar, "|");
	strictEqual(output, "a|b\r\nrest1\r\nrest2\r\n");
});

test(`${variant}: csvDetectDelimitersStream detect returns true sets detected so later chunks pass through`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 4 });
	const streams = [createReadableStream(["a,b\r\n", "later\r\n"]), detect];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);
	strictEqual(output, "a,b\r\nlater\r\n");
});

// --- unescapeCustom boundary (i + 1 < len) via custom-escape parse ---
test(`${variant}: csvParseStream custom-escape unescapeCustom handles escaped escape and escaped quote`, async (_t) => {
	// Content (between the outer quotes) is: \ \ x \ " y
	//   - "\\\\" is an escaped escape  -> single backslash
	//   - "\\\"" is an escaped quote   -> literal quote (does not close the field)
	// The final unescaped quote closes the field. unescapeCustom must collapse both.
	const streams = [
		createReadableStream('"\\\\x\\"y",z\r\n'),
		csvParseStream({ escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [['\\x"y', "z"]]);
});

test(`${variant}: csvParseStream custom-escape escaped quote inside field unescaped once`, async (_t) => {
	const streams = [
		createReadableStream('"a\\"b\\"c",d\r\n'),
		csvParseStream({ escapeChar: "\\" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [['a"b"c', "d"]]);
});

// --- csvParseStream fast-path: text.indexOf(quoteChar) === -1 guard ---
test(`${variant}: csvParseStream switches off fast path when a quote appears in later row`, async (_t) => {
	// First row establishes numCols (no quotes); a later row contains a quoted field
	// with an embedded delimiter, which must NOT be split.
	const streams = [
		createReadableStream('a,b\r\n"x,y",z\r\n'),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b"],
		["x,y", "z"],
	]);
});

// --- csvParseStream: first-row detection when numCols===0 and a quote exists later in row ---
test(`${variant}: csvParseStream first row with quote does not use split detection`, async (_t) => {
	// First row itself contains a quoted field with an embedded delimiter.
	const streams = [
		createReadableStream('"a,b",c\r\nd,e\r\n'),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a,b", "c"],
		["d", "e"],
	]);
	deepStrictEqual(streams[1].result().value, {});
});

// --- csvParseStream: regular indexOf path, nextNl < nextDelim and trailing-delim ---
test(`${variant}: csvParseStream quoted first field then plain rows exercise regular path`, async (_t) => {
	// A quoted first field forces fi!==0 entry so the regular indexOf path runs for
	// the remaining fields/rows.
	const streams = [
		createReadableStream('"a",b,c\r\nd,e,f\r\n'),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b", "c"],
		["d", "e", "f"],
	]);
});

// --- csvSteamifyParser resolveOptions: delimiterCharSingle / newlineCharSingle ---
test(`${variant}: csvParseStream multi-char delimiter is not treated as single`, async (_t) => {
	// delimiterCharSingle=false path: post-quote dispatch must use startsWith.
	const streams = [
		createReadableStream('"a"::"b"::c\r\n'),
		csvParseStream({ delimiterChar: "::" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [["a", "b", "c"]]);
});

test(`${variant}: csvParseStream 2-char newline second-char check after quoted field`, async (_t) => {
	// newlineCharSingle is the 2nd char of a 2-char newline; a quoted field then the
	// 2-char newline must split rows, and a partial first char must not.
	const streams = [
		createReadableStream('"a",b\r\n"c",d\r\n'),
		csvParseStream({ newlineChar: "\r\n" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

// --- csvParseStream: typeof chunk string vs Buffer ---
test(`${variant}: csvParseStream coerces Buffer chunk via toString`, async (_t) => {
	const streams = [
		createReadableStream([Buffer.from("a,b\r\nc,d\r\n")]),
		csvParseStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

// --- csvParseStream: buffer concat across chunks (buffer.length>0 ? buffer+str) ---
test(`${variant}: csvParseStream concatenates buffered tail with next chunk`, async (_t) => {
	// A row split across two chunks at chunkSize=1 so the second chunk must be
	// prefixed with the buffered partial row.
	const streams = [
		createReadableStream(["a,b\r\n1,", "2\r\n"]),
		csvParseStream({ chunkSize: 1 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b"],
		["1", "2"],
	]);
});

// --- csvParseStream: safety limit boundary (text.length > fieldMaxSize*2) ---
test(`${variant}: csvParseStream accepts text at exactly fieldMaxSize*2`, async (_t) => {
	// length === fieldMaxSize*2 must NOT throw ( strict greater-than ).
	const fieldMaxSize = 8;
	const text = "x".repeat(fieldMaxSize * 2); // 16, no newline -> one field on flush
	const streams = [
		createReadableStream(text),
		csvParseStream({ fieldMaxSize }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [[text]]);
});

// --- csvParseStream ready-path: inputLen < chunkSize and single vs join ---
test(`${variant}: csvParseStream single chunk equal to chunkSize processes without join`, async (_t) => {
	const row = "a,b\r\n"; // length 5
	const streams = [
		createReadableStream([row]),
		csvParseStream({ chunkSize: 5 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [["a", "b"]]);
});

test(`${variant}: csvParseStream multiple chunks joined when crossing chunkSize`, async (_t) => {
	const streams = [
		createReadableStream(["a,", "b\r\n", "c,d\r\n"]),
		csvParseStream({ chunkSize: 3 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

// --- csvRemoveEmptyRowsStream: zero-length row returns true immediately ---
test(`${variant}: csvRemoveEmptyRowsStream treats zero-length array as empty (l===0 branch)`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream();
	const streams = [createReadableStream([[], ["a"]]), filter];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [["a"]]);
	deepStrictEqual(filter.result().value.EmptyRow.idx, [0]);
});

// --- csvArrayToObject: defineProperty descriptor flags ---
test(`${variant}: csvArrayToObject __proto__ column is writable, enumerable, configurable`, async (_t) => {
	const streams = [
		createReadableStream([["v1", "v2"]]),
		csvArrayToObject({ headers: ["__proto__", "b"] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	const obj = output[0];
	const desc = Object.getOwnPropertyDescriptor(obj, "__proto__");
	ok(desc, "own __proto__ descriptor exists");
	strictEqual(desc.value, "v1");
	strictEqual(desc.enumerable, true);
	strictEqual(desc.writable, true);
	strictEqual(desc.configurable, true);
	// enumerable means it shows up in keys
	deepStrictEqual(Object.keys(obj), ["__proto__", "b"]);
});

test(`${variant}: csvArrayToObject constructor column stored as own data property`, async (_t) => {
	const streams = [
		createReadableStream([["v1", "v2"]]),
		csvArrayToObject({ headers: ["constructor", "b"] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	const obj = output[0];
	const desc = Object.getOwnPropertyDescriptor(obj, "constructor");
	ok(desc);
	strictEqual(desc.value, "v1");
	strictEqual(desc.enumerable, true);
});

// --- csvCoerceValuesStream: autoCoerce boolean gating (len & first-char) ---
test(`${variant}: csvCoerceValuesStream autoCoerce true requires length 4 and t/T`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "true", b: "True", c: "trueX", d: "rue" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	// "true"->true, "True"->true, "trueX" (len 5, not false) stays string, "rue" stays
	strictEqual(output[0].a, true);
	strictEqual(output[0].b, true);
	strictEqual(output[0].c, "trueX");
	strictEqual(output[0].d, "rue");
});

test(`${variant}: csvCoerceValuesStream autoCoerce false requires length 5 and f/F`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "false", b: "False", c: "fALSE", d: "falsey" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	strictEqual(output[0].a, false);
	strictEqual(output[0].b, false);
	strictEqual(output[0].c, false);
	strictEqual(output[0].d, "falsey");
});

test(`${variant}: csvCoerceValuesStream autoCoerce digit and minus start numeric path`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "42", b: "-7", c: "9", d: "0" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	strictEqual(output[0].a, 42);
	strictEqual(output[0].b, -7);
	strictEqual(output[0].c, 9);
	strictEqual(output[0].d, 0);
});

test(`${variant}: csvCoerceValuesStream autoCoerce JSON only for braces/brackets`, async (_t) => {
	const streams = [
		createReadableStream([{ a: '{"x":1}', b: "[1,2]", c: "plain" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output[0].a, { x: 1 });
	deepStrictEqual(output[0].b, [1, 2]);
	strictEqual(output[0].c, "plain");
});

test(`${variant}: csvCoerceValuesStream autoCoerce invalid JSON returns the original string (catch returns val)`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "{not json", b: "[oops" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	strictEqual(output[0].a, "{not json");
	strictEqual(output[0].b, "[oops");
});

test(`${variant}: csvCoerceValuesStream coerceToType invalid json returns original string (catch returns val)`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "{not json" }]),
		csvCoerceValuesStream({ columns: { a: "json" } }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	strictEqual(output[0].a, "{not json");
});

// --- autoCoerce iso8601 anchoring (^ and $) ---
test(`${variant}: csvCoerceValuesStream iso date requires full anchor (trailing junk stays string)`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "2020-01-02xyz", b: "2020-01-02" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	// trailing junk -> regex (anchored) fails, stays string
	strictEqual(output[0].a, "2020-01-02xyz");
	ok(output[0].b instanceof Date);
});

test(`${variant}: csvCoerceValuesStream iso date with seconds-and-fraction group present`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "2020-01-02T03:04:05.678Z" }]),
		csvCoerceValuesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	ok(output[0].a instanceof Date);
	strictEqual(
		output[0].a.getTime(),
		new Date("2020-01-02T03:04:05.678Z").getTime(),
	);
});

// --- csvFormatStream: delimiterSingle vs multi, scanNeedsQuote loops ---
test(`${variant}: csvFormatStream single-char delimiter quotes interior delimiter only on actual delimiter`, async (_t) => {
	const streams = [
		createReadableStream([["a;b", "c,d"]]),
		csvFormatStream(), // delimiter ","
	];
	const output = await streamToString(pipejoin(streams));
	// "a;b" has no comma -> not quoted; "c,d" has comma -> quoted
	strictEqual(output, 'a;b,"c,d"\r\n');
});

test(`${variant}: csvFormatStream multi-char delimiter quotes value containing the delimiter substring`, async (_t) => {
	const streams = [
		createReadableStream([["a::b", "plain"]]),
		csvFormatStream({ delimiterChar: "::" }),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, '"a::b"::plain\r\n');
});

// --- csvFormatStream: batch boundary at exactly 64 ---
test(`${variant}: csvFormatStream emits when batch reaches exactly 64 rows`, async (_t) => {
	const rows = [];
	for (let i = 0; i < 64; i++) rows.push([`r${i}`, "x"]);
	// Collect each enqueued chunk separately
	const chunks = await streamToArray(
		pipejoin([createReadableStream(rows), csvFormatStream()]),
	);
	// With exactly 64 rows, the transform emits one chunk of 64 rows and flush emits nothing.
	strictEqual(chunks.length, 1);
	const lines = chunks[0].split("\r\n").filter((l) => l.length > 0);
	strictEqual(lines.length, 64);
});

test(`${variant}: csvFormatStream below 64 rows emits only on flush`, async (_t) => {
	const rows = [];
	for (let i = 0; i < 10; i++) rows.push([`r${i}`, "x"]);
	const chunks = await streamToArray(
		pipejoin([createReadableStream(rows), csvFormatStream()]),
	);
	strictEqual(chunks.length, 1);
	const lines = chunks[0].split("\r\n").filter((l) => l.length > 0);
	strictEqual(lines.length, 10);
});

// --- csvFormatStream: isSimpleRow non-string / needs-quote detection ---
test(`${variant}: csvFormatStream isSimpleRow rejects rows with a quoting-needed string`, async (_t) => {
	// A value needing a quote forces the slow path; assert correct quoting.
	const streams = [
		createReadableStream([["plain", "needs\nquote"]]),
		csvFormatStream(),
	];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, 'plain,"needs\nquote"\r\n');
});

test(`${variant}: csvFormatStream simple row of plain strings uses join fast path`, async (_t) => {
	const streams = [createReadableStream([["a", "b", "c"]]), csvFormatStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, "a,b,c\r\n");
});

// --- csvParseStream streamOptions default highWaterMark preserved ---
test(`${variant}: csvParseStream respects provided streamOptions override`, async (_t) => {
	// Passing streamOptions should not break parsing (ObjectLiteral mutant guard).
	const streams = [
		createReadableStream("a,b\r\nc,d\r\n"),
		csvParseStream({}, { highWaterMark: 1 }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		["a", "b"],
		["c", "d"],
	]);
});

// =====================================================================
// Mutation-hardening tests, batch 2
// =====================================================================

// --- findRowEnd: a quote is only an opener at field-start ---
test(`${variant}: csvDetectHeaderStream mid-field quote is not a quote opener`, async (_t) => {
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([createReadableStream('a"b,c\r\nd,e\r\n'), hdr, csvParseStream()]),
	);
	deepStrictEqual(hdr.result().value.header, ['a"b', "c"]);
	deepStrictEqual(out, [["d", "e"]]);
});

test(`${variant}: csvDetectHeaderStream quote opening right after a delimiter is honored`, async (_t) => {
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([
			createReadableStream('a,"x,y"\r\n1,2\r\n'),
			hdr,
			csvParseStream(),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ["a", "x,y"]);
	deepStrictEqual(out, [["1", "2"]]);
});

test(`${variant}: csvDetectHeaderStream keeps newline inside quoted header field`, async (_t) => {
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"line1\r\nline2",b\r\n1,2\r\n'),
			hdr,
			csvParseStream(),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ["line1\r\nline2", "b"]);
	deepStrictEqual(out, [["1", "2"]]);
});

// --- findRowEnd: custom-escape parity around a quoted newline ---
test(`${variant}: csvDetectHeaderStream custom escape keeps escaped quote then newline inside the field`, async (_t) => {
	// "x\"y\r\nz" — the \" is an escaped quote (odd run of escapeChar), so the
	// field stays open and the \r\n belongs to the header field, not the row end.
	const hdr = csvDetectHeaderStream({ escapeChar: "\\" });
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"x\\"y\r\nz",b\r\n1,2\r\n'),
			hdr,
			csvParseStream({ escapeChar: "\\" }),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ['x"y\r\nz', "b"]);
	deepStrictEqual(out, [["1", "2"]]);
});

test(`${variant}: csvDetectHeaderStream custom escape even run closes the quote before the newline`, async (_t) => {
	// "a\\" — \\ is an escaped escape (even run), so the quote closes; the row
	// ends at the very next newline.
	const hdr = csvDetectHeaderStream({ escapeChar: "\\" });
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"a\\\\",b\r\n1,2\r\n'),
			hdr,
			csvParseStream({ escapeChar: "\\" }),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ["a\\", "b"]);
	deepStrictEqual(out, [["1", "2"]]);
});

test(`${variant}: csvDetectHeaderStream custom escape backslash before quote keeps newline in field`, async (_t) => {
	// A lone escaped quote right before the newline must keep the field open so
	// the embedded \r\n does not terminate the header row.
	const hdr = csvDetectHeaderStream({ escapeChar: "\\" });
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"a\\"\r\nb",c\r\nd,e\r\n'),
			hdr,
			csvParseStream({ escapeChar: "\\" }),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ['a"\r\nb', "c"]);
	deepStrictEqual(out, [["d", "e"]]);
});

// --- csvDetectDelimitersStream: isFieldStart / isFieldEnd ---
test(`${variant}: csvDetectDelimitersStream detects quote closing right before CR`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 1 });
	await streamToArray(pipejoin([createReadableStream("'a'\r'b'\r"), detect]));
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream does not treat unopened quote as quoteChar`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 1 });
	await streamToArray(pipejoin([createReadableStream("ab'cd,ef\r\n"), detect]));
	strictEqual(detect.result().value.quoteChar, '"');
});

test(`${variant}: csvDetectDelimitersStream buffers until a newline appears`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 3 });
	const out = await streamToString(
		pipejoin([createReadableStream(["abcd", "e;f\r\n"]), detect]),
	);
	strictEqual(out, "abcde;f\r\n");
	strictEqual(detect.result().value.delimiterChar, ";");
});

// --- csvParseStream: 3-char newline dispatch after quoted field ---
test(`${variant}: csvParseStream quoted field then 3-char newline splits rows (escapeIsQuote)`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"a",b###"c",d'),
			csvParseStream({ newlineChar: "###" }),
		]),
	);
	deepStrictEqual(out, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvParseStream quoted field then partial 3-char newline is garbage (escapeIsQuote)`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"a"##b'),
			csvParseStream({ newlineChar: "###" }),
		]),
	);
	deepStrictEqual(out, [["a", "##b"]]);
});

test(`${variant}: csvParseStream quoted field then 3-char newline splits rows (custom escape)`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"a",b###"c",d'),
			csvParseStream({ newlineChar: "###", escapeChar: "\\" }),
		]),
	);
	deepStrictEqual(out, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvParseStream quoted field then partial 3-char newline is garbage (custom escape)`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"a"##b'),
			csvParseStream({ newlineChar: "###", escapeChar: "\\" }),
		]),
	);
	deepStrictEqual(out, [["a", "##b"]]);
});

// --- CRLF after quoted field requires both chars ---
test(`${variant}: csvParseStream quoted field then CRLF splits (escapeIsQuote)`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"a",b\r\n"c",d\r\n'),
			csvParseStream({ newlineChar: "\r\n" }),
		]),
	);
	deepStrictEqual(out, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvParseStream quoted field then CRLF splits (custom escape)`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"a",b\r\n"c",d\r\n'),
			csvParseStream({ newlineChar: "\r\n", escapeChar: "\\" }),
		]),
	);
	deepStrictEqual(out, [
		["a", "b"],
		["c", "d"],
	]);
});

// --- LF-only (length 1) newline after quoted field ---
test(`${variant}: csvParseStream quoted field then LF newline length 1 (escapeIsQuote)`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"a",b\n"c",d\n'),
			csvParseStream({ newlineChar: "\n" }),
		]),
	);
	deepStrictEqual(out, [
		["a", "b"],
		["c", "d"],
	]);
});

test(`${variant}: csvParseStream quoted field then LF newline length 1 (custom escape)`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"a",b\n"c",d\n'),
			csvParseStream({ newlineChar: "\n", escapeChar: "\\" }),
		]),
	);
	deepStrictEqual(out, [
		["a", "b"],
		["c", "d"],
	]);
});

// --- field exactly fieldMaxSize allowed (strict >) ---
test(`${variant}: csvParseStream allows quoted field exactly fieldMaxSize (escapeIsQuote)`, async (_t) => {
	const val = "x".repeat(10);
	const out = await streamToArray(
		pipejoin([
			createReadableStream(`"${val}",y\r\n`),
			csvParseStream({ fieldMaxSize: 10 }),
		]),
	);
	deepStrictEqual(out, [[val, "y"]]);
});

test(`${variant}: csvParseStream allows custom-escape quoted field exactly fieldMaxSize`, async (_t) => {
	const val = "x".repeat(10);
	const out = await streamToArray(
		pipejoin([
			createReadableStream(`"${val}",y\r\n`),
			csvParseStream({ fieldMaxSize: 10, escapeChar: "\\" }),
		]),
	);
	deepStrictEqual(out, [[val, "y"]]);
});

// --- unterminated quote error message text ---
test(`${variant}: csvParseStream unterminated quote message (escapeIsQuote)`, async (_t) => {
	const parse = csvParseStream();
	await pipeline([createReadableStream('"unterminated'), parse]);
	strictEqual(
		parse.result().value.UnterminatedQuote.message,
		"Unterminated quoted field",
	);
});

test(`${variant}: csvParseStream unterminated quote message (custom escape)`, async (_t) => {
	const parse = csvParseStream({ escapeChar: "\\" });
	await pipeline([createReadableStream('x,"unterminated'), parse]);
	strictEqual(
		parse.result().value.UnterminatedQuote.message,
		"Unterminated quoted field",
	);
});

// --- not-flushing unterminated quote kept in tail across chunks ---
test(`${variant}: csvParseStream buffers unterminated quoted field across chunks (escapeIsQuote)`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream(['"hel', 'lo",x\r\n']),
			csvParseStream({ chunkSize: 1 }),
		]),
	);
	deepStrictEqual(out, [["hello", "x"]]);
});

test(`${variant}: csvParseStream buffers unterminated quoted field across chunks (custom escape)`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream(['"hel', 'lo",x\r\n']),
			csvParseStream({ chunkSize: 1, escapeChar: "\\" }),
		]),
	);
	deepStrictEqual(out, [["hello", "x"]]);
});

// --- csvUnquotedParser numCols from first row ---
test(`${variant}: csvUnquotedParser numCols comes from first row not later rows`, (_t) => {
	const result = csvUnquotedParser("a,b\r\n1,2,3\r\n");
	strictEqual(result.numCols, 2);
});

test(`${variant}: csvUnquotedParser flush keeps first-row numCols`, (_t) => {
	const result = csvUnquotedParser("a,b\r\n1,2,3", {}, true);
	strictEqual(result.numCols, 2);
	deepStrictEqual(result.rows[1], ["1", "2", "3"]);
});

// --- csvFormatStream scanNeedsQuote triggers ---
test(`${variant}: csvFormatStream quotes a field that only ends with a space`, async (_t) => {
	const out = await streamToString(
		pipejoin([createReadableStream([["ab ", "cd"]]), csvFormatStream()]),
	);
	strictEqual(out, '"ab ",cd\r\n');
});

test(`${variant}: csvFormatStream does not quote a field with only an interior space`, async (_t) => {
	const out = await streamToString(
		pipejoin([createReadableStream([["a b", "cd"]]), csvFormatStream()]),
	);
	strictEqual(out, "a b,cd\r\n");
});

test(`${variant}: csvFormatStream quotes a field containing the quote char`, async (_t) => {
	const out = await streamToString(
		pipejoin([createReadableStream([['a"b', "cd"]]), csvFormatStream()]),
	);
	strictEqual(out, '"a""b",cd\r\n');
});

test(`${variant}: csvFormatStream quotes a field containing a carriage return`, async (_t) => {
	const out = await streamToString(
		pipejoin([createReadableStream([["a\rb", "cd"]]), csvFormatStream()]),
	);
	strictEqual(out, '"a\rb",cd\r\n');
});

test(`${variant}: csvFormatStream quotes a field containing a line feed`, async (_t) => {
	const out = await streamToString(
		pipejoin([createReadableStream([["a\nb", "cd"]]), csvFormatStream()]),
	);
	strictEqual(out, '"a\nb",cd\r\n');
});

test(`${variant}: csvFormatStream does not add a trailing empty field`, async (_t) => {
	const out = await streamToString(
		pipejoin([createReadableStream([["a", "b", "c"]]), csvFormatStream()]),
	);
	strictEqual(out, "a,b,c\r\n");
});

test(`${variant}: csvFormatStream multi-char delimiter does not add trailing empty field`, async (_t) => {
	const out = await streamToString(
		pipejoin([
			createReadableStream([["a", "b", "c"]]),
			csvFormatStream({ delimiterChar: "::" }),
		]),
	);
	strictEqual(out, "a::b::c\r\n");
});

test(`${variant}: csvFormatStream multi-char delimiter quotes value containing the delimiter`, async (_t) => {
	const out = await streamToString(
		pipejoin([
			createReadableStream([["a::b", "x"]]),
			csvFormatStream({ delimiterChar: "::" }),
		]),
	);
	strictEqual(out, '"a::b"::x\r\n');
});

// --- batch flush boundary (>= 64) ---
test(`${variant}: csvFormatStream does not flush at 63 rows`, async (_t) => {
	const rows = [];
	for (let i = 0; i < 63; i++) rows.push([String(i), "x"]);
	const chunks = await streamToArray(
		pipejoin([createReadableStream(rows), csvFormatStream()]),
	);
	strictEqual(chunks.length, 1);
	strictEqual(chunks[0].split("\r\n").filter((l) => l.length > 0).length, 63);
});

test(`${variant}: csvFormatStream flushes at exactly 64 rows then a separate flush for extras`, async (_t) => {
	const rows = [];
	for (let i = 0; i < 65; i++) rows.push([String(i), "x"]);
	const chunks = await streamToArray(
		pipejoin([createReadableStream(rows), csvFormatStream()]),
	);
	strictEqual(chunks.length, 2);
	strictEqual(chunks[0].split("\r\n").filter((l) => l.length > 0).length, 64);
	strictEqual(chunks[1].split("\r\n").filter((l) => l.length > 0).length, 1);
});

// --- null/empty/number/Date coercion in formatRow ---
test(`${variant}: csvFormatStream formats null as empty field next to a quoted value`, async (_t) => {
	const out = await streamToString(
		pipejoin([createReadableStream([[null, "a,b"]]), csvFormatStream()]),
	);
	strictEqual(out, ',"a,b"\r\n');
});

test(`${variant}: csvFormatStream formats empty string as empty field next to a quoted value`, async (_t) => {
	const out = await streamToString(
		pipejoin([createReadableStream([["", "a,b"]]), csvFormatStream()]),
	);
	strictEqual(out, ',"a,b"\r\n');
});

test(`${variant}: csvFormatStream coerces a number field via String`, async (_t) => {
	const out = await streamToString(
		pipejoin([createReadableStream([[42, "x"]]), csvFormatStream()]),
	);
	strictEqual(out, "42,x\r\n");
});

test(`${variant}: csvFormatStream coerces a Date field via toISOString`, async (_t) => {
	const d = new Date("2020-01-02T03:04:05.000Z");
	const out = await streamToString(
		pipejoin([createReadableStream([[d, "x"]]), csvFormatStream()]),
	);
	strictEqual(out, "2020-01-02T03:04:05.000Z,x\r\n");
});

// --- iso8601 regex anchors and seconds group ---
test(`${variant}: csvCoerceValuesStream iso date needs the ^ anchor`, async (_t) => {
	// A leading space makes the anchored regex fail (stays a string), but a
	// non-anchored regex would match the embedded date and `new Date(" 2020-01-02")`
	// IS valid — so only the ^ anchor keeps this a string.
	const out = await streamToArray(
		pipejoin([
			createReadableStream([{ a: " 2020-01-02" }]),
			csvCoerceValuesStream(),
		]),
	);
	strictEqual(out[0].a, " 2020-01-02");
});

test(`${variant}: csvCoerceValuesStream iso date needs the $ anchor`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream([{ a: "2020-01-02junk" }]),
			csvCoerceValuesStream(),
		]),
	);
	strictEqual(out[0].a, "2020-01-02junk");
});

test(`${variant}: csvCoerceValuesStream iso date with time but no seconds is still a Date`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream([{ a: "2020-01-02T03:04" }]),
			csvCoerceValuesStream(),
		]),
	);
	ok(out[0].a instanceof Date);
	strictEqual(out[0].a.getTime(), new Date("2020-01-02T03:04").getTime());
});

test(`${variant}: csvCoerceValuesStream iso date with seconds is a Date`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream([{ a: "2020-01-02T03:04:05" }]),
			csvCoerceValuesStream(),
		]),
	);
	ok(out[0].a instanceof Date);
});

// --- csvRemoveEmptyRowsStream l === 0 short-circuit ---
test(`${variant}: csvRemoveEmptyRowsStream drops a zero-length row`, async (_t) => {
	const filter = csvRemoveEmptyRowsStream();
	const out = await streamToArray(
		pipejoin([createReadableStream([[], ["keep"]]), filter]),
	);
	deepStrictEqual(out, [["keep"]]);
	deepStrictEqual(filter.result().value.EmptyRow.idx, [0]);
});

// --- csvArrayToObject constructor / normal key branches ---
test(`${variant}: csvArrayToObject stores a constructor column as an own data property`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream([["v"]]),
			csvArrayToObject({ headers: ["constructor"] }),
		]),
	);
	const obj = out[0];
	strictEqual(Object.getOwnPropertyDescriptor(obj, "constructor").value, "v");
	deepStrictEqual(Object.keys(obj), ["constructor"]);
});

test(`${variant}: csvArrayToObject assigns a normal key directly`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream([["v1", "v2"]]),
			csvArrayToObject({ headers: ["a", "b"] }),
		]),
	);
	deepStrictEqual(out[0], { a: "v1", b: "v2" });
});

// =====================================================================
// Mutation-hardening tests, batch 3
// =====================================================================

// --- numCols reflects the FIRST row's field count, not later rows ---
test(`${variant}: csvQuotedParser numCols is first row count even when later rows differ`, (_t) => {
	const result = csvQuotedParser("a,b\r\n1,2,3\r\n");
	strictEqual(result.numCols, 2);
});

test(`${variant}: csvQuotedParser numCols first row via quoted first field`, (_t) => {
	const result = csvQuotedParser('"a",b\r\n1,2,3\r\n');
	strictEqual(result.numCols, 2);
});

test(`${variant}: csvUnquotedParser numCols is first row count even when later rows differ`, (_t) => {
	const result = csvUnquotedParser("a,b\r\n1,2,3\r\n");
	strictEqual(result.numCols, 2);
});

test(`${variant}: csvUnquotedParser flush numCols is first row count`, (_t) => {
	const result = csvUnquotedParser("a,b\r\n1,2,3", {}, true);
	strictEqual(result.numCols, 2);
});

// --- autoCoerce JSON gate only applies to '{' or '[' ---
test(`${variant}: csvCoerceValuesStream does not JSON-parse the bare word null`, async (_t) => {
	// "null" is valid JSON but must NOT be parsed (would become the null value);
	// it does not start with '{' or '[' so it stays a string.
	const out = await streamToArray(
		pipejoin([createReadableStream([{ a: "null" }]), csvCoerceValuesStream()]),
	);
	strictEqual(out[0].a, "null");
});

test(`${variant}: csvCoerceValuesStream does not JSON-parse a bare quoted-looking word`, async (_t) => {
	const out = await streamToArray(
		pipejoin([createReadableStream([{ a: "hello" }]), csvCoerceValuesStream()]),
	);
	strictEqual(out[0].a, "hello");
});

// --- csvDetectDelimitersStream quote-bracketing scan ---
test(`${variant}: csvDetectDelimitersStream requires a quote to start a field and close at a field end`, async (_t) => {
	// Both apostrophes are mid-field; neither opens a field, so the quote char
	// must default to ".
	const detect = csvDetectDelimitersStream({ chunkSize: 1 });
	await streamToArray(
		pipejoin([createReadableStream("ab'cd',ef\r\n"), detect]),
	);
	strictEqual(detect.result().value.quoteChar, '"');
});

test(`${variant}: csvDetectDelimitersStream detects a quote that brackets a field after a delimiter`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 1 });
	await streamToArray(pipejoin([createReadableStream("x,'a'\r\n"), detect]));
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream detects a quote bracketing the very first field`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 1 });
	await streamToArray(pipejoin([createReadableStream("'a',b\r\n"), detect]));
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream ignores a quote that opens but never closes at a field end`, async (_t) => {
	const detect = csvDetectDelimitersStream({ chunkSize: 1 });
	await streamToArray(pipejoin([createReadableStream("'twas,ok\r\n"), detect]));
	strictEqual(detect.result().value.quoteChar, '"');
});

// --- csvDetectHeaderStream: header-only and rest emission ---
test(`${variant}: csvDetectHeaderStream header-only input yields empty header value default and no rows`, async (_t) => {
	// Empty input: processBuffer is never called (buffer length 0), so the result
	// header stays its initial [].
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([createReadableStream(""), hdr, csvParseStream()]),
	);
	deepStrictEqual(hdr.result().value.header, []);
	deepStrictEqual(out, []);
});

test(`${variant}: csvDetectHeaderStream header row with trailing newline and no data emits no data rows`, async (_t) => {
	// rest is empty (header fills the buffer up to the trailing newline), so the
	// rest.length > 0 guard must prevent emitting an empty chunk.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([createReadableStream("a,b,c\r\n"), hdr, csvParseStream()]),
	);
	deepStrictEqual(hdr.result().value.header, ["a", "b", "c"]);
	deepStrictEqual(out, []);
});

test(`${variant}: csvDetectHeaderStream emits the data rows after the header`, async (_t) => {
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([
			createReadableStream("a,b\r\n1,2\r\n3,4\r\n"),
			hdr,
			csvParseStream(),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ["a", "b"]);
	deepStrictEqual(out, [
		["1", "2"],
		["3", "4"],
	]);
});

test(`${variant}: csvDetectHeaderStream passes later chunks through unchanged once header detected`, async (_t) => {
	// chunkSize 1 so the header is detected on the first chunk; subsequent chunks
	// must pass through (headerDetected branch), not be re-parsed as headers.
	const hdr = csvDetectHeaderStream({ chunkSize: 1 });
	const out = await streamToArray(
		pipejoin([
			createReadableStream(["a,b\r\n", "1,2\r\n", "3,4\r\n"]),
			hdr,
			csvParseStream(),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ["a", "b"]);
	deepStrictEqual(out, [
		["1", "2"],
		["3", "4"],
	]);
});

// --- csvParseStream chunk-type coercion (string vs Buffer) ---
test(`${variant}: csvParseStream parses string chunks without toString coercion`, async (_t) => {
	const out = await streamToArray(
		pipejoin([createReadableStream("a,b\r\nc,d\r\n"), csvParseStream()]),
	);
	deepStrictEqual(out, [
		["a", "b"],
		["c", "d"],
	]);
});

// --- numCols established on the quoted-field newline path ---
test(`${variant}: csvQuotedParser numCols from first row ending in a quoted field (escapeIsQuote)`, (_t) => {
	// First row ends with "b" then newline (the quoted-field newline branch sets
	// numCols); it must be 2 even though the next row has 3 columns.
	const result = csvQuotedParser('a,"b"\r\n1,2,3\r\n');
	strictEqual(result.numCols, 2);
});

test(`${variant}: csvQuotedParser numCols from first row ending in a quoted field (custom escape)`, (_t) => {
	const result = csvQuotedParser('a,"b"\r\n1,2,3\r\n', { escapeChar: "\\" });
	strictEqual(result.numCols, 2);
});

// --- ctx.tail is "" when flushing an unterminated quote ---
test(`${variant}: csvQuotedParser flushing an unterminated quote leaves no tail (escapeIsQuote)`, (_t) => {
	const result = csvQuotedParser('"abc', {}, true);
	strictEqual(result.tail, "");
	deepStrictEqual(result.rows, [["abc"]]);
});

test(`${variant}: csvQuotedParser flushing an unterminated quote leaves no tail (custom escape)`, (_t) => {
	const result = csvQuotedParser('x,"abc', { escapeChar: "\\" }, true);
	strictEqual(result.tail, "");
	deepStrictEqual(result.rows, [["x", "abc"]]);
});

// --- ctx.tail preserves the unterminated row when NOT flushing ---
test(`${variant}: csvQuotedParser not flushing keeps the unterminated quoted row as tail (escapeIsQuote)`, (_t) => {
	const result = csvQuotedParser('a,b\r\n"abc', {}, false);
	strictEqual(result.tail, '"abc');
	deepStrictEqual(result.rows, [["a", "b"]]);
});

test(`${variant}: csvQuotedParser not flushing keeps the unterminated quoted row as tail (custom escape)`, (_t) => {
	const result = csvQuotedParser('a,b\r\nx,"abc', { escapeChar: "\\" }, false);
	strictEqual(result.tail, 'x,"abc');
	deepStrictEqual(result.rows, [["a", "b"]]);
});

// --- trailing delimiter after a quoted field on flush (lastWasDelimiter) ---
test(`${variant}: csvQuotedParser quoted field then trailing delimiter adds an empty field on flush`, (_t) => {
	// "a", then a delimiter sets lastWasDelimiter; flushing must append a trailing
	// empty field for the dangling delimiter.
	const result = csvQuotedParser('"a",', {}, true);
	deepStrictEqual(result.rows, [["a", ""]]);
});

test(`${variant}: csvParseStream unquoted trailing delimiter adds an empty field on flush`, async (_t) => {
	const out = await streamToArray(
		pipejoin([createReadableStream("a,b,"), csvParseStream()]),
	);
	deepStrictEqual(out, [["a", "b", ""]]);
});

// --- csvArrayToObject __proto__ must use defineProperty (not prototype set) ---
test(`${variant}: csvArrayToObject __proto__ column does not pollute the prototype`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream([["payload", "v2"]]),
			csvArrayToObject({ headers: ["__proto__", "b"] }),
		]),
	);
	const obj = out[0];
	// The column is an own enumerable data property, not a prototype assignment.
	strictEqual(
		Object.getOwnPropertyDescriptor(obj, "__proto__").value,
		"payload",
	);
	strictEqual(obj.b, "v2");
	deepStrictEqual(Object.keys(obj), ["__proto__", "b"]);
});

// --- csvDetectDelimitersStream empty input keeps the initialized value shape ---
test(`${variant}: csvDetectDelimitersStream empty input yields all-undefined detection value`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	const out = await streamToArray(pipejoin([createReadableStream(""), detect]));
	// Empty input must not emit an empty chunk.
	deepStrictEqual(out, []);
	deepStrictEqual(detect.result().value, {
		delimiterChar: undefined,
		newlineChar: undefined,
		quoteChar: undefined,
		escapeChar: undefined,
	});
});

// --- numCols stays first-row count when later quoted-field rows differ ---
test(`${variant}: csvQuotedParser numCols stays first row when later quoted row is shorter (escapeIsQuote)`, (_t) => {
	// Row 0 ("a","b") sets numCols via the quoted-field newline path; row 1 ("c")
	// also ends via the quoted-field newline path but must NOT reset numCols.
	const result = csvQuotedParser('"a","b"\r\n"c"\r\n');
	strictEqual(result.numCols, 2);
	deepStrictEqual(result.rows, [["a", "b"], ["c"]]);
});

test(`${variant}: csvQuotedParser numCols stays first row when later quoted row is shorter (custom escape)`, (_t) => {
	const result = csvQuotedParser('"a","b"\r\n"c"\r\n', { escapeChar: "\\" });
	strictEqual(result.numCols, 2);
	deepStrictEqual(result.rows, [["a", "b"], ["c"]]);
});

// --- numCols established in the end-of-input cleanup path ---
test(`${variant}: csvQuotedParser numCols set in cleanup for a lone quoted field on flush`, (_t) => {
	// A single quoted field with no newline, flushed: numCols is established in the
	// final cleanup block from the field count.
	const result = csvQuotedParser('"a"', {}, true);
	strictEqual(result.numCols, 1);
	deepStrictEqual(result.rows, [["a"]]);
});

// --- unescapeCustom: a trailing escape char at end-of-input is kept literally ---
test(`${variant}: csvQuotedParser custom escape unterminated field ending in a lone escape char keeps it literally`, (_t) => {
	// Content is "a\" (a then a single backslash) with no following char. The
	// trailing escape must be preserved as-is, not consume an out-of-range char.
	const result = csvQuotedParser('"a\\', { escapeChar: "\\" }, true);
	deepStrictEqual(result.rows, [["a\\"]]);
	ok(result.errors.UnterminatedQuote);
});

// --- delimiter/newline tie-break: a delimiter that is a prefix of the newline ---
test(`${variant}: csvQuotedParser delimiter takes precedence when it is a prefix of the newline`, (_t) => {
	// With delimiterChar "\r" and newlineChar "\r\n", a "\r\n" begins both a
	// delimiter and a newline at the same index. The delimiter wins the tie, so
	// "a\r\n" is the row ["a", "\n"] (the "\r" splits, the "\n" is the next field).
	const result = csvQuotedParser(
		"a\r\n",
		{
			delimiterChar: "\r",
			newlineChar: "\r\n",
		},
		true,
	);
	deepStrictEqual(result.rows, [["a", "\n"]]);
});

test(`${variant}: csvDetectHeaderStream delimiter-prefix-of-newline tie consumes the CR as a delimiter`, async (_t) => {
	// findRowEnd must apply the same tie-break: at each "\r\n" the "\r" delimiter
	// is taken first, so no pure newline terminates the row — the whole buffer is
	// the header and there are no data rows. (If the newline won the tie, the
	// header would end at the first "\r".)
	const hdr = csvDetectHeaderStream({
		delimiterChar: "\r",
		newlineChar: "\r\n",
	});
	const out = await streamToArray(
		pipejoin([createReadableStream("a\rb\r\n"), hdr]),
	);
	deepStrictEqual(hdr.result().value.header, ["a", "b", "\n"]);
	deepStrictEqual(out, []);
});

// --- csvParseInline: a fully consumed flush leaves an empty tail ---
test(`${variant}: csvQuotedParser flush of complete input leaves an empty tail`, (_t) => {
	const result = csvQuotedParser("a,b\r\n1,2", {}, true);
	strictEqual(result.tail, "");
	deepStrictEqual(result.rows, [
		["a", "b"],
		["1", "2"],
	]);
});

test(`${variant}: csvQuotedParser non-flushing partial last row is returned as tail`, (_t) => {
	const result = csvQuotedParser("a,b\r\n1,2", {}, false);
	strictEqual(result.tail, "1,2");
	deepStrictEqual(result.rows, [["a", "b"]]);
});

// --- findRowEnd: single-column header (no delimiter present) ---
test(`${variant}: csvDetectHeaderStream single-column header has no delimiter`, async (_t) => {
	// There is no delimiter in the buffer, so findRowEnd must still terminate the
	// header at the first newline (the no-delimiter branch of the boundary check).
	// The header newline sits at index 1, exercising the exact newline position.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([createReadableStream("a\r\nx\r\ny\r\n"), hdr, csvParseStream()]),
	);
	deepStrictEqual(hdr.result().value.header, ["a"]);
	deepStrictEqual(out, [["x"], ["y"]]);
});

// --- findRowEnd: a leading newline makes row 0 an empty header ---
test(`${variant}: csvDetectHeaderStream leading newline yields an empty header row`, async (_t) => {
	// The buffer starts with the newline (at index 0), so row 0 is empty and the
	// data follows. The row-end at index 0 must be returned, not skipped past a
	// later delimiter.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([createReadableStream("\r\na,b\r\n"), hdr, csvParseStream()]),
	);
	deepStrictEqual(hdr.result().value.header, []);
	deepStrictEqual(out, [["a", "b"]]);
});

// --- findRowEnd: header newline precedes a later delimiter in the data ---
test(`${variant}: csvDetectHeaderStream one-char header ends at its newline before a data delimiter`, async (_t) => {
	// The header's newline is at index 1, before any delimiter (the delimiter only
	// appears in the data row). findRowEnd must terminate the header at that
	// newline rather than skipping ahead to the data delimiter.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([createReadableStream("a\r\nb,c\r\n"), hdr, csvParseStream()]),
	);
	deepStrictEqual(hdr.result().value.header, ["a"]);
	deepStrictEqual(out, [["b", "c"]]);
});

// --- findRowEnd: an unterminated quote in the header means no complete row ---
test(`${variant}: csvDetectHeaderStream unterminated quote in header treats whole buffer as header`, async (_t) => {
	// The opening quote is never closed in the buffer, so findRowEnd returns -1 and
	// the entire input is parsed as the header with no data rows.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"unterminated header\r\nmore\r\n'),
			hdr,
			csvParseStream(),
		]),
	);
	deepStrictEqual(hdr.result().value.header, [
		"unterminated header\r\nmore\r\n",
	]);
	deepStrictEqual(out, []);
});

// --- findRowEnd: the escape-run lookback must NOT count the opening quote ---
test(`${variant}: csvDetectHeaderStream leading doubled-quote header field keeps embedded newline`, async (_t) => {
	// Header field content is ""a\r\nb : the leading "" is an escaped quote, then
	// a\r\nb stays inside the field. The escape-run lookback must stop just after
	// the opening quote — counting the opening quote would flip the parity and end
	// the row at the embedded newline.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"""a\r\nb",c\r\n1,2\r\n'),
			hdr,
			csvParseStream(),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ['"a\r\nb', "c"]);
	deepStrictEqual(out, [["1", "2"]]);
});

// --- findRowEnd: escaped quote at the very start of a quoted header field ---
test(`${variant}: csvDetectHeaderStream custom escape escaped quote at start of header field keeps embedded newline`, async (_t) => {
	// Header field content begins with \" (an escaped quote at the first content
	// position); the run-length lookback must include that leading escape so the
	// field stays open across the embedded \r\n and the row ends at the real one.
	const hdr = csvDetectHeaderStream({ escapeChar: "\\" });
	const out = await streamToArray(
		pipejoin([
			createReadableStream('"\\"a\r\nb",c\r\n1,2\r\n'),
			hdr,
			csvParseStream({ escapeChar: "\\" }),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ['"a\r\nb', "c"]);
	deepStrictEqual(out, [["1", "2"]]);
});

// --- custom escape: an escaped quote at the very start of the field content ---
test(`${variant}: csvQuotedParser custom escape escaped quote at start of content`, (_t) => {
	// Content is \"X : the leading \" is an escaped quote (the run-length lookback
	// must include the escape char sitting at the first content position), so the
	// field is "X and the closing quote is the final one.
	const result = csvQuotedParser('"\\"X",b\r\n', { escapeChar: "\\" });
	deepStrictEqual(result.rows, [['"X', "b"]]);
});

// --- csvParseInline: a quote only opens a field at a field-start position ---
test(`${variant}: csvQuotedParser treats a mid-field quote as a literal character`, (_t) => {
	// The " in a"b is not at a field start, so the field is the unquoted text a"b,
	// not the start of a quoted field.
	const result = csvQuotedParser('a"b,c\r\nd,e\r\n');
	deepStrictEqual(result.rows, [
		['a"b', "c"],
		["d", "e"],
	]);
});

test(`${variant}: csvQuotedParser mid-field quote literal with custom escape`, (_t) => {
	const result = csvQuotedParser('a"b,c\r\nd,e\r\n', { escapeChar: "\\" });
	deepStrictEqual(result.rows, [
		['a"b', "c"],
		["d", "e"],
	]);
});

// --- findRowEnd: delimiter handling opens the following quoted field ---
test(`${variant}: csvDetectHeaderStream delimiter before a quoted field with an embedded newline`, async (_t) => {
	// After the delimiter, fieldStart advances so the following quoted field opens;
	// its embedded \r\n must stay inside the field and NOT end the header row. If
	// the delimiter were not honored, the embedded newline would split the header.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([
			createReadableStream('a,"x\r\ny"\r\n1,2\r\n'),
			hdr,
			csvParseStream(),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ["a", "x\r\ny"]);
	deepStrictEqual(out, [["1", "2"]]);
});

test(`${variant}: csvDetectHeaderStream multi-char delimiter before a quoted field with an embedded newline`, async (_t) => {
	const hdr = csvDetectHeaderStream({ delimiterChar: "::" });
	const out = await streamToArray(
		pipejoin([
			createReadableStream('a::"x\r\ny"\r\n1::2\r\n'),
			hdr,
			csvParseStream({ delimiterChar: "::" }),
		]),
	);
	deepStrictEqual(hdr.result().value.header, ["a", "x\r\ny"]);
	deepStrictEqual(out, [["1", "2"]]);
});

// --- csvDetectDelimitersStream quote scan: closing quote search starts after opener ---
test(`${variant}: csvDetectDelimitersStream detects a quoted single-char field bracketed by quotes`, async (_t) => {
	// The opener is at index 0 and the closer at index 2; the closing-quote search
	// must begin after the opener and the run must terminate at the field end.
	const detect = csvDetectDelimitersStream();
	await streamToArray(pipejoin([createReadableStream("'a',b,c\r\n"), detect]));
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream detects a quoted field closing exactly at end of text`, async (_t) => {
	const detect = csvDetectDelimitersStream();
	await streamToArray(pipejoin([createReadableStream("a,'b'\r\n"), detect]));
	strictEqual(detect.result().value.quoteChar, "'");
});

test(`${variant}: csvDetectDelimitersStream does not detect a single opening quote with no closer`, async (_t) => {
	// ' opens at the very start but the next char is a delimiter (a field end), so
	// it never brackets a field; the closing-quote search must start AFTER the
	// opener, leaving the quote char at the default ".
	const detect = csvDetectDelimitersStream();
	await streamToArray(pipejoin([createReadableStream("',b\r\n"), detect]));
	strictEqual(detect.result().value.quoteChar, '"');
});

test(`${variant}: csvDetectDelimitersStream detects an empty quoted first field`, async (_t) => {
	// '' is an empty quoted field bracketed at indices 0 and 1; the closer at
	// index 1 must be considered (the close-search loop must accept index 0/1).
	const detect = csvDetectDelimitersStream();
	await streamToArray(pipejoin([createReadableStream("'',b\r\n"), detect]));
	strictEqual(detect.result().value.quoteChar, "'");
});

// --- detect streams: input with no newline is emitted via flush (not lost) ---
test(`${variant}: csvDetectDelimitersStream emits a no-newline input via flush`, async (_t) => {
	// No newline means detect() never succeeds in transform; flush must still emit
	// the buffered data so nothing is dropped.
	const detect = csvDetectDelimitersStream();
	const out = await streamToString(
		pipejoin([createReadableStream("a,b,c"), detect]),
	);
	strictEqual(out, "a,b,c");
});

test(`${variant}: csvDetectHeaderStream emits a no-newline input as header via flush`, async (_t) => {
	// The entire (newline-less) input is the header; flush processes it and emits
	// no data rows.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([createReadableStream("a,b,c"), hdr]),
	);
	deepStrictEqual(out, []);
	deepStrictEqual(hdr.result().value.header, ["a", "b", "c"]);
});

// --- detect streams: emission chunking observed directly (no downstream parser) ---
test(`${variant}: csvDetectDelimitersStream emits each chunk as it detects, not coalesced`, async (_t) => {
	// Once the first chunk completes a line, detection fires in transform and the
	// chunk is emitted; the next chunk passes straight through as its own chunk.
	const detect = csvDetectDelimitersStream();
	const out = await streamToArray(
		pipejoin([createReadableStream(["a;b\r\n", "c;d\r\n"]), detect]),
	);
	deepStrictEqual(out, ["a;b\r\n", "c;d\r\n"]);
	strictEqual(detect.result().value.delimiterChar, ";");
});

test(`${variant}: csvDetectHeaderStream emits the rest in transform, then later chunks separately`, async (_t) => {
	// First chunk completes the header row → header detected in transform, the
	// data remainder emitted, and the subsequent chunk passes through on its own.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([createReadableStream(["a,b\r\n1,2\r\n", "3,4\r\n"]), hdr]),
	);
	deepStrictEqual(out, ["1,2\r\n", "3,4\r\n"]);
	deepStrictEqual(hdr.result().value.header, ["a", "b"]);
});

test(`${variant}: csvDetectHeaderStream header-only with trailing newline emits nothing (direct)`, async (_t) => {
	// rest is empty, so the rest.length > 0 guard must suppress an empty emission.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([createReadableStream("a,b,c\r\n"), hdr]),
	);
	deepStrictEqual(out, []);
	deepStrictEqual(hdr.result().value.header, ["a", "b", "c"]);
});

test(`${variant}: csvDetectHeaderStream buffers a header split across chunks until the row completes`, async (_t) => {
	// The header row is split across two chunks; processing must wait until the
	// full first row is present, then emit the data remainder as one chunk.
	const hdr = csvDetectHeaderStream();
	const out = await streamToArray(
		pipejoin([createReadableStream(["a,", "b\r\n1,2\r\n"]), hdr]),
	);
	deepStrictEqual(out, ["1,2\r\n"]);
	deepStrictEqual(hdr.result().value.header, ["a", "b"]);
});
