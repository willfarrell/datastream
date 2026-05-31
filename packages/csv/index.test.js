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
