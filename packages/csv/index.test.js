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
	csvCoerceValuesStream,
	csvDetectDelimitersStream,
	csvDetectHeaderStream,
	csvFormatStream,
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

// *** csvFormatStream *** //
test(`${variant}: csvFormatStream should format csv from object[]`, async (_t) => {
	const streams = [
		createReadableStream([
			{ a: "1", b: "2", c: "3", d: "4" },
			{ a: "1", b: "2", c: "3", d: "4" },
		]),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "a,b,c,d\r\n1,2,3,4\r\n1,2,3,4\r\n");
});

test(`${variant}: csvFormatStream should format csv from object[] with columns`, async (_t) => {
	const streams = [
		createReadableStream([
			{ a: "1", b: "2", c: "3", d: "4" },
			{ a: "1", b: "2", c: "3", d: "4" },
		]),
		csvFormatStream({ header: ["d", "c", "b", "a"] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "d,c,b,a\r\n4,3,2,1\r\n4,3,2,1\r\n");
});

test(`${variant}: csvFormatStream should format csv from string[]`, async (_t) => {
	const streams = [
		createReadableStream([
			["1", "2", "3", "4"],
			["1", "2", "3", "4"],
		]),
		csvFormatStream({ header: ["a", "b", "c", "d"] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "a,b,c,d\r\n1,2,3,4\r\n1,2,3,4\r\n");
});

test(`${variant}: csvFormatStream should quote values containing delimiter`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "hello, world", b: "plain" }]),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, 'a,b\r\n"hello, world",plain\r\n');
});

test(`${variant}: csvFormatStream should escape values containing quote char`, async (_t) => {
	const streams = [
		createReadableStream([{ a: 'say "hi"', b: "plain" }]),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, 'a,b\r\n"say ""hi""",plain\r\n');
});

test(`${variant}: csvFormatStream should handle values containing newlines`, async (_t) => {
	// Note: csv-rex does not auto-quote embedded newlines in values
	const streams = [
		createReadableStream([{ a: "line1\nline2", b: "plain" }]),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "a,b\r\nline1\nline2,plain\r\n");
});

test(`${variant}: csvFormatStream should skip header when header is false`, async (_t) => {
	const streams = [
		createReadableStream([{ a: "1", b: "2" }]),
		csvFormatStream({ header: false }),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "1,2\r\n");
});

test(`${variant}: csvFormatStream should auto-detect header from object keys`, async (_t) => {
	const streams = [
		createReadableStream([
			{ x: "1", y: "2" },
			{ x: "3", y: "4" },
		]),
		csvFormatStream({ header: true }),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "x,y\r\n1,2\r\n3,4\r\n");
});

test(`${variant}: csvFormatStream does not auto-quote CSV injection formula triggers`, async (_t) => {
	// WARNING: csv-rex does not quote formula-trigger characters (=, +, -, @).
	// Downstream consumers opening CSV in spreadsheet apps must sanitize separately.
	// See: https://owasp.org/www-community/attacks/CSV_Injection
	const streams = [
		createReadableStream([
			{ name: "=1+2", val: "safe" },
			{ name: "+1", val: "safe" },
			{ name: "-1", val: "safe" },
			{ name: "@SUM(1)", val: "safe" },
		]),
		csvFormatStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);
	const lines = output.split("\r\n");

	// Formula triggers are NOT quoted — this is a known limitation of csv-rex
	strictEqual(lines[1], "=1+2,safe");
	strictEqual(lines[2], "+1,safe");
	strictEqual(lines[3], "-1,safe");
	strictEqual(lines[4], "@SUM(1),safe");
});

test(`${variant}: csvFormatStream roundtrip should preserve CSV injection payloads`, async (_t) => {
	const payloads = [
		"=cmd|'/C calc'!A0",
		"+cmd|'/C calc'!A0",
		"-cmd|'/C calc'!A0",
		"@SUM(1+1)",
		'=HYPERLINK("http://evil.com","Click")',
	];
	const objects = payloads.map((p) => ({ val: p }));
	const formatStreams = [createReadableStream(objects), csvFormatStream()];
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
