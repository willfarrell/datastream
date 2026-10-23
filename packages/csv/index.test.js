import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";
import {
	csvArrayToObjectStream,
	csvCoerceValuesStream,
	csvDetectDelimitersStream,
	csvDetectHeaderStream,
	csvFormatStream,
	csvInjectHeaderStream,
	csvObjectToArrayStream,
	csvParseStream,
	csvQuotedParser,
	csvRemoveEmptyRowsStream,
	csvRemoveMalformedRowsStream,
	csvUnquotedParser,
} from "@datastream/csv";
import { variant } from "../variant.js";

describe(`@datastream/csv (${variant})`, () => {
	test(`csvParseStream should parse csv to string[]`, async (_t) => {
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

	test(`csvParseStream should decode multi-byte UTF-8 split across byte chunks`, async (_t) => {
		const bytes = new TextEncoder().encode("a,é\r\nb,€\r\n");
		const streams = [
			createReadableStream([
				bytes.subarray(0, 3),
				bytes.subarray(3, 9),
				bytes.subarray(9),
			]),
			csvParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));

		deepStrictEqual(output, [
			["a", "é"],
			["b", "€"],
		]);
	});

	test(`csvParseStream should flush an incomplete trailing UTF-8 sequence`, async (_t) => {
		const streams = [
			createReadableStream([new Uint8Array([97, 44, 195])]),
			csvParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));

		deepStrictEqual(output, [["a", "\uFFFD"]]);
	});

	test(`csvParseStream should strip a leading UTF-8 BOM`, async (_t) => {
		const parse = csvParseStream();
		const output = await streamToArray(
			pipejoin([
				createReadableStream(["\uFEFFa,b\r\n", "\uFEFFc,d\r\n"]),
				parse,
			]),
		);
		deepStrictEqual(output, [
			["a", "b"],
			["\uFEFFc", "d"],
		]);
	});

	test(`csvParseStream does not re-parse an incomplete row on every chunk`, async (_t) => {
		const chunks = Array.from({ length: 1_000 }, () => "abcdefgh");
		chunks.push("\r\n");
		let calls = 0;
		const parser = (...args) => {
			calls++;
			return csvQuotedParser(...args);
		};
		const output = await streamToArray(
			pipejoin([createReadableStream(chunks), csvParseStream({ parser })]),
		);
		ok(calls < 50, `${calls} parser calls`);
		deepStrictEqual(output, [["abcdefgh".repeat(1_000)]]);
	});

	test(`csvParseStream re-parses an incomplete row once the buffer doubles`, async (_t) => {
		const texts = [];
		const parser = (text, ...args) => {
			texts.push(text);
			return csvQuotedParser(text, ...args);
		};
		const output = await streamToArray(
			pipejoin([
				createReadableStream([..."abc\r\nd\r\n"]),
				csvParseStream({ parser }),
			]),
		);
		deepStrictEqual(texts, ["a", "ab", "abc\r", "abc\r\nd\r\n"]);
		deepStrictEqual(output, [["abc"], ["d"]]);
	});

	test(`csvParseStream strips only one leading BOM from byte chunks`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([
					new Uint8Array([0xef, 0xbb, 0xbf, 0xef, 0xbb, 0xbf, 97, 13, 10]),
				]),
				csvParseStream(),
			]),
		);
		deepStrictEqual(output, [["\uFEFFa"]]);
	});

	test(`csvParseStream should strip a leading UTF-8 BOM after an empty first chunk`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream(["", "\uFEFFa,b\r\n", "\uFEFFc\r\n"]),
				csvParseStream(),
			]),
		);
		deepStrictEqual(output, [["a", "b"], ["\uFEFFc"]]);
	});

	test(`csvDetectHeaderStream should strip a leading UTF-8 BOM after an empty first chunk`, async (_t) => {
		const detect = csvDetectHeaderStream();
		const output = await streamToArray(
			pipejoin([createReadableStream(["", "\uFEFFa,b\r\n1,2\r\n"]), detect]),
		);
		deepStrictEqual(detect.result().value.header, ["a", "b"]);
		deepStrictEqual(output, ["1,2\r\n"]);
	});

	test(`csvParseStream should strip a leading UTF-8 BOM from byte chunks`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([new Uint8Array([0xef, 0xbb, 0xbf, 97, 13, 10])]),
				csvParseStream(),
			]),
		);
		deepStrictEqual(output, [["a"]]);
	});

	test(`csvParseStream should parse with custom delimiter`, async (_t) => {
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

	test(`csvParseStream should handle chunk boundaries`, async (_t) => {
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

	test(`csvParseStream should handle quoted fields`, async (_t) => {
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

	test(`csvParseStream should parse last row without trailing newline`, async (_t) => {
		const streams = [createReadableStream("1,2\r\n3,4"), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			["1", "2"],
			["3", "4"],
		]);
	});

	test(`csvParseStream should track unterminated quote errors`, async (_t) => {
		const streams = [
			createReadableStream('"unterminated\r\n'),
			csvParseStream(),
		];
		const result = await pipeline(streams);

		const { key, value } = streams[1].result();
		strictEqual(key, "csvErrors");
		deepStrictEqual(value.UnterminatedQuote.idx, [0]);
		deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [0]);
	});

	test(`csvParseStream should support lazy options`, async (_t) => {
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

	test(`csvParseStream should use custom resultKey`, async (_t) => {
		const streams = [
			createReadableStream("1,2\r\n"),
			csvParseStream({ resultKey: "parseErrors" }),
		];
		const result = await pipeline(streams);

		deepStrictEqual(result.parseErrors, {});
	});

	test(`csvParseStream should handle quoted field containing newline`, async (_t) => {
		const streams = [
			createReadableStream('"line1\r\nline2",val\r\n'),
			csvParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["line1\r\nline2", "val"]]);
	});

	test(`csvParseStream should handle quoted field containing delimiter`, async (_t) => {
		const streams = [createReadableStream('"a,b",c\r\n'), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["a,b", "c"]]);
	});

	test(`csvParseStream should handle empty input`, async (_t) => {
		const streams = [createReadableStream(""), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, []);
	});

	test(`csvParseStream should parse single-column CSV`, async (_t) => {
		const streams = [createReadableStream("a\r\nb\r\nc\r\n"), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["a"], ["b"], ["c"]]);
	});

	test(`csvParseStream should parse a blank row between rows`, async (_t) => {
		const streams = [createReadableStream("a\r\n\r\nb\r\n"), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["a"], [""], ["b"]]);
	});

	test(`csvParseStream should handle chunk boundary inside quoted field`, async (_t) => {
		const streams = [
			createReadableStream(['"hello,', ' world",42\r\n']),
			csvParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["hello, world", "42"]]);
	});

	test(`csvParseStream should return empty errors when no issues`, async (_t) => {
		const streams = [createReadableStream("1,2\r\n3,4\r\n"), csvParseStream()];
		const result = await pipeline(streams);

		deepStrictEqual(result.csvErrors, {});
	});

	test(`csvParseStream should preserve CSV injection payloads as-is`, async (_t) => {
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

	test(`csvParseStream should preserve formula triggers in unquoted fields`, async (_t) => {
		const streams = [
			createReadableStream("=1+2,+1,-1,@SUM\r\n"),
			csvParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["=1+2", "+1", "-1", "@SUM"]]);
	});

	test(`csvParseStream should accumulate error idx across chunks (custom parser)`, async (_t) => {
		let call = 0;
		const customParser = (_text, _ctx, _isFlushing) => {
			call += 1;
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
			csvParseStream({ parser: customParser }),
		];
		const result = await pipeline(streams);

		deepStrictEqual(result.csvErrors.Synthetic.idx, [1, 2, 3]);
	});

	test(`csvParseStream should track unterminated quote on flush`, async (_t) => {
		const streams = [
			createReadableStream('"ok",val\r\n"unterminated'),
			csvParseStream(),
		];
		const result = await pipeline(streams);

		deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [1]);
	});

	test(`csvParseStream should handle escaped quotes inside quoted field`, async (_t) => {
		const streams = [
			createReadableStream('"he""llo",world\r\n'),
			csvParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [['he"llo', "world"]]);
	});

	test(`csvParseStream should handle quoted field at end of chunk`, async (_t) => {
		const streams = [createReadableStream('a,"b"'), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [["a", "b"]]);
	});

	test(`csvParseStream should keep content after closing quote in the same field and record an error`, async (_t) => {
		const parse = csvParseStream();
		const streams = [createReadableStream('"a"x,b\r\n1,2\r\n"c"y\r\n'), parse];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [["ax", "b"], ["1", "2"], ["cy"]]);
		deepStrictEqual(parse.result().value, {
			UnexpectedQuote: {
				id: "UnexpectedQuote",
				message: "Unexpected text after closing quote",
				idx: [0, 2],
				count: 2,
			},
		});
	});

	test(`csvParseStream caps recorded error rows at maxErrorRows and counts the rest`, async (_t) => {
		const parse = csvParseStream({ maxErrorRows: 2 });
		await pipeline([
			createReadableStream([
				'"a"x\r\n"b"x\r\n"c"x\r\n',
				'"d"x\r\n1\r\n"e"x\r\n',
			]),
			parse,
		]);
		deepStrictEqual(parse.result().value, {
			UnexpectedQuote: {
				id: "UnexpectedQuote",
				message: "Unexpected text after closing quote",
				idx: [0, 1],
				count: 5,
			},
		});
	});

	test(`csvParseStream fills maxErrorRows across chunks`, async (_t) => {
		const parse = csvParseStream({ maxErrorRows: 3 });
		await pipeline([
			createReadableStream(['"a"x\r\n"b"x\r\n', '"c"x\r\n"d"x\r\n']),
			parse,
		]);
		deepStrictEqual(parse.result().value.UnexpectedQuote.idx, [0, 1, 2]);
		strictEqual(parse.result().value.UnexpectedQuote.count, 4);
	});

	test(`csvParseStream maxErrorRows null keeps every error row`, async (_t) => {
		const parse = csvParseStream({ maxErrorRows: null });
		await pipeline([createReadableStream('"a"x\r\n'.repeat(1_002)), parse]);
		const error = parse.result().value.UnexpectedQuote;
		deepStrictEqual(
			error.idx,
			Array.from({ length: 1_002 }, (_, i) => i),
		);
		strictEqual(error.count, 1_002);
	});

	test(`csvParseStream keeps 1000 error rows by default`, async (_t) => {
		const parse = csvParseStream();
		await pipeline([createReadableStream('"a"x\r\n'.repeat(1_002)), parse]);
		const error = parse.result().value.UnexpectedQuote;
		deepStrictEqual(
			error.idx,
			Array.from({ length: 1_000 }, (_, i) => i),
		);
		strictEqual(error.count, 1_002);
	});

	test(`csvParseStream treats a quote right after a closing quote as literal text`, async (_t) => {
		const parse = csvParseStream({ escapeChar: "\\" });
		const output = await streamToArray(
			pipejoin([createReadableStream('"a""b",c\r\n"d",e\r\n'), parse]),
		);
		deepStrictEqual(output, [
			['a"b"', "c"],
			["d", "e"],
		]);
		deepStrictEqual(parse.result().value.UnexpectedQuote.idx, [0]);
	});

	test(`csvParseStream records text after a closing quote once when the row spans chunks`, async (_t) => {
		const parse = csvParseStream();
		const output = await streamToArray(
			pipejoin([createReadableStream(['"a"x,', "b\r\n", '"c"\r\n']), parse]),
		);
		deepStrictEqual(output, [["ax", "b"], ["c"]]);
		deepStrictEqual(parse.result().value, {
			UnexpectedQuote: {
				id: "UnexpectedQuote",
				message: "Unexpected text after closing quote",
				idx: [0],
				count: 1,
			},
		});
	});

	test(`csvParseStream starts the next row fresh after text after a closing quote`, async (_t) => {
		const output = await streamToArray(
			pipejoin([createReadableStream('"a"x\r\n"b",c\r\n'), csvParseStream()]),
		);
		deepStrictEqual(output, [["ax"], ["b", "c"]]);
	});

	test(`csvParseStream records text after a closing quote and an unterminated quote in one row`, async (_t) => {
		const parse = csvParseStream();
		const output = await streamToArray(
			pipejoin([createReadableStream('1,2\r\n"a"x,"b'), parse]),
		);
		deepStrictEqual(output, [
			["1", "2"],
			["ax", "b"],
		]);
		deepStrictEqual(parse.result().value, {
			UnexpectedQuote: {
				id: "UnexpectedQuote",
				message: "Unexpected text after closing quote",
				idx: [1],
				count: 1,
			},
			UnterminatedQuote: {
				id: "UnterminatedQuote",
				message: "Unterminated quoted field",
				idx: [1],
				count: 1,
			},
		});
	});

	test(`csvParseStream should handle unterminated quote with escaped quotes`, async (_t) => {
		const streams = [createReadableStream('"he""llo'), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [['he"llo']]);
	});

	test(`csvParseStream should handle custom escape char`, async (_t) => {
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

	test(`csvParseStream should handle custom escape at end of chunk`, async (_t) => {
		const streams = [
			createReadableStream('"val\\""'),
			csvParseStream({ escapeChar: "\\" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [['val"']]);
	});

	test(`csvParseStream should handle unterminated quote with custom escape`, async (_t) => {
		const streams = [
			createReadableStream('"he\\"llo'),
			csvParseStream({ escapeChar: "\\" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [['he"llo']]);
	});

	test(`csvParseStream should handle multi-char delimiter`, async (_t) => {
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

	test(`csvParseStream should handle LF-only newline in quoted field`, async (_t) => {
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

	test(`csvParseStream should handle trailing delimiter on flush`, async (_t) => {
		const streams = [createReadableStream("a,b,"), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [["a", "b", ""]]);
	});

	test(`csvParseStream should handle quoted field as last field before newline`, async (_t) => {
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

	test(`csvParseStream should handle quoted field as last field before newline with custom escape`, async (_t) => {
		const parse = csvParseStream({ escapeChar: "\\", newlineChar: "\n" });
		const streams = [createReadableStream('a,"b"\nc,d\n'), parse];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [
			["a", "b"],
			["c", "d"],
		]);
		deepStrictEqual(parse.result().value, {});
	});

	test(`csvParseStream should handle escaped quoted field as last field before newline`, async (_t) => {
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

	test(`csvParseStream should handle escaped quoted field as last field before newline with custom escape`, async (_t) => {
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

	test(`csvInjectHeaderStream should inject header before first chunk`, async (_t) => {
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

	test(`csvInjectHeaderStream should pass through when no data`, async (_t) => {
		const streams = [
			createReadableStream([]),
			csvInjectHeaderStream({ header: ["a", "b"] }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, []);
	});

	test(`csvFormatStream should format csv from object[] using compose`, async (_t) => {
		const headers = ["a", "b", "c", "d"];
		const streams = [
			createReadableStream([
				{ a: "1", b: "2", c: "3", d: "4" },
				{ a: "1", b: "2", c: "3", d: "4" },
			]),
			csvObjectToArrayStream({ headers }),
			csvInjectHeaderStream({ header: headers }),
			csvFormatStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, "a,b,c,d\r\n1,2,3,4\r\n1,2,3,4\r\n");
	});

	test(`csvFormatStream should format csv from object[] with column order`, async (_t) => {
		const headers = ["d", "c", "b", "a"];
		const streams = [
			createReadableStream([
				{ a: "1", b: "2", c: "3", d: "4" },
				{ a: "1", b: "2", c: "3", d: "4" },
			]),
			csvObjectToArrayStream({ headers }),
			csvInjectHeaderStream({ header: headers }),
			csvFormatStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, "d,c,b,a\r\n4,3,2,1\r\n4,3,2,1\r\n");
	});

	test(`csvFormatStream should format csv from string[] with header`, async (_t) => {
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

	test(`csvFormatStream should quote values containing delimiter`, async (_t) => {
		const streams = [
			createReadableStream([["hello, world", "plain"]]),
			csvFormatStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, '"hello, world",plain\r\n');
	});

	test(`csvFormatStream should escape values containing quote char`, async (_t) => {
		const streams = [
			createReadableStream([['say "hi"', "plain"]]),
			csvFormatStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, '"say ""hi""",plain\r\n');
	});

	test(`csvFormatStream should quote values containing newlines`, async (_t) => {
		const streams = [
			createReadableStream([["line1\nline2", "plain"]]),
			csvFormatStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, '"line1\nline2",plain\r\n');
	});

	test(`csvFormatStream should format arrays without header`, async (_t) => {
		const streams = [createReadableStream([["1", "2"]]), csvFormatStream()];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, "1,2\r\n");
	});

	test(`csvFormatStream should escape CSV injection formula triggers`, async (_t) => {
		const output = await streamToString(
			pipejoin([
				createReadableStream([
					["=1+2", "@SUM(1)", "+cmd", "-cmd"],
					["\tcmd", "\rcmd", "a=1", "'=1"],
				]),
				csvFormatStream(),
			]),
		);
		strictEqual(
			output,
			`'=1+2,'@SUM(1),'+cmd,'-cmd\r\n'\tcmd,"'\rcmd",a=1,'=1\r\n`,
		);
	});

	test(`csvFormatStream should not escape signed numbers`, async (_t) => {
		const output = await streamToString(
			pipejoin([
				createReadableStream([
					["+1", "-12", "-1.5", "+2e10", "-3.25E-7", -4],
					["-1.", "+.5", "-1e", "--1", "-1x", "-1-1"],
				]),
				csvFormatStream(),
			]),
		);
		strictEqual(
			output,
			`"+1","-12","-1.5","+2e10","-3.25E-7","-4"\r\n'-1.,'+.5,'-1e,'--1,'-1x,'-1-1\r\n`,
		);
	});

	test(`csvFormatStream escapeFormulae: false only quotes formula triggers`, async (_t) => {
		const output = await streamToString(
			pipejoin([
				createReadableStream([
					["=1+2", "+1", "-1", "@SUM(1)", "\tcmd", "\rcmd"],
				]),
				csvFormatStream({ escapeFormulae: false }),
			]),
		);
		strictEqual(output, '"=1+2","+1","-1","@SUM(1)",\tcmd,"\rcmd"\r\n');
	});

	test(`csvFormatStream roundtrip should preserve CSV injection payloads with escapeFormulae: false`, async (_t) => {
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
			csvFormatStream({ escapeFormulae: false }),
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

	test(`csvArrayToObjectStream should convert array to object`, async (_t) => {
		const streams = [
			createReadableStream([["1", "2", "3"]]),
			csvArrayToObjectStream({ headers: ["a", "b", "c"] }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: "1", b: "2", c: "3" }]);
	});

	test(`csvObjectToArrayStream should convert object to array`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "1", b: "2", c: "3" }]),
			csvObjectToArrayStream({ headers: ["a", "b", "c"] }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["1", "2", "3"]]);
	});

	test(`csvDetectDelimitersStream should detect comma delimiter`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a,b,c\n1,2,3\n"), detect];
		await pipeline(streams);

		const { key, value } = detect.result();
		strictEqual(key, "csvDetectDelimiters");
		strictEqual(value.delimiterChar, ",");
		strictEqual(value.newlineChar, "\n");
		strictEqual(value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream should detect from a single line without newline`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("a;b"), detect]);

		const { value } = detect.result();
		strictEqual(value.delimiterChar, ";");
		strictEqual(value.newlineChar, undefined);
		strictEqual(value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream should detect tab delimiter`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a\tb\tc\n1\t2\t3\n"), detect];
		await pipeline(streams);

		strictEqual(detect.result().value.delimiterChar, "\t");
	});

	test(`csvDetectDelimitersStream should detect pipe delimiter`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a|b|c\n1|2|3\n"), detect];
		await pipeline(streams);

		strictEqual(detect.result().value.delimiterChar, "|");
	});

	test(`csvDetectDelimitersStream should detect semicolon delimiter`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a;b;c\n1;2;3\n"), detect];
		await pipeline(streams);

		strictEqual(detect.result().value.delimiterChar, ";");
	});

	test(`csvDetectDelimitersStream should detect CRLF newline`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a,b\r\n1,2\r\n"), detect];
		await pipeline(streams);

		strictEqual(detect.result().value.newlineChar, "\r\n");
	});

	test(`csvDetectDelimitersStream should pass data through unchanged`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a,b\n1,2\n"), detect];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		strictEqual(output, "a,b\n1,2\n");
	});

	test(`csvDetectDelimitersStream should use custom resultKey`, async (_t) => {
		const detect = csvDetectDelimitersStream({ resultKey: "delim" });
		const streams = [createReadableStream("a,b\n1,2\n"), detect];
		const result = await pipeline(streams);

		deepStrictEqual(result.delim.delimiterChar, ",");
	});

	test(`csvDetectDelimitersStream should detect CR-only newline`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a,b\r1,2\r"), detect];
		await pipeline(streams);

		strictEqual(detect.result().value.newlineChar, "\r");
	});

	test(`csvDetectDelimitersStream should detect CRLF split across chunks`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [
			createReadableStream(["a,b\r", "\n1,2\r\n3,4\r\n"]),
			detect,
		];
		const output = await streamToArray(pipejoin(streams));

		strictEqual(detect.result().value.newlineChar, "\r\n");
		deepStrictEqual(output, ["a,b\r\n1,2\r\n3,4\r\n"]);
	});

	test(`csvDetectDelimitersStream emits immediately when a CR newline is not at buffer end`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream(["a,b\r1,2\r", "3,4\r"]), detect];
		const output = await streamToArray(pipejoin(streams));

		strictEqual(detect.result().value.newlineChar, "\r");
		deepStrictEqual(output, ["a,b\r1,2\r", "3,4\r"]);
	});

	test(`csvDetectDelimitersStream emits immediately when an LF newline ends the buffer`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream(["a,b\n", "1,2\n"]), detect];
		const output = await streamToArray(pipejoin(streams));

		strictEqual(detect.result().value.newlineChar, "\n");
		deepStrictEqual(output, ["a,b\n", "1,2\n"]);
	});

	test(`csvDetectDelimitersStream should detect trailing CR-only newline on flush`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream(["a;b\r"]), detect];
		const output = await streamToArray(pipejoin(streams));

		deepStrictEqual(detect.result().value, {
			delimiterChar: ";",
			newlineChar: "\r",
			quoteChar: '"',
			escapeChar: '"',
		});
		deepStrictEqual(output, ["a;b\r"]);
	});

	test(`csvDetectDelimitersStream should throw when buffer exceeds maxBufferSize`, async (_t) => {
		await rejects(
			pipeline([
				createReadableStream(["abcd", "e"]),
				csvDetectDelimitersStream({ maxBufferSize: 4 }),
			]),
			{
				name: "RangeError",
				message:
					"csvDetectDelimitersStream buffer (5) exceeds maxBufferSize (4), newline not found",
			},
		);
	});

	test(`csvDetectDelimitersStream maxBufferSize null disables the limit`, async (_t) => {
		const detect = csvDetectDelimitersStream({ maxBufferSize: null });
		const input = "x".repeat(16_777_217);
		const output = await streamToArray(
			pipejoin([createReadableStream([input, ";y\n"]), detect]),
		);
		deepStrictEqual(output, [`${input};y\n`]);
	});

	test(`csvDetectDelimitersStream allows a buffer of exactly maxBufferSize`, async (_t) => {
		const detect = csvDetectDelimitersStream({ maxBufferSize: 4 });
		const output = await streamToArray(
			pipejoin([createReadableStream(["ab", "cd"]), detect]),
		);
		deepStrictEqual(output, ["abcd"]);
	});

	test(`csvDetectDelimitersStream does not limit a buffer that completes the first line`, async (_t) => {
		const detect = csvDetectDelimitersStream({ maxBufferSize: 4 });
		const output = await streamToArray(
			pipejoin([createReadableStream(["ab", "c;d\n"]), detect]),
		);
		strictEqual(detect.result().value.delimiterChar, ";");
		deepStrictEqual(output, ["abc;d\n"]);
	});

	test(`csvDetectDelimitersStream should detect single-quote character`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [
			createReadableStream("'first name','last name'\n'Alice','Smith'\n"),
			detect,
		];
		await pipeline(streams);

		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream detects the quote char from the first line only`, async (_t) => {
		const text = "name,note\nbob,'hi, there'\n";
		const whole = csvDetectDelimitersStream();
		await pipeline([createReadableStream(text), whole]);
		const split = csvDetectDelimitersStream();
		await pipeline([
			createReadableStream(["name,note\n", "bob,'hi, there'\n"]),
			split,
		]);

		strictEqual(whole.result().value.quoteChar, '"');
		deepStrictEqual(whole.result().value, split.result().value);
	});

	test(`csvDetectDelimitersStream detects a quoted last header field before CRLF`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("a,'b'\r\n1,2\r\n"), detect]);

		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream should handle small input via flush`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a\tb\n1\t2\n"), detect];
		await pipeline(streams);

		strictEqual(detect.result().value.delimiterChar, "\t");
	});

	test(`csvDetectDelimitersStream should not confuse delimiter inside quotes`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream('"a\tb"\tc\n"d\te"\tf\n'), detect];
		await pipeline(streams);

		strictEqual(detect.result().value.delimiterChar, "\t");
	});

	test(`csvDetectDelimitersStream should handle multiple chunks`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream(["a;b;c", "\n1;2;3\n"]), detect];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		strictEqual(detect.result().value.delimiterChar, ";");
		strictEqual(output, "a;b;c\n1;2;3\n");
	});

	test(`csvDetectDelimitersStream should detect from large input`, async (_t) => {
		const row = "a,b,c,d,e\n";
		const input = row.repeat(120);
		const streams = [createReadableStream(input), csvDetectDelimitersStream()];
		const result = await pipeline(streams);
		strictEqual(result.csvDetectDelimiters.delimiterChar, ",");
	});

	test(`csvDetectDelimitersStream should pass through chunks after detection`, async (_t) => {
		const row = "a,b,c\n";
		const input = [row.repeat(120), "x,y,z\n"];
		const streams = [createReadableStream(input), csvDetectDelimitersStream()];
		const output = await streamToString(pipejoin(streams));
		ok(output.includes("x,y,z"));
	});

	test(`csvDetectDelimitersStream quote detection is linear in the input size`, async (_t) => {
		const n = 5_000;
		const input = `${',"a'.repeat(n)},'b'\n`;
		const detect = csvDetectDelimitersStream();
		const indexOf = String.prototype.indexOf;
		let calls = 0;
		String.prototype.indexOf = function (...args) {
			calls++;
			return indexOf.apply(this, args);
		};
		try {
			await pipeline([createReadableStream([input]), detect]);
		} finally {
			String.prototype.indexOf = indexOf;
		}
		ok(calls < 10 * n, `${calls} indexOf calls`);
		deepStrictEqual(detect.result().value, {
			delimiterChar: ",",
			newlineChar: "\n",
			quoteChar: "'",
			escapeChar: "'",
		});
	});

	test(`csvDetectDelimitersStream does not rescan the buffer per chunk before a newline`, async (_t) => {
		const chunks = Array.from({ length: 1_000 }, () => "abcdefgh");
		chunks.push(";\n");
		const detect = csvDetectDelimitersStream();
		const match = String.prototype.match;
		let calls = 0;
		String.prototype.match = function (...args) {
			calls++;
			return match.apply(this, args);
		};
		let output;
		try {
			output = await streamToArray(
				pipejoin([createReadableStream(chunks), detect]),
			);
		} finally {
			String.prototype.match = match;
		}
		strictEqual(calls, 1);
		strictEqual(detect.result().value.delimiterChar, ";");
		deepStrictEqual(output, [chunks.join("")]);
	});

	test(`csvDetectDelimitersStream should decode multi-byte UTF-8 split across byte chunks`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const bytes = new TextEncoder().encode("h1;é\r\nü;€\r\n");
		const output = await streamToArray(
			pipejoin([
				createReadableStream([
					bytes.subarray(0, 4),
					bytes.subarray(4, 11),
					bytes.subarray(11),
				]),
				detect,
			]),
		);
		strictEqual(detect.result().value.delimiterChar, ";");
		deepStrictEqual(output, ["h1;é\r\nü;", "€\r\n"]);
	});

	test(`csvDetectDelimitersStream should flush an incomplete trailing UTF-8 sequence`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const output = await streamToArray(
			pipejoin([
				createReadableStream([
					new Uint8Array([97, 10]),
					new Uint8Array([98, 195]),
				]),
				detect,
			]),
		);
		deepStrictEqual(output, ["a\n", "b", "\uFFFD"]);
	});

	test(`csvDetectDelimitersStream should flush an incomplete UTF-8 sequence before detection`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const output = await streamToArray(
			pipejoin([createReadableStream([new Uint8Array([97, 44, 195])]), detect]),
		);
		strictEqual(detect.result().value.delimiterChar, ",");
		deepStrictEqual(output, ["a,\uFFFD"]);
	});

	test(`csvDetectDelimitersStream + csvDetectHeaderStream + csvParseStream decode byte chunks`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const lazy = {
			delimiterChar: () => detect.result().value.delimiterChar,
			newlineChar: () => detect.result().value.newlineChar,
		};
		const header = csvDetectHeaderStream(lazy);
		const bytes = new TextEncoder().encode("h1,h2\r\né,ü\r\n");
		const output = await streamToArray(
			pipejoin([
				createReadableStream([bytes.subarray(0, 8), bytes.subarray(8)]),
				detect,
				header,
				csvParseStream(lazy),
			]),
		);
		deepStrictEqual(header.result().value.header, ["h1", "h2"]);
		deepStrictEqual(output, [["é", "ü"]]);
	});

	test(`csvDetectHeaderStream does not rescan the buffer on every chunk`, async (_t) => {
		const chunks = Array.from({ length: 1_000 }, () => "abcdefgh");
		chunks.push("\r\n1\r\n");
		const detect = csvDetectHeaderStream();
		const indexOf = String.prototype.indexOf;
		let calls = 0;
		String.prototype.indexOf = function (...args) {
			calls++;
			return indexOf.apply(this, args);
		};
		let output;
		try {
			output = await streamToArray(
				pipejoin([createReadableStream(chunks), detect]),
			);
		} finally {
			String.prototype.indexOf = indexOf;
		}
		ok(calls < 100, `${calls} indexOf calls`);
		deepStrictEqual(detect.result().value.header, ["abcdefgh".repeat(1_000)]);
		deepStrictEqual(output, ["1\r\n"]);
	});

	test(`csvDetectHeaderStream retries a partial header once the buffer doubles`, async (_t) => {
		const detect = csvDetectHeaderStream();
		const output = await streamToArray(
			pipejoin([createReadableStream([..."abc\r\nxyz\r\n"]), detect]),
		);
		deepStrictEqual(detect.result().value.header, ["abc"]);
		deepStrictEqual(output, ["xyz", "\r", "\n"]);
	});

	test(`csvDetectHeaderStream caps the retry point just past maxBufferSize`, async (_t) => {
		const detect = csvDetectHeaderStream({ maxBufferSize: 10 });
		const output = await streamToArray(
			pipejoin([createReadableStream(["abcdef", "\r\n12", "3"]), detect]),
		);
		deepStrictEqual(detect.result().value.header, ["abcdef"]);
		deepStrictEqual(output, ["123"]);
	});

	test(`csvDetectHeaderStream should decode multi-byte UTF-8 split across byte chunks`, async (_t) => {
		const detect = csvDetectHeaderStream();
		const bytes = new TextEncoder().encode("h1,é\r\nü,€\r\n");
		const output = await streamToArray(
			pipejoin([
				createReadableStream([
					bytes.subarray(0, 4),
					bytes.subarray(4, 11),
					bytes.subarray(11),
				]),
				detect,
			]),
		);
		deepStrictEqual(detect.result().value.header, ["h1", "é"]);
		deepStrictEqual(output, ["ü,", "€\r\n"]);
	});

	test(`csvDetectHeaderStream should flush an incomplete trailing UTF-8 sequence`, async (_t) => {
		const detect = csvDetectHeaderStream();
		const output = await streamToArray(
			pipejoin([
				createReadableStream([
					new Uint8Array([97, 13, 10]),
					new Uint8Array([98, 195]),
				]),
				detect,
			]),
		);
		deepStrictEqual(detect.result().value.header, ["a"]);
		deepStrictEqual(output, ["b", "\uFFFD"]);
	});

	test(`csvDetectHeaderStream should flush an incomplete UTF-8 sequence before detection`, async (_t) => {
		const detect = csvDetectHeaderStream();
		const output = await streamToArray(
			pipejoin([createReadableStream([new Uint8Array([97, 44, 195])]), detect]),
		);
		deepStrictEqual(detect.result().value.header, ["a", "\uFFFD"]);
		deepStrictEqual(output, []);
	});

	test(`csvDetectHeaderStream should extract headers and remove header row`, async (_t) => {
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

	test(`csvDetectHeaderStream ends the header at a newline in text after a closing quote`, async (_t) => {
		const detect = csvDetectHeaderStream({ escapeChar: "\\" });
		const output = await streamToArray(
			pipejoin([createReadableStream(['"a""b\r\nc",d\r\n']), detect]),
		);
		deepStrictEqual(detect.result().value.header, ['a"b']);
		deepStrictEqual(output, ['c",d\r\n']);
	});

	test(`csvDetectHeaderStream should throw when buffer exceeds maxBufferSize`, async (_t) => {
		await rejects(
			pipeline([
				createReadableStream(["abcd", "e"]),
				csvDetectHeaderStream({ maxBufferSize: 4 }),
			]),
			{
				name: "RangeError",
				message:
					"csvDetectHeaderStream buffer (5) exceeds maxBufferSize (4), header row not found",
			},
		);
	});

	test(`csvDetectHeaderStream maxBufferSize null disables the limit`, async (_t) => {
		const detect = csvDetectHeaderStream({ maxBufferSize: null });
		const input = "x".repeat(16_777_217);
		const output = await streamToArray(
			pipejoin([createReadableStream([input, ",y\r\nz"]), detect]),
		);
		deepStrictEqual(detect.result().value.header, [input, "y"]);
		deepStrictEqual(output, ["z"]);
	});

	test(`csvDetectHeaderStream allows a buffer of exactly maxBufferSize`, async (_t) => {
		const detect = csvDetectHeaderStream({ maxBufferSize: 4 });
		const output = await streamToArray(
			pipejoin([createReadableStream(["a,", "bc"]), detect]),
		);
		deepStrictEqual(detect.result().value.header, ["a", "bc"]);
		deepStrictEqual(output, []);
	});

	test(`csvDetectHeaderStream does not limit a buffer that completes the header row`, async (_t) => {
		const detect = csvDetectHeaderStream({ maxBufferSize: 4 });
		const output = await streamToArray(
			pipejoin([createReadableStream(["a,", "bc\r\n1,2"]), detect]),
		);
		deepStrictEqual(detect.result().value.header, ["a", "bc"]);
		deepStrictEqual(output, ["1,2"]);
	});

	test(`csvDetectHeaderStream should work with lazy delimiterChar`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const headers = csvDetectHeaderStream({
			delimiterChar: () => detect.result().value.delimiterChar,
			newlineChar: () => detect.result().value.newlineChar,
			quoteChar: () => detect.result().value.quoteChar,
		});
		const streams = [
			createReadableStream("a\tb\tc\n1\t2\t3\n"),
			detect,
			headers,
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(headers.result().value.header, ["a", "b", "c"]);
		strictEqual(output, "1\t2\t3\n");
	});

	test(`csvDetectHeaderStream should handle quoted headers`, async (_t) => {
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

	test(`csvDetectHeaderStream should use custom resultKey`, async (_t) => {
		const headers = csvDetectHeaderStream({
			newlineChar: "\n",
			resultKey: "hdr",
		});
		const streams = [createReadableStream("a,b\n1,2\n"), headers];
		const result = await pipeline(streams);

		deepStrictEqual(result.hdr.header, ["a", "b"]);
	});

	test(`csvDetectHeaderStream should handle header-only input`, async (_t) => {
		const headers = csvDetectHeaderStream();
		const streams = [createReadableStream("a,b,c"), headers];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(headers.result().value.header, ["a", "b", "c"]);
		strictEqual(output, "");
	});

	test(`csvDetectHeaderStream should handle CRLF newlines`, async (_t) => {
		const headers = csvDetectHeaderStream({ newlineChar: "\r\n" });
		const streams = [createReadableStream("x,y\r\n1,2\r\n3,4\r\n"), headers];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(headers.result().value.header, ["x", "y"]);
		strictEqual(output, "1,2\r\n3,4\r\n");
	});

	test(`csvDetectHeaderStream should handle escaped quotes in header`, async (_t) => {
		const headers = csvDetectHeaderStream({ newlineChar: "\n" });
		const streams = [
			createReadableStream('"col""1",col2\nval1,val2\n'),
			headers,
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(headers.result().value.header, ['col"1', "col2"]);
		ok(output.includes("val1,val2"));
	});

	test(`csvDetectHeaderStream should detect from large input`, async (_t) => {
		const headers = csvDetectHeaderStream({ newlineChar: "\n" });
		const input = `name,age,city\n${"Alice,30,NYC\n".repeat(100)}`;
		const streams = [createReadableStream(input), headers];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(headers.result().value.header, ["name", "age", "city"]);
		ok(output.includes("Alice,30,NYC"));
	});

	test(`csvDetectHeaderStream should pass through chunks after detection`, async (_t) => {
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

	test(`csvDetectHeaderStream should handle small input via flush`, async (_t) => {
		const headers = csvDetectHeaderStream({
			newlineChar: "\n",
		});
		const streams = [createReadableStream("col1,col2\nval1,val2\n"), headers];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(headers.result().value.header, ["col1", "col2"]);
		strictEqual(output, "val1,val2\n");
	});

	test(`csvRemoveMalformedRowsStream should drop rows with wrong column count`, async (_t) => {
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

	test(`csvRemoveMalformedRowsStream should pass malformed rows with onErrorEnqueue`, async (_t) => {
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

	test(`csvRemoveMalformedRowsStream should accept lazy headers`, async (_t) => {
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

	test(`csvRemoveMalformedRowsStream should use custom resultKey`, async (_t) => {
		const filter = csvRemoveMalformedRowsStream({ resultKey: "bad" });
		const streams = [createReadableStream([["1", "2"], ["3"]]), filter];
		const result = await pipeline(streams);

		deepStrictEqual(result.bad.MalformedRow.idx, [1]);
	});

	test(`csvRemoveMalformedRowsStream should return empty value when all rows valid`, async (_t) => {
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

	test(`csvRemoveMalformedRowsStream should track both too-few and too-many columns`, async (_t) => {
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

	test(`csvRemoveMalformedRowsStream should infer columns from first row when no headers`, async (_t) => {
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

	test(`csvRemoveMalformedRowsStream maxErrorRows null keeps every error row`, async (_t) => {
		const filter = csvRemoveMalformedRowsStream({ maxErrorRows: null });
		const rows = [["a", "b"], ...Array.from({ length: 1_002 }, () => ["1"])];
		await pipeline([createReadableStream(rows), filter]);
		deepStrictEqual(filter.result().value.MalformedRow, {
			id: "MalformedRow",
			message: "Row has incorrect number of fields",
			idx: Array.from({ length: 1_002 }, (_, i) => i + 1),
			count: 1_002,
		});
	});

	test(`csvRemoveMalformedRowsStream keeps 1000 error rows by default`, async (_t) => {
		const filter = csvRemoveMalformedRowsStream({ maxErrorRows: undefined });
		const rows = [["a", "b"], ...Array.from({ length: 1_002 }, () => ["1"])];
		await pipeline([createReadableStream(rows), filter]);
		deepStrictEqual(
			filter.result().value.MalformedRow.idx,
			Array.from({ length: 1_000 }, (_, i) => i + 1),
		);
	});

	test(`csvRemoveMalformedRowsStream caps recorded rows at maxErrorRows and counts the rest`, async (_t) => {
		const filter = csvRemoveMalformedRowsStream({ maxErrorRows: 2 });
		const output = await streamToArray(
			pipejoin([
				createReadableStream([["a", "b"], ["1"], ["2"], ["3", "4"], ["5"]]),
				filter,
			]),
		);
		deepStrictEqual(output, [
			["a", "b"],
			["3", "4"],
		]);
		deepStrictEqual(filter.result().value, {
			MalformedRow: {
				id: "MalformedRow",
				message: "Row has incorrect number of fields",
				idx: [1, 2],
				count: 3,
			},
		});
	});

	test(`csvRemoveMalformedRowsStream keeps 1000 rows by default`, async (_t) => {
		const filter = csvRemoveMalformedRowsStream({ headers: ["a", "b"] });
		await pipeline([
			createReadableStream(Array.from({ length: 1_002 }, () => ["x"])),
			filter,
		]);
		const error = filter.result().value.MalformedRow;
		deepStrictEqual(
			error.idx,
			Array.from({ length: 1_000 }, (_, i) => i),
		);
		strictEqual(error.count, 1_002);
	});

	test(`csvRemoveEmptyRowsStream should drop empty rows`, async (_t) => {
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

	test(`csvRemoveEmptyRowsStream maxErrorRows null keeps every error row`, async (_t) => {
		const filter = csvRemoveEmptyRowsStream({ maxErrorRows: null });
		const rows = Array.from({ length: 1_002 }, () => [""]);
		await pipeline([createReadableStream(rows), filter]);
		deepStrictEqual(filter.result().value.EmptyRow, {
			id: "EmptyRow",
			message: "Row is empty",
			idx: Array.from({ length: 1_002 }, (_, i) => i),
			count: 1_002,
		});
	});

	test(`csvRemoveEmptyRowsStream keeps 1000 error rows by default`, async (_t) => {
		const filter = csvRemoveEmptyRowsStream({ maxErrorRows: undefined });
		const rows = Array.from({ length: 1_002 }, () => [""]);
		await pipeline([createReadableStream(rows), filter]);
		deepStrictEqual(
			filter.result().value.EmptyRow.idx,
			Array.from({ length: 1_000 }, (_, i) => i),
		);
	});

	test(`csvRemoveEmptyRowsStream caps recorded rows at maxErrorRows and counts the rest`, async (_t) => {
		const filter = csvRemoveEmptyRowsStream({ maxErrorRows: 2 });
		const output = await streamToArray(
			pipejoin([
				createReadableStream([["a"], [""], [""], ["b"], [""]]),
				filter,
			]),
		);
		deepStrictEqual(output, [["a"], ["b"]]);
		deepStrictEqual(filter.result().value, {
			EmptyRow: {
				id: "EmptyRow",
				message: "Row is empty",
				idx: [1, 2],
				count: 3,
			},
		});
	});

	test(`csvRemoveEmptyRowsStream keeps 1000 rows by default`, async (_t) => {
		const filter = csvRemoveEmptyRowsStream();
		await pipeline([
			createReadableStream(Array.from({ length: 1_002 }, () => [""])),
			filter,
		]);
		const error = filter.result().value.EmptyRow;
		deepStrictEqual(
			error.idx,
			Array.from({ length: 1_000 }, (_, i) => i),
		);
		strictEqual(error.count, 1_002);
	});

	test(`csvRemoveEmptyRowsStream should drop zero-length rows`, async (_t) => {
		const filter = csvRemoveEmptyRowsStream();
		const streams = [
			createReadableStream([["1", "2"], [], ["3", "4"]]),
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

	test(`csvRemoveEmptyRowsStream should pass empty rows with onErrorEnqueue`, async (_t) => {
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

	test(`csvRemoveEmptyRowsStream should track multiple empty rows`, async (_t) => {
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

	test(`csvRemoveEmptyRowsStream should use custom resultKey`, async (_t) => {
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

	test(`csvRemoveEmptyRowsStream should return empty value when no empty rows`, async (_t) => {
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

	test(`csvCoerceValuesStream keeps a __proto__ column as an own key (auto)`, async (_t) => {
		const streams = [
			createReadableStream([['{"isAdmin":true}', "2"]]),
			csvArrayToObjectStream({ headers: ["__proto__", "b"] }),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		strictEqual(output.length, 1);
		deepStrictEqual(Object.keys(output[0]), ["__proto__", "b"]);
		strictEqual(Object.getPrototypeOf(output[0]), Object.prototype);
		strictEqual(output[0].isAdmin, undefined);
		deepStrictEqual(Object.getOwnPropertyDescriptor(output[0], "__proto__"), {
			value: { isAdmin: true },
			writable: true,
			enumerable: true,
			configurable: true,
		});
		strictEqual(output[0].b, 2);
	});

	test(`csvCoerceValuesStream keeps a __proto__ column as an own key (columns)`, async (_t) => {
		const streams = [
			createReadableStream([['{"isAdmin":true}', "2"]]),
			csvArrayToObjectStream({ headers: ["__proto__", "b"] }),
			// JSON.parse makes "__proto__" an own key (a literal would set the prototype)
			csvCoerceValuesStream({
				columns: JSON.parse('{"__proto__":"json","b":"number"}'),
			}),
		];
		const output = await streamToArray(pipejoin(streams));
		strictEqual(output.length, 1);
		deepStrictEqual(Object.keys(output[0]), ["__proto__", "b"]);
		strictEqual(Object.getPrototypeOf(output[0]), Object.prototype);
		strictEqual(output[0].isAdmin, undefined);
		deepStrictEqual(Object.getOwnPropertyDescriptor(output[0], "__proto__"), {
			value: { isAdmin: true },
			writable: true,
			enumerable: true,
			configurable: true,
		});
		strictEqual(output[0].b, 2);
	});

	test(`csvCoerceValuesStream should auto-coerce types`, async (_t) => {
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

		strictEqual(coerce.result, undefined);
		deepStrictEqual(result, {});
	});

	test(`csvCoerceValuesStream should auto-coerce date values`, async (_t) => {
		const streams = [
			createReadableStream([{ date: "2024-01-15" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ date: new Date("2024-01-15") }]);
	});

	test(`csvCoerceValuesStream should auto-coerce JSON values`, async (_t) => {
		const streams = [
			createReadableStream([{ arr: "[1,2,3]", obj: '{"a":1}' }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ arr: [1, 2, 3], obj: { a: 1 } }]);
	});

	test(`csvCoerceValuesStream should coerce with explicit column types`, async (_t) => {
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

	test(`csvCoerceValuesStream should not take column types from Object.prototype`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "1", constructor: "2", toString: "true" }]),
			csvCoerceValuesStream({ columns: { a: "number" } }),
		];
		const output = await streamToArray(pipejoin(streams));

		deepStrictEqual(output, [{ a: 1, constructor: 2, toString: true }]);
	});

	test(`csvCoerceValuesStream explicit number maps whitespace-only to null`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "  ", b: 5 }]),
			csvCoerceValuesStream({ columns: { a: "number", b: "number" } }),
		];
		const output = await streamToArray(pipejoin(streams));

		deepStrictEqual(output, [{ a: null, b: 5 }]);
	});

	test(`csvCoerceValuesStream should handle scientific notation`, async (_t) => {
		const streams = [
			createReadableStream([{ val: "1.5e3" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ val: 1500 }]);
	});

	test(`csvCoerceValuesStream should coerce negative numbers`, async (_t) => {
		const streams = [
			createReadableStream([{ val: "-42", neg_float: "-3.14" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ val: -42, neg_float: -3.14 }]);
	});

	test(`csvCoerceValuesStream should keep invalid JSON as string`, async (_t) => {
		const streams = [
			createReadableStream([{ val: "{not json", arr: "[broken" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ val: "{not json", arr: "[broken" }]);
	});

	test(`csvCoerceValuesStream should coerce ISO 8601 datetime`, async (_t) => {
		const streams = [
			createReadableStream([{ ts: "2024-01-15T10:30:00Z" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ ts: new Date("2024-01-15T10:30:00Z") }]);
	});

	test(`csvCoerceValuesStream should pass through non-string values`, async (_t) => {
		const streams = [
			createReadableStream([{ num: 42, bool: true, nil: null }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ num: 42, bool: true, nil: null }]);
	});

	test(`csvCoerceValuesStream should coerce explicit null type`, async (_t) => {
		const streams = [
			createReadableStream([{ val: "anything" }]),
			csvCoerceValuesStream({ columns: { val: "null" } }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ val: null }]);
	});

	test(`csvCoerceValuesStream should coerce explicit date type`, async (_t) => {
		const streams = [
			createReadableStream([{ val: "2024-06-15" }]),
			csvCoerceValuesStream({ columns: { val: "date" } }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ val: new Date("2024-06-15") }]);
	});

	test(`csvCoerceValuesStream should coerce explicit json type`, async (_t) => {
		const streams = [
			createReadableStream([{ val: '{"a":1}' }]),
			csvCoerceValuesStream({ columns: { val: "json" } }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ val: { a: 1 } }]);
	});

	test(`csvCoerceValuesStream should return original string when number coercion yields NaN`, async (_t) => {
		const streams = [
			createReadableStream([{ val: "not-a-number" }]),
			csvCoerceValuesStream({ columns: { val: "number" } }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ val: "not-a-number" }]);
	});

	test(`csvCoerceValuesStream should not coerce CSV injection formula triggers`, async (_t) => {
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

	test(`csvCoerceValuesStream coerces leading-zero strings to numbers`, async (_t) => {
		const streams = [
			createReadableStream([{ zip: "07001", phone: "0123456789" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(output[0].zip, 7001);
		strictEqual(output[0].phone, 123456789);
	});

	test(`csvCoerceValuesStream should preserve leading-zero strings with explicit string type`, async (_t) => {
		const streams = [
			createReadableStream([{ zip: "07001" }]),
			csvCoerceValuesStream({ columns: { zip: "string" } }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(output[0].zip, "07001");
	});

	test(`csvCoerceValuesStream should return original string when explicit json coercion fails`, async (_t) => {
		const streams = [
			createReadableStream([{ val: "not-valid-json" }]),
			csvCoerceValuesStream({ columns: { val: "json" } }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ val: "not-valid-json" }]);
	});

	test(`csvCoerceValuesStream should return null for empty string with number type`, async (_t) => {
		const streams = [
			createReadableStream([{ val: "" }]),
			csvCoerceValuesStream({ columns: { val: "number" } }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ val: null }]);
	});

	test(`csvUnquotedParser should parse simple unquoted CSV`, (_t) => {
		const result = csvUnquotedParser("a,b,c\r\n1,2,3\r\n");
		deepStrictEqual(result.rows, [
			["a", "b", "c"],
			["1", "2", "3"],
		]);
		strictEqual(result.tail, "");
	});

	test(`csvUnquotedParser should return tail for incomplete row`, (_t) => {
		const result = csvUnquotedParser("a,b,c\r\n1,2,3");
		deepStrictEqual(result.rows, [["a", "b", "c"]]);
		strictEqual(result.tail, "1,2,3");
	});

	test(`csvUnquotedParser should flush incomplete row`, (_t) => {
		const result = csvUnquotedParser("a,b,c\r\n1,2,3", {}, true);
		deepStrictEqual(result.rows, [
			["a", "b", "c"],
			["1", "2", "3"],
		]);
		strictEqual(result.tail, "");
	});

	test(`csvUnquotedParser should flush single row without newline`, (_t) => {
		const result = csvUnquotedParser("a,b,c", {}, true);
		deepStrictEqual(result.rows, [["a", "b", "c"]]);
		strictEqual(result.numCols, 3);
	});

	test(`csvParseStream should work with custom parser (csvUnquotedParser)`, async (_t) => {
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

	test(`csvParseStream with custom parser should flush remaining`, async (_t) => {
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

	test(`csvParseStream should handle multi-row input`, async (_t) => {
		const row = "a,b,c\r\n";
		const input = [row.repeat(50), "x,y,z\r\n"];
		const streams = [createReadableStream(input), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(output.length, 51);
		deepStrictEqual(output[output.length - 1], ["x", "y", "z"]);
	});

	test(`csvDetectDelimitersStream should pass through in transform after detection`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [
			createReadableStream(["a,b,c\n", "1,2,3\n", "4,5,6\n"]),
			detect,
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		strictEqual(detect.result().value.delimiterChar, ",");
		ok(output.includes("4,5,6"));
	});

	test(`csvDetectDelimitersStream should buffer when no newline found`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream(["abc", "d\n1\n"]), detect];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		strictEqual(output, "abcd\n1\n");
	});

	test(`csvParseStream should handle unterminated quote with custom escape and fewer fields than numCols`, async (_t) => {
		const streams = [
			createReadableStream('a,b\r\n"unterminated'),
			csvParseStream({ escapeChar: "\\" }),
		];
		const result = await pipeline(streams);

		deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [1]);
	});

	test(`csvParseStream should handle content after closing quote with custom escape`, async (_t) => {
		const streams = [
			createReadableStream('"a"x,b\r\n'),
			csvParseStream({ escapeChar: "\\" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["ax", "b"]]);
		deepStrictEqual(streams[1].result().value.UnexpectedQuote.idx, [0]);
	});

	test(`csvParseStream should join multiple input chunks`, async (_t) => {
		const streams = [
			createReadableStream(["1,2\r\n", "3,4\r\n"]),
			csvParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			["1", "2"],
			["3", "4"],
		]);
	});

	test(`csvQuotedParser should parse with default options`, (_t) => {
		const result = csvQuotedParser("a,b\r\n1,2\r\n");
		deepStrictEqual(result.rows, [
			["a", "b"],
			["1", "2"],
		]);
		strictEqual(result.tail, "");
		deepStrictEqual(result.errors, {});
	});

	test(`csvQuotedParser should unescape consecutive "" pairs in one field`, (_t) => {
		const text = '"a""b""c",x';
		const result = csvQuotedParser(`${text}\r\n`, {}, true);
		deepStrictEqual(result.rows, [['a"b"c', "x"]]);
		const partial = csvQuotedParser(text, {}, false);
		deepStrictEqual(partial.rows, []);
		strictEqual(partial.tail, text);
	});

	test(`csvParseStream should handle unterminated quote with fewer fields than established numCols`, async (_t) => {
		const streams = [
			createReadableStream('a,b\r\n"unterminated'),
			csvParseStream(),
		];
		const result = await pipeline(streams);

		deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [1]);
	});

	test(`csvParseStream should handle 3+ char custom newline`, async (_t) => {
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

	test(`csvParseStream should handle 3+ char custom newline with custom escape`, async (_t) => {
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

	test(`csvParseStream should handle quoted field at start of subsequent row`, async (_t) => {
		const streams = [
			createReadableStream('a,b\r\n"c",d\r\n'),
			csvParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			["a", "b"],
			["c", "d"],
		]);
	});

	test(`csvParseStream should handle delimiter at end of chunk followed by quote`, async (_t) => {
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

	test(`csvParseStream should flush short row with fewer fields`, async (_t) => {
		const streams = [createReadableStream("a,b,c\r\nd"), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["a", "b", "c"], ["d"]]);
	});

	test(`csvParseStream should handle trailing delimiter on flush with established numCols`, async (_t) => {
		const streams = [createReadableStream("a,b\r\nc,"), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			["a", "b"],
			["c", ""],
		]);
	});

	test(`csvParseStream should handle malformed unquoted row with missing delimiter`, async (_t) => {
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

	test(`csvParseStream should handle single unquoted row exactly filling input`, async (_t) => {
		const streams = [createReadableStream("a,b,c\r\n"), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["a", "b", "c"]]);
	});

	test(`csvDetectHeaderStream should handle BOM-only input`, async (_t) => {
		const headers = csvDetectHeaderStream();
		const streams = [createReadableStream("\uFEFF"), headers];
		const stream = pipejoin(streams);
		await streamToString(stream);

		deepStrictEqual(headers.result().value.header, []);
	});

	test(`csvDetectHeaderStream should handle input starting with newline`, async (_t) => {
		const headers = csvDetectHeaderStream({ newlineChar: "\n" });
		const streams = [createReadableStream("\na,b\n"), headers];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(headers.result().value.header, []);
		ok(output.includes("a,b"));
	});

	test(`csvDetectDelimitersStream should strip BOM`, async (_t) => {
		const bom = "\uFEFF";
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream(`${bom}a,b\n1,2\n`), detect];
		await pipeline(streams);

		strictEqual(detect.result().value.delimiterChar, ",");
	});

	test(`csvParseStream should handle quoted field followed by 3+ char newline`, async (_t) => {
		const streams = [
			createReadableStream('"a"---"b"---'),
			csvParseStream({ newlineChar: "---" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["a"], ["b"]]);
	});

	test(`csvParseStream should handle quoted field with multi-char delimiter and custom escape`, async (_t) => {
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

	test(`csvParseStream should handle quoted field with 3+ char newline and custom escape`, async (_t) => {
		const streams = [
			createReadableStream('"a"---"b"---'),
			csvParseStream({ newlineChar: "---", escapeChar: "\\" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [["a"], ["b"]]);
	});

	test(`csvParseStream should handle short quoted row with fewer fields after newline`, async (_t) => {
		const streams = [
			createReadableStream('a,b,c\r\n"d"\r\n'),
			csvParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output[0], ["a", "b", "c"]);
		strictEqual(output[1].length, 1);
	});

	test(`csvParseStream should handle short quoted row with custom escape after newline`, async (_t) => {
		const streams = [
			createReadableStream('a,b,c\r\n"d"\r\n'),
			csvParseStream({ escapeChar: "\\" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output[0], ["a", "b", "c"]);
		strictEqual(output[1].length, 1);
	});

	test(`csvParseStream should handle Buffer chunks with custom parser`, async (_t) => {
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

	test(`csvParseStream should handle multiple chunks with custom parser and buffer`, async (_t) => {
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

	test(`csvParseStream should collect errors from custom parser during streaming`, async (_t) => {
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

	test(`csvParseStream should collect errors from custom parser during flush`, async (_t) => {
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

	test(`csvParseStream should collect inline parser errors during streaming`, async (_t) => {
		const streams = [
			createReadableStream(['"a,b\r\n', "1,2\r\n"]),
			csvParseStream(),
		];
		const result = await pipeline(streams);

		ok(result.csvErrors.UnterminatedQuote);
	});

	test(`csvParseStream should join multiple input chunks`, async (_t) => {
		const streams = [
			createReadableStream(["a,b\r\n", "1,2\r\n", "3,4\r\n"]),
			csvParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			["a", "b"],
			["1", "2"],
			["3", "4"],
		]);
	});

	test(`csvCoerceValuesStream should handle uppercase boolean`, async (_t) => {
		const streams = [
			createReadableStream([{ val: "TRUE", val2: "FALSE" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ val: true, val2: false }]);
	});

	test(`csvParseStream should not merge extra columns in fast unquoted path`, async (_t) => {
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

	test(`csvParseStream fast and slow paths agree on over-long rows`, async (_t) => {
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

	test(`csvParseStream over-long rows are detectable as malformed`, async (_t) => {
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

	test(`csvDetectDelimitersStream should not detect apostrophe in data as quote char`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [
			createReadableStream(
				"quote,author\n'twas the night,Moore\nhello,World\n",
			),
			detect,
		];
		await pipeline(streams);

		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvParseStream should parse data with apostrophes after auto-detect`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const parse = csvParseStream({
			delimiterChar: () => detect.result().value.delimiterChar,
			newlineChar: () => detect.result().value.newlineChar,
			quoteChar: () => detect.result().value.quoteChar,
		});
		const streams = [
			createReadableStream(
				"quote,author\n'twas the night,Moore\nhello,World\n",
			),
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

	test(`csvFormatStream/csvParseStream round-trip preserves escape char with custom escapeChar`, async (_t) => {
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

	test(`csvParseStream should treat even run of escape chars as unescaped closing quote`, async (_t) => {
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

	test(`csvDetectHeaderStream should respect quoted newline in header field`, async (_t) => {
		const headers = csvDetectHeaderStream({ newlineChar: "\n" });
		const streams = [
			createReadableStream('"col\nwith newline",col2\nval1,val2\n'),
			headers,
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(headers.result().value.header, [
			"col\nwith newline",
			"col2",
		]);
		strictEqual(output, "val1,val2\n");
	});

	test(`csvDetectHeaderStream should respect custom-escape quoted newline in header`, async (_t) => {
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

	test(`csvCoerceValuesStream should keep invalid date string as string`, async (_t) => {
		const streams = [
			createReadableStream([{ d: "2024-13-99" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ d: "2024-13-99" }]);
	});

	test(`csvArrayToObjectStream should keep a __proto__ header as an own data property`, async (_t) => {
		const streams = [
			createReadableStream([["polluted", "ok"]]),
			csvArrayToObjectStream({ headers: ["__proto__", "safe"] }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(output.length, 1);
		const row = output[0];
		ok(Object.hasOwn(row, "__proto__"));
		// biome-ignore lint/suspicious/noProto: intentionally reading the own data property literally named "__proto__" to assert prototype-pollution safety.
		strictEqual(row.__proto__, "polluted");
		strictEqual(row.safe, "ok");
	});

	test(`csvFormatStream should quote field with leading space`, async (_t) => {
		const streams = [
			createReadableStream([[" lead", "ok"]]),
			csvFormatStream(),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '" lead",ok\r\n');
	});

	test(`csvFormatStream should quote field with trailing space`, async (_t) => {
		const streams = [
			createReadableStream([["trail ", "ok"]]),
			csvFormatStream(),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"trail ",ok\r\n');
	});

	test(`csvFormatStream should quote field with leading BOM`, async (_t) => {
		const streams = [createReadableStream([["﻿bom", "ok"]]), csvFormatStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"﻿bom",ok\r\n');
	});

	test(`csvFormatStream should not quote plain interior space`, async (_t) => {
		const streams = [
			createReadableStream([["a b c", "ok"]]),
			csvFormatStream(),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, "a b c,ok\r\n");
	});

	test(`csvFormatStream should quote field containing carriage return`, async (_t) => {
		const streams = [createReadableStream([["a\rb", "ok"]]), csvFormatStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a\rb",ok\r\n');
	});

	test(`csvFormatStream should quote value containing multi-char delimiter`, async (_t) => {
		const streams = [
			createReadableStream([["a::b", "plain"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a::b"::plain\r\n');
	});

	test(`csvFormatStream should quote value containing a regexp-special delimiter`, async (_t) => {
		const streams = [
			createReadableStream([["a.b", "plain"]]),
			csvFormatStream({ delimiterChar: "." }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a.b".plain\r\n');
	});

	test(`csvFormatStream multi-char delimiter should quote on quote char`, async (_t) => {
		const streams = [
			createReadableStream([['has"quote', "plain"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"has""quote"::plain\r\n');
	});

	test(`csvFormatStream multi-char delimiter should not quote plain value`, async (_t) => {
		const streams = [
			createReadableStream([["plain", "value"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, "plain::value\r\n");
	});

	test(`csvFormatStream custom escape should escape escape char and quote char`, async (_t) => {
		const streams = [
			createReadableStream([['a"b\\c', "ok"]]),
			csvFormatStream({ escapeChar: "\\" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a\\"b\\\\c",ok\r\n');
	});

	test(`csvFormatStream custom escape should escape only escape char when no quote`, async (_t) => {
		const streams = [
			createReadableStream([["a\\b,c", "ok"]]),
			csvFormatStream({ escapeChar: "\\" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a\\\\b,c",ok\r\n');
	});

	test(`csvFormatStream should format Date as ISO string`, async (_t) => {
		const d = new Date("2024-01-15T10:30:00.000Z");
		const streams = [createReadableStream([[d, "ok"]]), csvFormatStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, "2024-01-15T10:30:00.000Z,ok\r\n");
	});

	test(`csvFormatStream should format number via String`, async (_t) => {
		const streams = [createReadableStream([[42, 3.14]]), csvFormatStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, "42,3.14\r\n");
	});

	test(`csvFormatStream should format null and undefined as empty`, async (_t) => {
		const streams = [
			createReadableStream([[null, undefined, "x"]]),
			csvFormatStream(),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, ",,x\r\n");
	});

	test(`csvFormatStream should format boolean via String in slow path`, async (_t) => {
		const streams = [createReadableStream([[true, false]]), csvFormatStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, "true,false\r\n");
	});

	test(`csvFormatStream should quote number-derived string needing quote`, async (_t) => {
		const streams = [createReadableStream([[-42, "ok"]]), csvFormatStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"-42",ok\r\n');
	});

	test(`csvFormatStream should emit batch at 64 rows then flush remainder`, async (_t) => {
		const rows = [];
		for (let i = 0; i < 70; i++) rows.push([String(i), "x"]);
		const streams = [createReadableStream(rows), csvFormatStream()];
		const output = await streamToString(pipejoin(streams));
		const lines = output.split("\r\n").filter((l) => l.length > 0);
		strictEqual(lines.length, 70);
		strictEqual(lines[0], "0,x");
		strictEqual(lines[69], "69,x");
	});

	test(`csvFormatStream should quote values containing a custom newlineChar`, async (_t) => {
		const streams = [
			createReadableStream([["a|b", "c"]]),
			csvFormatStream({ newlineChar: "|" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a|b",c|');
	});

	test(`csvFormatStream should use custom newlineChar`, async (_t) => {
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

	test(`csvFormatStream should use custom quoteChar`, async (_t) => {
		const streams = [
			createReadableStream([["a,b", "ok"]]),
			csvFormatStream({ quoteChar: "'" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, "'a,b',ok\r\n");
	});

	test(`csvFormatStream multi-char delimiter should quote trailing space`, async (_t) => {
		const streams = [
			createReadableStream([["trail ", "ok"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"trail "::ok\r\n');
	});

	test(`csvFormatStream multi-char delimiter should quote on newline`, async (_t) => {
		const streams = [
			createReadableStream([["a\nb", "ok"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a\nb"::ok\r\n');
	});

	test(`csvFormatStream multi-char delimiter should quote on carriage return`, async (_t) => {
		const streams = [
			createReadableStream([["a\rb", "ok"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a\rb"::ok\r\n');
	});

	test(`csvFormatStream single-char delimiter should quote on linefeed only`, async (_t) => {
		const streams = [createReadableStream([["a\nb", "ok"]]), csvFormatStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a\nb",ok\r\n');
	});

	test(`csvCoerceValuesStream should coerce exponent with explicit sign and multi-digit exponent`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "1e+10", b: "2e-05", c: "1.5E3" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 1e10, b: 2e-5, c: 1500 }]);
	});

	test(`csvCoerceValuesStream should keep malformed exponent as string`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "1e", b: "1e+", c: "1.2.3" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "1e", b: "1e+", c: "1.2.3" }]);
	});

	test(`csvCoerceValuesStream should not coerce string with trailing non-number text`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "12abc", d: "2024-01-15extra" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "12abc", d: "2024-01-15extra" }]);
	});

	test(`csvCoerceValuesStream should not coerce date missing leading anchor`, async (_t) => {
		const streams = [
			createReadableStream([{ d: "x2024-01-15" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ d: "x2024-01-15" }]);
	});

	test(`csvCoerceValuesStream should coerce date with time and timezone offset`, async (_t) => {
		const streams = [
			createReadableStream([{ d: "2024-01-15T10:30:00+05:30" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ d: new Date("2024-01-15T10:30:00+05:30") }]);
	});

	test(`csvCoerceValuesStream should not treat 4-char non-true string as boolean`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "trUE", b: "tree", c: "True" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: true, b: "tree", c: true }]);
	});

	test(`csvCoerceValuesStream should not coerce non-t/f boolean-length words`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "yes", b: "nope" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "yes", b: "nope" }]);
	});

	test(`csvCoerceValuesStream should not coerce truthy when length is wrong`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "truer", b: "falsey" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "truer", b: "falsey" }]);
	});

	test(`csvCoerceValuesStream should return null for empty string auto-coerce`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: null }]);
	});

	test(`csvCoerceValuesStream should coerce single-digit zero and nine`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "0", b: "9" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 0, b: 9 }]);
	});

	test(`csvCoerceValuesStream should keep plain non-numeric non-bool string`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "hello", b: "world" }]),
			csvCoerceValuesStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "hello", b: "world" }]);
	});

	test(`csvCoerceValuesStream explicit boolean should lowercase compare string`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "TRUE", b: "no", c: "true" }]),
			csvCoerceValuesStream({
				columns: { a: "boolean", b: "boolean", c: "boolean" },
			}),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: true, b: false, c: true }]);
	});

	test(`csvCoerceValuesStream explicit boolean should use Boolean for non-string`, async (_t) => {
		const streams = [
			createReadableStream([{ a: 1, b: 0 }]),
			csvCoerceValuesStream({ columns: { a: "boolean", b: "boolean" } }),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: true, b: false }]);
	});

	test(`csvCoerceValuesStream explicit json should parse array`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "[1,2,3]" }]),
			csvCoerceValuesStream({ columns: { a: "json" } }),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: [1, 2, 3] }]);
	});

	test(`csvCoerceValuesStream unknown column type should pass value unchanged`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "raw" }]),
			csvCoerceValuesStream({ columns: { a: "string" } }),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: "raw" }]);
	});

	test(`csvCoerceValuesStream explicit number coerces decimal and hex`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "3.14", b: "0x1F" }]),
			csvCoerceValuesStream({ columns: { a: "number", b: "number" } }),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [{ a: 3.14, b: 31 }]);
	});

	test(`csvDetectDelimitersStream should detect quote opening at text start`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("'a',b\n1,2\n"), detect];
		await pipeline(streams);
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream should detect quote after delimiter and closing at line end`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a,'b'\n1,2\n"), detect];
		await pipeline(streams);
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream should detect quote closing before CR`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("'a',b\r\n1,2\r\n"), detect];
		await pipeline(streams);
		strictEqual(detect.result().value.quoteChar, "'");
		strictEqual(detect.result().value.newlineChar, "\r\n");
	});

	test(`csvDetectDelimitersStream should detect quote closing right before LF`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a,'b'\nc,d\n"), detect];
		await pipeline(streams);
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream should NOT detect quote that never closes at a field end`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [
			createReadableStream("name,note\n'x,it's fine\nhello,world\n"),
			detect,
		];
		await pipeline(streams);
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream should set escapeChar equal to detected quoteChar`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("'a','b'\n1,2\n"), detect];
		await pipeline(streams);
		strictEqual(detect.result().value.escapeChar, "'");
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream should pick first delimiter by precedence (tab over comma)`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a\tb,c\n1\t2,3\n"), detect];
		await pipeline(streams);
		strictEqual(detect.result().value.delimiterChar, "\t");
	});

	test(`csvDetectDelimitersStream should pick pipe over semicolon and comma`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a|b;c,d\n1|2;3,4\n"), detect];
		await pipeline(streams);
		strictEqual(detect.result().value.delimiterChar, "|");
	});

	test(`csvDetectDelimitersStream should pick semicolon over comma`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a;b,c\n1;2,3\n"), detect];
		await pipeline(streams);
		strictEqual(detect.result().value.delimiterChar, ";");
	});

	test(`csvDetectDelimitersStream should default to double-quote when no quote present`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a,b,c\n1,2,3\n"), detect];
		await pipeline(streams);
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream should set newlineChar from header match`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream("a,b\r1,2\r"), detect];
		await pipeline(streams);
		strictEqual(detect.result().value.newlineChar, "\r");
	});

	test(`csvQuotedParser establishes numCols from first row`, (_t) => {
		const result = csvQuotedParser("a,b,c\r\n1,2,3\r\n");
		strictEqual(result.numCols, 3);
		deepStrictEqual(result.rows, [
			["a", "b", "c"],
			["1", "2", "3"],
		]);
	});

	test(`csvQuotedParser returns tail for incomplete trailing row`, (_t) => {
		const result = csvQuotedParser("a,b\r\n1,2\r\n3,4");
		deepStrictEqual(result.rows, [
			["a", "b"],
			["1", "2"],
		]);
		strictEqual(result.tail, "3,4");
	});

	test(`csvQuotedParser preserves quoted field with escaped quotes (no escapes flag)`, (_t) => {
		const result = csvQuotedParser('"plain",x\r\n');
		deepStrictEqual(result.rows, [["plain", "x"]]);
	});

	test(`csvQuotedParser keeps doubled-quote escapes`, (_t) => {
		const result = csvQuotedParser('"a""b","c"\r\n');
		deepStrictEqual(result.rows, [['a"b', "c"]]);
	});

	test(`csvQuotedParser handles short quoted row truncating to fi`, (_t) => {
		const result = csvQuotedParser('a,b,c\r\n"d"\r\n');
		deepStrictEqual(result.rows[0], ["a", "b", "c"]);
		strictEqual(result.rows[1].length, 1);
		deepStrictEqual(result.rows[1], ["d"]);
	});

	test(`csvQuotedParser handles multi-char newline length>2`, (_t) => {
		const result = csvQuotedParser(
			"a,b||~c,d||~",
			{ newlineChar: "||~" },
			true,
		);
		deepStrictEqual(result.rows, [
			["a", "b"],
			["c", "d"],
		]);
	});

	test(`csvQuotedParser CRLF requires both chars (lone CR does not split)`, (_t) => {
		const result = csvQuotedParser(
			'"a\rb",c\r\n',
			{ newlineChar: "\r\n" },
			true,
		);
		deepStrictEqual(result.rows, [["a\rb", "c"]]);
	});

	test(`csvQuotedParser tracks unterminated quote error on flush`, (_t) => {
		const result = csvQuotedParser('"abc', {}, true);
		deepStrictEqual(result.rows, [["abc"]]);
		ok(result.errors.UnterminatedQuote);
		deepStrictEqual(result.errors.UnterminatedQuote.idx, [0]);
	});

	test(`csvQuotedParser unterminated quote not flushing returns tail`, (_t) => {
		const result = csvQuotedParser('a,b\r\n"abc', {}, false);
		deepStrictEqual(result.rows, [["a", "b"]]);
		strictEqual(result.tail, '"abc');
	});

	test(`csvQuotedParser custom escape unterminated keeps unescaped value on flush`, (_t) => {
		const result = csvQuotedParser('"a\\"b', { escapeChar: "\\" }, true);
		deepStrictEqual(result.rows, [['a"b']]);
		ok(result.errors.UnterminatedQuote);
	});

	test(`csvQuotedParser custom escape even run is real closing quote`, (_t) => {
		const result = csvQuotedParser('"a\\\\",b\r\n', { escapeChar: "\\" }, true);
		deepStrictEqual(result.rows, [["a\\", "b"]]);
	});

	test(`csvQuotedParser custom escape odd run escapes the quote`, (_t) => {
		const result = csvQuotedParser('"a\\"b",c\r\n', { escapeChar: "\\" }, true);
		deepStrictEqual(result.rows, [['a"b', "c"]]);
	});

	test(`csvParseStream should throw when quoted field exceeds maxFieldSize`, async (_t) => {
		const val = "x".repeat(11);
		const streams = [
			createReadableStream(`"${val}",y\r\n`),
			csvParseStream({ maxFieldSize: 10 }),
		];
		await rejects(pipeline(streams), {
			name: "RangeError",
			message: "CSV field size (11) exceeds maxFieldSize (10 bytes)",
		});
	});

	test(`csvParseStream should throw when an unquoted field exceeds maxFieldSize`, async (_t) => {
		const val = "x".repeat(11);
		for (const input of [`${val},y\r\n`, `y,${val}\r\n`, `y,${val}`]) {
			await rejects(
				pipeline([
					createReadableStream(input),
					csvParseStream({ maxFieldSize: 10 }),
				]),
				{
					name: "RangeError",
					message: "CSV field size (11) exceeds maxFieldSize (10 bytes)",
				},
				input,
			);
		}
	});

	test(`csvParseStream should accept unquoted fields at exactly maxFieldSize`, async (_t) => {
		const val = "x".repeat(10);
		const output = await streamToArray(
			pipejoin([
				createReadableStream(`${val},y\r\ny,${val}\r\ny,${val}`),
				csvParseStream({ maxFieldSize: 10 }),
			]),
		);
		deepStrictEqual(output, [
			[val, "y"],
			["y", val],
			["y", val],
		]);
	});

	test(`csvParseStream should accept quoted field at exactly maxFieldSize`, async (_t) => {
		const val = "x".repeat(10);
		const streams = [
			createReadableStream(`"${val}",y\r\n`),
			csvParseStream({ maxFieldSize: 10 }),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [[val, "y"]]);
	});

	test(`csvParseStream should throw when custom-escape quoted field exceeds maxFieldSize`, async (_t) => {
		const val = "x".repeat(11);
		const streams = [
			createReadableStream(`"${val}",y\r\n`),
			csvParseStream({ escapeChar: "\\", maxFieldSize: 10 }),
		];
		await rejects(pipeline(streams), {
			name: "RangeError",
			message: "CSV field size (11) exceeds maxFieldSize (10 bytes)",
		});
	});

	test(`csvParseStream should throw on buffer exceeding safety limit`, async (_t) => {
		const big = `"${"x".repeat(60)}`;
		const streams = [
			createReadableStream(big),
			csvParseStream({ maxFieldSize: 10 }),
		];
		await rejects(pipeline(streams), {
			name: "RangeError",
			message:
				"CSV buffer size (61) exceeds safety limit, likely unterminated quoted field",
		});
	});

	test(`csvParseStream maxFieldSize null disables the field and buffer limits`, async (_t) => {
		const val = "x".repeat(33_554_433);
		const output = await streamToArray(
			pipejoin([
				createReadableStream([`"${val}`, `",y\r\n`]),
				csvParseStream({ maxFieldSize: null }),
			]),
		);
		deepStrictEqual(output, [[val, "y"]]);
	});

	test(`csvParseStream maxFieldSize undefined uses the 16MiB default`, async (_t) => {
		const val = "x".repeat(16_777_217);
		await rejects(
			pipeline([
				createReadableStream(`"${val}",y\r\n`),
				csvParseStream({ maxFieldSize: undefined }),
			]),
			{
				name: "RangeError",
				message:
					"CSV field size (16777217) exceeds maxFieldSize (16777216 bytes)",
			},
		);
	});

	test(`csvParseStream fast path keeps too-few-column row as-is`, async (_t) => {
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

	test(`csvParseStream fast path keeps multiple full rows`, async (_t) => {
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

	test(`csvParseStream fast path partial last row without newline`, async (_t) => {
		const streams = [
			createReadableStream("a,b\r\n1,2\r\n3,4"),
			csvParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [
			["a", "b"],
			["1", "2"],
			["3", "4"],
		]);
	});

	test(`csvParseStream supports 2-char custom newline`, async (_t) => {
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

	test(`csvParseStream 2-char newline does not split on partial match`, async (_t) => {
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

	test(`csvUnquotedParser sets numCols and parses many rows`, (_t) => {
		const result = csvUnquotedParser("a,b\r\n1,2\r\n3,4\r\n");
		strictEqual(result.numCols, 2);
		deepStrictEqual(result.rows, [
			["a", "b"],
			["1", "2"],
			["3", "4"],
		]);
	});

	test(`csvUnquotedParser custom delimiter and newline`, (_t) => {
		const result = csvUnquotedParser("a;b\nc;d\n", {
			delimiterChar: ";",
			newlineChar: "\n",
		});
		deepStrictEqual(result.rows, [
			["a", "b"],
			["c", "d"],
		]);
	});

	test(`csvDetectHeaderStream multi-char delimiter splits header`, async (_t) => {
		const headers = csvDetectHeaderStream({
			newlineChar: "\n",
			delimiterChar: "::",
		});
		const streams = [createReadableStream("a::b::c\n1::2::3\n"), headers];
		const output = await streamToString(pipejoin(streams));
		deepStrictEqual(headers.result().value.header, ["a", "b", "c"]);
		strictEqual(output, "1::2::3\n");
	});

	test(`csvDetectHeaderStream respects delimiter inside quoted header field`, async (_t) => {
		const headers = csvDetectHeaderStream({ newlineChar: "\n" });
		const streams = [createReadableStream('"a,b",c\n1,2\n'), headers];
		const output = await streamToString(pipejoin(streams));
		deepStrictEqual(headers.result().value.header, ["a,b", "c"]);
		strictEqual(output, "1,2\n");
	});

	test(`csvDetectHeaderStream custom-escape even run closes header quote`, async (_t) => {
		const headers = csvDetectHeaderStream({
			newlineChar: "\n",
			escapeChar: "\\",
		});
		const streams = [createReadableStream('"a\\\\",b\nv1,v2\n'), headers];
		const output = await streamToString(pipejoin(streams));
		deepStrictEqual(headers.result().value.header, ["a\\", "b"]);
		strictEqual(output, "v1,v2\n");
	});

	test(`csvDetectHeaderStream passes through data after a quoted header newline`, async (_t) => {
		const headers = csvDetectHeaderStream({ newlineChar: "\n" });
		const streams = [
			createReadableStream('"h1\nstill h1",h2\nr1a,r1b\nr2a,r2b\n'),
			headers,
		];
		const output = await streamToString(pipejoin(streams));
		deepStrictEqual(headers.result().value.header, ["h1\nstill h1", "h2"]);
		strictEqual(output, "r1a,r1b\nr2a,r2b\n");
	});

	test(`csvDetectHeaderStream CRLF header newline length`, async (_t) => {
		const headers = csvDetectHeaderStream({ newlineChar: "\r\n" });
		const streams = [createReadableStream("a,b,c\r\n1,2,3\r\n"), headers];
		const output = await streamToString(pipejoin(streams));
		deepStrictEqual(headers.result().value.header, ["a", "b", "c"]);
		strictEqual(output, "1,2,3\r\n");
	});

	test(`csvDetectHeaderStream emits remaining rest only when non-empty`, async (_t) => {
		const headers = csvDetectHeaderStream({ newlineChar: "\n" });
		const streams = [createReadableStream("only,header\n"), headers];
		const output = await streamToString(pipejoin(streams));
		deepStrictEqual(headers.result().value.header, ["only", "header"]);
		strictEqual(output, "");
	});

	test(`csvRemoveMalformedRowsStream default resultKey and message`, async (_t) => {
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

	test(`csvRemoveMalformedRowsStream first row sets count, later mismatch dropped`, async (_t) => {
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

	test(`csvRemoveEmptyRowsStream default resultKey and message`, async (_t) => {
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

	test(`csvRemoveEmptyRowsStream keeps row with one non-empty field`, async (_t) => {
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

	test(`csvRemoveEmptyRowsStream keeps row whose only non-empty field is last`, async (_t) => {
		const filter = csvRemoveEmptyRowsStream();
		const streams = [createReadableStream([["", "", "z"]]), filter];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [["", "", "z"]]);
		deepStrictEqual(filter.result().value, {});
	});

	test(`csvInjectHeaderStream injects header exactly once`, async (_t) => {
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

	test(`csvParseStream single input chunk processes immediately`, async (_t) => {
		const streams = [
			createReadableStream(["a,b,c,d,e\r\n", "1,2,3,4,5\r\n"]),
			csvParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [
			["a", "b", "c", "d", "e"],
			["1", "2", "3", "4", "5"],
		]);
	});

	test(`csvParseStream partial row joins on flush`, async (_t) => {
		const streams = [
			createReadableStream(["a,", "b\r\n", "1,", "2\r\n"]),
			csvParseStream(),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [
			["a", "b"],
			["1", "2"],
		]);
	});

	test(`csvParseStream default resultKey is csvErrors`, async (_t) => {
		const streams = [createReadableStream("a,b\r\n"), csvParseStream()];
		const result = await pipeline(streams);
		strictEqual(streams[1].result().key, "csvErrors");
		deepStrictEqual(result.csvErrors, {});
	});

	test(`csvFormatStream multi-char delimiter quotes leading equals`, async (_t) => {
		const streams = [
			createReadableStream([["=x", "ok"]]),
			csvFormatStream({ delimiterChar: "::", escapeFormulae: false }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"=x"::ok\r\n');
	});

	test(`csvFormatStream multi-char delimiter quotes leading plus`, async (_t) => {
		const streams = [
			createReadableStream([["+x", "ok"]]),
			csvFormatStream({ delimiterChar: "::", escapeFormulae: false }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"+x"::ok\r\n');
	});

	test(`csvFormatStream multi-char delimiter quotes leading minus`, async (_t) => {
		const streams = [
			createReadableStream([["-x", "ok"]]),
			csvFormatStream({ delimiterChar: "::", escapeFormulae: false }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"-x"::ok\r\n');
	});

	test(`csvFormatStream multi-char delimiter quotes leading at-sign`, async (_t) => {
		const streams = [
			createReadableStream([["@x", "ok"]]),
			csvFormatStream({ delimiterChar: "::", escapeFormulae: false }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"@x"::ok\r\n');
	});

	test(`csvFormatStream multi-char delimiter quotes leading space`, async (_t) => {
		const streams = [
			createReadableStream([[" x", "ok"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '" x"::ok\r\n');
	});

	test(`csvFormatStream multi-char delimiter quotes leading BOM`, async (_t) => {
		const streams = [
			createReadableStream([["﻿x", "ok"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"﻿x"::ok\r\n');
	});

	test(`csvFormatStream multi-char delimiter does NOT quote plain leading char`, async (_t) => {
		const streams = [
			createReadableStream([["xyz", "ok"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		strictEqual(await streamToString(pipejoin(streams)), "xyz::ok\r\n");
	});

	test(`csvFormatStream multi-char delimiter quotes trailing space`, async (_t) => {
		const streams = [
			createReadableStream([["trail ", "ok"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"trail "::ok\r\n');
	});

	test(`csvFormatStream multi-char delimiter quotes on newline`, async (_t) => {
		const a = await streamToString(
			pipejoin([
				createReadableStream([["a\nb", "ok"]]),
				csvFormatStream({ delimiterChar: "::" }),
			]),
		);
		strictEqual(a, '"a\nb"::ok\r\n');
	});

	test(`csvFormatStream multi-char delimiter quotes on carriage return`, async (_t) => {
		const b = await streamToString(
			pipejoin([
				createReadableStream([["a\rb", "ok"]]),
				csvFormatStream({ delimiterChar: "::" }),
			]),
		);
		strictEqual(b, '"a\rb"::ok\r\n');
	});

	test(`csvFormatStream single-char delimiter quotes leading at-sign`, async (_t) => {
		const streams = [
			createReadableStream([["@x", "ok"]]),
			csvFormatStream({ escapeFormulae: false }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"@x",ok\r\n');
	});

	test(`csvFormatStream single-char delimiter quotes leading plus`, async (_t) => {
		const streams = [
			createReadableStream([["+x", "ok"]]),
			csvFormatStream({ escapeFormulae: false }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"+x",ok\r\n');
	});

	test(`csvFormatStream single-char delimiter quotes leading minus`, async (_t) => {
		const streams = [
			createReadableStream([["-x", "ok"]]),
			csvFormatStream({ escapeFormulae: false }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"-x",ok\r\n');
	});

	test(`csvFormatStream single-char delimiter quotes leading equals`, async (_t) => {
		const streams = [
			createReadableStream([["=x", "ok"]]),
			csvFormatStream({ escapeFormulae: false }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"=x",ok\r\n');
	});

	test(`csvFormatStream single-char delimiter does NOT quote plain value`, async (_t) => {
		const streams = [createReadableStream([["xyz", "ok"]]), csvFormatStream()];
		strictEqual(await streamToString(pipejoin(streams)), "xyz,ok\r\n");
	});

	test(`csvFormatStream single-char delimiter quotes trailing space`, async (_t) => {
		const streams = [
			createReadableStream([["trail ", "ok"]]),
			csvFormatStream(),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"trail ",ok\r\n');
	});

	test(`csvFormatStream should quote field with leading BOM`, async (_t) => {
		const streams = [createReadableStream([["﻿bom", "ok"]]), csvFormatStream()];
		strictEqual(await streamToString(pipejoin(streams)), '"﻿bom",ok\r\n');
	});

	test(`csvFormatStream should quote field with carriage return`, async (_t) => {
		const streams = [createReadableStream([["a\rb", "ok"]]), csvFormatStream()];
		strictEqual(await streamToString(pipejoin(streams)), '"a\rb",ok\r\n');
	});

	test(`csvFormatStream should format Date as ISO string`, async (_t) => {
		const d = new Date("2024-01-15T10:30:00.000Z");
		const streams = [createReadableStream([[d, "ok"]]), csvFormatStream()];
		strictEqual(
			await streamToString(pipejoin(streams)),
			"2024-01-15T10:30:00.000Z,ok\r\n",
		);
	});

	test(`csvFormatStream should format number via String`, async (_t) => {
		const streams = [createReadableStream([[42, 3.14]]), csvFormatStream()];
		strictEqual(await streamToString(pipejoin(streams)), "42,3.14\r\n");
	});

	test(`csvFormatStream should format boolean via String in slow path`, async (_t) => {
		const streams = [createReadableStream([[true, false]]), csvFormatStream()];
		strictEqual(await streamToString(pipejoin(streams)), "true,false\r\n");
	});

	test(`csvFormatStream should quote negative number string`, async (_t) => {
		const streams = [createReadableStream([[-42, "ok"]]), csvFormatStream()];
		strictEqual(await streamToString(pipejoin(streams)), '"-42",ok\r\n');
	});

	test(`csvFormatStream should format null and undefined as empty`, async (_t) => {
		const streams = [
			createReadableStream([[null, undefined, "x"]]),
			csvFormatStream(),
		];
		strictEqual(await streamToString(pipejoin(streams)), ",,x\r\n");
	});

	test(`csvFormatStream custom escape escapes escape and quote chars`, async (_t) => {
		const streams = [
			createReadableStream([['a"b\\c', "ok"]]),
			csvFormatStream({ escapeChar: "\\" }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"a\\"b\\\\c",ok\r\n');
	});

	test(`csvFormatStream custom escape escapes only escape char when no quote`, async (_t) => {
		const streams = [
			createReadableStream([["a\\b,c", "ok"]]),
			csvFormatStream({ escapeChar: "\\" }),
		];
		strictEqual(await streamToString(pipejoin(streams)), '"a\\\\b,c",ok\r\n');
	});

	test(`csvFormatStream batch boundary flushes at 64 rows`, async (_t) => {
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

	test(`csvFormatStream custom newlineChar separator`, async (_t) => {
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

	test(`csvFormatStream custom quoteChar`, async (_t) => {
		const output = await streamToString(
			pipejoin([
				createReadableStream([["a,b", "ok"]]),
				csvFormatStream({ quoteChar: "'" }),
			]),
		);
		strictEqual(output, "'a,b',ok\r\n");
	});

	test(`csvQuotedParser quoted field then LF newline (length 1)`, (_t) => {
		const result = csvQuotedParser('"a"\n"b"\n', { newlineChar: "\n" }, true);
		deepStrictEqual(result.rows, [["a"], ["b"]]);
	});

	test(`csvQuotedParser quoted field then CRLF newline (length 2)`, (_t) => {
		const result = csvQuotedParser(
			'"a"\r\n"b"\r\n',
			{ newlineChar: "\r\n" },
			true,
		);
		deepStrictEqual(result.rows, [["a"], ["b"]]);
	});

	test(`csvQuotedParser quoted field then lone CR is not a CRLF newline`, (_t) => {
		const result = csvQuotedParser(
			'"a"\rb,c\r\n',
			{ newlineChar: "\r\n" },
			true,
		);
		deepStrictEqual(result.rows, [["a\rb", "c"]]);
		deepStrictEqual(result.errors.UnexpectedQuote.idx, [0]);
	});

	test(`csvQuotedParser quoted field then 3-char newline (length > 2)`, (_t) => {
		const result = csvQuotedParser(
			'"a"~|~"b"~|~',
			{ newlineChar: "~|~" },
			true,
		);
		deepStrictEqual(result.rows, [["a"], ["b"]]);
	});

	test(`csvQuotedParser quoted field then partial 3-char newline is garbage`, (_t) => {
		const result = csvQuotedParser('"a"~|x~|~', { newlineChar: "~|~" }, true);
		deepStrictEqual(result.rows, [["a~|x"]]);
		deepStrictEqual(result.errors.UnexpectedQuote.idx, [0]);
	});

	test(`csvQuotedParser custom escape quoted field then LF newline`, (_t) => {
		const result = csvQuotedParser(
			'"a"\n"b"\n',
			{ newlineChar: "\n", escapeChar: "\\" },
			true,
		);
		deepStrictEqual(result.rows, [["a"], ["b"]]);
	});

	test(`csvQuotedParser custom escape quoted field then CRLF newline`, (_t) => {
		const result = csvQuotedParser(
			'"a"\r\n"b"\r\n',
			{ newlineChar: "\r\n", escapeChar: "\\" },
			true,
		);
		deepStrictEqual(result.rows, [["a"], ["b"]]);
	});

	test(`csvQuotedParser custom escape quoted field then 3-char newline`, (_t) => {
		const result = csvQuotedParser(
			'"a"~|~"b"~|~',
			{ newlineChar: "~|~", escapeChar: "\\" },
			true,
		);
		deepStrictEqual(result.rows, [["a"], ["b"]]);
	});

	test(`csvQuotedParser custom escape quoted field then lone CR not CRLF`, (_t) => {
		const result = csvQuotedParser(
			'"a"\rb,c\r\n',
			{ newlineChar: "\r\n", escapeChar: "\\" },
			true,
		);
		deepStrictEqual(result.rows, [["a\rb", "c"]]);
		deepStrictEqual(result.errors.UnexpectedQuote.idx, [0]);
	});

	test(`csvCoerceValuesStream should coerce date with multi-digit fractional seconds`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ d: "2024-01-15T10:30:00.123Z" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ d: new Date("2024-01-15T10:30:00.123Z") }]);
	});

	test(`csvCoerceValuesStream should coerce date with timezone offset without colon`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ d: "2024-01-15T10:30:00+0530" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ d: new Date("2024-01-15T10:30:00+0530") }]);
	});

	test(`csvCoerceValuesStream should coerce date with timezone offset with colon`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ d: "2024-01-15T10:30:00+05:30" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ d: new Date("2024-01-15T10:30:00+05:30") }]);
	});

	test(`csvCoerceValuesStream should coerce date with space separator and seconds`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ d: "2024-01-15 10:30:45" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ d: new Date("2024-01-15 10:30:45") }]);
	});

	test(`csvCoerceValuesStream should coerce plain date-only value`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ d: "2024-12-31" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ d: new Date("2024-12-31") }]);
	});

	test(`csvCoerceValuesStream should keep date with non-digit fractional as string`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ d: "2024-01-15T10:30:00.abc" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ d: "2024-01-15T10:30:00.abc" }]);
	});

	test(`csvCoerceValuesStream should coerce exponent forms`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "1e+10", b: "2e-05", c: "1.5E3" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ a: 1e10, b: 2e-5, c: 1500 }]);
	});

	test(`csvCoerceValuesStream should keep malformed exponent as string`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "1e", b: "1e+", c: "1.2.3" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ a: "1e", b: "1e+", c: "1.2.3" }]);
	});

	test(`csvCoerceValuesStream should not coerce trailing non-number text`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "12abc", d: "2024-01-15extra" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ a: "12abc", d: "2024-01-15extra" }]);
	});

	test(`csvCoerceValuesStream should not coerce date missing leading anchor`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ d: "x2024-01-15" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ d: "x2024-01-15" }]);
	});

	test(`csvCoerceValuesStream boolean first-char/length gating`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "trUE", b: "tree", c: "True", d: "truer" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ a: true, b: "tree", c: true, d: "truer" }]);
	});

	test(`csvCoerceValuesStream should return null for empty string auto-coerce`, async (_t) => {
		const output = await streamToArray(
			pipejoin([createReadableStream([{ a: "" }]), csvCoerceValuesStream()]),
		);
		deepStrictEqual(output, [{ a: null }]);
	});

	test(`csvCoerceValuesStream should coerce single-digit zero and nine`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "0", b: "9" }]),
				csvCoerceValuesStream(),
			]),
		);
		deepStrictEqual(output, [{ a: 0, b: 9 }]);
	});

	test(`csvCoerceValuesStream explicit boolean lowercase compare`, async (_t) => {
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

	test(`csvCoerceValuesStream explicit boolean Boolean for non-string`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ a: 1, b: 0 }]),
				csvCoerceValuesStream({ columns: { a: "boolean", b: "boolean" } }),
			]),
		);
		deepStrictEqual(output, [{ a: true, b: false }]);
	});

	test(`csvCoerceValuesStream explicit json parses array`, async (_t) => {
		const output = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "[1,2,3]" }]),
				csvCoerceValuesStream({ columns: { a: "json" } }),
			]),
		);
		deepStrictEqual(output, [{ a: [1, 2, 3] }]);
	});

	test(`csvDetectDelimitersStream detects quote closing at end of text`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("x,'y'"), detect]);
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream detects quote closing before CR-only newline`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("a,'b'\rc,d\r"), detect]);
		strictEqual(detect.result().value.quoteChar, "'");
		strictEqual(detect.result().value.newlineChar, "\r");
	});

	test(`csvDetectDelimitersStream detects quote closing before a delimiter`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("'a',b,c\n1,2,3\n"), detect]);
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream detects quote opening at text start`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("'a',b\n1,2\n"), detect]);
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream ignores a quote at index 1 that is not at a field start`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("a'b',c\n1,2\n"), detect]);
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream ignores a quote at index 1 that does not close a field`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("''a,b\n1,2\n"), detect]);
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream ignores a quote that only brackets a field after the first line`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("a,b\n'c',d\n"), detect]);
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream does NOT detect a quote that never closes at a field end`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([
			createReadableStream("name,note\n'x,it's fine\nhello,world\n"),
			detect,
		]);
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream sets escapeChar equal to detected quoteChar`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("'a','b'\n1,2\n"), detect]);
		strictEqual(detect.result().value.escapeChar, "'");
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream picks tab over comma by precedence`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("a\tb,c\n1\t2,3\n"), detect]);
		strictEqual(detect.result().value.delimiterChar, "\t");
	});

	test(`csvDetectDelimitersStream picks pipe over semicolon and comma`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("a|b;c,d\n1|2;3,4\n"), detect]);
		strictEqual(detect.result().value.delimiterChar, "|");
	});

	test(`csvDetectDelimitersStream picks semicolon over comma`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("a;b,c\n1;2,3\n"), detect]);
		strictEqual(detect.result().value.delimiterChar, ";");
	});

	test(`csvDetectDelimitersStream defaults to double-quote when none present`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await pipeline([createReadableStream("a,b,c\n1,2,3\n"), detect]);
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvFormatStream should format null field alongside a number (formatRowSlow null branch)`, async (_t) => {
		const streams = [createReadableStream([[null, 42]]), csvFormatStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, ",42\r\n");
	});

	test(`csvFormatStream should format empty-string field alongside a quoting-required value (formatRowSlow empty-string branch)`, async (_t) => {
		const streams = [
			createReadableStream([["hello, world", ""]]),
			csvFormatStream(),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"hello, world",\r\n');
	});

	test(`csvCoerceValuesStream explicit date type should return original string for invalid date`, async (_t) => {
		const streams = [
			createReadableStream([{ val: "not-a-date" }]),
			csvCoerceValuesStream({ columns: { val: "date" } }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [{ val: "not-a-date" }]);
	});

	test(`csvFormatStream custom escape should quote value with delimiter but no escape char`, async (_t) => {
		const streams = [
			createReadableStream([["a,b", "ok"]]),
			csvFormatStream({ escapeChar: "\\" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a,b",ok\r\n');
	});

	test(`csvQuotedParser reports idx equal to number of rows (default path)`, (_t) => {
		const result = csvQuotedParser("a,b\r\nc,d\r\ne,f\r\n");
		strictEqual(result.idx, 3);
		strictEqual(result.numCols, 2);
	});

	test(`csvQuotedParser reports idx for quoted-field rows`, (_t) => {
		const result = csvQuotedParser('"a",b\r\n"c",d\r\n"e",f\r\n');
		strictEqual(result.idx, 3);
		deepStrictEqual(result.rows, [
			["a", "b"],
			["c", "d"],
			["e", "f"],
		]);
	});

	test(`csvQuotedParser reports idx for custom-escape quoted rows`, (_t) => {
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

	test(`csvQuotedParser flush short row truncates to fi (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('a,b,c\r\n"d"', {}, true);
		strictEqual(result.numCols, 3);
		strictEqual(result.idx, 2);
		deepStrictEqual(result.rows[1], ["d"]);
		strictEqual(result.rows[1].length, 1);
	});

	test(`csvQuotedParser flush short row truncates to fi (custom escape)`, (_t) => {
		const result = csvQuotedParser('a,b,c\r\n"d"', { escapeChar: "\\" }, true);
		strictEqual(result.numCols, 3);
		strictEqual(result.idx, 2);
		deepStrictEqual(result.rows[1], ["d"]);
		strictEqual(result.rows[1].length, 1);
	});

	test(`csvQuotedParser unterminated quote short row truncates to fi (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('a,b,c\r\n"unterminated', {}, true);
		strictEqual(result.numCols, 3);
		strictEqual(result.idx, 2);
		deepStrictEqual(result.rows[1], ["unterminated"]);
		strictEqual(result.rows[1].length, 1);
		deepStrictEqual(result.errors.UnterminatedQuote.idx, [1]);
	});

	test(`csvQuotedParser unterminated quote short row truncates to fi (custom escape)`, (_t) => {
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

	test(`csvQuotedParser unterminated quote establishes numCols when first (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('"unterminated', {}, true);
		strictEqual(result.numCols, 1);
		strictEqual(result.idx, 1);
		deepStrictEqual(result.rows, [["unterminated"]]);
	});

	test(`csvQuotedParser unterminated quote establishes numCols when first (custom escape)`, (_t) => {
		const result = csvQuotedParser('"unterminated', { escapeChar: "\\" }, true);
		strictEqual(result.numCols, 1);
		strictEqual(result.idx, 1);
		deepStrictEqual(result.rows, [["unterminated"]]);
	});

	test(`csvQuotedParser unterminated quote not flushing returns rowStart tail (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('a,b\r\n"unterm', {}, false);
		strictEqual(result.tail, '"unterm');
		strictEqual(result.idx, 1);
	});

	test(`csvQuotedParser unterminated quote not flushing returns rowStart tail (custom escape)`, (_t) => {
		const result = csvQuotedParser(
			'a,b\r\nx,"unterm',
			{ escapeChar: "\\" },
			false,
		);
		strictEqual(result.tail, 'x,"unterm');
		strictEqual(result.idx, 1);
	});

	test(`csvQuotedParser quoted first field establishes numCols (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('"a","b"\r\nc,d\r\n');
		strictEqual(result.numCols, 2);
		strictEqual(result.idx, 2);
		deepStrictEqual(result.rows, [
			["a", "b"],
			["c", "d"],
		]);
	});

	test(`csvQuotedParser quoted first field establishes numCols (custom escape)`, (_t) => {
		const result = csvQuotedParser('"a","b"\r\nc,d\r\n', { escapeChar: "\\" });
		strictEqual(result.numCols, 2);
		strictEqual(result.idx, 2);
		deepStrictEqual(result.rows, [
			["a", "b"],
			["c", "d"],
		]);
	});

	test(`csvQuotedParser quoted-field row after established numCols truncates surplus (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('"a","b"\r\n"c"\r\n');
		strictEqual(result.rows[1].length, 1);
		deepStrictEqual(result.rows[1], ["c"]);
		strictEqual(result.idx, 2);
	});

	test(`csvQuotedParser keeps garbage after closing quote in the same field (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('"a"x,b\r\n');
		deepStrictEqual(result.rows, [["ax", "b"]]);
		deepStrictEqual(result.errors, {
			UnexpectedQuote: {
				id: "UnexpectedQuote",
				message: "Unexpected text after closing quote",
				idx: [0],
			},
		});
	});

	test(`csvQuotedParser keeps garbage after closing quote in the same field (custom escape)`, (_t) => {
		const result = csvQuotedParser('"a"x,b\r\n', { escapeChar: "\\" });
		deepStrictEqual(result.rows, [["ax", "b"]]);
		deepStrictEqual(result.errors, {
			UnexpectedQuote: {
				id: "UnexpectedQuote",
				message: "Unexpected text after closing quote",
				idx: [0],
			},
		});
	});

	test(`csvQuotedParser quoted field at exact end of input keeps field (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('a,"b"', {}, true);
		deepStrictEqual(result.rows, [["a", "b"]]);
		strictEqual(result.idx, 1);
	});

	test(`csvQuotedParser quoted field at exact end of input keeps field (custom escape)`, (_t) => {
		const result = csvQuotedParser('a,"b"', { escapeChar: "\\" }, true);
		deepStrictEqual(result.rows, [["a", "b"]]);
		strictEqual(result.idx, 1);
	});

	test(`csvQuotedParser quoted field then delimiter then trailing flush field (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('"a",', {}, true);
		deepStrictEqual(result.rows, [["a", ""]]);
		strictEqual(result.idx, 1);
	});

	test(`csvQuotedParser quoted field then delimiter then trailing flush field (custom escape)`, (_t) => {
		const result = csvQuotedParser('"a",', { escapeChar: "\\" }, true);
		deepStrictEqual(result.rows, [["a", ""]]);
		strictEqual(result.idx, 1);
	});

	test(`csvQuotedParser quoted field then CR without LF is garbage not newline (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('"a"\r', { newlineChar: "\r\n" }, true);
		deepStrictEqual(result.rows, [["a\r"]]);
		deepStrictEqual(result.errors.UnexpectedQuote.idx, [0]);
	});

	test(`csvQuotedParser quoted field then CR without LF is garbage not newline (custom escape)`, (_t) => {
		const result = csvQuotedParser(
			'"a"\r',
			{ newlineChar: "\r\n", escapeChar: "\\" },
			true,
		);
		deepStrictEqual(result.rows, [["a\r"]]);
		deepStrictEqual(result.errors.UnexpectedQuote.idx, [0]);
	});

	test(`csvQuotedParser fast unquoted path reports idx for many rows`, (_t) => {
		const result = csvQuotedParser("a,b\r\n1,2\r\n3,4\r\n5,6\r\n7,8\r\n");
		strictEqual(result.idx, 5);
		strictEqual(result.numCols, 2);
		deepStrictEqual(result.rows[4], ["7", "8"]);
	});

	test(`csvQuotedParser fast path malformed too-few columns split fallback`, (_t) => {
		const result = csvQuotedParser("a,b,c\r\n1,2\r\n");
		deepStrictEqual(result.rows[1], ["1", "2"]);
		strictEqual(result.rows[1].length, 2);
	});

	test(`csvQuotedParser fast path surplus columns kept separate via split`, (_t) => {
		const result = csvQuotedParser("a,b\r\n1,2,3,4\r\n");
		deepStrictEqual(result.rows[1], ["1", "2", "3", "4"]);
		strictEqual(result.rows[1].length, 4);
	});

	test(`csvQuotedParser fast path exact column count keeps fields`, (_t) => {
		const result = csvQuotedParser("a,b,c\r\n1,2,3\r\n4,5,6\r\n");
		deepStrictEqual(result.rows[1], ["1", "2", "3"]);
		deepStrictEqual(result.rows[2], ["4", "5", "6"]);
		strictEqual(result.idx, 3);
	});

	test(`csvQuotedParser fast path partial last row without newline returns tail`, (_t) => {
		const result = csvQuotedParser("a,b\r\n1,2\r\n3,4", {}, false);
		strictEqual(result.tail, "3,4");
		strictEqual(result.idx, 2);
	});

	test(`csvUnquotedParser reports idx and numCols`, (_t) => {
		const result = csvUnquotedParser("a,b\r\n1,2\r\n3,4\r\n");
		strictEqual(result.idx, 3);
		strictEqual(result.numCols, 2);
	});

	test(`csvUnquotedParser flush increments idx for trailing row`, (_t) => {
		const result = csvUnquotedParser("a,b\r\n1,2", {}, true);
		strictEqual(result.idx, 2);
		strictEqual(result.numCols, 2);
	});

	test(`csvUnquotedParser preserves explicit idx and numCols options`, (_t) => {
		const result = csvUnquotedParser("1,2\r\n", { idx: 5, numCols: 2 });
		strictEqual(result.idx, 6);
		strictEqual(result.numCols, 2);
	});

	test(`csvUnquotedParser no trailing row when input ends with newline`, (_t) => {
		const result = csvUnquotedParser("a,b\r\n", {}, true);
		strictEqual(result.idx, 1);
		deepStrictEqual(result.rows, [["a", "b"]]);
	});

	test(`csvParseStream reports unterminated quote idx after several rows`, async (_t) => {
		const streams = [
			createReadableStream('a,b\r\nc,d\r\ne,f\r\n"unterminated'),
			csvParseStream(),
		];
		const result = await pipeline(streams);
		deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [3]);
	});

	test(`csvParseStream reports unterminated quote idx after several rows (custom escape)`, async (_t) => {
		const streams = [
			createReadableStream('a,b\r\nc,d\r\ne,f\r\nx,"unterminated'),
			csvParseStream({ escapeChar: "\\" }),
		];
		const result = await pipeline(streams);
		deepStrictEqual(result.csvErrors.UnterminatedQuote.idx, [3]);
	});

	test(`csvDetectHeaderStream handles quoted header with escaped quote and embedded newline`, async (_t) => {
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

	test(`csvDetectHeaderStream custom-escape header skips escaped quote in quoted field`, async (_t) => {
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

	test(`csvDetectHeaderStream custom-escape even run closes quote in findRowEnd`, async (_t) => {
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

	test(`csvDetectHeaderStream delimiter inside header advances fieldStart`, async (_t) => {
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

	test(`csvDetectHeaderStream multi-char delimiter inside findRowEnd`, async (_t) => {
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

	test(`csvDetectDelimitersStream detects on the first complete line`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream(["a;b\r\n", "c;d\r\n"]), detect];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);
		strictEqual(detect.result().value.delimiterChar, ";");
		strictEqual(output, "a;b\r\nc;d\r\n");
	});

	test(`csvDetectDelimitersStream emits buffered content exactly once`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [
			createReadableStream(["a|b\r\n", "rest1\r\n", "rest2\r\n"]),
			detect,
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);
		strictEqual(detect.result().value.delimiterChar, "|");
		strictEqual(output, "a|b\r\nrest1\r\nrest2\r\n");
	});

	test(`csvDetectDelimitersStream detect returns true sets detected so later chunks pass through`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream(["a,b\r\n", "later\r\n"]), detect];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);
		strictEqual(output, "a,b\r\nlater\r\n");
	});

	test(`csvParseStream custom-escape unescapeCustom handles escaped escape and escaped quote`, async (_t) => {
		const streams = [
			createReadableStream('"\\\\x\\"y",z\r\n'),
			csvParseStream({ escapeChar: "\\" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [['\\x"y', "z"]]);
	});

	test(`csvParseStream custom-escape escaped quote inside field unescaped once`, async (_t) => {
		const streams = [
			createReadableStream('"a\\"b\\"c",d\r\n'),
			csvParseStream({ escapeChar: "\\" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [['a"b"c', "d"]]);
	});

	test(`csvParseStream switches off fast path when a quote appears in later row`, async (_t) => {
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

	test(`csvParseStream first row with quote does not use split detection`, async (_t) => {
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

	test(`csvParseStream quoted first field then plain rows exercise regular path`, async (_t) => {
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

	test(`csvParseStream multi-char delimiter is not treated as single`, async (_t) => {
		const streams = [
			createReadableStream('"a"::"b"::c\r\n'),
			csvParseStream({ delimiterChar: "::" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [["a", "b", "c"]]);
	});

	test(`csvParseStream 2-char newline second-char check after quoted field`, async (_t) => {
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

	test(`csvParseStream coerces Buffer chunk via toString`, async (_t) => {
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

	test(`csvParseStream concatenates buffered tail with next chunk`, async (_t) => {
		const streams = [
			createReadableStream(["a,b\r\n1,", "2\r\n"]),
			csvParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [
			["a", "b"],
			["1", "2"],
		]);
	});

	test(`csvParseStream accepts an incomplete row of exactly maxFieldSize*2`, async (_t) => {
		const maxFieldSize = 8;
		const text = `${"x".repeat(7)},${"y".repeat(8)}`;
		const output = await streamToArray(
			pipejoin([createReadableStream(text), csvParseStream({ maxFieldSize })]),
		);
		deepStrictEqual(output, [["x".repeat(7), "y".repeat(8)]]);
	});

	test(`csvParseStream rejects an incomplete row one past maxFieldSize*2`, async (_t) => {
		const text = `${"x".repeat(8)},${"y".repeat(8)}`;
		await rejects(
			pipeline([
				createReadableStream(text),
				csvParseStream({ maxFieldSize: 8 }),
			]),
			{
				name: "RangeError",
				message:
					"CSV buffer size (17) exceeds safety limit, likely unterminated quoted field",
			},
		);
	});

	test(`csvParseStream accepts a chunk of many small rows larger than maxFieldSize*2`, async (_t) => {
		const text = "ab,cd\r\n".repeat(10);
		const output = await streamToArray(
			pipejoin([
				createReadableStream(text),
				csvParseStream({ maxFieldSize: 8 }),
			]),
		);
		deepStrictEqual(
			output,
			Array.from({ length: 10 }, () => ["ab", "cd"]),
		);
	});

	test(`csvParseStream single chunk processes without join`, async (_t) => {
		const row = "a,b\r\n";
		const streams = [createReadableStream([row]), csvParseStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [["a", "b"]]);
	});

	test(`csvParseStream multiple chunks joined across a row`, async (_t) => {
		const streams = [
			createReadableStream(["a,", "b\r\n", "c,d\r\n"]),
			csvParseStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [
			["a", "b"],
			["c", "d"],
		]);
	});

	test(`csvRemoveEmptyRowsStream treats zero-length array as empty (l===0 branch)`, async (_t) => {
		const filter = csvRemoveEmptyRowsStream();
		const streams = [createReadableStream([[], ["a"]]), filter];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [["a"]]);
		deepStrictEqual(filter.result().value.EmptyRow.idx, [0]);
	});

	test(`csvArrayToObjectStream __proto__ column is writable, enumerable, configurable`, async (_t) => {
		const streams = [
			createReadableStream([["v1", "v2"]]),
			csvArrayToObjectStream({ headers: ["__proto__", "b"] }),
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
		deepStrictEqual(Object.keys(obj), ["__proto__", "b"]);
	});

	test(`csvArrayToObjectStream constructor column stored as own data property`, async (_t) => {
		const streams = [
			createReadableStream([["v1", "v2"]]),
			csvArrayToObjectStream({ headers: ["constructor", "b"] }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		const obj = output[0];
		const desc = Object.getOwnPropertyDescriptor(obj, "constructor");
		ok(desc);
		strictEqual(desc.value, "v1");
		strictEqual(desc.enumerable, true);
	});

	test(`csvCoerceValuesStream autoCoerce true requires length 4 and t/T`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "true", b: "True", c: "trueX", d: "rue" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		strictEqual(output[0].a, true);
		strictEqual(output[0].b, true);
		strictEqual(output[0].c, "trueX");
		strictEqual(output[0].d, "rue");
	});

	test(`csvCoerceValuesStream autoCoerce false requires length 5 and f/F`, async (_t) => {
		const streams = [
			createReadableStream([
				{ a: "false", b: "False", c: "fALSE", d: "falsey" },
			]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		strictEqual(output[0].a, false);
		strictEqual(output[0].b, false);
		strictEqual(output[0].c, false);
		strictEqual(output[0].d, "falsey");
	});

	test(`csvCoerceValuesStream autoCoerce digit and minus start numeric path`, async (_t) => {
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

	test(`csvCoerceValuesStream autoCoerce JSON only for braces/brackets`, async (_t) => {
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

	test(`csvCoerceValuesStream autoCoerce invalid JSON returns the original string (catch returns val)`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "{not json", b: "[oops" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		strictEqual(output[0].a, "{not json");
		strictEqual(output[0].b, "[oops");
	});

	test(`csvCoerceValuesStream coerceToType invalid json returns original string (catch returns val)`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "{not json" }]),
			csvCoerceValuesStream({ columns: { a: "json" } }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		strictEqual(output[0].a, "{not json");
	});

	test(`csvCoerceValuesStream iso date requires full anchor (trailing junk stays string)`, async (_t) => {
		const streams = [
			createReadableStream([{ a: "2020-01-02xyz", b: "2020-01-02" }]),
			csvCoerceValuesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		strictEqual(output[0].a, "2020-01-02xyz");
		ok(output[0].b instanceof Date);
	});

	test(`csvCoerceValuesStream iso date with seconds-and-fraction group present`, async (_t) => {
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

	test(`csvFormatStream single-char delimiter quotes interior delimiter only on actual delimiter`, async (_t) => {
		const streams = [createReadableStream([["a;b", "c,d"]]), csvFormatStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, 'a;b,"c,d"\r\n');
	});

	test(`csvFormatStream multi-char delimiter quotes value containing the delimiter substring`, async (_t) => {
		const streams = [
			createReadableStream([["a::b", "plain"]]),
			csvFormatStream({ delimiterChar: "::" }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, '"a::b"::plain\r\n');
	});

	test(`csvFormatStream emits when batch reaches exactly 64 rows`, async (_t) => {
		const rows = [];
		for (let i = 0; i < 64; i++) rows.push([`r${i}`, "x"]);
		const chunks = await streamToArray(
			pipejoin([createReadableStream(rows), csvFormatStream()]),
		);
		strictEqual(chunks.length, 1);
		const lines = chunks[0].split("\r\n").filter((l) => l.length > 0);
		strictEqual(lines.length, 64);
	});

	test(`csvFormatStream below 64 rows emits only on flush`, async (_t) => {
		const rows = [];
		for (let i = 0; i < 10; i++) rows.push([`r${i}`, "x"]);
		const chunks = await streamToArray(
			pipejoin([createReadableStream(rows), csvFormatStream()]),
		);
		strictEqual(chunks.length, 1);
		const lines = chunks[0].split("\r\n").filter((l) => l.length > 0);
		strictEqual(lines.length, 10);
	});

	test(`csvFormatStream isSimpleRow rejects rows with a quoting-needed string`, async (_t) => {
		const streams = [
			createReadableStream([["plain", "needs\nquote"]]),
			csvFormatStream(),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, 'plain,"needs\nquote"\r\n');
	});

	test(`csvFormatStream simple row of plain strings uses join fast path`, async (_t) => {
		const streams = [
			createReadableStream([["a", "b", "c"]]),
			csvFormatStream(),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, "a,b,c\r\n");
	});

	test(`csvParseStream respects provided streamOptions override`, async (_t) => {
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

	test(`csvDetectHeaderStream mid-field quote is not a quote opener`, async (_t) => {
		const hdr = csvDetectHeaderStream();
		const out = await streamToArray(
			pipejoin([
				createReadableStream('a"b,c\r\nd,e\r\n'),
				hdr,
				csvParseStream(),
			]),
		);
		deepStrictEqual(hdr.result().value.header, ['a"b', "c"]);
		deepStrictEqual(out, [["d", "e"]]);
	});

	test(`csvDetectHeaderStream quote opening right after a delimiter is honored`, async (_t) => {
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

	test(`csvDetectHeaderStream keeps newline inside quoted header field`, async (_t) => {
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

	test(`csvDetectHeaderStream custom escape keeps escaped quote then newline inside the field`, async (_t) => {
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

	test(`csvDetectHeaderStream custom escape even run closes the quote before the newline`, async (_t) => {
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

	test(`csvDetectHeaderStream custom escape backslash before quote keeps newline in field`, async (_t) => {
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

	test(`csvDetectDelimitersStream detects quote closing right before CR`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await streamToArray(pipejoin([createReadableStream("'a'\r'b'\r"), detect]));
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream does not treat unopened quote as quoteChar`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await streamToArray(
			pipejoin([createReadableStream("ab'cd,ef\r\n"), detect]),
		);
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream buffers until a newline appears`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const out = await streamToString(
			pipejoin([createReadableStream(["abcd", "e;f\r\n"]), detect]),
		);
		strictEqual(out, "abcde;f\r\n");
		strictEqual(detect.result().value.delimiterChar, ";");
	});

	test(`csvParseStream quoted field then 3-char newline splits rows (escapeIsQuote)`, async (_t) => {
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

	test(`csvParseStream quoted field then partial 3-char newline is garbage (escapeIsQuote)`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream('"a"##b'),
				csvParseStream({ newlineChar: "###" }),
			]),
		);
		deepStrictEqual(out, [["a##b"]]);
	});

	test(`csvParseStream quoted field then 3-char newline splits rows (custom escape)`, async (_t) => {
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

	test(`csvParseStream quoted field then partial 3-char newline is garbage (custom escape)`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream('"a"##b'),
				csvParseStream({ newlineChar: "###", escapeChar: "\\" }),
			]),
		);
		deepStrictEqual(out, [["a##b"]]);
	});

	test(`csvParseStream quoted field then CRLF splits (escapeIsQuote)`, async (_t) => {
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

	test(`csvParseStream quoted field then CRLF splits (custom escape)`, async (_t) => {
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

	test(`csvParseStream quoted field then LF newline length 1 (escapeIsQuote)`, async (_t) => {
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

	test(`csvParseStream quoted field then LF newline length 1 (custom escape)`, async (_t) => {
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

	test(`csvParseStream allows quoted field exactly maxFieldSize (escapeIsQuote)`, async (_t) => {
		const val = "x".repeat(10);
		const out = await streamToArray(
			pipejoin([
				createReadableStream(`"${val}",y\r\n`),
				csvParseStream({ maxFieldSize: 10 }),
			]),
		);
		deepStrictEqual(out, [[val, "y"]]);
	});

	test(`csvParseStream allows custom-escape quoted field exactly maxFieldSize`, async (_t) => {
		const val = "x".repeat(10);
		const out = await streamToArray(
			pipejoin([
				createReadableStream(`"${val}",y\r\n`),
				csvParseStream({ maxFieldSize: 10, escapeChar: "\\" }),
			]),
		);
		deepStrictEqual(out, [[val, "y"]]);
	});

	test(`csvParseStream unterminated quote message (escapeIsQuote)`, async (_t) => {
		const parse = csvParseStream();
		await pipeline([createReadableStream('"unterminated'), parse]);
		strictEqual(
			parse.result().value.UnterminatedQuote.message,
			"Unterminated quoted field",
		);
	});

	test(`csvParseStream unterminated quote message (custom escape)`, async (_t) => {
		const parse = csvParseStream({ escapeChar: "\\" });
		await pipeline([createReadableStream('x,"unterminated'), parse]);
		strictEqual(
			parse.result().value.UnterminatedQuote.message,
			"Unterminated quoted field",
		);
	});

	test(`csvParseStream buffers unterminated quoted field across chunks (escapeIsQuote)`, async (_t) => {
		const out = await streamToArray(
			pipejoin([createReadableStream(['"hel', 'lo",x\r\n']), csvParseStream()]),
		);
		deepStrictEqual(out, [["hello", "x"]]);
	});

	test(`csvParseStream buffers unterminated quoted field across chunks (custom escape)`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream(['"hel', 'lo",x\r\n']),
				csvParseStream({ escapeChar: "\\" }),
			]),
		);
		deepStrictEqual(out, [["hello", "x"]]);
	});

	test(`csvUnquotedParser numCols comes from first row not later rows`, (_t) => {
		const result = csvUnquotedParser("a,b\r\n1,2,3\r\n");
		strictEqual(result.numCols, 2);
	});

	test(`csvUnquotedParser flush keeps first-row numCols`, (_t) => {
		const result = csvUnquotedParser("a,b\r\n1,2,3", {}, true);
		strictEqual(result.numCols, 2);
		deepStrictEqual(result.rows[1], ["1", "2", "3"]);
	});

	test(`csvFormatStream quotes a field that only ends with a space`, async (_t) => {
		const out = await streamToString(
			pipejoin([createReadableStream([["ab ", "cd"]]), csvFormatStream()]),
		);
		strictEqual(out, '"ab ",cd\r\n');
	});

	test(`csvFormatStream does not quote a field with only an interior space`, async (_t) => {
		const out = await streamToString(
			pipejoin([createReadableStream([["a b", "cd"]]), csvFormatStream()]),
		);
		strictEqual(out, "a b,cd\r\n");
	});

	test(`csvFormatStream quotes a field containing the quote char`, async (_t) => {
		const out = await streamToString(
			pipejoin([createReadableStream([['a"b', "cd"]]), csvFormatStream()]),
		);
		strictEqual(out, '"a""b",cd\r\n');
	});

	test(`csvFormatStream quotes a field containing a carriage return`, async (_t) => {
		const out = await streamToString(
			pipejoin([createReadableStream([["a\rb", "cd"]]), csvFormatStream()]),
		);
		strictEqual(out, '"a\rb",cd\r\n');
	});

	test(`csvFormatStream quotes a field containing a line feed`, async (_t) => {
		const out = await streamToString(
			pipejoin([createReadableStream([["a\nb", "cd"]]), csvFormatStream()]),
		);
		strictEqual(out, '"a\nb",cd\r\n');
	});

	test(`csvFormatStream does not add a trailing empty field`, async (_t) => {
		const out = await streamToString(
			pipejoin([createReadableStream([["a", "b", "c"]]), csvFormatStream()]),
		);
		strictEqual(out, "a,b,c\r\n");
	});

	test(`csvFormatStream multi-char delimiter does not add trailing empty field`, async (_t) => {
		const out = await streamToString(
			pipejoin([
				createReadableStream([["a", "b", "c"]]),
				csvFormatStream({ delimiterChar: "::" }),
			]),
		);
		strictEqual(out, "a::b::c\r\n");
	});

	test(`csvFormatStream multi-char delimiter quotes value containing the delimiter`, async (_t) => {
		const out = await streamToString(
			pipejoin([
				createReadableStream([["a::b", "x"]]),
				csvFormatStream({ delimiterChar: "::" }),
			]),
		);
		strictEqual(out, '"a::b"::x\r\n');
	});

	test(`csvFormatStream does not flush at 63 rows`, async (_t) => {
		const rows = [];
		for (let i = 0; i < 63; i++) rows.push([String(i), "x"]);
		const chunks = await streamToArray(
			pipejoin([createReadableStream(rows), csvFormatStream()]),
		);
		strictEqual(chunks.length, 1);
		strictEqual(chunks[0].split("\r\n").filter((l) => l.length > 0).length, 63);
	});

	test(`csvFormatStream flushes at exactly 64 rows then a separate flush for extras`, async (_t) => {
		const rows = [];
		for (let i = 0; i < 65; i++) rows.push([String(i), "x"]);
		const chunks = await streamToArray(
			pipejoin([createReadableStream(rows), csvFormatStream()]),
		);
		strictEqual(chunks.length, 2);
		strictEqual(chunks[0].split("\r\n").filter((l) => l.length > 0).length, 64);
		strictEqual(chunks[1].split("\r\n").filter((l) => l.length > 0).length, 1);
	});

	test(`csvFormatStream formats null as empty field next to a quoted value`, async (_t) => {
		const out = await streamToString(
			pipejoin([createReadableStream([[null, "a,b"]]), csvFormatStream()]),
		);
		strictEqual(out, ',"a,b"\r\n');
	});

	test(`csvFormatStream formats empty string as empty field next to a quoted value`, async (_t) => {
		const out = await streamToString(
			pipejoin([createReadableStream([["", "a,b"]]), csvFormatStream()]),
		);
		strictEqual(out, ',"a,b"\r\n');
	});

	test(`csvFormatStream coerces a number field via String`, async (_t) => {
		const out = await streamToString(
			pipejoin([createReadableStream([[42, "x"]]), csvFormatStream()]),
		);
		strictEqual(out, "42,x\r\n");
	});

	test(`csvFormatStream coerces a Date field via toISOString`, async (_t) => {
		const d = new Date("2020-01-02T03:04:05.000Z");
		const out = await streamToString(
			pipejoin([createReadableStream([[d, "x"]]), csvFormatStream()]),
		);
		strictEqual(out, "2020-01-02T03:04:05.000Z,x\r\n");
	});

	test(`csvCoerceValuesStream iso date needs the ^ anchor`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([{ a: " 2020-01-02" }]),
				csvCoerceValuesStream(),
			]),
		);
		strictEqual(out[0].a, " 2020-01-02");
	});

	test(`csvCoerceValuesStream iso date needs the $ anchor`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "2020-01-02junk" }]),
				csvCoerceValuesStream(),
			]),
		);
		strictEqual(out[0].a, "2020-01-02junk");
	});

	test(`csvCoerceValuesStream iso date with time but no seconds is still a Date`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "2020-01-02T03:04" }]),
				csvCoerceValuesStream(),
			]),
		);
		ok(out[0].a instanceof Date);
		strictEqual(out[0].a.getTime(), new Date("2020-01-02T03:04").getTime());
	});

	test(`csvCoerceValuesStream iso date with seconds is a Date`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "2020-01-02T03:04:05" }]),
				csvCoerceValuesStream(),
			]),
		);
		ok(out[0].a instanceof Date);
	});

	test(`csvRemoveEmptyRowsStream drops a zero-length row`, async (_t) => {
		const filter = csvRemoveEmptyRowsStream();
		const out = await streamToArray(
			pipejoin([createReadableStream([[], ["keep"]]), filter]),
		);
		deepStrictEqual(out, [["keep"]]);
		deepStrictEqual(filter.result().value.EmptyRow.idx, [0]);
	});

	test(`csvArrayToObjectStream stores a constructor column as an own data property`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([["v"]]),
				csvArrayToObjectStream({ headers: ["constructor"] }),
			]),
		);
		const obj = out[0];
		strictEqual(Object.getOwnPropertyDescriptor(obj, "constructor").value, "v");
		deepStrictEqual(Object.keys(obj), ["constructor"]);
	});

	test(`csvArrayToObjectStream assigns a normal key directly`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([["v1", "v2"]]),
				csvArrayToObjectStream({ headers: ["a", "b"] }),
			]),
		);
		deepStrictEqual(out[0], { a: "v1", b: "v2" });
	});

	test(`csvQuotedParser numCols is first row count even when later rows differ`, (_t) => {
		const result = csvQuotedParser("a,b\r\n1,2,3\r\n");
		strictEqual(result.numCols, 2);
	});

	test(`csvQuotedParser numCols first row via quoted first field`, (_t) => {
		const result = csvQuotedParser('"a",b\r\n1,2,3\r\n');
		strictEqual(result.numCols, 2);
	});

	test(`csvUnquotedParser numCols is first row count even when later rows differ`, (_t) => {
		const result = csvUnquotedParser("a,b\r\n1,2,3\r\n");
		strictEqual(result.numCols, 2);
	});

	test(`csvUnquotedParser flush numCols is first row count`, (_t) => {
		const result = csvUnquotedParser("a,b\r\n1,2,3", {}, true);
		strictEqual(result.numCols, 2);
	});

	test(`csvCoerceValuesStream does not JSON-parse the bare word null`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "null" }]),
				csvCoerceValuesStream(),
			]),
		);
		strictEqual(out[0].a, "null");
	});

	test(`csvCoerceValuesStream does not JSON-parse a bare quoted-looking word`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([{ a: "hello" }]),
				csvCoerceValuesStream(),
			]),
		);
		strictEqual(out[0].a, "hello");
	});

	test(`csvDetectDelimitersStream requires a quote to start a field and close at a field end`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await streamToArray(
			pipejoin([createReadableStream("ab'cd',ef\r\n"), detect]),
		);
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream detects a quote that brackets a field after a delimiter`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await streamToArray(pipejoin([createReadableStream("x,'a'\r\n"), detect]));
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream detects a quote bracketing the very first field`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await streamToArray(pipejoin([createReadableStream("'a',b\r\n"), detect]));
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream ignores a quote that opens but never closes at a field end`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await streamToArray(
			pipejoin([createReadableStream("'twas,ok\r\n"), detect]),
		);
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvDetectHeaderStream header-only input yields empty header value default and no rows`, async (_t) => {
		const hdr = csvDetectHeaderStream();
		const out = await streamToArray(
			pipejoin([createReadableStream(""), hdr, csvParseStream()]),
		);
		deepStrictEqual(hdr.result().value.header, []);
		deepStrictEqual(out, []);
	});

	test(`csvDetectHeaderStream header row with trailing newline and no data emits no data rows`, async (_t) => {
		const hdr = csvDetectHeaderStream();
		const out = await streamToArray(
			pipejoin([createReadableStream("a,b,c\r\n"), hdr, csvParseStream()]),
		);
		deepStrictEqual(hdr.result().value.header, ["a", "b", "c"]);
		deepStrictEqual(out, []);
	});

	test(`csvDetectHeaderStream emits the data rows after the header`, async (_t) => {
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

	test(`csvDetectHeaderStream passes later chunks through unchanged once header detected`, async (_t) => {
		const hdr = csvDetectHeaderStream();
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

	test(`csvParseStream parses string chunks without toString coercion`, async (_t) => {
		const out = await streamToArray(
			pipejoin([createReadableStream("a,b\r\nc,d\r\n"), csvParseStream()]),
		);
		deepStrictEqual(out, [
			["a", "b"],
			["c", "d"],
		]);
	});

	test(`csvQuotedParser numCols from first row ending in a quoted field (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('a,"b"\r\n1,2,3\r\n');
		strictEqual(result.numCols, 2);
	});

	test(`csvQuotedParser numCols from first row ending in a quoted field (custom escape)`, (_t) => {
		const result = csvQuotedParser('a,"b"\r\n1,2,3\r\n', { escapeChar: "\\" });
		strictEqual(result.numCols, 2);
	});

	test(`csvQuotedParser flushing an unterminated quote leaves no tail (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('"abc', {}, true);
		strictEqual(result.tail, "");
		deepStrictEqual(result.rows, [["abc"]]);
	});

	test(`csvQuotedParser flushing an unterminated quote leaves no tail (custom escape)`, (_t) => {
		const result = csvQuotedParser('x,"abc', { escapeChar: "\\" }, true);
		strictEqual(result.tail, "");
		deepStrictEqual(result.rows, [["x", "abc"]]);
	});

	test(`csvQuotedParser not flushing keeps the unterminated quoted row as tail (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('a,b\r\n"abc', {}, false);
		strictEqual(result.tail, '"abc');
		deepStrictEqual(result.rows, [["a", "b"]]);
	});

	test(`csvQuotedParser not flushing keeps the unterminated quoted row as tail (custom escape)`, (_t) => {
		const result = csvQuotedParser(
			'a,b\r\nx,"abc',
			{ escapeChar: "\\" },
			false,
		);
		strictEqual(result.tail, 'x,"abc');
		deepStrictEqual(result.rows, [["a", "b"]]);
	});

	test(`csvQuotedParser quoted field then trailing delimiter adds an empty field on flush`, (_t) => {
		const result = csvQuotedParser('"a",', {}, true);
		deepStrictEqual(result.rows, [["a", ""]]);
	});

	test(`csvParseStream unquoted trailing delimiter adds an empty field on flush`, async (_t) => {
		const out = await streamToArray(
			pipejoin([createReadableStream("a,b,"), csvParseStream()]),
		);
		deepStrictEqual(out, [["a", "b", ""]]);
	});

	test(`csvArrayToObjectStream __proto__ column does not pollute the prototype`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream([["payload", "v2"]]),
				csvArrayToObjectStream({ headers: ["__proto__", "b"] }),
			]),
		);
		const obj = out[0];
		strictEqual(
			Object.getOwnPropertyDescriptor(obj, "__proto__").value,
			"payload",
		);
		strictEqual(obj.b, "v2");
		deepStrictEqual(Object.keys(obj), ["__proto__", "b"]);
	});

	test(`csvDetectDelimitersStream empty input yields all-undefined detection value`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const out = await streamToArray(
			pipejoin([createReadableStream(""), detect]),
		);
		deepStrictEqual(out, []);
		deepStrictEqual(detect.result().value, {
			delimiterChar: undefined,
			newlineChar: undefined,
			quoteChar: undefined,
			escapeChar: undefined,
		});
	});

	test(`csvQuotedParser numCols stays first row when later quoted row is shorter (escapeIsQuote)`, (_t) => {
		const result = csvQuotedParser('"a","b"\r\n"c"\r\n');
		strictEqual(result.numCols, 2);
		deepStrictEqual(result.rows, [["a", "b"], ["c"]]);
	});

	test(`csvQuotedParser numCols stays first row when later quoted row is shorter (custom escape)`, (_t) => {
		const result = csvQuotedParser('"a","b"\r\n"c"\r\n', { escapeChar: "\\" });
		strictEqual(result.numCols, 2);
		deepStrictEqual(result.rows, [["a", "b"], ["c"]]);
	});

	test(`csvQuotedParser numCols set in cleanup for a lone quoted field on flush`, (_t) => {
		const result = csvQuotedParser('"a"', {}, true);
		strictEqual(result.numCols, 1);
		deepStrictEqual(result.rows, [["a"]]);
	});

	test(`csvQuotedParser custom escape unterminated field ending in a lone escape char keeps it literally`, (_t) => {
		const result = csvQuotedParser('"a\\', { escapeChar: "\\" }, true);
		deepStrictEqual(result.rows, [["a\\"]]);
		ok(result.errors.UnterminatedQuote);
	});

	test(`csvQuotedParser delimiter takes precedence when it is a prefix of the newline`, (_t) => {
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

	test(`csvDetectHeaderStream delimiter-prefix-of-newline tie consumes the CR as a delimiter`, async (_t) => {
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

	test(`csvQuotedParser flush of complete input leaves an empty tail`, (_t) => {
		const result = csvQuotedParser("a,b\r\n1,2", {}, true);
		strictEqual(result.tail, "");
		deepStrictEqual(result.rows, [
			["a", "b"],
			["1", "2"],
		]);
	});

	test(`csvQuotedParser non-flushing partial last row is returned as tail`, (_t) => {
		const result = csvQuotedParser("a,b\r\n1,2", {}, false);
		strictEqual(result.tail, "1,2");
		deepStrictEqual(result.rows, [["a", "b"]]);
	});

	test(`csvDetectHeaderStream single-column header has no delimiter`, async (_t) => {
		const hdr = csvDetectHeaderStream();
		const out = await streamToArray(
			pipejoin([
				createReadableStream("a\r\nx\r\ny\r\n"),
				hdr,
				csvParseStream(),
			]),
		);
		deepStrictEqual(hdr.result().value.header, ["a"]);
		deepStrictEqual(out, [["x"], ["y"]]);
	});

	test(`csvDetectHeaderStream leading newline yields an empty header row`, async (_t) => {
		const hdr = csvDetectHeaderStream();
		const out = await streamToArray(
			pipejoin([createReadableStream("\r\na,b\r\n"), hdr, csvParseStream()]),
		);
		deepStrictEqual(hdr.result().value.header, []);
		deepStrictEqual(out, [["a", "b"]]);
	});

	test(`csvDetectHeaderStream one-char header ends at its newline before a data delimiter`, async (_t) => {
		const hdr = csvDetectHeaderStream();
		const out = await streamToArray(
			pipejoin([createReadableStream("a\r\nb,c\r\n"), hdr, csvParseStream()]),
		);
		deepStrictEqual(hdr.result().value.header, ["a"]);
		deepStrictEqual(out, [["b", "c"]]);
	});

	test(`csvDetectHeaderStream unterminated quote in header treats whole buffer as header`, async (_t) => {
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

	test(`csvDetectHeaderStream leading doubled-quote header field keeps embedded newline`, async (_t) => {
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

	test(`csvDetectHeaderStream custom escape escaped quote at start of header field keeps embedded newline`, async (_t) => {
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

	test(`csvQuotedParser custom escape escaped quote at start of content`, (_t) => {
		const result = csvQuotedParser('"\\"X",b\r\n', { escapeChar: "\\" });
		deepStrictEqual(result.rows, [['"X', "b"]]);
	});

	test(`csvQuotedParser treats a mid-field quote as a literal character`, (_t) => {
		const result = csvQuotedParser('a"b,c\r\nd,e\r\n');
		deepStrictEqual(result.rows, [
			['a"b', "c"],
			["d", "e"],
		]);
	});

	test(`csvQuotedParser mid-field quote literal with custom escape`, (_t) => {
		const result = csvQuotedParser('a"b,c\r\nd,e\r\n', { escapeChar: "\\" });
		deepStrictEqual(result.rows, [
			['a"b', "c"],
			["d", "e"],
		]);
	});

	test(`csvDetectHeaderStream delimiter before a quoted field with an embedded newline`, async (_t) => {
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

	test(`csvDetectHeaderStream multi-char delimiter before a quoted field with an embedded newline`, async (_t) => {
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

	test(`csvDetectDelimitersStream detects a quoted single-char field bracketed by quotes`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await streamToArray(
			pipejoin([createReadableStream("'a',b,c\r\n"), detect]),
		);
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream detects a quoted field closing exactly at end of text`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await streamToArray(pipejoin([createReadableStream("a,'b'\r\n"), detect]));
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream does not detect a single opening quote with no closer`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await streamToArray(pipejoin([createReadableStream("',b\r\n"), detect]));
		strictEqual(detect.result().value.quoteChar, '"');
	});

	test(`csvDetectDelimitersStream detects an empty quoted first field`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		await streamToArray(pipejoin([createReadableStream("'',b\r\n"), detect]));
		strictEqual(detect.result().value.quoteChar, "'");
	});

	test(`csvDetectDelimitersStream emits a no-newline input via flush`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const out = await streamToString(
			pipejoin([createReadableStream("a,b,c"), detect]),
		);
		strictEqual(out, "a,b,c");
	});

	test(`csvDetectHeaderStream emits a no-newline input as header via flush`, async (_t) => {
		const hdr = csvDetectHeaderStream();
		const out = await streamToArray(
			pipejoin([createReadableStream("a,b,c"), hdr]),
		);
		deepStrictEqual(out, []);
		deepStrictEqual(hdr.result().value.header, ["a", "b", "c"]);
	});

	test(`csvDetectDelimitersStream emits each chunk as it detects, not coalesced`, async (_t) => {
		const detect = csvDetectDelimitersStream();
		const out = await streamToArray(
			pipejoin([createReadableStream(["a;b\r\n", "c;d\r\n"]), detect]),
		);
		deepStrictEqual(out, ["a;b\r\n", "c;d\r\n"]);
		strictEqual(detect.result().value.delimiterChar, ";");
	});

	test(`csvDetectHeaderStream emits the rest in transform, then later chunks separately`, async (_t) => {
		const hdr = csvDetectHeaderStream();
		const out = await streamToArray(
			pipejoin([createReadableStream(["a,b\r\n1,2\r\n", "3,4\r\n"]), hdr]),
		);
		deepStrictEqual(out, ["1,2\r\n", "3,4\r\n"]);
		deepStrictEqual(hdr.result().value.header, ["a", "b"]);
	});

	test(`csvDetectHeaderStream header-only with trailing newline emits nothing (direct)`, async (_t) => {
		const hdr = csvDetectHeaderStream();
		const out = await streamToArray(
			pipejoin([createReadableStream("a,b,c\r\n"), hdr]),
		);
		deepStrictEqual(out, []);
		deepStrictEqual(hdr.result().value.header, ["a", "b", "c"]);
	});

	test(`csvDetectHeaderStream buffers a header split across chunks until the row completes`, async (_t) => {
		const hdr = csvDetectHeaderStream();
		const out = await streamToArray(
			pipejoin([createReadableStream(["a,", "b\r\n1,2\r\n"]), hdr]),
		);
		deepStrictEqual(out, ["1,2\r\n"]);
		deepStrictEqual(hdr.result().value.header, ["a", "b"]);
	});
});
