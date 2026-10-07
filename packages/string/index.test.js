import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	stringCountStream,
	stringLengthStream,
	stringMinimumChunkSizeStream,
	stringMinimumFirstChunkSizeStream,
	stringReplaceStream,
	stringSkipConsecutiveDuplicatesStream,
	stringSplitStream,
} from "@datastream/string";
import { variant } from "../variant.js";

describe(`@datastream/string (${variant})`, () => {
	// *** Major-version API *** //
	test(`exports only *Stream names (no default, aliases or stringReadableStream)`, async (_t) => {
		const mod = await import("@datastream/string");
		for (const name of [
			"default",
			"stringReadableStream",
			"stringMinimumFirstChunkSize",
			"stringMinimumChunkSize",
			"stringSkipConsecutiveDuplicates",
		]) {
			strictEqual(mod[name], undefined, name);
		}
	});

	// *** stringLengthStream *** //
	test(`stringLengthStream should count length of chunks`, async (_t) => {
		const input = ["1", "2", "3"];
		const streams = [createReadableStream(input), stringLengthStream()];

		const result = await pipeline(streams);
		const { key, value } = streams[1].result();

		strictEqual(key, "length");
		strictEqual(result.length, 3);
		strictEqual(value, 3);
	});

	test(`stringSizeStream should count length of chunks with custom key`, async (_t) => {
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

	// *** stringSkipConsecutiveDuplicatesStream *** //
	test(`stringSkipConsecutiveDuplicatesStream should skip consecutive duplicates`, async (_t) => {
		const input = ["1", "2", "2", "3"];
		const streams = [
			createReadableStream(input),
			stringSkipConsecutiveDuplicatesStream(),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, ["1", "2", "3"]);
	});

	// *** stringSplitStream *** //
	test(`stringSplitStream should split into empty strings`, async (_t) => {
		const input = [",,", ",,"];
		const streams = [
			createReadableStream(input),
			stringSplitStream({ separator: "," }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, ["", "", "", "", ""]);
	});

	test(`stringSplitStream should split across chunk boundaries`, async (_t) => {
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
	test(`stringCountStream should count occurrences of substring`, async (_t) => {
		const input = ["hello world", "hello universe"];
		const streams = [
			createReadableStream(input),
			stringCountStream({ substr: "hello" }),
		];

		const result = await pipeline(streams);
		const { key, value } = streams[1].result();

		strictEqual(key, "stringCount");
		strictEqual(result.stringCount, 2);
		strictEqual(value, 2);
	});

	test(`stringCountStream should count multiple occurrences in single chunk`, async (_t) => {
		const input = ["aaa aaa aaa"];
		const streams = [
			createReadableStream(input),
			stringCountStream({ substr: "a" }),
		];

		await pipeline(streams);
		const { value } = streams[1].result();

		strictEqual(value, 9);
	});

	test(`stringCountStream should not recount single char substr across chunks`, async (_t) => {
		const streams = [
			createReadableStream(["a", "b", "c"]),
			stringCountStream({ substr: "a" }),
		];

		await pipeline(streams);

		strictEqual(streams[1].result().value, 1);
	});

	test(`stringCountStream should count substr spanning chunks once`, async (_t) => {
		const streams = [
			createReadableStream(["xa", "bx", "ab"]),
			stringCountStream({ substr: "ab" }),
		];

		await pipeline(streams);

		strictEqual(streams[1].result().value, 2);
	});

	test(`stringCountStream should use custom result key`, async (_t) => {
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
	test(`stringReplaceStream should replace pattern across chunks`, async (_t) => {
		const input = ["hello world", "hello universe"];
		const streams = [
			createReadableStream(input),
			stringReplaceStream({ pattern: /hello/g, replacement: "hi" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(output.join(""), "hi worldhi universe");
	});

	test(`stringReplaceStream should handle pattern spanning chunks`, async (_t) => {
		const input = ["hel", "lo world"];
		const streams = [
			createReadableStream(input),
			stringReplaceStream({ pattern: /hello/g, replacement: "hi" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(output.join(""), "hi world");
	});

	test(`stringReplaceStream should replace with string pattern`, async (_t) => {
		const input = ["hello world"];
		const streams = [
			createReadableStream(input),
			stringReplaceStream({ pattern: "hello", replacement: "hi" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(output.join(""), "hi world");
	});

	test(`stringReplaceStream should not re-replace already replaced output (string)`, async (_t) => {
		const streams = [
			createReadableStream(["a", "b"]),
			stringReplaceStream({ pattern: "a", replacement: "aa" }),
		];

		const output = await streamToArray(pipejoin(streams));

		strictEqual(output.join(""), "aab");
	});

	test(`stringReplaceStream should not re-replace already replaced output (RegExp)`, async (_t) => {
		const streams = [
			createReadableStream(["a", "b"]),
			stringReplaceStream({ pattern: /a/g, replacement: "aa" }),
		];

		const output = await streamToArray(pipejoin(streams));

		strictEqual(output.join(""), "aab");
	});

	test(`stringReplaceStream should replace string pattern spanning many chunks`, async (_t) => {
		const streams = [
			createReadableStream(["xhe", "l", "l", "ox"]),
			stringReplaceStream({ pattern: "hello", replacement: "hi" }),
		];

		const output = await streamToArray(pipejoin(streams));

		strictEqual(output.join(""), "xhix");
	});

	test(`stringReplaceStream should handle zero-length RegExp matches`, async (_t) => {
		const streams = [
			createReadableStream(["ab", "c"]),
			stringReplaceStream({ pattern: /(?=b)/g, replacement: "-" }),
		];

		const output = await streamToArray(pipejoin(streams));

		strictEqual(output.join(""), "a-bc");
	});

	test(`stringReplaceStream should replace from the start regardless of RegExp lastIndex`, async (_t) => {
		const pattern = /a/g;
		pattern.lastIndex = 1;
		const streams = [
			createReadableStream(["xa", "b"]),
			stringReplaceStream({ pattern, replacement: "c" }),
		];

		const output = await streamToArray(pipejoin(streams));

		deepStrictEqual(output, ["xc", "b"]);
		// the caller's RegExp is never used directly, so its lastIndex is untouched
		strictEqual(pattern.lastIndex, 1);
	});

	test(`stringReplaceStream should throw on empty string pattern`, (_t) => {
		let threw = false;
		try {
			stringReplaceStream({ pattern: "", replacement: "x" });
		} catch (e) {
			threw = true;
			ok(e.message.includes("non-empty pattern"));
		}
		ok(threw);
	});

	// *** stringReplaceStream: whole-string semantics across chunks *** //
	const replaceChunks = (input, options) =>
		streamToArray(
			pipejoin([createReadableStream(input), stringReplaceStream(options)]),
		);

	test(`stringReplaceStream should see lookahead context past the emitted slice`, async (_t) => {
		const output = await replaceChunks(["ab", "cd"], {
			pattern: /b(?=c)/g,
			replacement: "X",
		});
		deepStrictEqual(output, ["aX", "cd"]);
	});

	test(`stringReplaceStream should see lookbehind context before the buffer`, async (_t) => {
		const output = await replaceChunks(["ab", "cd"], {
			pattern: /(?<=b)c/g,
			replacement: "X",
		});
		strictEqual(output.join(""), "abXd");
	});

	test(`stringReplaceStream should limit lookbehind context to the lookbehind option`, async (_t) => {
		const pattern = /(?<=ab)c/g;
		strictEqual(
			(await replaceChunks(["ab", "c"], { pattern, replacement: "X" })).join(
				"",
			),
			"abX",
		);
		strictEqual(
			(
				await replaceChunks(["ab", "c"], {
					pattern,
					replacement: "X",
					lookbehind: 1,
				})
			).join(""),
			"abc",
		);
	});

	test(`stringReplaceStream should keep lookbehind context when the first emit is short`, async (_t) => {
		const output = await replaceChunks(["a", "bcdef"], {
			pattern: /(?<=a)b/g,
			replacement: "X",
			lookbehind: 2,
		});
		strictEqual(output.join(""), "aXcdef");
	});

	test(`stringReplaceStream should stop a sticky RegExp at its first failed match`, async (_t) => {
		const output = await replaceChunks(["ba", "b"], {
			pattern: /b/gy,
			replacement: "X",
		});
		strictEqual(output.join(""), "Xab");
	});

	test(`stringReplaceStream should wait for more data before failing a sticky match`, async (_t) => {
		const output = await replaceChunks(["a", "b"], {
			pattern: /ab/y,
			replacement: "X",
		});
		strictEqual(output.join(""), "X");
	});

	test(`stringReplaceStream should not stop a sticky RegExp on a held match`, async (_t) => {
		const output = await replaceChunks(["a", "aa"], {
			pattern: /a+/gy,
			replacement: "X",
		});
		strictEqual(output.join(""), "X");
	});

	test(`stringReplaceStream should keep matching a global RegExp after a miss`, async (_t) => {
		const output = await replaceChunks(["ab", "cd", "ef", "a"], {
			pattern: /a/g,
			replacement: "X",
		});
		strictEqual(output.join(""), "XbcdefX");
	});

	test(`stringReplaceStream should expand $' with the rest of the stream (at flush)`, async (_t) => {
		deepStrictEqual(
			await replaceChunks(["ab", "cd"], { pattern: /b/g, replacement: "[$']" }),
			["a[cd]cd"],
		);
		deepStrictEqual(
			await replaceChunks(["ab", "cd"], { pattern: "b", replacement: "[$']" }),
			["a[cd]cd"],
		);
	});

	test("stringReplaceStream should expand $` with the stream before the match (at flush)", async (_t) => {
		deepStrictEqual(
			await replaceChunks(["ab", "cd"], { pattern: /c/g, replacement: "[$`]" }),
			["ab[ab]d"],
		);
	});

	test(`stringReplaceStream should stream a template with a bare quote or backtick`, async (_t) => {
		deepStrictEqual(
			await replaceChunks(["ab", "cd"], { pattern: /b/g, replacement: "'`" }),
			["a'`", "cd"],
		);
		deepStrictEqual(
			await replaceChunks(["ab", "cd"], {
				pattern: /(b)/g,
				replacement: "[$$$&$1]",
			}),
			["a[$bb]", "cd"],
		);
	});

	test(`stringReplaceStream should not treat a function's source as a $' template`, async (_t) => {
		const output = await replaceChunks(["ab", "cd"], {
			pattern: /b/g,
			replacement: () => "$'",
		});
		deepStrictEqual(output, ["a$'", "cd"]);
	});

	test(`stringReplaceStream should match ^ only at the start of the stream`, async (_t) => {
		const output = await replaceChunks(["x", "y"], {
			pattern: /^/g,
			replacement: ">",
		});
		strictEqual(output.join(""), ">xy");
	});

	test(`stringReplaceStream should match $ only at the end of the stream`, async (_t) => {
		const output = await replaceChunks(["x", "y"], {
			pattern: /$/g,
			replacement: "<",
		});
		strictEqual(output.join(""), "xy<");
	});

	test(`stringReplaceStream should hold a greedy match that touches the buffer end`, async (_t) => {
		const output = await replaceChunks(["xa", "a", "a"], {
			pattern: /a+/g,
			replacement: "X",
		});
		deepStrictEqual(output, ["x", "X"]);
	});

	test(`stringReplaceStream should hold a match starting in the latest chunk`, async (_t) => {
		const output = await replaceChunks(["x", "abc", "d"], {
			pattern: /abcd|ab/g,
			replacement: "X",
		});
		strictEqual(output.join(""), "xX");
	});

	test(`stringReplaceStream should not emit held-back text before a held match`, async (_t) => {
		const output = await replaceChunks(["xab", "c"], {
			pattern: /abc|b/g,
			replacement: "X",
		});
		strictEqual(output.join(""), "xX");
	});

	test(`stringReplaceStream should expand replacement tokens like String.prototype.replace`, async (_t) => {
		const cases = [
			[/(a)(b)?/g, "[$$|$&|$1|$01|$2|$0|$00|$3|$05|$20|$12|$<x>|$<$&>|$<]"],
			[/(?<x>a)(?<y>b)?/g, "[$<x>|$<y>|$<z>|$<>|$<x]"],
			[/(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)?/g, "[$10|$11|$1x]"],
			["a", "[$1|$&|$<x>]"],
		];
		for (const [pattern, replacement] of cases) {
			const input = ["zabcdefghijz", "a"];
			const whole = input.join("");
			const expected =
				typeof pattern === "string"
					? whole.replaceAll(pattern, replacement)
					: whole.replace(new RegExp(pattern), replacement);
			const output = await replaceChunks(input, { pattern, replacement });
			strictEqual(output.join(""), expected, `${pattern} ${replacement}`);
		}
	});

	test(`stringReplaceStream should call a function replacement with stream offsets`, async (_t) => {
		const calls = [];
		const output = await replaceChunks(["ab", "cab"], {
			pattern: /a(b)/g,
			replacement: (...args) => {
				calls.push(args);
				return "X";
			},
		});
		strictEqual(output.join(""), "XcX");
		deepStrictEqual(calls, [
			["ab", "b", 0, "abcab", undefined],
			["ab", "b", 3, "abcab", undefined],
		]);
	});

	test(`stringReplaceStream should advance zero-length matches by code point`, async (_t) => {
		const input = ["\u{1F600}x", "￿y"];
		for (const pattern of [/(?:)/g, /(?:)/gu, /(?:)/gv]) {
			const output = await replaceChunks(input, { pattern, replacement: "-" });
			strictEqual(
				output.join(""),
				input.join("").replace(pattern, "-"),
				String(pattern),
			);
		}
	});

	test(`stringReplaceStream should emit safe text before flush`, async (_t) => {
		deepStrictEqual(
			await replaceChunks(["ab", "cd"], { pattern: /x/g, replacement: "X" }),
			["ab", "cd"],
		);
		deepStrictEqual(
			await replaceChunks(["xa", "b"], { pattern: /a/g, replacement: "c" }),
			["xc", "b"],
		);
	});

	test(`stringReplaceStream should miss a RegExp match longer than the latest chunk by default`, async (_t) => {
		const output = await replaceChunks(["a", "b", "c"], {
			pattern: /abc/g,
			replacement: "X",
		});
		strictEqual(output.join(""), "abc");
	});

	test(`stringReplaceStream should hold back maxMatchLength for a RegExp`, async (_t) => {
		const output = await replaceChunks(["a", "b", "c"], {
			pattern: /abc/g,
			replacement: "X",
			maxMatchLength: 3,
		});
		strictEqual(output.join(""), "X");
	});

	test(`stringReplaceStream should emit all but maxMatchLength - 1 chars`, async (_t) => {
		const output = await replaceChunks(["abcd", "ef"], {
			pattern: /x/g,
			replacement: "X",
			maxMatchLength: 2,
		});
		deepStrictEqual(output, ["abc", "de", "f"]);
	});

	test(`stringReplaceStream should hold a variable-length match at the chunk start`, async (_t) => {
		const output = await replaceChunks(["a", "a"], {
			pattern: /a+/g,
			replacement: "X",
		});
		deepStrictEqual(output, ["X"]);
	});

	test(`stringReplaceStream should replace adjacent matches straddling a chunk start`, async (_t) => {
		const output = await replaceChunks(["aba", "b"], {
			pattern: /ab/g,
			replacement: "X",
		});
		deepStrictEqual(output, ["X", "X"]);
	});

	test(`stringReplaceStream should drain a RegExp buffer before flush`, async (_t) => {
		const output = await replaceChunks(["aaaa", "bbbb", "cccc"], {
			pattern: /a/g,
			replacement: "X",
			maxBufferSize: 8,
		});
		deepStrictEqual(output, ["XXXX", "bbbb", "cccc"]);
	});

	test(`stringReplaceStream should hold back only pattern.length - 1 chars for a string`, async (_t) => {
		const output = await replaceChunks(["aaaa", "a"], {
			pattern: "zzz",
			replacement: "y",
			maxBufferSize: 4,
		});
		deepStrictEqual(output, ["aa", "a", "aa"]);
	});

	test(`stringReplaceStream should not emit empty chunks`, async (_t) => {
		deepStrictEqual(
			await replaceChunks(["a", "b"], { pattern: /a/g, replacement: "" }),
			["b"],
		);
		deepStrictEqual(
			await replaceChunks(["a"], { pattern: /a/g, replacement: "" }),
			[],
		);
	});

	// *** stringMinimumFirstChunkSizeStream *** //
	test(`stringMinimumFirstChunkSizeStream should buffer until chunkSize reached`, async (_t) => {
		const input = ["ab", "cd", "ef"];
		const streams = [
			createReadableStream(input),
			stringMinimumFirstChunkSizeStream({ chunkSize: 4 }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, ["abcd", "ef"]);
	});

	test(`stringMinimumFirstChunkSizeStream should flush small input`, async (_t) => {
		const input = ["ab"];
		const streams = [
			createReadableStream(input),
			stringMinimumFirstChunkSizeStream({ chunkSize: 100 }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, ["ab"]);
	});

	test(`stringMinimumFirstChunkSizeStream should pass through after first chunk met`, async (_t) => {
		const input = ["abcdef", "gh", "ij"];
		const streams = [
			createReadableStream(input),
			stringMinimumFirstChunkSizeStream({ chunkSize: 4 }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, ["abcdef", "gh", "ij"]);
	});

	// *** stringMinimumChunkSizeStream *** //
	test(`stringMinimumChunkSizeStream should buffer until chunkSize reached`, async (_t) => {
		const input = ["ab", "cd", "ef"];
		const streams = [
			createReadableStream(input),
			stringMinimumChunkSizeStream({ chunkSize: 4 }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, ["abcd", "ef"]);
	});

	test(`stringMinimumChunkSizeStream should flush small input`, async (_t) => {
		const input = ["ab"];
		const streams = [
			createReadableStream(input),
			stringMinimumChunkSizeStream({ chunkSize: 100 }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, ["ab"]);
	});

	test(`stringMinimumChunkSizeStream should buffer all chunks to minimum size`, async (_t) => {
		const input = ["abcdef", "gh", "ij"];
		const streams = [
			createReadableStream(input),
			stringMinimumChunkSizeStream({ chunkSize: 4 }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, ["abcdef", "ghij"]);
	});

	// *** stringReplaceStream buffer limit regression *** //
	test(`stringReplaceStream should throw when buffer exceeds maxBufferSize`, async (_t) => {
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
	test(`stringSplitStream should throw when buffer exceeds maxBufferSize`, async (_t) => {
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
	test(`stringSplitStream should throw on empty separator at construction`, (_t) => {
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
	test(`stringCountStream should count occurrences spanning chunk boundaries`, async (_t) => {
		const input = ["hel", "lo"];
		const streams = [
			createReadableStream(input),
			stringCountStream({ substr: "hello" }),
		];

		await pipeline(streams);
		const { value } = streams[1].result();

		strictEqual(value, 1);
	});

	// *** stringCountStream guard: typeof check *** //
	test(`stringCountStream should throw when substr is not a string (number)`, (_t) => {
		let threw = false;
		try {
			stringCountStream({ substr: 42 });
		} catch (e) {
			threw = true;
			strictEqual(e.message, "stringCountStream requires a non-empty substr");
		}
		ok(threw, "stringCountStream({ substr: 42 }) should throw");
	});

	test(`stringCountStream should throw when substr is not a string (undefined)`, (_t) => {
		let threw = false;
		try {
			stringCountStream({});
		} catch (e) {
			threw = true;
			strictEqual(e.message, "stringCountStream requires a non-empty substr");
		}
		ok(threw, "stringCountStream({}) should throw");
	});

	test(`stringCountStream should throw on empty substr`, (_t) => {
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
	test(`stringCountStream should count match at the very end of combined string`, async (_t) => {
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
	test(`stringCountStream should NOT double-count across chunk boundary when carry is exact`, async (_t) => {
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

	test(`stringCountStream carry should not include extra characters that cause false positives`, async (_t) => {
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

	test(`stringCountStream carry boundary with 3-char substr`, async (_t) => {
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

	// *** stringMinimumFirstChunkSizeStream: buffer reset after emit *** //
	test(`stringMinimumFirstChunkSizeStream should output exact chunk size when met and not carry leftovers`, async (_t) => {
		// chunkSize=4, input ["ab","cd","ef"]
		// After emitting "abcd", buffer must be "" so next chunk "ef" is passed through as-is
		// If buffer="" mutant used "Stryker was here!", subsequent chunk would be wrong
		const input = ["ab", "cd", "ef"];
		const streams = [
			createReadableStream(input),
			stringMinimumFirstChunkSizeStream({ chunkSize: 4 }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, ["abcd", "ef"]);
	});

	test(`stringMinimumFirstChunkSizeStream should not emit in flush when done=true and buffer empty`, async (_t) => {
		// When chunkSize is met exactly, done=true and buffer="" => flush should emit nothing
		const input = ["abcd"];
		const streams = [
			createReadableStream(input),
			stringMinimumFirstChunkSizeStream({ chunkSize: 4 }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		// "abcd" meets chunkSize=4 exactly, emitted in transform; flush emits nothing (buffer empty, done=true)
		deepStrictEqual(output, ["abcd"]);
	});

	test(`stringMinimumFirstChunkSizeStream should not flush empty buffer when done=false`, async (_t) => {
		// Input is empty => done=false, buffer="" => flush should NOT emit empty string
		const input = [""];
		const streams = [
			createReadableStream(input),
			stringMinimumFirstChunkSizeStream({ chunkSize: 4 }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		// buffer is "" — flush condition `buffer.length > 0` must prevent enqueue
		deepStrictEqual(output, []);
	});

	// *** stringReplaceStream: RegExp guard *** //
	test(`stringReplaceStream should throw on RegExp without g or y flag`, (_t) => {
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

	test(`stringReplaceStream should NOT throw on RegExp with g flag`, async (_t) => {
		const input = ["hello"];
		const streams = [
			createReadableStream(input),
			stringReplaceStream({ pattern: /hello/g, replacement: "hi" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		strictEqual(output.join(""), "hi");
	});

	test(`stringReplaceStream should NOT throw on RegExp with y flag`, async (_t) => {
		const input = ["hello"];
		const streams = [
			createReadableStream(input),
			stringReplaceStream({ pattern: /hello/y, replacement: "hi" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		strictEqual(output.join(""), "hi");
	});

	test(`stringReplaceStream should NOT throw on RegExp with both g and y flags`, async (_t) => {
		const input = ["hello"];
		const streams = [
			createReadableStream(input),
			stringReplaceStream({ pattern: /hello/gy, replacement: "hi" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		strictEqual(output.join(""), "hi");
	});

	// *** stringReplaceStream: useReplaceAll (string vs RegExp path) *** //
	test(`stringReplaceStream should replace ALL occurrences with string pattern (replaceAll path)`, async (_t) => {
		// String pattern uses replaceAll, regex uses replace
		// With string "a", input "aaa" => all 3 replaced
		const input = ["aaa"];
		const streams = [
			createReadableStream(input),
			stringReplaceStream({ pattern: "a", replacement: "b" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		strictEqual(output.join(""), "bbb");
	});

	test(`stringReplaceStream with regex /a/g should replace all occurrences (replace path with global)`, async (_t) => {
		const input = ["aaa"];
		const streams = [
			createReadableStream(input),
			stringReplaceStream({ pattern: /a/g, replacement: "b" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		strictEqual(output.join(""), "bbb");
	});

	// *** stringReplaceStream: maxBufferSize exact boundary (> vs >=) *** //
	test(`stringReplaceStream should NOT throw when buffer equals maxBufferSize exactly`, async (_t) => {
		// buffer.length === maxBufferSize should NOT throw (only > maxBufferSize should)
		// Use pattern that never matches so buffer grows; first chunk "aaaa" (4 chars) with maxBufferSize=4
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
		strictEqual(output.join(""), "aaaa");
	});

	test(`stringReplaceStream should throw when buffer exceeds maxBufferSize by 1`, async (_t) => {
		// buffer.length = 5 > maxBufferSize = 4 => throws
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
	test(`stringSplitStream should throw when separator is not a string (number)`, (_t) => {
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

	test(`stringSplitStream should throw when separator is undefined`, (_t) => {
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
	test(`stringSplitStream should NOT throw when buffer equals maxBufferSize exactly`, async (_t) => {
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

	test(`stringSplitStream should throw when buffer exceeds maxBufferSize by 1`, async (_t) => {
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

	// *** Major-version limits: null = unlimited, RangeError on overflow *** //
	test(`string maxBufferSize accepts null as unlimited`, async (_t) => {
		const input = ["a".repeat(10), "b".repeat(10)];
		deepStrictEqual(
			await streamToArray(
				pipejoin([
					createReadableStream(input),
					stringReplaceStream({
						pattern: "z",
						replacement: "y",
						maxBufferSize: null,
					}),
				]),
			),
			input,
		);
		deepStrictEqual(
			await streamToArray(
				pipejoin([
					createReadableStream(input),
					stringSplitStream({ separator: "z", maxBufferSize: null }),
				]),
			),
			[input.join("")],
		);
	});

	test(`string maxBufferSize overflow is a RangeError`, async (_t) => {
		await rejects(
			pipeline([
				createReadableStream(["aaa"]),
				stringReplaceStream({
					pattern: "z",
					replacement: "y",
					maxBufferSize: 2,
				}),
			]),
			{
				name: "RangeError",
				message: "stringReplaceStream buffer (3) exceeds maxBufferSize (2)",
			},
		);
		await rejects(
			pipeline([
				createReadableStream(["aaa"]),
				stringSplitStream({ separator: "z", maxBufferSize: 2 }),
			]),
			{
				name: "RangeError",
				message:
					"stringSplitStream buffer (3) exceeds maxBufferSize (2), separator not found",
			},
		);
	});
});
