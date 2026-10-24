import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import { Buffer } from "node:buffer";
import test, { describe } from "node:test";
import { base64DecodeStream, base64EncodeStream } from "@datastream/base64";
import {
	createReadableStream,
	createWritableStream,
	pipejoin,
	pipeline,
	streamToArray,
	streamToBuffer,
	streamToString,
} from "@datastream/core";
import { variant } from "../variant.js";

const decoder = new TextDecoder();

describe(`@datastream/base64 (${variant})`, () => {
	test(`base64EncodeStream should encode`, async (_t) => {
		const input = "encode";
		const streams = [createReadableStream(input), base64EncodeStream()];
		const output = await streamToString(pipejoin(streams));

		deepStrictEqual(output, btoa(input));
	});

	test(`base64DecodeStream should decode`, async (_t) => {
		const input = "decode";
		const streams = [createReadableStream(btoa(input)), base64DecodeStream()];
		const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

		deepStrictEqual(output, input);
	});

	test(`base64Stream should encode/decode`, async (_t) => {
		for (let i = 1; i <= 16; i++) {
			const input = "x".repeat(i);
			const streams = [
				createReadableStream(input),
				base64EncodeStream(),
				base64DecodeStream(),
			];
			const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

			deepStrictEqual(output, input);
		}
	});

	test(`base64EncodeStream should handle multiple chunks with remainders`, async (_t) => {
		const input = [
			Buffer.from("aaaa"),
			Buffer.from("bbbb"),
			Buffer.from("cccc"),
		];
		const streams = [createReadableStream(input), base64EncodeStream()];
		const output = await streamToString(pipejoin(streams));

		deepStrictEqual(output, btoa("aaaabbbbcccc"));
	});

	test(`base64DecodeStream should handle multiple chunks with remainders`, async (_t) => {
		const input = btoa("aaaabbbbcccc");
		const chunks = [input.slice(0, 4), input.slice(4, 8), input.slice(8)];
		const streams = [createReadableStream(chunks), base64DecodeStream()];
		const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

		deepStrictEqual(output, "aaaabbbbcccc");
	});

	test(`base64EncodeStream should encode single byte`, async (_t) => {
		const input = "a";
		const streams = [createReadableStream(input), base64EncodeStream()];
		const output = await streamToString(pipejoin(streams));

		deepStrictEqual(output, btoa("a"));
	});

	test(`base64EncodeStream should encode two bytes`, async (_t) => {
		const input = "ab";
		const streams = [createReadableStream(input), base64EncodeStream()];
		const output = await streamToString(pipejoin(streams));

		deepStrictEqual(output, btoa("ab"));
	});

	test(`base64DecodeStream should decode partial base64`, async (_t) => {
		const input = btoa("hello");
		const chunks = [input.slice(0, 2), input.slice(2)];
		const streams = [createReadableStream(chunks), base64DecodeStream()];
		const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

		deepStrictEqual(output, "hello");
	});

	test(`base64DecodeStream should flush remaining characters`, async (_t) => {
		const input = btoa("ab");
		const chunks = [input.slice(0, 2), input.slice(2)];
		const streams = [createReadableStream(chunks), base64DecodeStream()];
		const output = await streamToBuffer(pipejoin(streams));

		deepStrictEqual(output.byteLength, 2);
	});

	test(`base64EncodeStream should encode all 256 byte values`, async (_t) => {
		const bytes = new Uint8Array(256);
		for (let i = 0; i < 256; i++) bytes[i] = i;
		const streams = [createReadableStream([bytes]), base64EncodeStream()];
		const output = await streamToString(pipejoin(streams));
		deepStrictEqual(output, Buffer.from(bytes).toString("base64"));
	});

	test(`base64DecodeStream should round-trip all 256 byte values`, async (_t) => {
		const bytes = new Uint8Array(256);
		for (let i = 0; i < 256; i++) bytes[i] = i;
		const b64 = Buffer.from(bytes).toString("base64");
		const streams = [createReadableStream([b64]), base64DecodeStream()];
		const output = await streamToBuffer(pipejoin(streams));
		deepStrictEqual(Uint8Array.from(output), bytes);
	});

	test(`base64EncodeStream should encode Uint8Array chunks across boundary`, async (_t) => {
		const chunks = [
			new Uint8Array([1, 2, 3, 4]),
			new Uint8Array([5, 6, 7, 8]),
			new Uint8Array([9, 10, 11, 12]),
		];
		const streams = [createReadableStream(chunks), base64EncodeStream()];
		const output = await streamToString(pipejoin(streams));
		const flat = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
		deepStrictEqual(output, Buffer.from(flat).toString("base64"));
	});

	test(`exports only the named stream functions (no default export)`, async (_t) => {
		const mod = await import("@datastream/base64");
		deepStrictEqual(Object.keys(mod).sort(), [
			"base64DecodeStream",
			"base64EncodeStream",
		]);
	});

	const malformedPaddingCases = [
		{ input: "YQ=", label: "3-char group with single pad (YQ=)" },
		{ input: "Pw=", label: "3-char group with single pad (Pw=)" },
		{ input: "QQ=", label: "3-char group with single pad (QQ=)" },
		{ input: "==", label: "padding-only (==)" },
		{ input: "AAAA==", label: "length-6 with trailing padding (AAAA==)" },
	];

	for (const { input, label } of malformedPaddingCases) {
		test(`base64DecodeStream should reject malformed padding: ${label}`, async (_t) => {
			let threw = false;
			try {
				await pipeline([createReadableStream([input]), base64DecodeStream()]);
			} catch (_e) {
				threw = true;
			}
			deepStrictEqual(
				threw,
				true,
				`Expected decode of ${JSON.stringify(input)} to throw`,
			);
		});
	}

	const validPaddedCases = [
		{
			input: "YQ==",
			expected: [0x61],
			label: "2-char group double-pad (YQ==) -> 'a'",
		},
		{
			input: "YWI=",
			expected: [0x61, 0x62],
			label: "3-char group single-pad (YWI=) -> 'ab'",
		},
		{
			input: "AAAA",
			expected: [0x00, 0x00, 0x00],
			label: "unpadded 4-char group (AAAA) -> zeros",
		},
	];

	for (const { input, expected, label } of validPaddedCases) {
		test(`base64DecodeStream should accept valid padded input: ${label}`, async (_t) => {
			const streams = [createReadableStream([input]), base64DecodeStream()];
			const output = await streamToBuffer(pipejoin(streams));
			deepStrictEqual(Array.from(output), expected);
		});
	}

	test(`base64DecodeStream should reject input with invalid leading char`, async (_t) => {
		let threw = false;
		try {
			await pipeline([createReadableStream(["!AAA"]), base64DecodeStream()]);
		} catch (_e) {
			threw = true;
		}
		deepStrictEqual(threw, true, "Expected decode of '!AAA' to throw");
	});

	test(`base64DecodeStream should reject input with invalid trailing char`, async (_t) => {
		let threw = false;
		try {
			await pipeline([createReadableStream(["AAA!"]), base64DecodeStream()]);
		} catch (_e) {
			threw = true;
		}
		deepStrictEqual(threw, true, "Expected decode of 'AAA!' to throw");
	});

	test(`base64DecodeStream error message should include input and label`, async (_t) => {
		let errorMessage = "";
		try {
			await pipeline([createReadableStream(["!AAA"]), base64DecodeStream()]);
		} catch (e) {
			errorMessage = e.message;
		}
		deepStrictEqual(
			errorMessage.includes("Invalid base64 string"),
			true,
			"Error message must contain 'Invalid base64 string'",
		);
		deepStrictEqual(
			errorMessage.includes("!AAA"),
			true,
			"Error message must include the bad input",
		);
	});

	test(`base64DecodeStream should decode Buffer chunks correctly`, async (_t) => {
		const originalText = "Hello, World!";
		const b64 = Buffer.from(originalText).toString("base64");
		const streams = [
			createReadableStream([Buffer.from(b64)]),
			base64DecodeStream(),
		];
		const output = await streamToBuffer(pipejoin(streams));
		deepStrictEqual(decoder.decode(output), originalText);
	});

	test(`base64DecodeStream should track extra correctly across non-mod4 boundaries`, async (_t) => {
		const originalText = "Hello World!";
		const b64 = Buffer.from(originalText).toString("base64");
		const chunks = [b64.slice(0, 6), b64.slice(6)];
		const streams = [createReadableStream(chunks), base64DecodeStream()];
		const output = await streamToBuffer(pipejoin(streams));
		deepStrictEqual(decoder.decode(output), originalText);
	});

	test(`base64EncodeStream should emit aligned group then trailing byte as separate chunks`, async (_t) => {
		const input = Buffer.from("aaaa");
		const streams = [createReadableStream([input]), base64EncodeStream()];
		const chunks = await streamToArray(pipejoin(streams));
		deepStrictEqual(chunks, [
			Buffer.from("aaa").toString("base64"),
			Buffer.from("a").toString("base64"),
		]);
	});

	test(`base64EncodeStream should emit a single chunk for aligned input`, async (_t) => {
		const input = Buffer.from("aaa");
		const streams = [createReadableStream([input]), base64EncodeStream()];
		const chunks = await streamToArray(pipejoin(streams));
		deepStrictEqual(chunks, [Buffer.from("aaa").toString("base64")]);
	});

	test(`base64EncodeStream should emit only from flush for a single byte`, async (_t) => {
		const input = Buffer.from("a");
		const streams = [createReadableStream([input]), base64EncodeStream()];
		const chunks = await streamToArray(pipejoin(streams));
		deepStrictEqual(chunks, [Buffer.from("a").toString("base64")]);
	});

	test(`base64DecodeStream should not emit for a sub-quartet chunk`, async (_t) => {
		const emitted = [];
		await rejects(
			pipeline([
				createReadableStream(["YQ"]),
				base64DecodeStream(),
				createWritableStream((chunk) => emitted.push(chunk)),
			]),
			{ message: 'Invalid base64 string: "YQ"' },
		);
		deepStrictEqual(emitted, []);
	});

	test(`base64DecodeStream should emit a single buffer chunk for an aligned quartet`, async (_t) => {
		const b64 = Buffer.from("abc").toString("base64");
		const streams = [createReadableStream([b64]), base64DecodeStream()];
		const chunks = await streamToArray(pipejoin(streams));
		deepStrictEqual(chunks.length, 1);
		deepStrictEqual(Buffer.concat(chunks).toString(), "abc");
	});

	test(`base64EncodeStream accepts plain byte array and ArrayBuffer chunks`, async (_t) => {
		for (const chunk of [[104, 105], Uint8Array.of(104, 105).buffer]) {
			const streams = [createReadableStream([chunk]), base64EncodeStream()];
			strictEqual(await streamToString(pipejoin(streams)), "aGk=");
		}
	});

	test(`base64DecodeStream accepts Uint8Array chunks of base64 text`, async (_t) => {
		const b64 = new TextEncoder().encode(btoa("Hello, bytes"));
		const chunks = [b64.subarray(0, 5), b64.subarray(5)];
		const streams = [createReadableStream(chunks), base64DecodeStream()];
		strictEqual(
			decoder.decode(await streamToBuffer(pipejoin(streams))),
			"Hello, bytes",
		);
	});

	test(`base64EncodeStream encodes non-ASCII strings as UTF-8`, async (_t) => {
		const streams = [createReadableStream(["é"]), base64EncodeStream()];
		strictEqual(
			await streamToString(pipejoin(streams)),
			Buffer.from("é").toString("base64"),
		);
	});

	if (variant === "browser") {
		test(`base64DecodeStream error quotes the offending input verbatim`, async (_t) => {
			await rejects(
				pipeline([createReadableStream(["é"]), base64DecodeStream()]),
				/Invalid base64 string: "é"/,
			);
		});
	}

	test(`base64DecodeStream rejects byte chunks with high-bit bytes`, async (_t) => {
		await rejects(
			pipeline([
				createReadableStream([new Uint8Array([0xd9, 0x57, 0x4a, 0x6a])]),
				base64DecodeStream(),
			]),
			/Invalid base64 string/,
		);
	});

	test(`base64DecodeStream rejects data after padding in a later chunk`, async (_t) => {
		await rejects(
			pipeline([
				createReadableStream(["YWJjYQ==", "ZGVm"]),
				base64DecodeStream(),
			]),
			{ message: 'Invalid base64 string: "ZGVm" follows "=" padding' },
		);
	});

	test(`base64DecodeStream rejects padding mid-chunk`, async (_t) => {
		await rejects(
			pipeline([createReadableStream(["YWJjYQ==ZGVm"]), base64DecodeStream()]),
			{ message: 'Invalid base64 string: "YWJjYQ==ZGVm"' },
		);
	});

	test(`base64DecodeStream emits one chunk per aligned input chunk`, async (_t) => {
		const streams = [
			createReadableStream(["YWJj", "ZGVm"]),
			base64DecodeStream(),
		];
		const chunks = await streamToArray(pipejoin(streams));
		deepStrictEqual(
			chunks.map((c) => decoder.decode(c)),
			["abc", "def"],
		);
	});
	test(`base64DecodeStream error quotes a 32-char chunk in full`, async (_t) => {
		const bad = `!${"A".repeat(31)}`;
		await rejects(
			pipeline([createReadableStream([bad]), base64DecodeStream()]),
			{ message: `Invalid base64 string: "${bad}"` },
		);
	});

	test(`base64DecodeStream error truncates a long chunk to 32 chars`, async (_t) => {
		const bad = `!${"A".repeat(35)}`;
		await rejects(
			pipeline([createReadableStream([bad]), base64DecodeStream()]),
			{ message: `Invalid base64 string: "!${"A".repeat(31)}…"` },
		);
	});

	test(`base64DecodeStream after-padding error truncates a long chunk`, async (_t) => {
		await rejects(
			pipeline([
				createReadableStream(["YQ==", "B".repeat(36)]),
				base64DecodeStream(),
			]),
			{
				message: `Invalid base64 string: "${"B".repeat(32)}…" follows "=" padding`,
			},
		);
	});
});
