import { deepStrictEqual } from "node:assert";
import { Buffer } from "node:buffer";
import test from "node:test";
import base64Default, {
	base64DecodeStream,
	base64EncodeStream,
} from "@datastream/base64";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
	streamToBuffer,
	streamToString,
} from "@datastream/core";

const decoder = new TextDecoder();

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** base64EncodeStream *** //
test(`${variant}: base64EncodeStream should encode`, async (_t) => {
	const input = "encode";
	const streams = [createReadableStream(input), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));

	deepStrictEqual(output, btoa(input));
});

// *** base64DecodeStream *** //
test(`${variant}: base64DecodeStream should decode`, async (_t) => {
	const input = "decode";
	const streams = [createReadableStream(btoa(input)), base64DecodeStream()];
	const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

	deepStrictEqual(output, input);
});

// *** Misc *** //
test(`${variant}: base64Stream should encode/decode`, async (_t) => {
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

// Test encoding with multiple chunks that have remainder bytes
test(`${variant}: base64EncodeStream should handle multiple chunks with remainders`, async (_t) => {
	const input = [Buffer.from("aaaa"), Buffer.from("bbbb"), Buffer.from("cccc")]; // 4 bytes each, 4 % 3 = 1 remainder
	const streams = [createReadableStream(input), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));

	deepStrictEqual(output, btoa("aaaabbbbcccc"));
});

// Test decoding with multiple chunks that have remainder characters
test(`${variant}: base64DecodeStream should handle multiple chunks with remainders`, async (_t) => {
	const input = btoa("aaaabbbbcccc");
	const chunks = [input.slice(0, 4), input.slice(4, 8), input.slice(8)]; // 4 chars each, 4 % 4 = 0, but test chunking
	const streams = [createReadableStream(chunks), base64DecodeStream()];
	const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

	deepStrictEqual(output, "aaaabbbbcccc");
});

// Test encoding single byte (1 % 3 = 1 remainder)
test(`${variant}: base64EncodeStream should encode single byte`, async (_t) => {
	const input = "a";
	const streams = [createReadableStream(input), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));

	deepStrictEqual(output, btoa("a"));
});

// Test encoding two bytes (2 % 3 = 2 remainder)
test(`${variant}: base64EncodeStream should encode two bytes`, async (_t) => {
	const input = "ab";
	const streams = [createReadableStream(input), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));

	deepStrictEqual(output, btoa("ab"));
});

// Test decode with non-multiple of 4 characters in chunk
test(`${variant}: base64DecodeStream should decode partial base64`, async (_t) => {
	const input = btoa("hello");
	const chunks = [input.slice(0, 2), input.slice(2)]; // 2 and rest
	const streams = [createReadableStream(chunks), base64DecodeStream()];
	const output = decoder.decode(await streamToBuffer(pipejoin(streams)));

	deepStrictEqual(output, "hello");
});

// Test decode flush with a valid padded quartet held across chunks
test(`${variant}: base64DecodeStream should flush remaining characters`, async (_t) => {
	const input = btoa("ab"); // "YWI="
	// Split so that the first chunk contributes only part of a 4-char group;
	// the decoder should buffer "YW" and combine with "I=" in the second chunk
	// to form the complete valid quartet "YWI=" before decoding.
	const chunks = [input.slice(0, 2), input.slice(2)]; // "YW" + "I="
	const streams = [createReadableStream(chunks), base64DecodeStream()];
	const output = await streamToBuffer(pipejoin(streams));

	deepStrictEqual(output.byteLength, 2); // "ab" is 2 bytes
});

// *** Binary correctness *** //
test(`${variant}: base64EncodeStream should encode all 256 byte values`, async (_t) => {
	const bytes = new Uint8Array(256);
	for (let i = 0; i < 256; i++) bytes[i] = i;
	const streams = [createReadableStream([bytes]), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));
	deepStrictEqual(output, Buffer.from(bytes).toString("base64"));
});

test(`${variant}: base64DecodeStream should round-trip all 256 byte values`, async (_t) => {
	const bytes = new Uint8Array(256);
	for (let i = 0; i < 256; i++) bytes[i] = i;
	const b64 = Buffer.from(bytes).toString("base64");
	const streams = [createReadableStream([b64]), base64DecodeStream()];
	const output = await streamToBuffer(pipejoin(streams));
	deepStrictEqual(Uint8Array.from(output), bytes);
});

test(`${variant}: base64EncodeStream should encode Uint8Array chunks across boundary`, async (_t) => {
	// 4 bytes per chunk, 4%3 = 1 remainder per chunk
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

// *** default export *** //
test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(base64Default).sort(), [
		"decodeStream",
		"encodeStream",
	]);
});

// *** Padding validation parity (HIGH finding) *** //
// These malformed inputs must be REJECTED identically on both Node and Web builds.
// "YQ=" is a 3-char fragment (not a multiple of 4) — Node's Buffer.from is lenient
// but atob() throws; after the fix BOTH builds must throw.
const malformedPaddingCases = [
	{ input: "YQ=", label: "3-char group with single pad (YQ=)" },
	{ input: "Pw=", label: "3-char group with single pad (Pw=)" },
	{ input: "QQ=", label: "3-char group with single pad (QQ=)" },
	{ input: "==", label: "padding-only (==)" },
	{ input: "AAAA==", label: "length-6 with trailing padding (AAAA==)" },
];

for (const { input, label } of malformedPaddingCases) {
	test(`${variant}: base64DecodeStream should reject malformed padding: ${label}`, async (_t) => {
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

// Valid padded inputs that must be ACCEPTED on both builds
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
	test(`${variant}: base64DecodeStream should accept valid padded input: ${label}`, async (_t) => {
		const streams = [createReadableStream([input]), base64DecodeStream()];
		const output = await streamToBuffer(pipejoin(streams));
		deepStrictEqual(Array.from(output), expected);
	});
}

// *** Regex anchor coverage *** //
// Inputs with invalid characters at the START or END (length % 4 == 0) must be rejected.
// Without the leading ^ anchor the regex would match a valid suffix and accept '!AAA'.
// Without the trailing $ anchor the regex would match a valid prefix and accept 'AAA!'.
test(`${variant}: base64DecodeStream should reject input with invalid leading char`, async (_t) => {
	let threw = false;
	try {
		await pipeline([createReadableStream(["!AAA"]), base64DecodeStream()]);
	} catch (_e) {
		threw = true;
	}
	deepStrictEqual(threw, true, "Expected decode of '!AAA' to throw");
});

test(`${variant}: base64DecodeStream should reject input with invalid trailing char`, async (_t) => {
	let threw = false;
	try {
		await pipeline([createReadableStream(["AAA!"]), base64DecodeStream()]);
	} catch (_e) {
		threw = true;
	}
	deepStrictEqual(threw, true, "Expected decode of 'AAA!' to throw");
});

// *** Error message content *** //
// Verify that the thrown error identifies the bad input string (not an empty message).
test(`${variant}: base64DecodeStream error message should include input and label`, async (_t) => {
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

// *** Buffer chunk through decode stream *** //
// Passing a Buffer (non-string) chunk exercises the toBuffer().toString('ascii') branch.
// A StringLiteral mutation that replaces 'ascii' with '' causes ERR_UNKNOWN_ENCODING,
// so the correct code must succeed and produce the expected bytes.
test(`${variant}: base64DecodeStream should decode Buffer chunks correctly`, async (_t) => {
	const originalText = "Hello, World!";
	const b64 = Buffer.from(originalText).toString("base64");
	// Pass the base64 string as a Buffer chunk (not a plain string)
	const streams = [
		createReadableStream([Buffer.from(b64)]),
		base64DecodeStream(),
	];
	const output = await streamToBuffer(pipejoin(streams));
	deepStrictEqual(output.toString(), originalText);
});

// *** Line-40 slice mutation: cross-boundary extra tracking *** //
// Split a 16-char base64 string at offset 6 (6 % 4 == 2 → remainder=2).
// The correct code stores only the LAST 2 chars as extra; the mutant stores the full
// 6-char string.  On the second chunk the mutant concatenates the wrong prefix,
// decoding different bytes (producing "HelHello World!" instead of "Hello World!").
test(`${variant}: base64DecodeStream should track extra correctly across non-mod4 boundaries`, async (_t) => {
	const originalText = "Hello World!";
	const b64 = Buffer.from(originalText).toString("base64"); // 16 chars
	// Split so first chunk length is 6 (6 % 4 == 2, non-zero remainder)
	const chunks = [b64.slice(0, 6), b64.slice(6)];
	const streams = [createReadableStream(chunks), base64DecodeStream()];
	const output = await streamToBuffer(pipejoin(streams));
	deepStrictEqual(output.toString(), originalText);
});

// *** Per-chunk emission shape (encode) *** //
// streamToArray preserves the discrete chunks each transform/flush enqueues.
// A single 4-byte chunk must emit the 3-byte aligned group from transform and
// the held trailing byte from flush as TWO separate chunks. This pins down the
// `% 3` arithmetic, the remaining/whole guards, and the flush emission so that
// mutants which keep the same concatenation but change the split are killed.
test(`${variant}: base64EncodeStream should emit aligned group then trailing byte as separate chunks`, async (_t) => {
	const input = Buffer.from("aaaa"); // 4 bytes -> 4 % 3 = 1 trailing byte
	const streams = [createReadableStream([input]), base64EncodeStream()];
	const chunks = await streamToArray(pipejoin(streams));
	deepStrictEqual(chunks, [
		Buffer.from("aaa").toString("base64"), // "YWFh"
		Buffer.from("a").toString("base64"), // "YQ=="
	]);
});

// A 3-byte aligned chunk emits exactly one chunk from transform and nothing
// from flush (extra stays undefined). Kills flush `if (extra) -> true` (which
// would call undefined.toString() and throw) and the encode guards that would
// emit an empty trailing chunk.
test(`${variant}: base64EncodeStream should emit a single chunk for aligned input`, async (_t) => {
	const input = Buffer.from("aaa"); // 3 bytes, 3 % 3 = 0
	const streams = [createReadableStream([input]), base64EncodeStream()];
	const chunks = await streamToArray(pipejoin(streams));
	deepStrictEqual(chunks, [Buffer.from("aaa").toString("base64")]);
});

// A single 1-byte chunk holds the whole byte as extra: transform emits NOTHING
// (whole === 0) and flush emits the encoded byte. Kills `whole > 0 -> true`
// and `whole > 0 -> whole >= 0` (which would emit an empty "" chunk from the
// transform) and flush `if (extra) -> false` (which would drop the byte).
test(`${variant}: base64EncodeStream should emit only from flush for a single byte`, async (_t) => {
	const input = Buffer.from("a"); // 1 byte, whole === 0
	const streams = [createReadableStream([input]), base64EncodeStream()];
	const chunks = await streamToArray(pipejoin(streams));
	deepStrictEqual(chunks, [Buffer.from("a").toString("base64")]); // ["YQ=="]
});

// *** Per-chunk emission shape (decode) *** //
// A short (< 4 char) single chunk holds everything as extra: transform emits
// NOTHING (whole === 0) and the leftover is rejected at flush. Kills the decode
// `s.length > 0 -> true / >= 0` mutants (which would enqueue an empty Buffer).
test(`${variant}: base64DecodeStream should not emit for a sub-quartet chunk`, async (_t) => {
	const emitted = [];
	let threw = false;
	await new Promise((resolve) => {
		const source = createReadableStream(["YQ"]);
		const decode = base64DecodeStream();
		decode.on("data", (chunk) => emitted.push(chunk));
		decode.on("end", resolve);
		decode.on("error", () => {
			threw = true;
			resolve();
		});
		source.pipe(decode);
	});
	// "YQ" is an incomplete quartet: the transform must emit no chunk and the
	// flush must reject it. Without the `s.length > 0` guard the transform would
	// enqueue an empty Buffer before the flush rejection.
	deepStrictEqual(threw, true, "Expected incomplete quartet to be rejected");
	deepStrictEqual(emitted, []);
});

// A single aligned 4-char chunk decodes to exactly one Buffer chunk from the
// transform; flush emits nothing (extra empty). Kills the `% 4 -> * 4` and
// `whole = length - remaining -> + remaining` mutants which change the split.
test(`${variant}: base64DecodeStream should emit a single buffer chunk for an aligned quartet`, async (_t) => {
	const b64 = Buffer.from("abc").toString("base64"); // "YWJj", 4 chars
	const streams = [createReadableStream([b64]), base64DecodeStream()];
	const chunks = await streamToArray(pipejoin(streams));
	deepStrictEqual(chunks.length, 1);
	deepStrictEqual(Buffer.concat(chunks).toString(), "abc");
});
