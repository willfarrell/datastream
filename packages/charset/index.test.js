import { deepStrictEqual, ok, strictEqual } from "node:assert";
import test from "node:test";
import {
	charsetDecodeStream,
	charsetDetectStream,
	charsetEncodeStream,
} from "@datastream/charset";
import { getSupportedEncoding } from "@datastream/charset/detect";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";
import iconv from "iconv-lite";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** charsetDetectStream *** //
test(`${variant}: charsetDetectStream should detect charset from content`, async (_t) => {
	const input = [Buffer.from("Hello World")];
	const streams = [createReadableStream(input), charsetDetectStream()];

	await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "charset");
	strictEqual(typeof value.charset, "string");
	strictEqual(typeof value.confidence, "number");
});

test(`${variant}: charsetDetectStream should detect charset with custom result key`, async (_t) => {
	const input = [Buffer.from("Test content")];
	const streams = [
		createReadableStream(input),
		charsetDetectStream({ resultKey: "encoding" }),
	];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "encoding");
	deepStrictEqual(result.encoding, value);
});

// *** charsetEncodeStream *** //
test(`${variant}: charsetEncodeStream should encode strings to UTF-8 by default`, async (_t) => {
	const input = ["Hello", " ", "World"];
	const streams = [createReadableStream(input), charsetEncodeStream()];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		Buffer.from("Hello"),
		Buffer.from(" "),
		Buffer.from("World"),
	]);
});

test(`${variant}: charsetEncodeStream should encode strings to specified charset`, async (_t) => {
	const input = ["Hello"];
	const streams = [
		createReadableStream(input),
		charsetEncodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [Buffer.from("Hello")]);
});

// *** charsetDecodeStream *** //
test(`${variant}: charsetDecodeStream should decode UTF-8 bytes to strings`, async (_t) => {
	const input = [Buffer.from("Hello"), Buffer.from(" "), Buffer.from("World")];
	const streams = [
		createReadableStream(input),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "Hello World");
});

test(`${variant}: charsetDecodeStream should decode bytes to strings with default charset`, async (_t) => {
	const input = [Buffer.from("Test")];
	const streams = [createReadableStream(input), charsetDecodeStream()];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "Test");
});

// *** Pipeline pattern: encode -> decode *** //
test(`${variant}: encode and decode pipeline should roundtrip correctly`, async (_t) => {
	const input = ["Hello", " ", "World"];
	const streams = [
		createReadableStream(input),
		charsetEncodeStream({ charset: "UTF-8" }),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "Hello World");
});

// *** Pipeline pattern: detect -> encode -> decode *** //
test(`${variant}: detect, encode, decode pipeline should work together`, async (_t) => {
	const input = ["Test", " ", "String"];
	const streams = [
		createReadableStream(input),
		charsetDetectStream(),
		charsetEncodeStream({ charset: "UTF-8" }),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "charset");
	strictEqual(typeof value.charset, "string");
});

// Test with streamOptions
test(`${variant}: charsetEncodeStream should accept streamOptions`, async (_t) => {
	const input = ["Hello"];
	const streams = [
		createReadableStream(input),
		charsetEncodeStream({}, { objectMode: true }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [Buffer.from("Hello")]);
});

test(`${variant}: charsetDecodeStream should accept streamOptions`, async (_t) => {
	const input = [Buffer.from("Hello")];
	const streams = [
		createReadableStream(input),
		charsetDecodeStream({}, { objectMode: true }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "Hello");
});

// Test flush with remaining bytes
test(`${variant}: charsetEncodeStream should handle empty input`, async (_t) => {
	const streams = [
		createReadableStream([]),
		charsetEncodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	strictEqual(output.length, 0);
});

test(`${variant}: charsetEncodeStream should work with UTF-16LE`, async (_t) => {
	const streams = [
		createReadableStream(["test"]),
		charsetEncodeStream({ charset: "UTF-16LE" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	const result = Buffer.concat(output);
	// UTF-16LE should have BOM and different byte pattern
	strictEqual(result.length > 0, true);
});

test(`${variant}: charsetEncodeStream should handle unsupported charset`, async (_t) => {
	const input = ["test"];
	const streams = [
		createReadableStream(input),
		charsetEncodeStream({ charset: "unsupported-charset" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// Should fallback to UTF-8
	deepStrictEqual(Buffer.concat(output).toString("utf8"), "test");
});

test(`${variant}: charsetDecodeStream should flush remaining bytes`, async (_t) => {
	// Split a multi-byte UTF-8 character across chunks
	const fullBytes = Buffer.from("测试", "utf8");
	const chunk1 = fullBytes.slice(0, 3); // First 3 bytes (partial)
	const chunk2 = fullBytes.slice(3); // Remaining bytes

	const streams = [
		createReadableStream([chunk1, chunk2]),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "测试");
});

test(`${variant}: charsetEncodeStream should flush with incomplete sequence`, async (_t) => {
	// Test with empty input to trigger flush with no data
	const streams = [
		createReadableStream([]),
		charsetEncodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// Should handle empty stream gracefully
	deepStrictEqual(output.length, 0);
});

test(`${variant}: charsetDecodeStream should flush with incomplete multi-byte sequence`, async (_t) => {
	// Send incomplete UTF-8 sequence (first byte of 2-byte sequence)
	const incompleteBytes = Buffer.from([0xc2]); // Start of 2-byte sequence

	const streams = [
		createReadableStream([incompleteBytes]),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	// Should output replacement character for incomplete sequence
	strictEqual(output.includes("\uFFFD"), true);
});

test(`${variant}: charsetDecodeStream should handle unsupported charset`, async (_t) => {
	const input = [Buffer.from("test")];
	const streams = [
		createReadableStream(input),
		charsetDecodeStream({ charset: "unsupported-charset" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	// Should fallback to UTF-8
	deepStrictEqual(output, "test");
});

// *** stateful-encoder flush: encoders such as UTF-7-IMAP buffer multibyte
// content and emit the trailing shift sequence ONLY from end(); the flush
// handler must enqueue conv.end()'s return value or that tail is lost. *** //
test(`${variant}: charsetEncodeStream should flush trailing bytes of a stateful encoder (UTF-7-IMAP)`, async (_t) => {
	const original = "Hello 世界";
	const streams = [
		createReadableStream([original]),
		charsetEncodeStream({ charset: "UTF-7-IMAP" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	const encoded = Buffer.concat(output.map((c) => Buffer.from(c)));

	// Round-trip through iconv to confirm the full multibyte content survived.
	const decoded = iconv.decode(encoded, "UTF-7-IMAP");
	deepStrictEqual(decoded, original);
});

test(`${variant}: charsetEncodeStream should handle ISO-8859-8-I charset`, async (_t) => {
	const input = ["test"];
	const streams = [
		createReadableStream(input),
		charsetEncodeStream({ charset: "ISO-8859-8-I" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// Should convert ISO-8859-8-I to ISO-8859-8
	deepStrictEqual(output.length, 1);
});

test(`${variant}: charsetDecodeStream should handle ISO-8859-8-I charset`, async (_t) => {
	const input = [Buffer.from("test")];
	const streams = [
		createReadableStream(input),
		charsetDecodeStream({ charset: "ISO-8859-8-I" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	// Should convert ISO-8859-8-I to ISO-8859-8
	deepStrictEqual(typeof output, "string");
});

// *** getSupportedEncoding *** //
test(`${variant}: getSupportedEncoding should convert ISO-8859-8-I to ISO-8859-8`, (_t) => {
	strictEqual(getSupportedEncoding("ISO-8859-8-I"), "ISO-8859-8");
});

test(`${variant}: getSupportedEncoding should pass through other charsets unchanged`, (_t) => {
	strictEqual(getSupportedEncoding("UTF-8"), "UTF-8");
});

test(`${variant}: charsetDecodeStream should handle empty chunks`, async (_t) => {
	const input = [Buffer.from(""), Buffer.from("test")];
	const streams = [
		createReadableStream(input),
		charsetDecodeStream({ charset: "UTF-8" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	deepStrictEqual(output, "test");
});

// *** charsetDetectStream identifies known encodings *** //
test(`${variant}: charsetDetectStream should identify UTF-8 for multibyte input`, async (_t) => {
	const input = [Buffer.from("测试 Hello 世界 Bonjour", "utf8")];
	const streams = [createReadableStream(input), charsetDetectStream()];

	await pipeline(streams);
	const { value } = streams[1].result();

	strictEqual(value.charset, "UTF-8");
	ok(value.confidence > 0, `confidence ${value.confidence} should be > 0`);
});

// *** ascii-classification: pure ASCII text (the most common input) must NOT
// be misreported as a high-confidence ISO-8859-1; ASCII is UTF-8-compatible
// so it should fold into UTF-8 rather than being dropped. *** //
test(`${variant}: charsetDetectStream should classify pure ASCII text as UTF-8`, async (_t) => {
	for (const text of ["Hello World", "Test content", "abc", "1234567890"]) {
		const streams = [
			createReadableStream([Buffer.from(text)]),
			charsetDetectStream(),
		];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(
			value.charset,
			"UTF-8",
			`ASCII text ${JSON.stringify(text)} should classify as UTF-8, got ${value.charset}@${value.confidence}`,
		);
		ok(value.confidence > 0, `confidence ${value.confidence} should be > 0`);
	}
});

// *** empty-input sentinel: an empty stream ran detection on zero bytes; the
// result must signal "nothing to detect" rather than a phantom UTF-8 guess. *** //
test(`${variant}: charsetDetectStream should signal unknown for empty input`, async (_t) => {
	const streams = [createReadableStream([]), charsetDetectStream()];
	await pipeline(streams);
	const { key, value } = streams[1].result();
	strictEqual(key, "charset");
	strictEqual(value.charset, undefined);
	strictEqual(value.confidence, 0);
});

// *** charsetDetectStream concurrent isolation regression *** //
test(`${variant}: charsetDetectStream instances should not share state`, async (_t) => {
	const input1 = [Buffer.from("Hello World")];
	const input2 = [Buffer.from("Bonjour le monde")];

	const streams1 = [createReadableStream(input1), charsetDetectStream()];
	const streams2 = [createReadableStream(input2), charsetDetectStream()];

	await Promise.all([pipeline(streams1), pipeline(streams2)]);

	const result1 = streams1[1].result();
	const result2 = streams2[1].result();

	// Each stream should have independent results
	strictEqual(result1.key, "charset");
	strictEqual(result2.key, "charset");
	// Confidence should reflect only the data from each respective stream
	strictEqual(typeof result1.value.confidence, "number");
	strictEqual(typeof result2.value.confidence, "number");
});

// *** charsetDetectStream confidence normalization *** //
test(`${variant}: charsetDetectStream should normalize confidence across chunks`, async (_t) => {
	// Send many chunks — confidence should be averaged, not sum to huge numbers
	const chunks = Array.from({ length: 100 }, () => Buffer.from("Hello World"));
	const streams = [createReadableStream(chunks), charsetDetectStream()];

	await pipeline(streams);
	const { value } = streams[1].result();

	// Confidence should be a reasonable value (0-100 range), not 100x the single-chunk value
	ok(
		value.confidence <= 100,
		`confidence ${value.confidence} should be <= 100`,
	);
});

// *** charsetEncodeStream charset validation (web-only) *** //
// Web implementation only supports UTF-8; node supports all charsets via iconv
if (variant === "webstream") {
	test(`${variant}: charsetEncodeStream should throw for non-UTF-8 charset`, {
		skip: "requires web implementation",
	}, async (_t) => {
		try {
			charsetEncodeStream({ charset: "iso-8859-1" });
			throw new Error("Expected error");
		} catch (e) {
			ok(e.message.includes("UTF-8"));
		}
	});

	test(`${variant}: charsetDecodeStream should throw for unsupported charset`, {
		skip: "requires web implementation",
	}, async (_t) => {
		try {
			charsetDecodeStream({ charset: "INVALID-CHARSET-999" });
			throw new Error("Expected error");
		} catch (e) {
			ok(e.message !== "Expected error");
		}
	});
}

// *** detect-per-chunk-analyse: detection must be accurate across multibyte
// chunk boundaries (buffer for detection, do not average per-chunk) *** //
test(`${variant}: charsetDetectStream should detect UTF-8 when a multibyte char is split across chunks`, async (_t) => {
	// A 3-byte UTF-8 character split mid-sequence so the first chunk is a single
	// leading byte. Per-chunk analysis mis-detects the fragments (chunk1 ->
	// UTF-32LE, chunk2 -> Shift_JIS) and averaging flips the winner away from
	// UTF-8; whole-sample analysis must still confidently report UTF-8.
	const full = Buffer.from("テスト", "utf8");
	const chunk1 = full.subarray(0, 1);
	const chunk2 = full.subarray(1);

	const streams = [
		createReadableStream([chunk1, chunk2]),
		charsetDetectStream(),
	];

	await pipeline(streams);
	const { value } = streams[1].result();

	strictEqual(value.charset, "UTF-8");
	ok(value.confidence > 0, `confidence ${value.confidence} should be > 0`);
});

// *** koi8r-key-typo: KOI8-R encoded Cyrillic must be detected as KOI8-R
// (the accumulator key must match chardet's output name) *** //
test(`${variant}: charsetDetectStream should detect KOI8-R Cyrillic text`, async (_t) => {
	// "Привет, как дела? ..." encoded as KOI8-R (single-byte Cyrillic).
	// Byte sequence produced by iconv-lite koi8-r encode of the Russian text.
	const koi8rBytes = Buffer.from([
		0xf0, 0xd2, 0xc9, 0xd7, 0xc5, 0xd4, 0x2c, 0x20, 0xcb, 0xc1, 0xcb, 0x20,
		0xc4, 0xc5, 0xcc, 0xc1, 0x3f, 0x20, 0xfa, 0xd4, 0xcf, 0x20, 0xd4, 0xc5,
		0xd3, 0xd4, 0xcf, 0xd7, 0xd9, 0xca, 0x20, 0xd4, 0xc5, 0xcb, 0xd3, 0xd4,
		0x20, 0xce, 0xc1, 0x20, 0xd2, 0xd5, 0xd3, 0xd3, 0xcb, 0xcf, 0xcd, 0x20,
		0xd1, 0xda, 0xd9, 0xcb, 0xc5, 0x2e,
	]);
	const streams = [createReadableStream([koi8rBytes]), charsetDetectStream()];

	await pipeline(streams);
	const { value } = streams[1].result();

	strictEqual(value.charset, "KOI8-R");
	ok(value.confidence > 0, `confidence ${value.confidence} should be > 0`);
});

// *** web-specific fixes: import the web sources directly. A bare
// "@datastream/charset" import always resolves to the node build, so the
// *.web.js code paths are exercised by importing them by file URL. *** //
const decodeWeb = await import(
	`file://${new URL("./decode.web.js", import.meta.url).pathname}`
);
const encodeWeb = await import(
	`file://${new URL("./encode.web.js", import.meta.url).pathname}`
);
// detect.js is a single cross-platform source built for both node and web. To
// prove it is web-safe we import the source directly and run it with the
// node-only `Buffer` global removed, simulating a browser runtime.
const detectSource = await import(
	`file://${new URL("./detect.js", import.meta.url).pathname}`
);

const webStreamToString = async (stream, inputs) => {
	const reader = stream.readable.getReader();
	const writer = stream.writable.getWriter();
	const collected = [];
	const pump = (async () => {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			collected.push(value);
		}
	})();
	for (const input of inputs) {
		await writer.write(input);
	}
	await writer.close();
	await pump;
	return collected.join("");
};

const webStreamToBytes = async (stream, inputs) => {
	const reader = stream.readable.getReader();
	const writer = stream.writable.getWriter();
	const collected = [];
	const pump = (async () => {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			collected.push(value);
		}
	})();
	for (const input of inputs) {
		await writer.write(input);
	}
	await writer.close();
	await pump;
	return Buffer.concat(collected.map((c) => Buffer.from(c)));
};

// *** web-default-charset-crash: no-arg / null charset must fall back to
// UTF-8 like node, not throw *** //
test(`${variant}: web charsetDecodeStream should default to UTF-8 with no charset`, async (_t) => {
	const stream = decodeWeb.charsetDecodeStream();
	const output = await webStreamToString(stream, [Buffer.from("Test", "utf8")]);
	strictEqual(output, "Test");
});

test(`${variant}: web charsetDecodeStream should treat null charset as UTF-8`, async (_t) => {
	const stream = decodeWeb.charsetDecodeStream({ charset: null });
	const output = await webStreamToString(stream, [
		Buffer.from("Hello", "utf8"),
	]);
	strictEqual(output, "Hello");
});

test(`${variant}: web charsetEncodeStream should default to UTF-8 with no charset`, async (_t) => {
	const stream = encodeWeb.charsetEncodeStream();
	const output = await webStreamToBytes(stream, ["Test"]);
	deepStrictEqual(output, Buffer.from("Test", "utf8"));
});

test(`${variant}: web charsetEncodeStream should treat null charset as UTF-8`, async (_t) => {
	const stream = encodeWeb.charsetEncodeStream({ charset: null });
	const output = await webStreamToBytes(stream, ["Hello"]);
	deepStrictEqual(output, Buffer.from("Hello", "utf8"));
});

// *** web-decode-allowlist-too-strict: ISO-8859-1 / ISO-8859-9 are accepted
// by TextDecoder and emitted by the detector, so decode must accept them *** //
test(`${variant}: web charsetDecodeStream should accept ISO-8859-1`, async (_t) => {
	// 0xE9 is "é" in ISO-8859-1 (latin1).
	const stream = decodeWeb.charsetDecodeStream({ charset: "ISO-8859-1" });
	const output = await webStreamToString(stream, [Buffer.from([0xe9])]);
	strictEqual(output, "é");
});

test(`${variant}: web charsetDecodeStream should accept ISO-8859-9`, async (_t) => {
	const stream = decodeWeb.charsetDecodeStream({ charset: "ISO-8859-9" });
	const output = await webStreamToString(stream, [
		Buffer.from("abc", "latin1"),
	]);
	strictEqual(output, "abc");
});

test(`${variant}: web charsetDecodeStream should still reject genuinely unknown encodings`, async (_t) => {
	try {
		decodeWeb.charsetDecodeStream({ charset: "INVALID-CHARSET-999" });
		throw new Error("Expected error");
	} catch (e) {
		ok(
			e.message.includes("Unsupported web encoding"),
			`unexpected error: ${e.message}`,
		);
	}
});

// Run fn with the node-only `Buffer` global removed so any reference to it
// throws ReferenceError, simulating a browser runtime. createReadableStream /
// pipeline themselves do not depend on the Buffer global, so any
// "Buffer is not defined" comes from detect.js itself.
const withoutBuffer = async (fn) => {
	const saved = globalThis.Buffer;
	// Remove the global entirely (not just set undefined) so any reference to
	// the bare `Buffer` identifier throws ReferenceError, matching a browser.
	Reflect.deleteProperty(globalThis, "Buffer");
	try {
		return await fn();
	} finally {
		globalThis.Buffer = saved;
	}
};

const driveDetect = async (inputs) => {
	const streams = [
		createReadableStream(inputs),
		detectSource.charsetDetectStream(),
	];
	await pipeline(streams);
	return streams[1].result();
};

// *** web-detect-buffer-crash: detect must not reference the node-only Buffer
// global in the web build. Exercise the source with BOTH a Uint8Array chunk
// and a string chunk (both pass through the byte-collection path that
// previously called Buffer.from / Buffer.concat) while Buffer is absent. *** //
test(`${variant}: web charsetDetectStream should detect ASCII without referencing Buffer`, async (_t) => {
	const { key, value } = await withoutBuffer(() =>
		driveDetect([new TextEncoder().encode("Hello World")]),
	);
	strictEqual(key, "charset");
	strictEqual(value.charset, "UTF-8");
	ok(value.confidence > 0, `confidence ${value.confidence} should be > 0`);
});

test(`${variant}: web charsetDetectStream should accept string chunks without Buffer`, async (_t) => {
	const { value } = await withoutBuffer(() => driveDetect(["Test content"]));
	strictEqual(value.charset, "UTF-8");
});

test(`${variant}: web charsetDetectStream should signal unknown for empty input`, async (_t) => {
	const { value } = await withoutBuffer(() => driveDetect([]));
	strictEqual(value.charset, undefined);
	strictEqual(value.confidence, 0);
});

// *** charset key string mutations: each charset name in the allowlist must be
// correct so that chardet results are stored under the right key and the
// top-confidence charset is reported accurately. Mutating any name to "" means
// the chardet result is lost and a wrong (zero-confidence) charset wins. *** //

test(`${variant}: charsetDetectStream should detect UTF-16BE`, async (_t) => {
	// UTF-16BE BOM + "Hello World"
	const bytes = Buffer.from([
		0xfe, 0xff, 0x00, 0x48, 0x00, 0x65, 0x00, 0x6c, 0x00, 0x6c, 0x00, 0x6f,
		0x00, 0x20, 0x00, 0x57, 0x00, 0x6f, 0x00, 0x72, 0x00, 0x6c, 0x00, 0x64,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "UTF-16BE");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect UTF-16LE`, async (_t) => {
	// UTF-16LE BOM + "Hello World"
	const bytes = Buffer.from([
		0xff, 0xfe, 0x48, 0x00, 0x65, 0x00, 0x6c, 0x00, 0x6c, 0x00, 0x6f, 0x00,
		0x20, 0x00, 0x57, 0x00, 0x6f, 0x00, 0x72, 0x00, 0x6c, 0x00, 0x64, 0x00,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "UTF-16LE");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect UTF-32BE`, async (_t) => {
	// UTF-32BE encoding of "Hello World"
	const bytes = Buffer.from([
		0, 0, 0, 72, 0, 0, 0, 101, 0, 0, 0, 108, 0, 0, 0, 108, 0, 0, 0, 111, 0, 0,
		0, 32, 0, 0, 0, 87, 0, 0, 0, 111, 0, 0, 0, 114, 0, 0, 0, 108, 0, 0, 0, 100,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "UTF-32BE");
	strictEqual(value.confidence, 100);
});

test(`${variant}: charsetDetectStream should detect UTF-32LE`, async (_t) => {
	// UTF-32LE encoding of "Hello World"
	const bytes = Buffer.from([
		72, 0, 0, 0, 101, 0, 0, 0, 108, 0, 0, 0, 108, 0, 0, 0, 111, 0, 0, 0, 32, 0,
		0, 0, 87, 0, 0, 0, 111, 0, 0, 0, 114, 0, 0, 0, 108, 0, 0, 0, 100, 0, 0, 0,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "UTF-32LE");
	strictEqual(value.confidence, 100);
});

test(`${variant}: charsetDetectStream should detect Shift_JIS`, async (_t) => {
	// Shift_JIS: "\u3053\u308C\u306F\u30C6\u30B9\u30C8\u3067\u3059\u3002\u65E5\u672C\u8A9E"
	const bytes = Buffer.from([
		130, 177, 130, 234, 130, 205, 131, 101, 131, 88, 131, 103, 130, 197, 130,
		183, 129, 66, 147, 250, 150, 123, 140, 234,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "Shift_JIS");
	strictEqual(value.confidence, 100);
});

test(`${variant}: charsetDetectStream should detect EUC-JP`, async (_t) => {
	// EUC-JP: "\u3053\u308C\u306F\u30C6\u30B9\u30C8\u3067\u3059\u3002\u65E5\u672C\u8A9E"
	const bytes = Buffer.from([
		164, 179, 164, 236, 164, 207, 165, 198, 165, 185, 165, 200, 164, 199, 164,
		185, 161, 163, 198, 252, 203, 220, 184, 236,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "EUC-JP");
	strictEqual(value.confidence, 100);
});

test(`${variant}: charsetDetectStream should detect EUC-KR`, async (_t) => {
	// EUC-KR: "\uC548\uB155\uD558\uC138\uC694 \uD55C\uAD6D\uC5B4 \uD14D\uC2A4\uD2B8"
	const bytes = Buffer.from([
		190, 200, 179, 231, 199, 207, 188, 188, 191, 228, 32, 199, 209, 177, 185,
		190, 238, 32, 197, 216, 189, 186, 198, 174,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "EUC-KR");
	strictEqual(value.confidence, 100);
});

test(`${variant}: charsetDetectStream should detect Big5`, async (_t) => {
	// Big5: "\u9019\u662F\u6E2C\u8A66\u3002\u7E41\u9AD4\u4E2D\u6587\u6587\u672C"
	const bytes = Buffer.from([
		179, 111, 172, 79, 180, 250, 184, 213, 161, 67, 193, 99, 197, 233, 164, 164,
		164, 229, 164, 229, 165, 187,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "Big5");
	strictEqual(value.confidence, 100);
});

test(`${variant}: charsetDetectStream should detect GB18030`, async (_t) => {
	// GB18030: "\u8FD9\u662F\u6D4B\u8BD5\u3002\u7B80\u4F53\u4E2D\u6587\u6587\u672C"
	const bytes = Buffer.from([
		213, 226, 202, 199, 178, 226, 202, 212, 161, 163, 188, 242, 204, 229, 214,
		208, 206, 196, 206, 196, 177, 190,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "GB18030");
	strictEqual(value.confidence, 100);
});

test(`${variant}: charsetDetectStream should detect ISO-2022-JP`, async (_t) => {
	// ISO-2022-JP escape-sequence encoded Japanese
	const bytes = Buffer.from([
		0x1b, 0x24, 0x42, 0x46, 0x7c, 0x38, 0x6b, 0x4c, 0x68, 0x1b, 0x28, 0x42,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "ISO-2022-JP");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect ISO-2022-KR`, async (_t) => {
	// ISO-2022-KR escape-sequence encoded Korean
	const bytes = Buffer.from([
		0x1b, 0x24, 0x29, 0x43, 0x0e, 0x4a, 0x49, 0x4e, 0x59, 0x0f,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "ISO-2022-KR");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect ISO-2022-CN`, async (_t) => {
	// ISO-2022-CN escape-sequence encoded Chinese
	const bytes = Buffer.from([
		0x1b, 0x24, 0x29, 0x41, 0x0e, 0x32, 0x36, 0x33, 0x37, 0x0f,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "ISO-2022-CN");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect ISO-8859-1`, async (_t) => {
	// ISO-8859-1: "La s\u00E9r\u00E9nade de pain fran\u00E7ais \u00E9tait tr\u00E8s \u00E9l\u00E9gante"
	const bytes = Buffer.from([
		0x4c, 0x61, 0x20, 0x73, 0xe9, 0x72, 0xe9, 0x6e, 0x61, 0x64, 0x65, 0x20,
		0x64, 0x65, 0x20, 0x70, 0x61, 0x69, 0x6e, 0x20, 0x66, 0x72, 0x61, 0xee,
		0xe7, 0x61, 0x69, 0x73, 0x20, 0xe9, 0x74, 0xe0, 0x69, 0x74, 0x20, 0x74,
		0x72, 0xe8, 0x73, 0x20, 0xe9, 0x6c, 0xe9, 0x67, 0x61, 0x6e, 0x74, 0x65,
		0x20, 0x63, 0x61, 0x72, 0x20, 0x65, 0x6c, 0x6c, 0x65, 0x20, 0x65, 0x73,
		0x74, 0x20, 0x74, 0x72, 0xe8, 0x73, 0x20, 0x62, 0x6f, 0x6e, 0x6e, 0x65,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "ISO-8859-1");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect ISO-8859-2`, async (_t) => {
	// ISO-8859-2: Polish "Polskie znaki: szcz\u0119\u015Bcie, \u017Ar\u00F3d\u0142o, ni\u017C, b\u0142\u0105d"
	const bytes = Buffer.from([
		80, 111, 108, 115, 107, 105, 101, 32, 122, 110, 97, 107, 105, 58, 32, 115,
		122, 99, 122, 234, 182, 99, 105, 101, 44, 32, 188, 114, 243, 100, 179, 111,
		44, 32, 110, 105, 191, 44, 32, 98, 179, 177, 100, 44, 32, 243, 119, 44, 32,
		179, 177, 107, 97, 44, 32, 112, 114, 122, 121, 115, 122, 179, 111, 182, 230,
		46,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "ISO-8859-2");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect ISO-8859-5`, async (_t) => {
	// ISO-8859-5: Cyrillic "\u042D\u0442\u043E \u0442\u0435\u0441\u0442\u043E\u0432\u044B\u0439 \u0442\u0435\u043A\u0441\u0442 \u043D\u0430 \u0440\u0443\u0441\u0441\u043A\u043E\u043C"
	const bytes = Buffer.from([
		205, 226, 222, 32, 226, 213, 225, 226, 222, 210, 235, 217, 32, 226, 213,
		218, 225, 226, 32, 221, 208, 32, 224, 227, 225, 225, 218, 222, 220, 32, 239,
		215, 235, 218, 213,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "ISO-8859-5");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect ISO-8859-6`, async (_t) => {
	// ISO-8859-6: Arabic long text
	const bytes = Buffer.from([
		231, 208, 199, 32, 230, 213, 32, 217, 209, 200, 234, 46, 32, 199, 228, 230,
		213, 32, 199, 228, 217, 209, 200, 234, 32, 228, 228, 199, 206, 202, 200,
		199, 209, 46, 32, 199, 228, 199, 206, 202, 200, 199, 209, 32, 229, 231, 229,
		32, 204, 207, 199, 235, 46, 32, 230, 213, 32, 199, 206, 202, 200, 199, 209,
		32, 217, 209, 200, 234, 32, 229, 215, 232, 228, 46,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "ISO-8859-6");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect ISO-8859-7`, async (_t) => {
	// ISO-8859-7: Greek "\u0391\u03C5\u03C4\u03CC \u03B5\u03AF\u03BD\u03B1\u03B9 \u03B5\u03BB\u03BB\u03B7\u03BD\u03B9\u03BA\u03CC \u03BA\u03B5\u03AF\u03BC\u03B5\u03BD\u03BF"
	const bytes = Buffer.from([
		193, 245, 244, 252, 32, 229, 223, 237, 225, 233, 32, 229, 235, 235, 231,
		237, 233, 234, 252, 32, 234, 229, 223, 236, 229, 237, 239,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "ISO-8859-7");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect ISO-8859-8`, async (_t) => {
	// ISO-8859-8: Hebrew "\u05D6\u05D4\u05D5 \u05D8\u05E7\u05E1\u05D8 \u05D1\u05D3\u05D9\u05E7\u05D4 \u05D1\u05E2\u05D1\u05E8\u05D9\u05EA"
	const bytes = Buffer.from([
		230, 228, 229, 32, 232, 247, 241, 232, 32, 225, 227, 233, 247, 228, 32, 225,
		242, 225, 248, 233, 250,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "ISO-8859-8");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect ISO-8859-9`, async (_t) => {
	// ISO-8859-9: Turkish "Bu T\u00FCrk\u00E7e bir test metni G\u00FC\u015F\u0130\u00D6\u00C7\u015E\u0130"
	const bytes = Buffer.from([
		66, 117, 32, 84, 252, 114, 107, 231, 101, 32, 98, 105, 114, 32, 116, 101,
		115, 116, 32, 109, 101, 116, 110, 105, 32, 71, 252, 254, 221, 214, 199, 222,
		221,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "ISO-8859-9");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect windows-1251`, async (_t) => {
	// windows-1251: Cyrillic "\u041F\u0440\u0438\u0432\u0435\u0442 \u043C\u0438\u0440 \u042D\u0442\u043E \u0442\u0435\u0441\u0442\u043E\u0432\u044B\u0439 \u0442\u0435\u043A\u0441\u0442 \u043D\u0430 \u0440\u0443\u0441\u0441\u043A\u043E\u043C"
	const bytes = Buffer.from([
		207, 240, 232, 226, 229, 242, 32, 236, 232, 240, 32, 221, 242, 238, 32, 242,
		229, 241, 242, 238, 226, 251, 233, 32, 242, 229, 234, 241, 242, 32, 237,
		224, 32, 240, 243, 241, 241, 234, 238, 236,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "windows-1251");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect windows-1252`, async (_t) => {
	// windows-1252 specific code points in 0x80-0x9F range (EUR, ellipsis, quotes, dashes)
	const bytes = Buffer.from([
		0x80, 0x85, 0x93, 0x94, 0x96, 0x97, 0x20, 0x48, 0x65, 0x6c, 0x6c, 0x6f,
		0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x20, 0x54, 0x65, 0x73, 0x74,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "windows-1252");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect windows-1250`, async (_t) => {
	// windows-1250: Polish "Polskie znaki: szcz\u0119\u015Bcie, \u017Ar\u00F3d\u0142o, ni\u017C, b\u0142\u0105d"
	const bytes = Buffer.from([
		80, 111, 108, 115, 107, 105, 101, 32, 122, 110, 97, 107, 105, 58, 32, 115,
		122, 99, 122, 234, 156, 99, 105, 101, 44, 32, 159, 114, 243, 100, 179, 111,
		44, 32, 110, 105, 191, 44, 32, 98, 179, 185, 100, 44, 32, 243, 119, 44, 32,
		179, 185, 107, 97, 44, 32, 112, 114, 122, 121, 115, 122, 179, 111, 156, 230,
		46,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "windows-1250");
	ok(value.confidence > 0);
});

test(`${variant}: charsetDetectStream should detect windows-1256`, async (_t) => {
	// windows-1256: Arabic long text
	const bytes = Buffer.from([
		229, 208, 199, 32, 228, 213, 32, 199, 206, 202, 200, 199, 209, 32, 200, 199,
		225, 225, 219, 201, 32, 199, 225, 218, 209, 200, 237, 201, 46, 32, 199, 225,
		223, 202, 199, 200, 201, 32, 199, 225, 218, 209, 200, 237, 201, 32, 204,
		227, 237, 225, 201, 32, 230, 227, 218, 222, 207, 201, 46,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "windows-1256");
	ok(value.confidence > 0);
});

// *** detect-string-chunk: a string chunk must be encoded to bytes via
// TextEncoder so detection works correctly. Mutating typeof chunk === "string"
// to false causes the string to be passed raw to Uint8Array.set(), which
// silently writes zeros and detection returns garbage/undefined charset. *** //
test(`${variant}: charsetDetectStream should correctly detect charset from a string chunk`, async (_t) => {
	// String chunk passed directly (not as Buffer). The passThrough function must
	// encode it to bytes; if the typeof guard is removed the result is zeros.
	const streams = [
		createReadableStream(["Hello World Test Content"]),
		charsetDetectStream(),
	];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(
		value.charset,
		"UTF-8",
		`string chunk should detect UTF-8, got ${value.charset}`,
	);
	ok(
		value.confidence > 0,
		`confidence ${value.confidence} should be > 0 for string input`,
	);
});

// *** decode-valid-charset-preserved: when iconv.encodingExists(charset) is
// true the fallback to UTF-8 must NOT fire. Mutating the guard to if(true)
// would always reset charset to UTF-8, breaking decodes of other charsets. *** //
test(`${variant}: charsetDecodeStream should preserve non-UTF-8 charset (not fall back)`, async (_t) => {
	// 0xE9 0xE0 0xE8 are "\u00E9\u00E0\u00E8" in ISO-8859-1; as UTF-8 these bytes are invalid
	// and would be replaced with the Unicode replacement character U+FFFD.
	const iso1bytes = Buffer.from([0xe9, 0xe0, 0xe8]);
	const streams = [
		createReadableStream([iso1bytes]),
		charsetDecodeStream({ charset: "ISO-8859-1" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);
	// Correct ISO-8859-1 decode yields "\u00E9\u00E0\u00E8"
	strictEqual(output, "\u00E9\u00E0\u00E8");
	// If charset were wrongly replaced with UTF-8, output would contain U+FFFD
	strictEqual(output.includes("\uFFFD"), false);
});

// *** decode-transform-empty-guard: conv.write() returns "" for partial
// multi-byte sequences; if the if(res?.length) guard is removed, empty strings
// are enqueued as extra chunks. A test counting chunks catches the mutant. *** //
test(`${variant}: charsetDecodeStream should not emit empty chunks for partial multi-byte sequences`, async (_t) => {
	// U+1F600 emoji is 4 bytes in UTF-8: F0 9F 98 80.
	// Split into 4 x 1-byte chunks so the first three write() calls return "".
	// With guard: only 1 non-empty chunk ("\uD83D\uDE00") is enqueued.
	// Without guard (if true): 4 chunks (3 x "" + "\uD83D\uDE00") are enqueued.
	const emojiBytes = Buffer.from("\uD83D\uDE00", "utf8");
	const byteChunks = [
		emojiBytes.subarray(0, 1),
		emojiBytes.subarray(1, 2),
		emojiBytes.subarray(2, 3),
		emojiBytes.subarray(3, 4),
	];
	const streams = [
		createReadableStream(byteChunks),
		charsetDecodeStream({ charset: "UTF-8" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	// Exactly one chunk: the complete emoji string
	strictEqual(output.length, 1);
	strictEqual(output[0], "\uD83D\uDE00");
});

// *** encode-transform-empty-guard: conv.write() returns a 0-length Buffer for
// empty string chunks; if the guard is removed, empty Buffers are enqueued.
// A test checking the exact chunk count catches this mutant. *** //
test(`${variant}: charsetEncodeStream should not emit empty chunks for empty string inputs`, async (_t) => {
	// Mix of empty strings and real content. Each empty string encodes to a
	// 0-length Buffer. With guard: only the 2 non-empty inputs produce chunks.
	// Without guard (if true): 5 chunks (3 x empty Buffer + 2 real).
	const streams = [
		createReadableStream(["", "Hello", "", "World", ""]),
		charsetEncodeStream({ charset: "UTF-8" }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	strictEqual(output.length, 2);
	deepStrictEqual(output[0], Buffer.from("Hello"));
	deepStrictEqual(output[1], Buffer.from("World"));
});

// *** detect-sample-cap: the passThrough must stop accumulating after
// MAX_DETECTION_SAMPLE (65536) bytes so memory is bounded and detection uses
// only the representative prefix. Mutating the guard to if(false) removes the
// cap entirely. This test confirms the stream completes without error and the
// sample cap code path is exercised. *** //
test(`${variant}: charsetDetectStream should handle input at and beyond MAX_DETECTION_SAMPLE`, async (_t) => {
	// Send exactly MAX_DETECTION_SAMPLE + 1 bytes of ASCII content.
	// Real code: returns early after 65536 bytes; extra byte is not sampled.
	const MAX = 64 * 1024; // 65536
	const chunk1 = Buffer.alloc(MAX, 65); // 65536 x "A"
	const chunk2 = Buffer.from([65]); // one more "A"
	const streams = [
		createReadableStream([chunk1, chunk2]),
		charsetDetectStream(),
	];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "UTF-8");
	ok(value.confidence > 0);
});

// *** slice-overflow: when a single chunk is larger than MAX_DETECTION_SAMPLE,
// the real code must truncate to exactly MAX bytes. Mutants that skip the
// truncation (if(false) conditional, MAX+sampleLength arithmetic, <=remaining
// comparison) all append the overflow bytes to the sample, which shifts
// chardet's answer from UTF-8 (ASCII prefix) to KOI8-R (Cyrillic overflow). *** //
test(`${variant}: charsetDetectStream should truncate a single oversized chunk to MAX_DETECTION_SAMPLE`, async (_t) => {
	// Build a single chunk: first MAX bytes are pure ASCII (→ UTF-8 when capped),
	// followed by 32 KB of KOI8-R Cyrillic bytes (→ KOI8-R when overflow leaks).
	const MAX = 64 * 1024;
	const koi8rPattern = Buffer.from([
		0xf0, 0xd2, 0xc9, 0xd7, 0xc5, 0xd4, 0x2c, 0x20, 0xcb, 0xc1, 0xcb, 0x20,
		0xc4, 0xc5, 0xcc, 0xc1, 0x3f, 0x20, 0xfa, 0xd4, 0xcf, 0x20, 0xd4, 0xc5,
		0xd3, 0xd4, 0xcf, 0xd7, 0xd9, 0xca, 0x20, 0xd4, 0xc5, 0xcb, 0xd3, 0xd4,
		0x20, 0xce, 0xc1, 0x20, 0xd2, 0xd5, 0xd3, 0xd3, 0xcb, 0xcf, 0xcd, 0x20,
		0xd1, 0xda, 0xd9, 0xcb, 0xc5, 0x2e,
	]);
	const overflow = Buffer.alloc(32 * 1024);
	for (let i = 0; i < overflow.length; i++)
		overflow[i] = koi8rPattern[i % koi8rPattern.length];

	const singleChunk = Buffer.concat([Buffer.alloc(MAX, 0x41), overflow]);
	const streams = [createReadableStream([singleChunk]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();

	// Real code caps at MAX bytes (pure ASCII → UTF-8 @ confidence 100).
	// Any overflow mutant includes KOI8-R bytes and detects KOI8-R instead.
	strictEqual(
		value.charset,
		"UTF-8",
		`oversized chunk: expected UTF-8 but got ${value.charset}@${value.confidence}`,
	);
	strictEqual(value.confidence, 100);
});

// *** charsets-allowlist-windows-1254: chardet can report "windows-1254"
// (Turkish encoding with c1 bytes). The charsetKeys array must contain the
// exact string "windows-1254" so the match is stored and wins. Mutating the
// key to "" means chardet's result is discarded and windows-1250 wins instead. *** //
test(`${variant}: charsetDetectStream should detect windows-1254`, async (_t) => {
	// windows-1254 Turkish text with c1-range bytes (0x80-0x9F) that force the
	// windows-1254 detector over ISO-8859-9. Bytes produced by:
	//   iconv.encode('Türkçe metin ...', 'windows-1254') plus c1 bytes.
	const bytes = Buffer.from([
		128, 156, 157, 158, 145, 146, 147, 148, 84, 252, 114, 107, 231, 101, 32,
		109, 101, 116, 105, 110, 32, 254, 105, 105, 114, 32, 107, 97, 108, 101, 109,
		32, 246, 240, 114, 101, 116, 109, 101, 110, 32, 103, 252, 122, 101, 108, 32,
		231, 111, 99, 117, 107, 32, 100, 252, 110, 121, 97, 32, 97, 105, 108, 101,
		128, 156, 157, 158, 145, 146, 147, 148,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(value.charset, "windows-1254");
	ok(value.confidence > 0);
});

// *** charsets-allowlist-filter: the "name in charsets" guard must filter
// chardet results to only the charsets listed in charsetKeys. Mutating the
// guard to "if(true)" lets unknown charset names (e.g. "windows-1255") bypass
// the filter and potentially win at high confidence, displacing the real winner
// from the charsetKeys allowlist. *** //
test(`${variant}: charsetDetectStream should filter chardet results to charsetKeys allowlist`, async (_t) => {
	// windows-1255 Hebrew bytes with c1 bytes. chardet returns "windows-1255" @
	// confidence 71 as the top match. "windows-1255" is NOT in charsetKeys, so
	// the real code discards it; GB18030 wins at 10 from the in-allowlist matches.
	// With if(true) mutant, "windows-1255" leaks through and wins at 71.
	const bytes = Buffer.from([
		128, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 249, 236, 229, 237,
		32, 242, 229, 236, 237, 128, 131, 132, 133, 134, 135, 136, 137, 138, 139,
		140, 249, 236, 229, 237, 32, 242, 229, 236, 237,
	]);
	const streams = [createReadableStream([bytes]), charsetDetectStream()];
	await pipeline(streams);
	const { value } = streams[1].result();
	// Real code: windows-1255 discarded → GB18030 wins at confidence 10.
	// Mutant (if true): windows-1255 leaks → wins at confidence 71.
	strictEqual(
		value.charset,
		"GB18030",
		`expected GB18030 (in-allowlist winner) but got ${value.charset}@${value.confidence}`,
	);
});

// *** remaining-arithmetic: "remaining = MAX - sampleLength" bounds how many
// bytes of each subsequent chunk are stored. Mutating to MAX + sampleLength
// makes remaining huge so the second chunk is never truncated, leaking
// post-cap bytes into the sample and shifting detection from UTF-8 to KOI8-R.
// Only observable with TWO chunks (when sampleLength === 0 both expressions
// give MAX, so the first chunk is handled identically). *** //
test(`${variant}: charsetDetectStream should correctly cap the sample across two chunks`, async (_t) => {
	const MAX = 64 * 1024;
	const koi8rPattern = Buffer.from([
		0xf0, 0xd2, 0xc9, 0xd7, 0xc5, 0xd4, 0x2c, 0x20, 0xcb, 0xc1, 0xcb, 0x20,
		0xc4, 0xc5, 0xcc, 0xc1, 0x3f, 0x20, 0xfa, 0xd4, 0xcf, 0x20, 0xd4, 0xc5,
		0xd3, 0xd4, 0xcf, 0xd7, 0xd9, 0xca, 0x20, 0xd4, 0xc5, 0xcb, 0xd3, 0xd4,
		0x20, 0xce, 0xc1, 0x20, 0xd2, 0xd5, 0xd3, 0xd3, 0xcb, 0xcf, 0xcd, 0x20,
		0xd1, 0xda, 0xd9, 0xcb, 0xc5, 0x2e,
	]);
	const koi8r = Buffer.alloc(MAX / 2);
	for (let i = 0; i < koi8r.length; i++)
		koi8r[i] = koi8rPattern[i % koi8rPattern.length];

	// Chunk 1 fills MAX/2 bytes (sampleLength = MAX/2 after it).
	// Chunk 2 has MAX/2 bytes ASCII followed by MAX/2 bytes KOI8-R.
	// Real: remaining = MAX - MAX/2 = MAX/2, so only first MAX/2 of chunk2 kept.
	// Mutant: remaining = MAX + MAX/2 (huge), full chunk2 kept, KOI8-R leaks in.
	const chunk1 = Buffer.alloc(MAX / 2, 0x41);
	const chunk2 = Buffer.concat([Buffer.alloc(MAX / 2, 0x41), koi8r]);
	const streams = [
		createReadableStream([chunk1, chunk2]),
		charsetDetectStream(),
	];
	await pipeline(streams);
	const { value } = streams[1].result();
	strictEqual(
		value.charset,
		"UTF-8",
		`two-chunk cap: expected UTF-8 but got ${value.charset}@${value.confidence}`,
	);
	strictEqual(value.confidence, 100);
});
