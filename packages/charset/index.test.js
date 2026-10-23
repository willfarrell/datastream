import { deepStrictEqual, ok, strictEqual, throws } from "node:assert";
import test, { describe } from "node:test";
import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";
import {
	charsetDecodeStream,
	charsetDetectStream,
	charsetEncodeStream,
} from "@datastream/charset";
import {
	createPassThroughStream,
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";
import iconv from "iconv-lite";
import { variant } from "../variant.js";

describe(`@datastream/charset (${variant})`, async () => {
	const nodeTest = variant === "node" ? test : test.skip;
	const bytes = (chunks) => chunks.map((chunk) => Buffer.from(chunk));

	test(`charsetDetectStream should detect charset from content`, async (_t) => {
		const input = [Buffer.from("Hello World")];
		const streams = [createReadableStream(input), charsetDetectStream()];

		await pipeline(streams);
		const { key, value } = streams[1].result();

		strictEqual(key, "charset");
		strictEqual(typeof value.charset, "string");
		strictEqual(typeof value.confidence, "number");
	});

	test(`charsetDetectStream should detect charset with custom result key`, async (_t) => {
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

	test(`charsetEncodeStream should encode strings to UTF-8 by default`, async (_t) => {
		const input = ["Hello", " ", "World"];
		const streams = [createReadableStream(input), charsetEncodeStream()];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(bytes(output), [
			Buffer.from("Hello"),
			Buffer.from(" "),
			Buffer.from("World"),
		]);
	});

	test(`charsetEncodeStream should encode strings to specified charset`, async (_t) => {
		const input = ["Hello"];
		const streams = [
			createReadableStream(input),
			charsetEncodeStream({ charset: "UTF-8" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(bytes(output), [Buffer.from("Hello")]);
	});

	test(`charsetDecodeStream should decode UTF-8 bytes to strings`, async (_t) => {
		const input = [
			Buffer.from("Hello"),
			Buffer.from(" "),
			Buffer.from("World"),
		];
		const streams = [
			createReadableStream(input),
			charsetDecodeStream({ charset: "UTF-8" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, "Hello World");
	});

	test(`charsetDecodeStream should decode bytes to strings with default charset`, async (_t) => {
		const input = [Buffer.from("Test")];
		const streams = [createReadableStream(input), charsetDecodeStream()];

		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, "Test");
	});

	test(`encode and decode pipeline should roundtrip correctly`, async (_t) => {
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

	test(`detect, encode, decode pipeline should work together`, async (_t) => {
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

	test(`charsetEncodeStream should accept streamOptions`, async (_t) => {
		const input = ["Hello"];
		const streams = [
			createReadableStream(input),
			charsetEncodeStream({}, { objectMode: true }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(bytes(output), [Buffer.from("Hello")]);
	});

	test(`charsetDecodeStream should accept streamOptions`, async (_t) => {
		const input = [Buffer.from("Hello")];
		const streams = [
			createReadableStream(input),
			charsetDecodeStream({}, { objectMode: true }),
		];

		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, "Hello");
	});

	test(`charsetEncodeStream should handle empty input`, async (_t) => {
		const streams = [
			createReadableStream([]),
			charsetEncodeStream({ charset: "UTF-8" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(output.length, 0);
	});

	nodeTest(`charsetEncodeStream should work with UTF-16LE`, async (_t) => {
		const streams = [
			createReadableStream(["test"]),
			charsetEncodeStream({ charset: "UTF-16LE" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		const result = Buffer.concat(output);
		strictEqual(result.length > 0, true);
	});

	test(`charsetDecodeStream should flush remaining bytes`, async (_t) => {
		const fullBytes = Buffer.from("测试", "utf8");
		const chunk1 = fullBytes.slice(0, 3);
		const chunk2 = fullBytes.slice(3);

		const streams = [
			createReadableStream([chunk1, chunk2]),
			charsetDecodeStream({ charset: "UTF-8" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, "测试");
	});

	test(`charsetEncodeStream should flush with incomplete sequence`, async (_t) => {
		const streams = [
			createReadableStream([]),
			charsetEncodeStream({ charset: "UTF-8" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output.length, 0);
	});

	test(`charsetDecodeStream should flush with incomplete multi-byte sequence`, async (_t) => {
		const incompleteBytes = Buffer.from([0xc2]);

		const streams = [
			createReadableStream([incompleteBytes]),
			charsetDecodeStream({ charset: "UTF-8" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		strictEqual(output.includes("\uFFFD"), true);
	});

	test(`charsetDecodeStream should throw for unsupported charset`, (_t) => {
		throws(
			() => charsetDecodeStream({ charset: "unsupported-charset" }),
			/^Error: charsetDecodeStream: Unsupported (web )?encoding "unsupported-charset"$/,
		);
	});

	test(`charsetEncodeStream should throw for unsupported charset`, (_t) => {
		throws(
			() => charsetEncodeStream({ charset: "unsupported-charset" }),
			/^Error: charsetEncodeStream: (Unsupported encoding|Web only supports UTF-8 encoding, got) "unsupported-charset"$/,
		);
	});

	nodeTest(
		`charsetEncodeStream should flush trailing bytes of a stateful encoder (UTF-7-IMAP)`,
		async (_t) => {
			const original = "Hello 世界";
			const streams = [
				createReadableStream([original]),
				charsetEncodeStream({ charset: "UTF-7-IMAP" }),
			];

			const stream = pipejoin(streams);
			const output = await streamToArray(stream);
			const encoded = Buffer.concat(output.map((c) => Buffer.from(c)));

			const decoded = iconv.decode(encoded, "UTF-7-IMAP");
			deepStrictEqual(decoded, original);
		},
	);

	nodeTest(
		`charsetEncodeStream should handle ISO-8859-8-I charset`,
		async (_t) => {
			const input = ["t\u05d0"];
			const streams = [
				createReadableStream(input),
				charsetEncodeStream({ charset: "ISO-8859-8-I" }),
			];

			const stream = pipejoin(streams);
			const output = await streamToArray(stream);

			deepStrictEqual(output, [Buffer.from([0x74, 0xe0])]);
		},
	);

	test(`charsetDecodeStream should handle ISO-8859-8-I charset`, async (_t) => {
		const input = [Buffer.from([0x74, 0xe0])];
		const streams = [
			createReadableStream(input),
			charsetDecodeStream({ charset: "ISO-8859-8-I" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		strictEqual(output, "t\u05d0");
	});

	test(`exports only the stream factories (no default, no getSupportedEncoding)`, async (_t) => {
		for (const [path, names] of [
			[
				"@datastream/charset",
				["charsetDecodeStream", "charsetDetectStream", "charsetEncodeStream"],
			],
			["@datastream/charset/decode", ["charsetDecodeStream"]],
			["@datastream/charset/detect", ["charsetDetectStream"]],
			["@datastream/charset/encode", ["charsetEncodeStream"]],
		]) {
			const mod = await import(path);
			deepStrictEqual(Object.keys(mod).sort(), names, path);
		}
	});

	test(`charsetDecodeStream should handle empty chunks`, async (_t) => {
		const input = [Buffer.from(""), Buffer.from("test")];
		const streams = [
			createReadableStream(input),
			charsetDecodeStream({ charset: "UTF-8" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, "test");
	});

	test(`charsetDetectStream should identify UTF-8 for multibyte input`, async (_t) => {
		const input = [Buffer.from("测试 Hello 世界 Bonjour", "utf8")];
		const streams = [createReadableStream(input), charsetDetectStream()];

		await pipeline(streams);
		const { value } = streams[1].result();

		strictEqual(value.charset, "UTF-8");
		ok(value.confidence > 0, `confidence ${value.confidence} should be > 0`);
	});

	test(`charsetDetectStream should classify pure ASCII text as UTF-8`, async (_t) => {
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

	test(`charsetDetectStream should signal unknown for empty input`, async (_t) => {
		const streams = [createReadableStream([]), charsetDetectStream()];
		await pipeline(streams);
		const { key, value } = streams[1].result();
		strictEqual(key, "charset");
		strictEqual(value.charset, undefined);
		strictEqual(value.confidence, 0);
	});

	test(`charsetDetectStream sample is not changed by a later write to the chunk`, async (_t) => {
		const chunk = Buffer.from("plain ascii text, again!");
		const sjis = [
			130, 177, 130, 234, 130, 205, 131, 101, 131, 88, 131, 103, 130, 197, 130,
			183, 129, 66, 147, 250, 150, 123, 140, 234,
		];
		const streams = [
			createReadableStream([chunk]),
			charsetDetectStream(),
			createPassThroughStream((passed) => passed.set(sjis)),
		];
		await pipeline(streams);
		strictEqual(streams[1].result().value.charset, "UTF-8");
	});

	test(`charsetDetectStream instances should not share state`, async (_t) => {
		const input1 = [Buffer.from("Hello World")];
		const input2 = [Buffer.from("Bonjour le monde")];

		const streams1 = [createReadableStream(input1), charsetDetectStream()];
		const streams2 = [createReadableStream(input2), charsetDetectStream()];

		await Promise.all([pipeline(streams1), pipeline(streams2)]);

		const result1 = streams1[1].result();
		const result2 = streams2[1].result();

		strictEqual(result1.key, "charset");
		strictEqual(result2.key, "charset");
		strictEqual(typeof result1.value.confidence, "number");
		strictEqual(typeof result2.value.confidence, "number");
	});

	test(`charsetDetectStream should normalize confidence across chunks`, async (_t) => {
		const chunks = Array.from({ length: 100 }, () =>
			Buffer.from("Hello World"),
		);
		const streams = [createReadableStream(chunks), charsetDetectStream()];

		await pipeline(streams);
		const { value } = streams[1].result();

		ok(
			value.confidence <= 100,
			`confidence ${value.confidence} should be <= 100`,
		);
	});

	if (variant === "browser") {
		for (const charset of ["iso-8859-1", "xutf-8", "utf-8x"]) {
			test(`charsetEncodeStream should throw for non-UTF-8 charset ${charset}`, (_t) => {
				throws(
					() => charsetEncodeStream({ charset }),
					new RegExp(
						`^Error: charsetEncodeStream: Web only supports UTF-8 encoding, got "${charset}"$`,
					),
				);
			});
		}
	}

	for (const charset of ["utf8", "utf-8"]) {
		test(`charsetEncodeStream should accept UTF-8 label ${charset}`, async (_t) => {
			const streams = [
				createReadableStream(["Hé"]),
				charsetEncodeStream({ charset }),
			];
			const output = await streamToArray(pipejoin(streams));
			deepStrictEqual(Buffer.concat(bytes(output)), Buffer.from("Hé", "utf8"));
		});
	}

	test(`charsetDetectStream should detect UTF-8 when a multibyte char is split across chunks`, async (_t) => {
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

	test(`charsetDetectStream should detect KOI8-R Cyrillic text`, async (_t) => {
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

	const decodeWeb = await import(
		`file://${new URL("./decode.browser.js", import.meta.url).pathname}`
	);
	const encodeWeb = await import(
		`file://${new URL("./encode.browser.js", import.meta.url).pathname}`
	);
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

	test(`web charsetDecodeStream should default to UTF-8 with no charset`, async (_t) => {
		const stream = decodeWeb.charsetDecodeStream();
		const output = await webStreamToString(stream, [
			Buffer.from("Test", "utf8"),
		]);
		strictEqual(output, "Test");
	});

	test(`web charsetDecodeStream should treat null charset as UTF-8`, async (_t) => {
		const stream = decodeWeb.charsetDecodeStream({ charset: null });
		const output = await webStreamToString(stream, [
			Buffer.from("Hello", "utf8"),
		]);
		strictEqual(output, "Hello");
	});

	test(`web charsetEncodeStream should default to UTF-8 with no charset`, async (_t) => {
		const stream = encodeWeb.charsetEncodeStream();
		const output = await webStreamToBytes(stream, ["Test"]);
		deepStrictEqual(output, Buffer.from("Test", "utf8"));
	});

	test(`web charsetEncodeStream should treat null charset as UTF-8`, async (_t) => {
		const stream = encodeWeb.charsetEncodeStream({ charset: null });
		const output = await webStreamToBytes(stream, ["Hello"]);
		deepStrictEqual(output, Buffer.from("Hello", "utf8"));
	});

	test(`web charsetDecodeStream should accept ISO-8859-1`, async (_t) => {
		const stream = decodeWeb.charsetDecodeStream({ charset: "ISO-8859-1" });
		const output = await webStreamToString(stream, [Buffer.from([0xe9])]);
		strictEqual(output, "é");
	});

	test(`web charsetDecodeStream should accept ISO-8859-9`, async (_t) => {
		const stream = decodeWeb.charsetDecodeStream({ charset: "ISO-8859-9" });
		const output = await webStreamToString(stream, [
			Buffer.from("abc", "latin1"),
		]);
		strictEqual(output, "abc");
	});

	test(`web charsetDecodeStream should still reject genuinely unknown encodings`, async (_t) => {
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

	const withoutBuffer = async (fn) => {
		const saved = globalThis.Buffer;
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

	test(`web charsetDetectStream stops encoding once the sample is full, without Buffer`, async (_t) => {
		const encode = TextEncoder.prototype.encode;
		let calls = 0;
		TextEncoder.prototype.encode = function (...args) {
			calls++;
			return encode.apply(this, args);
		};
		try {
			const { value } = await withoutBuffer(() =>
				driveDetect(["a".repeat(64 * 1024), "b", "c"]),
			);
			strictEqual(calls, 1);
			strictEqual(value.charset, "UTF-8");
		} finally {
			TextEncoder.prototype.encode = encode;
		}
	});

	test(`web charsetDetectStream should detect ASCII without referencing Buffer`, async (_t) => {
		const { key, value } = await withoutBuffer(() =>
			driveDetect([new TextEncoder().encode("Hello World")]),
		);
		strictEqual(key, "charset");
		strictEqual(value.charset, "UTF-8");
		ok(value.confidence > 0, `confidence ${value.confidence} should be > 0`);
	});

	test(`web charsetDetectStream should accept string chunks without Buffer`, async (_t) => {
		const { value } = await withoutBuffer(() => driveDetect(["Test content"]));
		strictEqual(value.charset, "UTF-8");
	});

	test(`web charsetDetectStream should signal unknown for empty input`, async (_t) => {
		const { value } = await withoutBuffer(() => driveDetect([]));
		strictEqual(value.charset, undefined);
		strictEqual(value.confidence, 0);
	});

	test(`charsetDetectStream should detect UTF-16BE`, async (_t) => {
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

	test(`charsetDetectStream should detect UTF-16LE`, async (_t) => {
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

	test(`charsetDetectStream should detect UTF-32BE`, async (_t) => {
		const bytes = Buffer.from([
			0, 0, 0, 72, 0, 0, 0, 101, 0, 0, 0, 108, 0, 0, 0, 108, 0, 0, 0, 111, 0, 0,
			0, 32, 0, 0, 0, 87, 0, 0, 0, 111, 0, 0, 0, 114, 0, 0, 0, 108, 0, 0, 0,
			100,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "UTF-32BE");
		strictEqual(value.confidence, 100);
	});

	test(`charsetDetectStream should detect UTF-32LE`, async (_t) => {
		const bytes = Buffer.from([
			72, 0, 0, 0, 101, 0, 0, 0, 108, 0, 0, 0, 108, 0, 0, 0, 111, 0, 0, 0, 32,
			0, 0, 0, 87, 0, 0, 0, 111, 0, 0, 0, 114, 0, 0, 0, 108, 0, 0, 0, 100, 0, 0,
			0,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "UTF-32LE");
		strictEqual(value.confidence, 100);
	});

	test(`charsetDetectStream should detect Shift_JIS`, async (_t) => {
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

	test(`charsetDetectStream should detect EUC-JP`, async (_t) => {
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

	test(`charsetDetectStream should detect EUC-KR`, async (_t) => {
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

	test(`charsetDetectStream should detect Big5`, async (_t) => {
		const bytes = Buffer.from([
			179, 111, 172, 79, 180, 250, 184, 213, 161, 67, 193, 99, 197, 233, 164,
			164, 164, 229, 164, 229, 165, 187,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "Big5");
		strictEqual(value.confidence, 100);
	});

	test(`charsetDetectStream should detect GB18030`, async (_t) => {
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

	test(`charsetDetectStream should detect ISO-2022-JP`, async (_t) => {
		const bytes = Buffer.from([
			0x1b, 0x24, 0x42, 0x46, 0x7c, 0x38, 0x6b, 0x4c, 0x68, 0x1b, 0x28, 0x42,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "ISO-2022-JP");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should detect ISO-2022-KR`, async (_t) => {
		const bytes = Buffer.from([
			0x1b, 0x24, 0x29, 0x43, 0x0e, 0x4a, 0x49, 0x4e, 0x59, 0x0f,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "ISO-2022-KR");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should detect ISO-2022-CN`, async (_t) => {
		const bytes = Buffer.from([
			0x1b, 0x24, 0x29, 0x41, 0x0e, 0x32, 0x36, 0x33, 0x37, 0x0f,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "ISO-2022-CN");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should detect ISO-8859-1`, async (_t) => {
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

	test(`charsetDetectStream should detect ISO-8859-2`, async (_t) => {
		const bytes = Buffer.from([
			80, 111, 108, 115, 107, 105, 101, 32, 122, 110, 97, 107, 105, 58, 32, 115,
			122, 99, 122, 234, 182, 99, 105, 101, 44, 32, 188, 114, 243, 100, 179,
			111, 44, 32, 110, 105, 191, 44, 32, 98, 179, 177, 100, 44, 32, 243, 119,
			44, 32, 179, 177, 107, 97, 44, 32, 112, 114, 122, 121, 115, 122, 179, 111,
			182, 230, 46,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "ISO-8859-2");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should detect ISO-8859-5`, async (_t) => {
		const bytes = Buffer.from([
			205, 226, 222, 32, 226, 213, 225, 226, 222, 210, 235, 217, 32, 226, 213,
			218, 225, 226, 32, 221, 208, 32, 224, 227, 225, 225, 218, 222, 220, 32,
			239, 215, 235, 218, 213,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "ISO-8859-5");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should detect ISO-8859-6`, async (_t) => {
		const bytes = Buffer.from([
			231, 208, 199, 32, 230, 213, 32, 217, 209, 200, 234, 46, 32, 199, 228,
			230, 213, 32, 199, 228, 217, 209, 200, 234, 32, 228, 228, 199, 206, 202,
			200, 199, 209, 46, 32, 199, 228, 199, 206, 202, 200, 199, 209, 32, 229,
			231, 229, 32, 204, 207, 199, 235, 46, 32, 230, 213, 32, 199, 206, 202,
			200, 199, 209, 32, 217, 209, 200, 234, 32, 229, 215, 232, 228, 46,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "ISO-8859-6");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should detect ISO-8859-7`, async (_t) => {
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

	test(`charsetDetectStream should detect ISO-8859-8`, async (_t) => {
		const bytes = Buffer.from([
			230, 228, 229, 32, 232, 247, 241, 232, 32, 225, 227, 233, 247, 228, 32,
			225, 242, 225, 248, 233, 250,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "ISO-8859-8");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should detect ISO-8859-9`, async (_t) => {
		const bytes = Buffer.from([
			66, 117, 32, 84, 252, 114, 107, 231, 101, 32, 98, 105, 114, 32, 116, 101,
			115, 116, 32, 109, 101, 116, 110, 105, 32, 71, 252, 254, 221, 214, 199,
			222, 221,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "ISO-8859-9");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should detect windows-1251`, async (_t) => {
		const bytes = Buffer.from([
			207, 240, 232, 226, 229, 242, 32, 236, 232, 240, 32, 221, 242, 238, 32,
			242, 229, 241, 242, 238, 226, 251, 233, 32, 242, 229, 234, 241, 242, 32,
			237, 224, 32, 240, 243, 241, 241, 234, 238, 236,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "windows-1251");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should detect windows-1252`, async (_t) => {
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

	test(`charsetDetectStream should detect windows-1250`, async (_t) => {
		const bytes = Buffer.from([
			80, 111, 108, 115, 107, 105, 101, 32, 122, 110, 97, 107, 105, 58, 32, 115,
			122, 99, 122, 234, 156, 99, 105, 101, 44, 32, 159, 114, 243, 100, 179,
			111, 44, 32, 110, 105, 191, 44, 32, 98, 179, 185, 100, 44, 32, 243, 119,
			44, 32, 179, 185, 107, 97, 44, 32, 112, 114, 122, 121, 115, 122, 179, 111,
			156, 230, 46,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "windows-1250");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should detect windows-1256`, async (_t) => {
		const bytes = Buffer.from([
			229, 208, 199, 32, 228, 213, 32, 199, 206, 202, 200, 199, 209, 32, 200,
			199, 225, 225, 219, 201, 32, 199, 225, 218, 209, 200, 237, 201, 46, 32,
			199, 225, 223, 202, 199, 200, 201, 32, 199, 225, 218, 209, 200, 237, 201,
			32, 204, 227, 237, 225, 201, 32, 230, 227, 218, 222, 207, 201, 46,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "windows-1256");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should correctly detect charset from a string chunk`, async (_t) => {
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

	test(`charsetDecodeStream should preserve non-UTF-8 charset (not fall back)`, async (_t) => {
		const iso1bytes = Buffer.from([0xe9, 0xe0, 0xe8]);
		const streams = [
			createReadableStream([iso1bytes]),
			charsetDecodeStream({ charset: "ISO-8859-1" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);
		strictEqual(output, "\u00E9\u00E0\u00E8");
		strictEqual(output.includes("\uFFFD"), false);
	});

	test(`charsetDecodeStream should not emit empty chunks for partial multi-byte sequences`, async (_t) => {
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
		strictEqual(output.length, 1);
		strictEqual(output[0], "\uD83D\uDE00");
	});

	test(`charsetEncodeStream should not emit empty chunks for empty string inputs`, async (_t) => {
		const streams = [
			createReadableStream(["", "Hello", "", "World", ""]),
			charsetEncodeStream({ charset: "UTF-8" }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(bytes(output), [
			Buffer.from("Hello"),
			Buffer.from("World"),
		]);
	});

	test(`charsetDetectStream should handle input at and beyond MAX_DETECTION_SAMPLE`, async (_t) => {
		const MAX = 64 * 1024;
		const chunk1 = Buffer.alloc(MAX, 65);
		const chunk2 = Buffer.from([65]);
		const streams = [
			createReadableStream([chunk1, chunk2]),
			charsetDetectStream(),
		];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "UTF-8");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should truncate a single oversized chunk to MAX_DETECTION_SAMPLE`, async (_t) => {
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
		const streams = [
			createReadableStream([singleChunk]),
			charsetDetectStream(),
		];
		await pipeline(streams);
		const { value } = streams[1].result();

		strictEqual(
			value.charset,
			"UTF-8",
			`oversized chunk: expected UTF-8 but got ${value.charset}@${value.confidence}`,
		);
		strictEqual(value.confidence, 100);
	});

	test(`charsetDetectStream should detect windows-1254`, async (_t) => {
		const bytes = Buffer.from([
			128, 156, 157, 158, 145, 146, 147, 148, 84, 252, 114, 107, 231, 101, 32,
			109, 101, 116, 105, 110, 32, 254, 105, 105, 114, 32, 107, 97, 108, 101,
			109, 32, 246, 240, 114, 101, 116, 109, 101, 110, 32, 103, 252, 122, 101,
			108, 32, 231, 111, 99, 117, 107, 32, 100, 252, 110, 121, 97, 32, 97, 105,
			108, 101, 128, 156, 157, 158, 145, 146, 147, 148,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(value.charset, "windows-1254");
		ok(value.confidence > 0);
	});

	test(`charsetDetectStream should filter chardet results to charsetKeys allowlist`, async (_t) => {
		const bytes = Buffer.from([
			128, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 249, 236, 229, 237,
			32, 242, 229, 236, 237, 128, 131, 132, 133, 134, 135, 136, 137, 138, 139,
			140, 249, 236, 229, 237, 32, 242, 229, 236, 237,
		]);
		const streams = [createReadableStream([bytes]), charsetDetectStream()];
		await pipeline(streams);
		const { value } = streams[1].result();
		strictEqual(
			value.charset,
			"GB18030",
			`expected GB18030 (in-allowlist winner) but got ${value.charset}@${value.confidence}`,
		);
	});

	test(`charsetDetectStream should correctly cap the sample across two chunks`, async (_t) => {
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

	if (variant === "browser") {
		test(`charsetEncodeStream rejects anything but UTF-8`, () => {
			throws(
				() => charsetEncodeStream({ charset: "UTF-16LE" }),
				/Web only supports UTF-8 encoding, got "UTF-16LE"/,
			);
			ok(charsetEncodeStream({ charset: "utf-8" }));
		});

		test(`charsetDecodeStream rejects an unknown encoding label`, () => {
			throws(
				() => charsetDecodeStream({ charset: "unsupported-charset" }),
				/Unsupported web encoding "unsupported-charset"/,
			);
		});
	}

	test(`charsetDecodeStream decodes UTF-16LE`, async (_t) => {
		const streams = [
			createReadableStream([Buffer.from("test", "utf16le")]),
			charsetDecodeStream({ charset: "UTF-16LE" }),
		];
		strictEqual(await streamToString(pipejoin(streams)), "test");
	});

	test(`charset null means UTF-8 for encode and decode`, async (_t) => {
		const streams = [
			createReadableStream(["héllo"]),
			charsetEncodeStream({ charset: null }),
			charsetDecodeStream({ charset: null }),
		];
		strictEqual(await streamToString(pipejoin(streams)), "héllo");
	});
	setFlagsFromString("--expose-gc");
	const gc = runInNewContext("gc");
	test(`charsetDetectStream should not retain streamed chunk buffers`, async (_t) => {
		const refs = [];
		async function* chunks() {
			for (const size of [100 * 1024, 1024, 1024]) {
				const chunk = new Uint8Array(size).fill(65);
				refs.push(new WeakRef(chunk.buffer));
				yield chunk;
			}
		}
		const streams = [createReadableStream(chunks()), charsetDetectStream()];
		await pipeline(streams);
		await new Promise((resolve) => setImmediate(resolve));
		gc();
		deepStrictEqual(
			refs.map((ref) => ref.deref() === undefined),
			[true, true, true],
		);
		strictEqual(streams[1].result().value.charset, "UTF-8");
	});

	test(`charsetDetectStream should stop encoding string chunks once the sample is full`, async (_t) => {
		const encode = TextEncoder.prototype.encode;
		let calls = 0;
		TextEncoder.prototype.encode = function (...args) {
			calls++;
			return encode.apply(this, args);
		};
		try {
			const streams = [
				createReadableStream(["a".repeat(64 * 1024), "b", "c"]),
				charsetDetectStream(),
			];
			await pipeline(streams);
			strictEqual(calls, 1);
		} finally {
			TextEncoder.prototype.encode = encode;
		}
	});
});
