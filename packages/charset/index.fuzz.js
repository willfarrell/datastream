import test from "node:test";
import {
	charsetDecodeStream,
	charsetDetectStream,
	charsetEncodeStream,
} from "@datastream/charset";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

const supportedCharsets = [
	"UTF-8",
	"UTF-16LE",
	"UTF-16BE",
	"ISO-8859-1",
	"ISO-8859-2",
	"ISO-8859-8-I",
	"windows-1251",
	"windows-1252",
	"windows-1256",
	"Shift_JIS",
	"EUC-JP",
	"EUC-KR",
	"Big5",
	"GB18030",
];

// *** charsetDetectStream *** //
test("fuzz charsetDetectStream w/ random buffers", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.uint8Array({ minLength: 1, maxLength: 256 }), {
				minLength: 1,
			}),
			async (input) => {
				try {
					const buffers = input.map((arr) => Buffer.from(arr));
					const streams = [
						createReadableStream(buffers),
						charsetDetectStream(),
					];
					await pipeline(streams);
					streams[1].result();
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** charsetEncodeStream *** //
test("fuzz charsetEncodeStream w/ string input and charset", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.string(), { minLength: 1 }),
			fc.constantFrom(...supportedCharsets),
			async (input, charset) => {
				try {
					const streams = [
						createReadableStream(input),
						charsetEncodeStream({ charset }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError({ input, charset }, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** charsetDecodeStream *** //
test("fuzz charsetDecodeStream w/ buffer input and charset", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.uint8Array({ minLength: 1, maxLength: 256 }), {
				minLength: 1,
			}),
			fc.constantFrom(...supportedCharsets),
			async (input, charset) => {
				try {
					const buffers = input.map((arr) => Buffer.from(arr));
					const streams = [
						createReadableStream(buffers),
						charsetDecodeStream({ charset }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError({ input, charset }, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** roundtrip encode -> decode *** //
test("fuzz charset roundtrip encode -> decode w/ UTF-8", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.string({ minLength: 1, maxLength: 500 }),
			async (input) => {
				try {
					const encodeStreams = [
						createReadableStream(input),
						charsetEncodeStream({ charset: "UTF-8" }),
					];
					const encoded = await streamToArray(pipejoin(encodeStreams));

					const decodeStreams = [
						createReadableStream(encoded),
						charsetDecodeStream({ charset: "UTF-8" }),
					];
					const decoded = await streamToString(pipejoin(decodeStreams));

					if (decoded !== input) {
						throw new Error(
							`Roundtrip failed: input=${JSON.stringify(input)}, decoded=${JSON.stringify(decoded)}`,
						);
					}
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** charsetEncodeStream w/ unsupported charset fallback *** //
test("fuzz charsetEncodeStream w/ random charset string", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.string(), { minLength: 1 }),
			fc.string({ minLength: 1, maxLength: 20 }),
			async (input, charset) => {
				try {
					const streams = [
						createReadableStream(input),
						charsetEncodeStream({ charset }),
					];
					const stream = pipejoin(streams);
					await streamToArray(stream);
				} catch (e) {
					catchError({ input, charset }, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});
