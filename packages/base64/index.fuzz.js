import test from "node:test";
import { base64DecodeStream, base64EncodeStream } from "@datastream/base64";
import {
	createReadableStream,
	pipejoin,
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

// Latin1-safe string arbitrary (btoa only supports code points 0-255)
const latin1String = fc.string({ unit: "binary-ascii", minLength: 1 });

// *** base64EncodeStream *** //
test("fuzz base64EncodeStream w/ string input", async () => {
	await fc.assert(
		fc.asyncProperty(latin1String, async (input) => {
			try {
				const streams = [createReadableStream(input), base64EncodeStream()];
				const stream = pipejoin(streams);
				await streamToArray(stream);
			} catch (e) {
				catchError(input, e);
			}
		}),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** base64DecodeStream *** //
test("fuzz base64DecodeStream w/ valid base64 input", async () => {
	await fc.assert(
		fc.asyncProperty(latin1String, async (input) => {
			try {
				const encoded = btoa(input);
				const streams = [createReadableStream(encoded), base64DecodeStream()];
				const stream = pipejoin(streams);
				await streamToArray(stream);
			} catch (e) {
				catchError(input, e);
			}
		}),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** roundtrip encode -> decode *** //
test("fuzz base64 roundtrip encode -> decode", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.string({ unit: "binary-ascii", minLength: 1, maxLength: 1000 }),
			async (input) => {
				try {
					const encodeStreams = [
						createReadableStream(input),
						base64EncodeStream(),
					];
					const encoded = await streamToString(pipejoin(encodeStreams));

					const decodeStreams = [
						createReadableStream(encoded),
						base64DecodeStream(),
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
