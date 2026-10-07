import { bench, suite } from "node:bench";
import { base64DecodeStream, base64EncodeStream } from "@datastream/base64";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToString,
} from "@datastream/core";

// -- Data generators --

const OPS = 10;
const options = { warmup: 2, samples: 30 };

const generateString = (size) => {
	const chars =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
	let result = "";
	for (let i = 0; i < size; i++) {
		result += chars[i % chars.length];
	}
	return result;
};

const smallString = generateString(1_024); // 1KB
const bigString = generateString(1_024 * 1_024); // 1MB

// -- Tests --

suite("base64EncodeStream", () => {
	bench("1KB string", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(smallString), base64EncodeStream()];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	bench("1MB string", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(bigString), base64EncodeStream()];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("base64DecodeStream", () => {
	const smallEncoded = btoa(smallString);
	const bigEncoded = btoa(bigString);

	bench("1KB encoded", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(smallEncoded),
				base64DecodeStream(),
			];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	bench("1MB encoded", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(bigEncoded), base64DecodeStream()];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("base64 roundtrip", () => {
	bench("1MB encode → decode", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(bigString),
				base64EncodeStream(),
				base64DecodeStream(),
			];
			await streamToString(pipejoin(streams));
		}
		b.end(OPS);
	});
});
