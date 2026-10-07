import { bench, suite } from "node:bench";
import { charsetDecodeStream } from "@datastream/charset/decode";
import { charsetDetectStream } from "@datastream/charset/detect";
import { charsetEncodeStream } from "@datastream/charset/encode";
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
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 \n";
	let result = "";
	for (let i = 0; i < size; i++) {
		result += chars[i % chars.length];
	}
	return result;
};

const bigString = generateString(1_024 * 1_024); // 1MB

// -- Tests --

suite("charsetDetectStream", () => {
	bench("1MB UTF-8 string", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const detect = charsetDetectStream();
			const streams = [createReadableStream(bigString), detect];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("charsetEncodeStream", () => {
	bench("1MB UTF-8", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(bigString),
				charsetEncodeStream({ charset: "UTF-8" }),
			];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	bench("1MB ISO-8859-1", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(bigString),
				charsetEncodeStream({ charset: "ISO-8859-1" }),
			];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("charsetDecodeStream", () => {
	bench("1MB UTF-8", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(bigString),
				charsetDecodeStream({ charset: "UTF-8" }),
			];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("charset roundtrip", () => {
	bench("1MB UTF-8 encode → decode", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(bigString),
				charsetEncodeStream({ charset: "UTF-8" }),
				charsetDecodeStream({ charset: "UTF-8" }),
			];
			await streamToString(pipejoin(streams));
		}
		b.end(OPS);
	});
});
