import { bench, suite } from "node:bench";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
	streamToString,
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

// -- Data generators --

const OPS = 10;
const options = { warmup: 2, samples: 30 };

const generateString = (size) => {
	const chars =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\n";
	let result = "";
	for (let i = 0; i < size; i++) {
		result += chars[i % chars.length];
	}
	return result;
};

const bigString = generateString(1_024 * 1_024); // 1MB

// -- Tests --

suite("stringLengthStream", () => {
	bench("1MB string", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const length = stringLengthStream();
			const streams = [createReadableStream(bigString), length];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("stringCountStream", () => {
	bench("1MB string, count newlines", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const count = stringCountStream({ substr: "\n" });
			const streams = [createReadableStream(bigString), count];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	bench("1MB string, count 'ABCDE'", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const count = stringCountStream({ substr: "ABCDE" });
			const streams = [createReadableStream(bigString), count];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("stringSplitStream", () => {
	bench("1MB string, split by newline", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(bigString),
				stringSplitStream({ separator: "\n" }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("stringReplaceStream", () => {
	bench("1MB string, replace char", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(bigString),
				stringReplaceStream({ pattern: /A/g, replacement: "X" }),
			];
			await streamToString(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("stringMinimumFirstChunkSizeStream", () => {
	bench("1MB string, 64KB min first chunk", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(bigString),
				stringMinimumFirstChunkSizeStream({ chunkSize: 64 * 1024 }),
			];
			await streamToString(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("stringMinimumChunkSizeStream", () => {
	bench("1MB string, 64KB min chunk", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(bigString),
				stringMinimumChunkSizeStream({ chunkSize: 64 * 1024 }),
			];
			await streamToString(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("stringSkipConsecutiveDuplicatesStream", () => {
	const chunks = Array.from({ length: 10_000 }, (_, i) =>
		i % 2 === 0 ? "aaa" : "bbb",
	);
	bench("10K chunks, 50% duplicates", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(chunks),
				stringSkipConsecutiveDuplicatesStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});

	const uniqueChunks = Array.from({ length: 10_000 }, (_, i) => `chunk_${i}`);
	bench("10K chunks, all unique", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(uniqueChunks),
				stringSkipConsecutiveDuplicatesStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});
