import test from "node:test";
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
	stringMinimumChunkSize,
	stringMinimumFirstChunkSize,
	stringReplaceStream,
	stringSkipConsecutiveDuplicates,
	stringSplitStream,
} from "@datastream/string";
import { Bench } from "tinybench";

// -- Data generators --

const time = Number(process.env.BENCH_TIME ?? 5_000);

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

test("perf: stringLengthStream", async () => {
	const bench = new Bench({ name: "stringLengthStream", time });

	bench.add("1MB string", async () => {
		const length = stringLengthStream();
		const streams = [createReadableStream(bigString), length];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: stringCountStream", async () => {
	const bench = new Bench({ name: "stringCountStream", time });

	bench.add("1MB string, count newlines", async () => {
		const count = stringCountStream({ substr: "\n" });
		const streams = [createReadableStream(bigString), count];
		await pipeline(streams);
	});

	bench.add("1MB string, count 'ABCDE'", async () => {
		const count = stringCountStream({ substr: "ABCDE" });
		const streams = [createReadableStream(bigString), count];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: stringSplitStream", async () => {
	const bench = new Bench({ name: "stringSplitStream", time });

	bench.add("1MB string, split by newline", async () => {
		const streams = [
			createReadableStream(bigString),
			stringSplitStream({ separator: "\n" }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: stringReplaceStream", async () => {
	const bench = new Bench({ name: "stringReplaceStream", time });

	bench.add("1MB string, replace char", async () => {
		const streams = [
			createReadableStream(bigString),
			stringReplaceStream({ pattern: /A/g, replacement: "X" }),
		];
		await streamToString(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: stringMinimumFirstChunkSize", async () => {
	const bench = new Bench({ name: "stringMinimumFirstChunkSize", time });

	bench.add("1MB string, 64KB min first chunk", async () => {
		const streams = [
			createReadableStream(bigString),
			stringMinimumFirstChunkSize({ chunkSize: 64 * 1024 }),
		];
		await streamToString(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: stringMinimumChunkSize", async () => {
	const bench = new Bench({ name: "stringMinimumChunkSize", time });

	bench.add("1MB string, 64KB min chunk", async () => {
		const streams = [
			createReadableStream(bigString),
			stringMinimumChunkSize({ chunkSize: 64 * 1024 }),
		];
		await streamToString(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: stringSkipConsecutiveDuplicates", async () => {
	const bench = new Bench({ name: "stringSkipConsecutiveDuplicates", time });

	const chunks = Array.from({ length: 10_000 }, (_, i) =>
		i % 2 === 0 ? "aaa" : "bbb",
	);
	bench.add("10K chunks, 50% duplicates", async () => {
		const streams = [
			createReadableStream(chunks),
			stringSkipConsecutiveDuplicates(),
		];
		await streamToArray(pipejoin(streams));
	});

	const uniqueChunks = Array.from({ length: 10_000 }, (_, i) => `chunk_${i}`);
	bench.add("10K chunks, all unique", async () => {
		const streams = [
			createReadableStream(uniqueChunks),
			stringSkipConsecutiveDuplicates(),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
