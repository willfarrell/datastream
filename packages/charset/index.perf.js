import test from "node:test";
import { charsetDecodeStream } from "@datastream/charset/decode";
import { charsetDetectStream } from "@datastream/charset/detect";
import { charsetEncodeStream } from "@datastream/charset/encode";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToString,
} from "@datastream/core";
import { Bench } from "tinybench";

// -- Data generators --

const time = Number(process.env.BENCH_TIME ?? 5_000);

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

test("perf: charsetDetectStream", async () => {
	const bench = new Bench({ name: "charsetDetectStream", time });

	bench.add("1MB UTF-8 string", async () => {
		const detect = charsetDetectStream();
		const streams = [createReadableStream(bigString), detect];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: charsetEncodeStream", async () => {
	const bench = new Bench({ name: "charsetEncodeStream", time });

	bench.add("1MB UTF-8", async () => {
		const streams = [
			createReadableStream(bigString),
			charsetEncodeStream({ charset: "UTF-8" }),
		];
		await pipeline(streams);
	});

	bench.add("1MB ISO-8859-1", async () => {
		const streams = [
			createReadableStream(bigString),
			charsetEncodeStream({ charset: "ISO-8859-1" }),
		];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: charsetDecodeStream", async () => {
	const bench = new Bench({ name: "charsetDecodeStream", time });

	bench.add("1MB UTF-8", async () => {
		const streams = [
			createReadableStream(bigString),
			charsetDecodeStream({ charset: "UTF-8" }),
		];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: charset roundtrip (encode → decode)", async () => {
	const bench = new Bench({ name: "charset roundtrip", time });

	bench.add("1MB UTF-8 encode → decode", async () => {
		const streams = [
			createReadableStream(bigString),
			charsetEncodeStream({ charset: "UTF-8" }),
			charsetDecodeStream({ charset: "UTF-8" }),
		];
		await streamToString(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
