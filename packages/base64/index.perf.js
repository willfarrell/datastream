import test from "node:test";
import { base64DecodeStream, base64EncodeStream } from "@datastream/base64";
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

test("perf: base64EncodeStream", async () => {
	const bench = new Bench({ name: "base64EncodeStream", time });

	bench.add("1KB string", async () => {
		const streams = [createReadableStream(smallString), base64EncodeStream()];
		await pipeline(streams);
	});

	bench.add("1MB string", async () => {
		const streams = [createReadableStream(bigString), base64EncodeStream()];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: base64DecodeStream", async () => {
	const bench = new Bench({ name: "base64DecodeStream", time });

	const smallEncoded = btoa(smallString);
	const bigEncoded = btoa(bigString);

	bench.add("1KB encoded", async () => {
		const streams = [createReadableStream(smallEncoded), base64DecodeStream()];
		await pipeline(streams);
	});

	bench.add("1MB encoded", async () => {
		const streams = [createReadableStream(bigEncoded), base64DecodeStream()];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: base64 roundtrip (encode → decode)", async () => {
	const bench = new Bench({ name: "base64 roundtrip", time });

	bench.add("1MB encode → decode", async () => {
		const streams = [
			createReadableStream(bigString),
			base64EncodeStream(),
			base64DecodeStream(),
		];
		await streamToString(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
