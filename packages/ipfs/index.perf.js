import test from "node:test";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { ipfsAddStream, ipfsGetStream } from "@datastream/ipfs";
import { Bench } from "tinybench";

// -- Data generators --

const time = Number(process.env.BENCH_TIME ?? 5_000);

const generateChunks = (count, size) =>
	Array.from({ length: count }, () => "x".repeat(size));

const smallChunks = generateChunks(100, 1_024); // 100 × 1KB
const largeChunks = generateChunks(1_000, 1_024); // 1000 × 1KB

// -- Tests --

test("perf: ipfsGetStream", async () => {
	const bench = new Bench({ name: "ipfsGetStream", time });

	bench.add("100 × 1KB chunks", async () => {
		const node = {
			get(_cid) {
				return createReadableStream(smallChunks);
			},
		};
		const stream = await ipfsGetStream({ node, cid: "QmPerf" });
		await streamToArray(stream);
	});

	bench.add("1000 × 1KB chunks", async () => {
		const node = {
			get(_cid) {
				return createReadableStream(largeChunks);
			},
		};
		const stream = await ipfsGetStream({ node, cid: "QmPerf" });
		await streamToArray(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: ipfsAddStream", async () => {
	const bench = new Bench({ name: "ipfsAddStream", time });

	bench.add("100 × 1KB chunks", async () => {
		const node = {
			async add(_data) {
				return { cid: "QmResult" };
			},
		};
		const streams = [
			createReadableStream(smallChunks),
			await ipfsAddStream({ node }),
		];
		await pipeline(streams);
	});

	bench.add("1000 × 1KB chunks", async () => {
		const node = {
			async add(_data) {
				return { cid: "QmResult" };
			},
		};
		const streams = [
			createReadableStream(largeChunks),
			await ipfsAddStream({ node }),
		];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
