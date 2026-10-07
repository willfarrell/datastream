import { bench, suite } from "node:bench";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { ipfsAddStream, ipfsGetStream } from "@datastream/ipfs";

// -- Data generators --

const OPS = 10;
const options = { warmup: 2, samples: 30 };

const generateChunks = (count, size) =>
	Array.from({ length: count }, () => "x".repeat(size));

const smallChunks = generateChunks(100, 1_024); // 100 × 1KB
const largeChunks = generateChunks(1_000, 1_024); // 1000 × 1KB

// -- Tests --

suite("ipfsGetStream", () => {
	bench("100 × 1KB chunks", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const node = {
				get(_cid) {
					return createReadableStream(smallChunks);
				},
			};
			const stream = await ipfsGetStream({ node, cid: "QmPerf" });
			await streamToArray(stream);
		}
		b.end(OPS);
	});

	bench("1000 × 1KB chunks", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const node = {
				get(_cid) {
					return createReadableStream(largeChunks);
				},
			};
			const stream = await ipfsGetStream({ node, cid: "QmPerf" });
			await streamToArray(stream);
		}
		b.end(OPS);
	});
});

suite("ipfsAddStream", () => {
	bench("100 × 1KB chunks", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
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
		}
		b.end(OPS);
	});

	bench("1000 × 1KB chunks", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
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
		}
		b.end(OPS);
	});
});
