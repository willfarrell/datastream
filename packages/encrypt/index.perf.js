import { randomBytes } from "node:crypto";
import test from "node:test";
import { createReadableStream, pipeline } from "@datastream/core";
import { decryptStream, encryptStream } from "@datastream/encrypt";
import { Bench } from "tinybench";

// -- Data generators --

const time = Number(process.env.BENCH_TIME ?? 5_000);

const smallBuffer = randomBytes(1_024); // 1KB
const bigBuffer = randomBytes(1_024 * 1_024); // 1MB
const key = randomBytes(32);

// -- Tests --

test("perf: encryptStream AES-256-GCM", async () => {
	const bench = new Bench({ name: "encryptStream AES-256-GCM", time });

	bench.add("1KB", async () => {
		const enc = encryptStream({ key });
		const streams = [createReadableStream(smallBuffer), enc];
		await pipeline(streams);
	});

	bench.add("1MB", async () => {
		const enc = encryptStream({ key });
		const streams = [createReadableStream(bigBuffer), enc];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: encryptStream AES-256-CTR", async () => {
	const bench = new Bench({ name: "encryptStream AES-256-CTR", time });

	bench.add("1KB", async () => {
		const enc = encryptStream({ key, algorithm: "AES-256-CTR" });
		const streams = [createReadableStream(smallBuffer), enc];
		await pipeline(streams);
	});

	bench.add("1MB", async () => {
		const enc = encryptStream({ key, algorithm: "AES-256-CTR" });
		const streams = [createReadableStream(bigBuffer), enc];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: encryptStream CHACHA20-POLY1305", async () => {
	const bench = new Bench({ name: "encryptStream CHACHA20-POLY1305", time });

	bench.add("1KB", async () => {
		const enc = encryptStream({ key, algorithm: "CHACHA20-POLY1305" });
		const streams = [createReadableStream(smallBuffer), enc];
		await pipeline(streams);
	});

	bench.add("1MB", async () => {
		const enc = encryptStream({ key, algorithm: "CHACHA20-POLY1305" });
		const streams = [createReadableStream(bigBuffer), enc];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: encrypt→decrypt roundtrip comparison", async () => {
	const bench = new Bench({ name: "roundtrip comparison 1MB", time });

	for (const algorithm of ["AES-256-GCM", "AES-256-CTR", "CHACHA20-POLY1305"]) {
		bench.add(algorithm, async () => {
			const enc = encryptStream({ key, algorithm });
			const encryptedChunks = [];
			const encStream = createReadableStream(bigBuffer).pipe(enc);
			for await (const chunk of encStream) {
				encryptedChunks.push(chunk);
			}
			const { iv, authTag } = enc.result().value;

			const dec = decryptStream({ key, iv, authTag, algorithm });
			const decStream = createReadableStream(encryptedChunks).pipe(dec);
			for await (const _chunk of decStream) {
				// consume
			}
		});
	}

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
