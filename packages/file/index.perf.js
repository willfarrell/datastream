import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pipeline, streamToString } from "@datastream/core";
import { fileReadStream, fileWriteStream } from "@datastream/file";
import { Bench } from "tinybench";

// -- Setup --

const time = Number(process.env.BENCH_TIME ?? 5_000);

const tmpDir = mkdtempSync(join(tmpdir(), "datastream-perf-"));
const tmpFile = join(tmpDir, "test.csv");
const bigString = Array.from(
	{ length: 10_000 },
	(_, i) => `${i},item_${i},${Math.random()}`,
).join("\n");
writeFileSync(tmpFile, bigString);

const tmpOutFile = join(tmpDir, "test-out.csv");

// -- Tests --

test("perf: fileReadStream", async () => {
	const bench = new Bench({ name: "fileReadStream", time });

	bench.add("10K row CSV file", async () => {
		const stream = fileReadStream({ path: tmpFile });
		await streamToString(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: fileWriteStream", async () => {
	const bench = new Bench({ name: "fileWriteStream", time });

	bench.add("10K row CSV file", async () => {
		const stream = fileReadStream({ path: tmpFile });
		const write = fileWriteStream({ path: tmpOutFile });
		await pipeline([stream, write]);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: fileReadStream → fileWriteStream roundtrip", async () => {
	const bench = new Bench({ name: "file roundtrip", time });

	bench.add("10K row CSV read → write", async () => {
		const read = fileReadStream({ path: tmpFile });
		const write = fileWriteStream({ path: tmpOutFile });
		await pipeline([read, write]);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

// Cleanup
test("cleanup temp files", () => {
	try {
		rmSync(tmpDir, { recursive: true });
	} catch {}
});
