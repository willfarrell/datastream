import { after, bench, suite } from "node:bench";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pipeline, streamToString } from "@datastream/core";
import { fileReadStream, fileWriteStream } from "@datastream/file";

// -- Setup --

const OPS = 10;
const options = { warmup: 2, samples: 30 };

const tmpDir = mkdtempSync(join(tmpdir(), "datastream-perf-"));
const tmpFile = join(tmpDir, "test.csv");
const bigString = Array.from(
	{ length: 10_000 },
	(_, i) => `${i},item_${i},${Math.random()}`,
).join("\n");
writeFileSync(tmpFile, bigString);

const tmpOutFile = join(tmpDir, "test-out.csv");

// -- Tests --

suite("fileReadStream", () => {
	bench("10K row CSV file", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const stream = await fileReadStream({ path: tmpFile });
			await streamToString(stream);
		}
		b.end(OPS);
	});
});

suite("fileWriteStream", () => {
	bench("10K row CSV file", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const stream = await fileReadStream({ path: tmpFile });
			const write = await fileWriteStream({ path: tmpOutFile });
			await pipeline([stream, write]);
		}
		b.end(OPS);
	});
});

suite("file roundtrip", () => {
	bench("10K row CSV read → write", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const read = await fileReadStream({ path: tmpFile });
			const write = await fileWriteStream({ path: tmpOutFile });
			await pipeline([read, write]);
		}
		b.end(OPS);
	});
});

// Cleanup
after(() => {
	try {
		rmSync(tmpDir, { recursive: true });
	} catch {}
});
