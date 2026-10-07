import { deepStrictEqual, ok, rejects } from "node:assert";
import test, { describe } from "node:test";
import { createReadableStream, pipeline } from "@datastream/core";
import { variant } from "../variant.js";
import { azureBackoff, azureBatchStream } from "./client.js";

// Fake SDK batch: holds at most `max` entries; entries named "big" never fit.
const fakeBatches = (max) => {
	const sent = [];
	return {
		sent,
		createBatch: async () => ({ items: [], count: 0, maxSizeInBytes: 1024 }),
		sendBatch: async (batch) => {
			sent.push(batch.items);
		},
		add: (batch, chunk) => {
			if (chunk === "big" || batch.count === max) return false;
			batch.items.push(chunk);
			batch.count++;
			return true;
		},
		oversizeMessage: "too big",
	};
};

describe(`@datastream/azure/client (${variant})`, () => {
	test(`azureBackoff floors at 50ms`, async () => {
		const start = Date.now();
		await azureBackoff(0, {});
		ok(Date.now() - start >= 45);
	});

	test(`azureBackoff aborts on signal`, async () => {
		const controller = new AbortController();
		controller.abort();
		await rejects(azureBackoff(20, { signal: controller.signal }));
	});

	test(`azureBatchStream flushes full batches and the remainder`, async () => {
		const batches = fakeBatches(2);
		await pipeline([
			createReadableStream(["a", "b", "c", "d", "e"]),
			azureBatchStream(batches, {}),
		]);
		deepStrictEqual(batches.sent, [["a", "b"], ["c", "d"], ["e"]]);
	});

	test(`azureBatchStream sends nothing for empty input`, async () => {
		const batches = fakeBatches(2);
		await pipeline([createReadableStream([]), azureBatchStream(batches, {})]);
		deepStrictEqual(batches.sent, []);
	});

	test(`azureBatchStream rejects an entry too big for an empty batch`, async () => {
		const batches = fakeBatches(2);
		await rejects(
			pipeline([createReadableStream(["big"]), azureBatchStream(batches, {})]),
			(e) => e instanceof RangeError && e.cause.limit === 1024,
		);
	});

	test(`azureBatchStream rejects oversize entry after flushing`, async () => {
		const batches = fakeBatches(2);
		await rejects(
			pipeline([
				createReadableStream(["a", "big"]),
				azureBatchStream(batches, {}),
			]),
			{ message: "too big" },
		);
		deepStrictEqual(batches.sent, [["a"]]);
	});
});
