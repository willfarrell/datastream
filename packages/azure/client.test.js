import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import { createReadableStream, pipeline } from "@datastream/core";
import { variant } from "../variant.js";
import { azureBackoff, azureBatchStream } from "./client.js";

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
		destroyMessage: "destroyed with a buffered batch",
	};
};

const writeAll = async (stream, chunks) => {
	for (const chunk of chunks) {
		await new Promise((resolve) => stream.write(chunk, resolve));
	}
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
		deepStrictEqual(batches.sent, []);
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

	test(`azureBatchStream destroy attaches buffered entries as failedMessages`, async () => {
		const batches = fakeBatches(2);
		const stream = azureBatchStream(batches, {});
		const error = new Promise((resolve) => stream.once("error", resolve));
		await writeAll(stream, ["a", "b", "c"]);
		stream.destroy();
		const e = await error;
		strictEqual(e.message, "destroyed with a buffered batch");
		deepStrictEqual(e.failedMessages, ["c"]);
		deepStrictEqual(batches.sent, [["a", "b"]]);
	});

	test(`azureBatchStream destroy keeps the caller's error`, async () => {
		const batches = fakeBatches(2);
		const stream = azureBatchStream(batches, {});
		const error = new Promise((resolve) => stream.once("error", resolve));
		await writeAll(stream, ["a"]);
		const cause = new Error("boom");
		stream.destroy(cause);
		strictEqual(await error, cause);
		deepStrictEqual(cause.failedMessages, ["a"]);
	});

	test(`azureBatchStream abort attaches buffered entries without sending`, async () => {
		const batches = fakeBatches(2);
		const controller = new AbortController();
		const stream = azureBatchStream(batches, { signal: controller.signal });
		const error = new Promise((resolve) => stream.once("error", resolve));
		await writeAll(stream, ["a", "b"]);
		controller.abort();
		const e = await error;
		strictEqual(e.name, "AbortError");
		deepStrictEqual(e.failedMessages, ["a", "b"]);
		deepStrictEqual(batches.sent, []);
	});

	test(`azureBatchStream auto-destroy after a clean end emits no error`, async () => {
		const batches = fakeBatches(2);
		const stream = azureBatchStream(batches, {});
		let errored = false;
		stream.on("error", () => {
			errored = true;
		});
		await writeAll(stream, ["a", "b", "c", "d"]);
		const closed = new Promise((resolve) => stream.once("close", resolve));
		await new Promise((resolve) => stream.end(resolve));
		await closed;
		strictEqual(errored, false);
		deepStrictEqual(batches.sent, [
			["a", "b"],
			["c", "d"],
		]);
	});

	test(`azureBatchStream destroy before any write emits no error`, async () => {
		const stream = azureBatchStream(fakeBatches(2), {});
		let errored = false;
		stream.on("error", () => {
			errored = true;
		});
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(errored, false);
	});
});
