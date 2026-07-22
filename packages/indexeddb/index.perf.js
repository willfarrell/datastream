// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import test from "node:test";
import { createReadableStream, pipeline } from "@datastream/core";
import { Bench } from "tinybench";
// Under `node --test` the package entry (index.node.mjs) throws "Not supported"
// because there is no IndexedDB in Node. Per project convention (see
// index.test.js), the real web implementation is exercised directly with an
// `idb`-shaped mock so the streams do real work instead of throwing.
import { indexedDBReadStream, indexedDBWriteStream } from "./index.web.js";

// -- Mock idb (mirrors the shapes used in index.test.js) --

// Stores/indexes return async iterators of IDBCursor-shaped objects ({ value }).
// `iterate(key)` filters by `name`, matching the real index semantics.
const makeCursors = (records) => ({
	async *[Symbol.asyncIterator]() {
		for (const record of records) {
			yield { value: record };
		}
	},
});
const makeStore = (records) => ({
	iterate: () => makeCursors(records),
	index: (_name) => ({
		iterate: (key) => makeCursors(records.filter((r) => r.name === key)),
	}),
	add: async (record) => {
		records.push(record);
	},
});
const makeDb = (records) => ({
	transaction: (_store, _mode) => ({
		store: makeStore(records),
		done: Promise.resolve(),
	}),
});

const drain = async (stream) => {
	for await (const _chunk of stream) {
		// consume
	}
};

// -- Data generators --

const time = Number(process.env.BENCH_TIME ?? 5_000);
const ITEMS = 10_000;

const records = Array.from({ length: ITEMS }, (_, i) => ({
	id: i,
	name: i % 2 === 0 ? "even" : "odd",
	value: `value_${i}`,
}));

// -- Tests --

test("perf: indexedDBReadStream", async () => {
	const bench = new Bench({ name: "indexedDBReadStream", time });

	bench.add(`${ITEMS} records`, async () => {
		const stream = await indexedDBReadStream({
			db: makeDb(records),
			store: "test-store",
		});
		await drain(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: indexedDBReadStream via index + key", async () => {
	const bench = new Bench({ name: "indexedDBReadStream index", time });

	bench.add(`${ITEMS} records, index "name"`, async () => {
		const stream = await indexedDBReadStream({
			db: makeDb(records),
			store: "test-store",
			index: "name",
			key: "even",
		});
		await drain(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: indexedDBWriteStream", async () => {
	const bench = new Bench({ name: "indexedDBWriteStream", time });

	bench.add(`${ITEMS} records`, async () => {
		// Fresh empty store per iteration so the backing array doesn't grow
		// unbounded across runs.
		const writeStream = await indexedDBWriteStream({
			db: makeDb([]),
			store: "test-store",
		});
		await pipeline([createReadableStream(records), writeStream]);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
