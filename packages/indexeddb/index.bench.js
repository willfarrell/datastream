// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
import { createReadableStream, pipeline } from "@datastream/core";
// Under `node --test` the package entry (index.node.mjs) throws "Not supported"
// because there is no IndexedDB in Node. Per project convention (see
// index.test.js), the real web implementation is exercised directly with an
// `idb`-shaped mock so the streams do real work instead of throwing.
import { indexedDBReadStream, indexedDBWriteStream } from "./index.browser.js";

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

const OPS = 10;
const options = { warmup: 2, samples: 30 };
const ITEMS = 10_000;

const records = Array.from({ length: ITEMS }, (_, i) => ({
	id: i,
	name: i % 2 === 0 ? "even" : "odd",
	value: `value_${i}`,
}));

// -- Tests --

suite("indexedDBReadStream", () => {
	bench(`${ITEMS} records`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const stream = await indexedDBReadStream({
				db: makeDb(records),
				store: "test-store",
			});
			await drain(stream);
		}
		b.end(OPS);
	});
});

suite("indexedDBReadStream index", () => {
	bench(`${ITEMS} records, index "name"`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const stream = await indexedDBReadStream({
				db: makeDb(records),
				store: "test-store",
				index: "name",
				key: "even",
			});
			await drain(stream);
		}
		b.end(OPS);
	});
});

suite("indexedDBWriteStream", () => {
	bench(`${ITEMS} records`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			// Fresh empty store per iteration so the backing array doesn't grow
			// unbounded across runs.
			const writeStream = await indexedDBWriteStream({
				db: makeDb([]),
				store: "test-store",
			});
			await pipeline([createReadableStream(records), writeStream]);
		}
		b.end(OPS);
	});
});
