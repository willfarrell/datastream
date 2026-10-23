// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
// Node has no IndexedDB; fake-indexeddb provides one for the real `idb`.
import "fake-indexeddb/auto";
import { createReadableStream, pipeline } from "@datastream/core";
// The package has no node export because there is no IndexedDB in Node.
// Per project convention (see index.test.js), the web implementation is
// exercised directly.
import {
	indexedDBConnect,
	indexedDBReadStream,
	indexedDBWriteStream,
} from "./index.browser.js";

const drain = async (stream) => {
	for await (const _chunk of stream) {
		// consume
	}
};

// -- Data generators --

const options = { warmup: 2, samples: 30 };
// fake-indexeddb is far slower than a browser IndexedDB; keep runs short.
const ITEMS = 1_000;

const records = Array.from({ length: ITEMS }, (_, i) => ({
	id: i,
	name: i % 2 === 0 ? "even" : "odd",
	value: `value_${i}`,
}));

const db = await indexedDBConnect("bench", 1, {
	upgrade(db) {
		db.createObjectStore("read", { keyPath: "id" }).createIndex("name", "name");
		db.createObjectStore("write", { keyPath: "id" });
	},
});
const seed = db.transaction("read", "readwrite");
for (const record of records) seed.store.add(record);
await seed.done;

// -- Tests --

suite("indexedDBReadStream", () => {
	bench(`${ITEMS} records`, options, async (b) => {
		b.start();
		await drain(await indexedDBReadStream({ db, store: "read" }));
		b.end(1);
	});
});

suite("indexedDBReadStream index", () => {
	bench(`${ITEMS} records, index "name"`, options, async (b) => {
		const stream = await indexedDBReadStream({
			db,
			store: "read",
			index: "name",
			key: "even",
		});
		b.start();
		await drain(stream);
		b.end(1);
	});
});

suite("indexedDBWriteStream", () => {
	bench(`${ITEMS} records`, options, async (b) => {
		// Empty the store first so add() doesn't hit existing keys.
		await db.clear("write");
		const writeStream = await indexedDBWriteStream({ db, store: "write" });
		b.start();
		await pipeline([createReadableStream(records), writeStream]);
		b.end(1);
	});
});
