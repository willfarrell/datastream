// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
import "fake-indexeddb/auto";
import { createReadableStream, pipeline } from "@datastream/core";
import {
	indexedDBConnect,
	indexedDBReadStream,
	indexedDBWriteStream,
} from "./index.browser.js";

const drain = async (stream) => {
	for await (const _chunk of stream) {
	}
};

const options = { warmup: 2, samples: 30 };
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
		await db.clear("write");
		const writeStream = await indexedDBWriteStream({ db, store: "write" });
		b.start();
		await pipeline([createReadableStream(records), writeStream]);
		b.end(1);
	});
});
