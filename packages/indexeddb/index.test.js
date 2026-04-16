import { deepStrictEqual, ok, strictEqual } from "node:assert";
import test from "node:test";
import {
	indexedDBReadStream,
	indexedDBWriteStream,
} from "@datastream/indexeddb";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

const isBrowser =
	typeof window !== "undefined" && typeof indexedDB !== "undefined";

if (isBrowser) {
	const { indexedDBConnect } = await import("@datastream/indexeddb");

	test(`${variant}: indexedDBConnect should open a database connection`, async (_t) => {
		const db = await indexedDBConnect("test-db", 1, {
			upgrade(db) {
				db.createObjectStore("test-store");
			},
		});

		ok(db);
		strictEqual(db.name, "test-db");
		db.close();
	});

	test(`${variant}: indexedDBReadStream should read from an object store`, async (_t) => {
		const db = await indexedDBConnect("test-db-read", 1, {
			upgrade(db) {
				const store = db.createObjectStore("test-store", {
					keyPath: "id",
				});
				store.add({ id: 1, value: "test" });
			},
		});

		const stream = await indexedDBReadStream({
			db,
			store: "test-store",
		});

		const results = [];
		for await (const chunk of stream) {
			results.push(chunk);
		}

		deepStrictEqual(results.length, 1);
		deepStrictEqual(results[0].value, "test");
		db.close();
	});

	test(`${variant}: indexedDBWriteStream should write to an object store`, async (_t) => {
		const db = await indexedDBConnect("test-db-write", 1, {
			upgrade(db) {
				db.createObjectStore("test-store", { keyPath: "id" });
			},
		});

		const writeStream = await indexedDBWriteStream({
			db,
			store: "test-store",
		});

		const data = [
			{ id: 1, value: "a" },
			{ id: 2, value: "b" },
		];

		for (const item of data) {
			writeStream.write(item);
		}
		writeStream.end();

		await new Promise((resolve) => writeStream.on("finish", resolve));

		const readStream = await indexedDBReadStream({ db, store: "test-store" });
		const results = [];
		for await (const chunk of readStream) {
			results.push(chunk);
		}

		deepStrictEqual(results.length, 2);
		deepStrictEqual(results[0].value, "a");
		deepStrictEqual(results[1].value, "b");
		db.close();
	});
}

if (!isBrowser) {
	test(`${variant}: indexedDBReadStream should throw error in Node.js environment`, async (_t) => {
		try {
			await indexedDBReadStream({});
			throw new Error("Expected error was not thrown");
		} catch (e) {
			strictEqual(e.message, "indexedDBReadStream: Not supported");
		}
	});

	test(`${variant}: indexedDBWriteStream should throw error in Node.js environment`, async (_t) => {
		try {
			await indexedDBWriteStream({});
			throw new Error("Expected error was not thrown");
		} catch (e) {
			strictEqual(e.message, "indexedDBWriteStream: Not supported");
		}
	});
}

// *** web variant: indexedDBReadStream with index *** //
if (variant === "webstream") {
	test(`${variant}: indexedDBReadStream should use index and key when provided`, async (_t) => {
		const mockCursor = {
			async *[Symbol.asyncIterator]() {
				yield { id: 1, name: "a" };
				yield { id: 2, name: "b" };
			},
		};
		const mockIndex = {
			iterate: (_key) => mockCursor,
		};
		const mockStore = {
			index: (_name) => mockIndex,
			[Symbol.asyncIterator]: async function* () {
				yield { id: 1, name: "a" };
				yield { id: 2, name: "b" };
				yield { id: 3, name: "c" };
			},
		};
		const mockDb = {
			transaction: (_store) => ({ store: mockStore }),
		};

		const stream = await indexedDBReadStream({
			db: mockDb,
			store: "test",
			index: "name",
			key: "a",
		});
		const { streamToArray } = await import("@datastream/core");
		const output = await streamToArray(stream);

		// Should return filtered results (2 items from index), not all 3 from store
		strictEqual(output.length, 2);
	});
}
