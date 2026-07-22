import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import test from "node:test";
import indexeddbDefault, {
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

	test(`${variant}: default export should expose readStream and writeStream`, (_t) => {
		strictEqual(typeof indexeddbDefault.readStream, "function");
		strictEqual(typeof indexeddbDefault.writeStream, "function");
		// They must be the named exports (same reference).
		strictEqual(indexeddbDefault.readStream, indexedDBReadStream);
		strictEqual(indexeddbDefault.writeStream, indexedDBWriteStream);
	});
}

// *** web variant: exercise the real web implementation directly *** //
// Per project memory, `--conditions=webstream` loads the node build, so the web
// code is exercised by importing `index.web.js` directly with mocked `idb`
// objects shaped like the real library (async iterators that yield IDBCursor
// objects exposing `.value`).
//
// NOTE: `streamToArray` uses event-based stream consumption which doesn't work
// under `--test-force-exit` (used by Stryker). Use `for await` instead so the
// test Promise settles before Node forces an exit.
const readAll = async (stream) => {
	const out = [];
	for await (const chunk of stream) {
		out.push(chunk);
	}
	return out;
};
{
	const {
		indexedDBReadStream: webReadStream,
		indexedDBWriteStream: webWriteStream,
	} = await import("./index.web.js");

	// Build a mock that mimics idb: stores/indexes return async iterators of
	// IDBCursor-shaped objects ({ value }). `iterate(key)` filters by `name`.
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
	const makeDb = (records, { onTransaction } = {}) => ({
		transaction: (_store, _mode) => {
			onTransaction?.();
			return { store: makeStore(records), done: Promise.resolve() };
		},
	});

	test(`web: indexedDBReadStream yields stored records (cursor.value), not raw cursors`, async (_t) => {
		const records = [
			{ id: 1, name: "a" },
			{ id: 2, name: "b" },
		];
		const stream = await webReadStream({
			db: makeDb(records),
			store: "test",
		});

		const output = await readAll(stream);

		deepStrictEqual(output.length, 2);
		// Real stored records, not IDBCursor objects.
		deepStrictEqual(output[0], { id: 1, name: "a" });
		deepStrictEqual(output[1], { id: 2, name: "b" });
		// Guard against the regression of emitting the wrapping cursor object.
		strictEqual(output[0].value, undefined);
	});

	test(`web: indexedDBReadStream uses index + key when provided`, async (_t) => {
		const records = [
			{ id: 1, name: "a" },
			{ id: 2, name: "b" },
			{ id: 3, name: "a" },
		];
		const stream = await webReadStream({
			db: makeDb(records),
			store: "test",
			index: "name",
			key: "a",
		});

		const output = await readAll(stream);

		// Only the two records whose name === "a" come back via the index.
		strictEqual(output.length, 2);
		deepStrictEqual(output[0], { id: 1, name: "a" });
		deepStrictEqual(output[1], { id: 3, name: "a" });
	});

	test(`web: indexedDBReadStream uses the index for a falsy-but-valid key (0)`, async (_t) => {
		// key === 0 is a valid IndexedDB key. The store records use name 0 vs 1.
		const records = [
			{ id: 1, name: 0 },
			{ id: 2, name: 1 },
			{ id: 3, name: 0 },
		];
		const stream = await webReadStream({
			db: makeDb(records),
			store: "test",
			index: "name",
			key: 0,
		});

		const output = await readAll(stream);

		// With the buggy `if (index && key)` guard, key 0 is falsy and the whole
		// store (all 3) would be returned instead of the 2 name===0 records.
		strictEqual(output.length, 2);
		deepStrictEqual(output[0], { id: 1, name: 0 });
		deepStrictEqual(output[1], { id: 3, name: 0 });
	});

	test(`web: indexedDBReadStream uses the index for an empty-string key ("")`, async (_t) => {
		const records = [
			{ id: 1, name: "" },
			{ id: 2, name: "x" },
		];
		const stream = await webReadStream({
			db: makeDb(records),
			store: "test",
			index: "name",
			key: "",
		});

		const output = await readAll(stream);

		strictEqual(output.length, 1);
		deepStrictEqual(output[0], { id: 1, name: "" });
	});

	test(`web: indexedDBReadStream removes the abort listener on normal completion`, async (_t) => {
		// Spy on a real AbortController's listener registration to assert the
		// wrapper's own "abort" listener is removed once iteration settles, so a
		// shared signal does not accumulate one listener per constructed stream.
		const controller = new AbortController();
		const { signal } = controller;
		let listeners = 0;
		const realAdd = signal.addEventListener.bind(signal);
		const realRemove = signal.removeEventListener.bind(signal);
		signal.addEventListener = (...args) => {
			listeners += 1;
			return realAdd(...args);
		};
		signal.removeEventListener = (...args) => {
			listeners -= 1;
			return realRemove(...args);
		};
		const records = [{ id: 1, name: "a" }];

		const stream = await webReadStream(
			{ db: makeDb(records), store: "test" },
			{ signal },
		);
		await readAll(stream);
		// Allow the underlying stream's terminal "close" handlers to run so any
		// listener teardown (wrapper + core) has settled before asserting.
		await new Promise((resolve) => setImmediate(resolve));

		// On normal completion every registered listener must be removed (the
		// wrapper's "abort" listener leaked here before the fix).
		strictEqual(listeners, 0);
	});

	test(`web: indexedDBReadStream stops iterating once the signal aborts`, async (_t) => {
		const controller = new AbortController();
		let produced = 0;
		const cursors = {
			async *[Symbol.asyncIterator]() {
				for (let id = 1; id <= 5; id++) {
					produced += 1;
					// Abort partway through to verify iteration breaks early.
					if (id === 2) controller.abort();
					yield { value: { id } };
				}
			},
		};
		const db = {
			transaction: () => ({
				store: { iterate: () => cursors, index: () => ({}) },
				done: Promise.resolve(),
			}),
		};

		const stream = await webReadStream(
			{ db, store: "test" },
			{ signal: controller.signal },
		);

		// Aborting must stop the stream rather than draining all 5 records.
		await rejects(
			async () => {
				for await (const _ of stream) {
					// drain
				}
			},
			{ name: "AbortError" },
		);
		ok(produced < 5, `expected early stop, produced ${produced}`);
	});

	test(`web: indexedDBWriteStream opens a fresh transaction per chunk (no auto-commit reuse)`, async (_t) => {
		// A shared transaction would auto-commit between async chunks and throw
		// TransactionInactiveError. Assert each write gets its own transaction.
		let transactions = 0;
		const records = [
			{ id: 1, value: "a" },
			{ id: 2, value: "b" },
			{ id: 3, value: "c" },
		];
		const db = makeDb([], {
			onTransaction: () => {
				transactions += 1;
			},
		});

		const writeStream = await webWriteStream({ db, store: "test" });
		for (const record of records) {
			writeStream.write(record);
		}
		writeStream.end();
		await new Promise((resolve) => writeStream.on("finish", resolve));

		strictEqual(transactions, records.length);
	});
}
