import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import { createReadableStream, pipeline } from "@datastream/core";
import { variant } from "../variant.js";

describe(`@datastream/indexeddb (${variant})`, async () => {
	// Browser-only package: node has no export condition, so the node run
	// imports the browser source directly and drives it with mocked idb objects.
	const nodeTest = variant === "node" ? test : test.skip;

	const isBrowser =
		typeof window !== "undefined" && typeof indexedDB !== "undefined";

	if (isBrowser) {
		const { indexedDBConnect, indexedDBReadStream, indexedDBWriteStream } =
			await import("@datastream/indexeddb");

		test(`indexedDBConnect should open a database connection`, async (_t) => {
			const db = await indexedDBConnect("test-db", 1, {
				upgrade(db) {
					db.createObjectStore("test-store");
				},
			});

			ok(db);
			strictEqual(db.name, "test-db");
			db.close();
		});

		test(`indexedDBReadStream should read from an object store`, async (_t) => {
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

		test(`indexedDBWriteStream should write to an object store`, async (_t) => {
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

	// *** Major: browser-only package (no node stub), named exports only *** //
	nodeTest(
		`node has no export condition (browser-only package)`,
		async (_t) => {
			await rejects(import("@datastream/indexeddb"), {
				code: "ERR_PACKAGE_PATH_NOT_EXPORTED",
			});
		},
	);

	test(`module has only named exports`, async (_t) => {
		const mod = await import(
			variant === "browser" ? "@datastream/indexeddb" : "./index.browser.js"
		);
		deepStrictEqual(Object.keys(mod).sort(), [
			"indexedDBConnect",
			"indexedDBReadStream",
			"indexedDBWriteStream",
		]);
	});

	// *** browser implementation, with mocked `idb` objects *** //
	// Under the browser run `@datastream/indexeddb` is the built browser bundle; the
	// node run imports the source directly so these paths are pinned there too. The
	// mocks are shaped like the real library (async iterators yielding IDBCursor
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
		} = await import(
			variant === "browser" ? "@datastream/indexeddb" : "./index.browser.js"
		);

		// Build a mock that mimics idb: stores/indexes return async iterators of
		// IDBCursor-shaped objects ({ value }). `iterate(key)` filters by `name`.
		const makeCursors = (records) => ({
			async *[Symbol.asyncIterator]() {
				for (const record of records) {
					yield { value: record };
				}
			},
		});
		// Like IDB: iterate(undefined) walks everything; the store filters by
		// primary key (id), an index by its key path (name).
		const makeStore = (records) => ({
			iterate: (key) =>
				makeCursors(
					key === undefined ? records : records.filter((r) => r.id === key),
				),
			index: (_name) => ({
				iterate: (key) =>
					makeCursors(
						key === undefined ? records : records.filter((r) => r.name === key),
					),
			}),
			add: async (record) => {
				records.push(record);
			},
		});
		const makeDb = (records, { onTransaction } = {}) => ({
			transaction: (_store, mode) => {
				onTransaction?.(mode);
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
				onTransaction: (mode) => {
					transactions += 1;
					strictEqual(mode, "readwrite");
				},
			});

			const writeStream = await webWriteStream({ db, store: "test" });
			await pipeline([createReadableStream(records), writeStream]);

			strictEqual(transactions, records.length);
		});

		test(`web: indexedDBReadStream applies index and key independently`, async (_t) => {
			const records = [
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
			];
			for (const [query, expected] of [
				[{}, records],
				[{ index: "name" }, records],
				[{ key: 2 }, [records[1]]],
				[{ index: null, key: 2 }, [records[1]]],
				[{ index: "name", key: "a" }, [records[0]]],
			]) {
				const stream = await webReadStream({
					db: makeDb(records),
					store: "test",
					...query,
				});
				deepStrictEqual(await readAll(stream), expected);
			}
		});
	}
});
