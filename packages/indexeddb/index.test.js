import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
// Real IndexedDB semantics (incl. transaction auto-commit) for the `idb` tests.
import "fake-indexeddb/auto";
import {
	createReadableStream,
	createTransformStream,
	pipeline,
} from "@datastream/core";
import { variant } from "../variant.js";

describe(`@datastream/indexeddb (${variant})`, async () => {
	// Browser-only package: node has no export condition, so the node run
	// imports the browser source directly and drives it with fake-indexeddb.
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

	// *** browser implementation *** //
	// Under the browser run `@datastream/indexeddb` is the built browser bundle; the
	// node run imports the source directly so these paths are pinned there too.
	// Reads run against the real `idb` over fake-indexeddb, which reproduces
	// transaction auto-commit (TransactionInactiveError) like a browser does.
	//
	// NOTE: `streamToArray` uses event-based stream consumption which doesn't work
	// under `--test-force-exit` (used by Stryker). Use `for await` instead so the
	// test Promise settles before Node forces an exit.
	const readAll = async (stream, onChunk) => {
		const out = [];
		for await (const chunk of stream) {
			out.push(chunk);
			await onChunk?.(out.length);
		}
		return out;
	};
	{
		const {
			indexedDBConnect: connect,
			indexedDBReadStream: webReadStream,
			indexedDBWriteStream: webWriteStream,
		} = await import(
			variant === "browser" ? "@datastream/indexeddb" : "./index.browser.js"
		);

		let dbCount = 0;
		// Fresh database with a "test" store (keyPath id) and a "name" index.
		const seed = async (records) => {
			dbCount += 1;
			const db = await connect(`db-${dbCount}`, 1, {
				upgrade(db) {
					const store = db.createObjectStore("test", { keyPath: "id" });
					store.createIndex("name", "name");
				},
			});
			const tx = db.transaction("test", "readwrite");
			for (const record of records) tx.store.add(record);
			await tx.done;
			return db;
		};
		// ids 1..n; names cycle so the index holds duplicates.
		const range = (n, name = (id) => `n${id % 3}`) =>
			Array.from({ length: n }, (_, i) => ({ id: i + 1, name: name(i + 1) }));
		// Count the transactions a read opens (one per batch).
		const countTransactions = (db) => {
			const counter = { count: 0 };
			const transaction = db.transaction.bind(db);
			db.transaction = (...args) => {
				counter.count += 1;
				return transaction(...args);
			};
			return counter;
		};
		const remove = async (db, ids) => {
			const tx = db.transaction("test", "readwrite");
			for (const id of ids) tx.store.delete(id);
			await tx.done;
		};

		test(`web: indexedDBReadStream survives a slow consumer (no TransactionInactiveError)`, async (_t) => {
			const records = range(250);
			const db = await seed(records);

			const stream = await webReadStream({ db, store: "test" });
			// A non-IDB await between reads lets an idle transaction auto-commit.
			const output = await readAll(stream, () => sleep(1));

			deepStrictEqual(output, records);
			db.close();
		});

		test(`web: indexedDBReadStream yields stored records, not raw cursors`, async (_t) => {
			const records = range(2);
			const db = await seed(records);

			const output = await readAll(await webReadStream({ db, store: "test" }));

			deepStrictEqual(output, records);
			strictEqual(output[0].value, undefined);
			db.close();
		});

		test(`web: indexedDBReadStream reads index duplicates across batches exactly once`, async (_t) => {
			// Every record shares the index key, so batches resume by primary key.
			const records = range(250, () => "a");
			const db = await seed([...records, { id: 251, name: "b" }]);

			const stream = await webReadStream({
				db,
				store: "test",
				index: "name",
				key: "a",
			});

			deepStrictEqual(await readAll(stream, () => sleep(0)), records);
			db.close();
		});

		test(`web: indexedDBReadStream walks a whole index in (key, primaryKey) order`, async (_t) => {
			const records = range(250);
			const db = await seed(records);

			const output = await readAll(
				await webReadStream({ db, store: "test", index: "name" }),
			);

			deepStrictEqual(
				output,
				records.toSorted((a, b) =>
					a.name === b.name ? a.id - b.id : a.name < b.name ? -1 : 1,
				),
			);
			db.close();
		});

		test(`web: indexedDBReadStream applies an IDBKeyRange on the store across batches`, async (_t) => {
			const records = range(250);
			const db = await seed(records);

			const output = await readAll(
				await webReadStream({
					db,
					store: "test",
					key: IDBKeyRange.bound(20, 230, true, false),
				}),
			);

			deepStrictEqual(output, records.slice(20, 230));
			db.close();
		});

		test(`web: indexedDBReadStream opens one transaction per batch of 100`, async (_t) => {
			for (const [n, expected] of [
				[0, 1],
				[100, 1],
				[101, 2],
				[200, 2],
				[201, 3],
			]) {
				const db = await seed(range(n));
				const counter = countTransactions(db);

				const output = await readAll(
					await webReadStream({ db, store: "test" }),
				);

				strictEqual(output.length, n);
				strictEqual(counter.count, expected, `${n} records`);
				db.close();
			}
		});

		test(`web: indexedDBReadStream resumes correctly when records are deleted between batches`, async (_t) => {
			const ids = (from, to) =>
				Array.from({ length: to - from + 1 }, (_, i) => from + i);
			for (const [deleted, expected] of [
				// first remaining record is past the last read one
				[ids(1, 100), ids(1, 150)],
				// first remaining record is the last read one
				[ids(1, 99), ids(1, 150)],
				// nothing left at or after the last read one
				[ids(100, 150), ids(1, 100)],
				// nothing left at all
				[ids(1, 150), ids(1, 100)],
			]) {
				const db = await seed(range(150));
				const stream = await webReadStream({ db, store: "test" });

				// Delete while batch 1 is being consumed, before batch 2 is read.
				const output = await readAll(stream, (count) =>
					count === 1 ? remove(db, deleted) : undefined,
				);

				deepStrictEqual(
					output.map(({ id }) => id),
					expected,
					`deleted ${deleted[0]}..${deleted.at(-1)}`,
				);
				db.close();
			}
		});

		test(`web: indexedDBReadStream resumes an index when records are deleted between batches`, async (_t) => {
			const db = await seed(range(150, () => "a"));
			const stream = await webReadStream({
				db,
				store: "test",
				index: "name",
				key: "a",
			});

			const output = await readAll(stream, (count) =>
				count === 1 ? remove(db, [99, 100, 101]) : undefined,
			);

			deepStrictEqual(
				output.map(({ id }) => id),
				[
					...range(100).map(({ id }) => id),
					...range(150)
						.slice(101)
						.map(({ id }) => id),
				],
			);
			db.close();
		});

		test(`web: indexedDBReadStream applies index and key independently`, async (_t) => {
			const records = [
				{ id: 0, name: "" },
				{ id: 1, name: "a" },
				{ id: 2, name: 0 },
				{ id: 3, name: "a" },
			];
			const db = await seed(records);
			for (const [query, expected] of [
				[{}, records],
				[{ index: "name" }, [records[2], records[0], records[1], records[3]]],
				[{ key: 2 }, [records[2]]],
				// falsy-but-valid keys still filter
				[{ key: 0 }, [records[0]]],
				[{ index: null, key: 2 }, [records[2]]],
				[{ index: "name", key: "a" }, [records[1], records[3]]],
				[{ index: "name", key: 0 }, [records[2]]],
				[{ index: "name", key: "" }, [records[0]]],
			]) {
				const stream = await webReadStream({ db, store: "test", ...query });
				deepStrictEqual(await readAll(stream), expected, JSON.stringify(query));
			}
			db.close();
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
			const db = await seed(range(1));

			const stream = await webReadStream({ db, store: "test" }, { signal });
			await readAll(stream);
			// Allow the underlying stream's terminal "close" handlers to run so any
			// listener teardown (wrapper + core) has settled before asserting.
			await new Promise((resolve) => setImmediate(resolve));

			strictEqual(listeners, 0);
			db.close();
		});

		test(`web: indexedDBReadStream stops reading once the signal aborts`, async (_t) => {
			const controller = new AbortController();
			const db = await seed(range(250));
			const counter = countTransactions(db);

			const stream = await webReadStream(
				{ db, store: "test" },
				{ signal: controller.signal },
			);

			// Aborting must stop the stream rather than reading all 3 batches.
			await rejects(
				readAll(stream, (count) => {
					if (count === 1) controller.abort();
				}),
				{ name: "AbortError" },
			);
			strictEqual(counter.count, 1);
			db.close();
		});

		test(`web: indexedDBReadStream surfaces a missing store as a stream error`, async (_t) => {
			const db = await seed([]);

			const stream = await webReadStream({ db, store: "missing" });

			await rejects(readAll(stream), { name: "NotFoundError" });
			db.close();
		});

		// A failed add() aborts its transaction, so tx.done rejects too; it must be
		// awaited with the add, or that AbortError surfaces as an unhandled rejection.
		test(`web: indexedDBWriteStream duplicate key rejects with ConstraintError only`, async (_t) => {
			const db = await seed([{ id: 1, name: "a" }]);
			const unhandled = [];
			const onUnhandled = (reason) => {
				unhandled.push(reason);
			};
			process.on("unhandledRejection", onUnhandled);
			try {
				const writeStream = await webWriteStream({ db, store: "test" });
				await rejects(
					pipeline([
						createReadableStream([{ id: 1, name: "duplicate" }]),
						writeStream,
					]),
					{ name: "ConstraintError" },
				);
				await sleep(10);
				deepStrictEqual(unhandled, []);
				deepStrictEqual(await db.getAll("test"), [{ id: 1, name: "a" }]);
			} finally {
				process.off("unhandledRejection", onUnhandled);
				db.close();
			}
		});

		test(`web: indexedDBWriteStream opens a fresh transaction per chunk (no auto-commit reuse)`, async (_t) => {
			// A shared transaction would auto-commit between async chunks and throw
			// TransactionInactiveError. Assert each write gets its own transaction.
			const records = range(3);
			const db = await seed([]);
			const counter = countTransactions(db);

			const writeStream = await webWriteStream({ db, store: "test" });
			await pipeline([
				createReadableStream(records),
				createTransformStream(async (chunk, enqueue) => {
					await sleep(1);
					enqueue(chunk);
				}),
				writeStream,
			]);

			strictEqual(counter.count, records.length);
			deepStrictEqual(await db.getAll("test"), records);
			db.close();
		});
	}
});
