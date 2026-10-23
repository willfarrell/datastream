// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
import { DatabaseSync } from "node:sqlite";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { sqliteInsertStream, sqliteQueryStream } from "@datastream/sqlite";

const OPS = 5;
const options = { warmup: 1, samples: 30 };
const ROW_COUNT = 10_000;

const objectRows = Array.from({ length: ROW_COUNT }, (_, i) => ({
	id: i,
	name: `name-${i}`,
}));
const arrayRows = objectRows.map(({ id, name }) => [id, name]);

const makeDb = () => {
	const db = new DatabaseSync(":memory:");
	db.exec("CREATE TABLE t (id INTEGER, name TEXT)");
	return db;
};

suite("sqliteInsertStream", () => {
	bench(`insert ${ROW_COUNT} object rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const db = makeDb();
			await pipeline([
				createReadableStream(objectRows),
				sqliteInsertStream({ db, table: "t" }),
			]);
			db.close();
		}
		b.end(OPS);
	});

	bench(`insert ${ROW_COUNT} array rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const db = makeDb();
			await pipeline([
				createReadableStream(arrayRows),
				sqliteInsertStream({ db, table: "t", columns: ["id", "name"] }),
			]);
			db.close();
		}
		b.end(OPS);
	});
});

suite("sqliteQueryStream", () => {
	const db = makeDb();
	const insert = db.prepare("INSERT INTO t (id, name) VALUES (?, ?)");
	db.exec("BEGIN");
	for (const [id, name] of arrayRows) insert.run(id, name);
	db.exec("COMMIT");

	bench(`query ${ROW_COUNT} rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await streamToArray(sqliteQueryStream({ db, sql: "SELECT * FROM t" }));
		}
		b.end(OPS);
	});
});
