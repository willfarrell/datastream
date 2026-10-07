import { after, before, bench, suite } from "node:bench";
import {
	arrowBatchFromObjectStream,
	arrowDetectSchemaStream,
} from "@datastream/arrow";
import { createReadableStream, pipeline } from "@datastream/core";
import {
	duckdbAppenderStream,
	duckdbArrowInsertStream,
	duckdbConnect,
} from "@datastream/duckdb";
import { Field, Int32, Schema, Utf8 } from "apache-arrow";

// -- Config --

const OPS = 5;
const options = { warmup: 1, samples: 30 };
const N = 10_000;

// -- Data generators --

const rows = Array.from({ length: N }, (_, i) => ({
	id: i,
	name: `user_${i}`,
}));

const arrowSchema = new Schema([
	new Field("id", new Int32(), true),
	new Field("name", new Utf8(), true),
]);

// -- Tests --

suite("duckdbAppenderStream", () => {
	let db;
	before(async () => {
		db = await duckdbConnect();
	});
	after(() => db.closeSync());
	let tableIdx = 0;

	bench(`${N} object rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const table = `bench_appender_${tableIdx++}`;
			await db.run(`CREATE TABLE ${table} (id INTEGER, name VARCHAR)`);
			await pipeline([
				createReadableStream(rows),
				await duckdbAppenderStream({ db, table }),
			]);
		}
		b.end(OPS);
	});
});

suite("duckdbArrowInsertStream", () => {
	let db;
	before(async () => {
		db = await duckdbConnect();
	});
	after(() => db.closeSync());
	let tableIdx = 0;

	bench(`${N} rows via Arrow batches (batchSize=1000)`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const table = `bench_arrow_${tableIdx++}`;
			await db.run(`CREATE TABLE ${table} (id INTEGER, name VARCHAR)`);
			await pipeline([
				createReadableStream(rows),
				arrowBatchFromObjectStream({ schema: arrowSchema, batchSize: 1_000 }),
				await duckdbArrowInsertStream({ db, table }),
			]);
		}
		b.end(OPS);
	});
});

suite("duckdbAppenderStream (schema + auto-create)", () => {
	let db;
	before(async () => {
		db = await duckdbConnect();
	});
	after(() => db.closeSync());
	let tableIdx = 0;

	bench(`${N} rows, schema provided`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const table = `bench_appender_schema_${tableIdx++}`;
			const detect = arrowDetectSchemaStream({ sampleSize: 10 });
			await pipeline([
				createReadableStream(rows),
				detect,
				await duckdbAppenderStream({
					db,
					table,
					schema: () => detect.result().value.schema,
				}),
			]);
		}
		b.end(OPS);
	});
});
