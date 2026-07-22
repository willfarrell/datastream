import test from "node:test";
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
import { Bench } from "tinybench";

// -- Config --

const time = Number(process.env.BENCH_TIME ?? 5_000);
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

test("perf: duckdbAppenderStream insert throughput", async () => {
	const bench = new Bench({ name: "duckdbAppenderStream", time });
	const db = await duckdbConnect();
	let tableIdx = 0;

	bench.add(`${N} object rows`, async () => {
		const table = `bench_appender_${tableIdx++}`;
		await db.run(`CREATE TABLE ${table} (id INTEGER, name VARCHAR)`);
		await pipeline([
			createReadableStream(rows),
			await duckdbAppenderStream({ db, table }),
		]);
	});

	await bench.run();
	db.closeSync();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: duckdbArrowInsertStream insert throughput", async () => {
	const bench = new Bench({ name: "duckdbArrowInsertStream", time });
	const db = await duckdbConnect();
	let tableIdx = 0;

	bench.add(`${N} rows via Arrow batches (batchSize=1000)`, async () => {
		const table = `bench_arrow_${tableIdx++}`;
		await db.run(`CREATE TABLE ${table} (id INTEGER, name VARCHAR)`);
		await pipeline([
			createReadableStream(rows),
			arrowBatchFromObjectStream({ schema: arrowSchema, batchSize: 1_000 }),
			await duckdbArrowInsertStream({ db, table }),
		]);
	});

	await bench.run();
	db.closeSync();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: duckdbAppenderStream with schema auto-create table", async () => {
	const bench = new Bench({
		name: "duckdbAppenderStream (schema + auto-create)",
		time,
	});
	const db = await duckdbConnect();
	let tableIdx = 0;

	bench.add(`${N} rows, schema provided`, async () => {
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
	});

	await bench.run();
	db.closeSync();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
