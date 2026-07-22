import test from "node:test";
import {
	arrowBatchFromObjectStream,
	arrowDetectSchemaStream,
	arrowToArrayStream,
	arrowToObjectStream,
} from "@datastream/arrow";
import {
	createReadableStream,
	pipejoin,
	streamToArray,
} from "@datastream/core";
import { Field, Int32, Schema, Utf8 } from "apache-arrow";
import { Bench } from "tinybench";

// -- Data generators --

const ITEMS = 10_000;
const time = Number(process.env.BENCH_TIME ?? 5_000);

const usersSchema = new Schema([
	new Field("id", new Int32(), true),
	new Field("name", new Utf8(), true),
]);

const objects = Array.from({ length: ITEMS }, (_, i) => ({
	id: i,
	name: `user_${i}`,
}));

// -- Tests --

test("perf: arrowDetectSchemaStream", async () => {
	const bench = new Bench({ name: "arrowDetectSchemaStream", time });

	bench.add(`${ITEMS} objects`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 100 });
		const streams = [createReadableStream(objects), detect];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: arrowBatchFromObjectStream", async () => {
	const bench = new Bench({ name: "arrowBatchFromObjectStream", time });

	bench.add(`${ITEMS} objects, batchSize 1000`, async () => {
		const streams = [
			createReadableStream(objects),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 1_000 }),
		];
		await streamToArray(pipejoin(streams));
	});

	bench.add(`${ITEMS} objects, batchSize 100`, async () => {
		const streams = [
			createReadableStream(objects),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 100 }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: arrowToObjectStream (batch -> objects)", async () => {
	const bench = new Bench({ name: "arrowToObjectStream", time });

	bench.add(`${ITEMS} objects, batchSize 1000`, async () => {
		const streams = [
			createReadableStream(objects),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 1_000 }),
			arrowToObjectStream(),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: arrowToArrayStream (batch -> arrays)", async () => {
	const bench = new Bench({ name: "arrowToArrayStream", time });

	bench.add(`${ITEMS} objects, batchSize 1000`, async () => {
		const streams = [
			createReadableStream(objects),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 1_000 }),
			arrowToArrayStream(),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: object roundtrip (detect -> batch -> objects)", async () => {
	const bench = new Bench({ name: "object roundtrip", time });

	bench.add(`${ITEMS} objects`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 100 });
		const streams = [
			createReadableStream(objects),
			detect,
			arrowBatchFromObjectStream({
				schema: () => detect.result().value.schema,
				batchSize: 1_000,
			}),
			arrowToObjectStream(),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
