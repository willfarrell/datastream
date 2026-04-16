import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	jsonFormatStream,
	jsonParseStream,
	ndjsonFormatStream,
	ndjsonParseStream,
} from "@datastream/json";
import { Bench } from "tinybench";

// -- Data generators --

const generateNdjsonString = (rows) => {
	const lines = Array.from({ length: rows }, (_, i) =>
		JSON.stringify({ id: i, name: `item_${i}`, value: Math.random() }),
	);
	return `${lines.join("\n")}\n`;
};

const generateJsonArrayString = (rows) => {
	const objects = Array.from({ length: rows }, (_, i) => ({
		id: i,
		name: `item_${i}`,
		value: Math.random(),
	}));
	return JSON.stringify(objects);
};

const generateObjects = (rows) =>
	Array.from({ length: rows }, (_, i) => ({
		id: i,
		name: `item_${i}`,
		value: Math.random(),
	}));

// -- Benchmark config --

const ROWS = 100_000;
const time = Number(process.env.BENCH_TIME ?? 5_000);

const ndjsonString = generateNdjsonString(ROWS);
const jsonArrayString = generateJsonArrayString(ROWS);
const objects = generateObjects(ROWS);

// -- Tests --

test("perf: ndjsonParseStream", async () => {
	const bench = new Bench({ name: "ndjsonParseStream", time });

	bench.add(`${ROWS} rows`, async () => {
		const streams = [createReadableStream(ndjsonString), ndjsonParseStream()];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: ndjsonFormatStream", async () => {
	const bench = new Bench({ name: "ndjsonFormatStream", time });

	bench.add(`${ROWS} rows`, async () => {
		const streams = [createReadableStream(objects), ndjsonFormatStream()];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: jsonParseStream", async () => {
	const bench = new Bench({ name: "jsonParseStream", time });

	bench.add(`${ROWS} rows`, async () => {
		const streams = [createReadableStream(jsonArrayString), jsonParseStream()];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: jsonFormatStream", async () => {
	const bench = new Bench({ name: "jsonFormatStream", time });

	bench.add(`${ROWS} rows`, async () => {
		const streams = [createReadableStream(objects), jsonFormatStream()];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: ndjson roundtrip (format → parse)", async () => {
	const bench = new Bench({ name: "ndjson roundtrip", time });

	bench.add(`${ROWS} rows`, async () => {
		const streams = [
			createReadableStream(objects),
			ndjsonFormatStream(),
			ndjsonParseStream(),
		];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
