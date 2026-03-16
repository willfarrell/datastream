import test from "node:test";
import {
	createPassThroughStream,
	createReadableStream,
	createTransformStream,
	createWritableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { Bench } from "tinybench";

// -- Data generators --

const ITEMS = 10_000;
const COLS = 10;
const time = Number(process.env.BENCH_TIME ?? 5_000);

// ~1MB string (matches CSV benchmark scale)
const generateString = (rows, cols, newline = "\r\n") => {
	const header = Array.from({ length: cols }, (_, i) => `col${i}`).join(",");
	const dataRows = Array.from({ length: rows }, (_, r) =>
		Array.from({ length: cols }, (_, c) => `val_${r}_${c}`).join(","),
	);
	return `${header}${newline}${dataRows.join(newline)}${newline}`;
};

const generateObjects = (rows, cols) =>
	Array.from({ length: rows }, (_, r) => {
		const obj = {};
		for (let c = 0; c < cols; c++) {
			obj[`col${c}`] = `val_${r}_${c}`;
		}
		return obj;
	});

const bigString = generateString(ITEMS, COLS);
const objects = generateObjects(ITEMS, COLS);

// -- Tests --

test("perf: createReadableStream → streamToArray (string)", async () => {
	const bench = new Bench({ name: "readable → streamToArray (string)", time });

	bench.add(`${bigString.length} chars, 16KB chunks`, async () => {
		const streams = [createReadableStream(bigString)];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: createReadableStream → streamToArray (objects)", async () => {
	const bench = new Bench({
		name: "readable → streamToArray (objects)",
		time,
	});

	bench.add(`${ITEMS} objects`, async () => {
		const streams = [createReadableStream(objects)];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: createReadableStream → createTransformStream → streamToArray (identity)", async () => {
	const bench = new Bench({
		name: "readable → transform(identity) → streamToArray",
		time,
	});

	bench.add(`${ITEMS} objects, identity transform`, async () => {
		const streams = [
			createReadableStream(objects),
			createTransformStream((chunk, enqueue) => {
				enqueue(chunk);
			}),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: createReadableStream → createTransformStream → streamToArray (1→N fan-out)", async () => {
	const bench = new Bench({
		name: "readable → transform(1→N) → streamToArray",
		time,
	});

	// Simulate csvParseStream: ~68 string chunks in, ~147 objects out per chunk = 10K total
	const itemsPerChunk = Math.ceil(
		ITEMS / Math.ceil(bigString.length / (16 * 1024)),
	);
	const row = Array.from({ length: COLS }, (_, c) => `val_0_${c}`);

	bench.add(
		`~68 chunks → ${ITEMS} objects (~${itemsPerChunk}/chunk)`,
		async () => {
			const streams = [
				createReadableStream(bigString),
				createTransformStream((chunk, enqueue) => {
					// Simulate parser: emit ~itemsPerChunk rows per chunk
					const count = Math.ceil((chunk.length / bigString.length) * ITEMS);
					for (let i = 0; i < count; i++) {
						enqueue(row);
					}
				}),
			];
			await streamToArray(pipejoin(streams));
		},
	);

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: createReadableStream → createPassThroughStream → streamToArray", async () => {
	const bench = new Bench({
		name: "readable → passThrough → streamToArray",
		time,
	});

	bench.add(`${ITEMS} objects`, async () => {
		const streams = [createReadableStream(objects), createPassThroughStream()];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: createReadableStream → createWritableStream via pipeline", async () => {
	const bench = new Bench({
		name: "readable → writable (pipeline)",
		time,
	});

	bench.add(`${ITEMS} objects`, async () => {
		const streams = [createReadableStream(objects), createWritableStream()];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
