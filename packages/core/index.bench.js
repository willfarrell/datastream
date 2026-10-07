import { bench, suite } from "node:bench";
import {
	createPassThroughStream,
	createReadableStream,
	createTransformStream,
	createWritableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

// -- Data generators --

const ITEMS = 10_000;
const COLS = 10;
const OPS = 10;
const options = { warmup: 2, samples: 30 };

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

suite("readable → streamToArray (string)", () => {
	bench(`${bigString.length} chars, 16KB chunks`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(bigString)];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("readable → streamToArray (objects)", () => {
	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(objects)];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("readable → transform(identity) → streamToArray", () => {
	bench(`${ITEMS} objects, identity transform`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				createTransformStream((chunk, enqueue) => {
					enqueue(chunk);
				}),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("readable → transform(1→N) → streamToArray", () => {
	// Simulate csvParseStream: ~68 string chunks in, ~147 objects out per chunk = 10K total
	const itemsPerChunk = Math.ceil(
		ITEMS / Math.ceil(bigString.length / (16 * 1024)),
	);
	const row = Array.from({ length: COLS }, (_, c) => `val_0_${c}`);

	bench(
		`~68 chunks → ${ITEMS} objects (~${itemsPerChunk}/chunk)`,
		options,
		async (b) => {
			b.start();
			for (let op = 0; op < OPS; op++) {
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
			}
			b.end(OPS);
		},
	);
});

suite("readable → passThrough → streamToArray", () => {
	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				createPassThroughStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("readable → writable (pipeline)", () => {
	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(objects), createWritableStream()];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});
