import { bench, suite } from "node:bench";
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
const OPS = 1;
const options = { warmup: 1, samples: 30 };

const ndjsonString = generateNdjsonString(ROWS);
const jsonArrayString = generateJsonArrayString(ROWS);
const objects = generateObjects(ROWS);

// -- Tests --

suite("ndjsonParseStream", () => {
	bench(`${ROWS} rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(ndjsonString), ndjsonParseStream()];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("ndjsonFormatStream", () => {
	bench(`${ROWS} rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(objects), ndjsonFormatStream()];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("jsonParseStream", () => {
	bench(`${ROWS} rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(jsonArrayString),
				jsonParseStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("jsonFormatStream", () => {
	bench(`${ROWS} rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(objects), jsonFormatStream()];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("ndjson roundtrip", () => {
	bench(`${ROWS} rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				ndjsonFormatStream(),
				ndjsonParseStream(),
			];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});
