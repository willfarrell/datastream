import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	csvCoerceValuesStream,
	csvDetectDelimitersStream,
	csvDetectHeaderStream,
	csvFormatStream,
	csvInjectHeaderStream,
	csvObjectToArray,
	csvParseStream,
	csvRemoveEmptyRowsStream,
	csvRemoveMalformedRowsStream,
} from "@datastream/csv";
import { Bench } from "tinybench";

// -- Data generators --

const generateCsvString = (rows, cols, newline = "\r\n") => {
	const header = Array.from({ length: cols }, (_, i) => `col${i}`).join(",");
	const dataRows = Array.from({ length: rows }, (_, r) =>
		Array.from({ length: cols }, (_, c) => `val_${r}_${c}`).join(","),
	);
	return `${header}${newline}${dataRows.join(newline)}${newline}`;
};

const generateCsvStringQuoted = (rows, cols, newline = "\r\n") => {
	const header = Array.from({ length: cols }, (_, i) => `"col${i}"`).join(",");
	const dataRows = Array.from({ length: rows }, (_, r) =>
		Array.from({ length: cols }, (_, c) => `"val ""${r}"" ${c}"`).join(","),
	);
	return `${header}${newline}${dataRows.join(newline)}${newline}`;
};

const generateObjectArray = (rows, cols) =>
	Array.from({ length: rows }, (_, r) => {
		const obj = {};
		for (let c = 0; c < cols; c++) {
			obj[`col${c}`] = `val_${r}_${c}`;
		}
		return obj;
	});

const generateRowArrays = (rows, cols) =>
	Array.from({ length: rows }, (_, r) =>
		Array.from({ length: cols }, (_, c) => `val_${r}_${c}`),
	);

// -- Benchmark config --

const ROWS = 1_000_000;
const COLS = 10;
const time = Number(process.env.BENCH_TIME ?? 5_000);

const csvSimple = generateCsvString(ROWS, COLS);
const csvQuoted = generateCsvStringQuoted(ROWS, COLS);
const objects = generateObjectArray(ROWS, COLS);
const arrays = generateRowArrays(ROWS, COLS);

// -- Tests --

test("perf: csvParseStream", async () => {
	const bench = new Bench({ name: "csvParseStream", time });

	bench.add(`${ROWS} rows, ${COLS} cols, simple`, async () => {
		const streams = [createReadableStream(csvSimple), csvParseStream()];
		await streamToArray(pipejoin(streams));
	});

	bench.add(`${ROWS} rows, ${COLS} cols, quoted`, async () => {
		const streams = [createReadableStream(csvQuoted), csvParseStream()];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: csvFormatStream", async () => {
	const bench = new Bench({ name: "csvFormatStream", time });
	const header = Array.from({ length: COLS }, (_, i) => `col${i}`);

	bench.add(`${ROWS} rows, ${COLS} cols, from objects`, async () => {
		const streams = [
			createReadableStream(objects),
			csvObjectToArray({ headers: header }),
			csvInjectHeaderStream({ header }),
			csvFormatStream(),
		];
		await pipeline(streams);
	});

	bench.add(`${ROWS} rows, ${COLS} cols, from arrays`, async () => {
		const streams = [
			createReadableStream(arrays),
			csvInjectHeaderStream({ header }),
			csvFormatStream(),
		];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: csvDetectDelimitersStream", async () => {
	const bench = new Bench({ name: "csvDetectDelimitersStream", time });

	bench.add(`${ROWS} rows, ${COLS} cols`, async () => {
		const detect = csvDetectDelimitersStream();
		const streams = [createReadableStream(csvSimple), detect];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: csvDetectHeaderStream", async () => {
	const bench = new Bench({ name: "csvDetectHeaderStream", time });

	bench.add(`${ROWS} rows, ${COLS} cols`, async () => {
		const hdr = csvDetectHeaderStream();
		const streams = [createReadableStream(csvSimple), hdr];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: csvRemoveMalformedRowsStream", async () => {
	const bench = new Bench({ name: "csvRemoveMalformedRowsStream", time });

	bench.add(`${ROWS} rows, all valid`, async () => {
		const filter = csvRemoveMalformedRowsStream();
		const streams = [createReadableStream(arrays), filter];
		await pipeline(streams);
	});

	const mixedArrays = arrays.map((row, i) =>
		i % 100 === 0 ? row.slice(0, -1) : row,
	);
	bench.add(`${ROWS} rows, 1% malformed`, async () => {
		const filter = csvRemoveMalformedRowsStream();
		const streams = [createReadableStream(mixedArrays), filter];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: csvRemoveEmptyRowsStream", async () => {
	const bench = new Bench({ name: "csvRemoveEmptyRowsStream", time });

	bench.add(`${ROWS} rows, all valid`, async () => {
		const filter = csvRemoveEmptyRowsStream();
		const streams = [createReadableStream(arrays), filter];
		await pipeline(streams);
	});

	const mixedArrays = arrays.map((row, i) =>
		i % 100 === 0 ? Array(COLS).fill("") : row,
	);
	bench.add(`${ROWS} rows, 1% empty`, async () => {
		const filter = csvRemoveEmptyRowsStream();
		const streams = [createReadableStream(mixedArrays), filter];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: csvCoerceValuesStream", async () => {
	const bench = new Bench({ name: "csvCoerceValuesStream", time });

	bench.add(`${ROWS} rows, auto-coerce`, async () => {
		const coerce = csvCoerceValuesStream();
		const data = objects.map((obj) => ({ ...obj, num: "42", bool: "true" }));
		const streams = [createReadableStream(data), coerce];
		await pipeline(streams);
	});

	bench.add(`${ROWS} rows, explicit types`, async () => {
		const coerce = csvCoerceValuesStream({
			columns: { col0: "string", col1: "number" },
		});
		const data = objects.map((obj) => ({ ...obj, col1: "42" }));
		const streams = [createReadableStream(data), coerce];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: full pipeline (detect → headers → parse → filter → coerce)", async () => {
	const bench = new Bench({ name: "full pipeline", time });

	bench.add(`${ROWS} rows, ${COLS} cols`, async () => {
		const detect = csvDetectDelimitersStream();
		const hdr = csvDetectHeaderStream({
			delimiterChar: () => detect.result().value.delimiterChar,
			newlineChar: () => detect.result().value.newlineChar,
			quoteChar: () => detect.result().value.quoteChar,
		});
		const parse = csvParseStream({
			delimiterChar: () => detect.result().value.delimiterChar,
			newlineChar: () => detect.result().value.newlineChar,
			quoteChar: () => detect.result().value.quoteChar,
		});
		const removeMalformed = csvRemoveMalformedRowsStream({
			headers: () => hdr.result().value.header,
		});
		const removeEmpty = csvRemoveEmptyRowsStream();
		const streams = [
			createReadableStream(csvSimple),
			detect,
			hdr,
			parse,
			removeMalformed,
			removeEmpty,
		];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
