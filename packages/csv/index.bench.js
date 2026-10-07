import { bench, suite } from "node:bench";
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
	csvObjectToArrayStream,
	csvParseStream,
	csvRemoveEmptyRowsStream,
	csvRemoveMalformedRowsStream,
} from "@datastream/csv";

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
const OPS = 1;
const options = { warmup: 1, samples: 5 };

const csvSimple = generateCsvString(ROWS, COLS);
const csvQuoted = generateCsvStringQuoted(ROWS, COLS);
const objects = generateObjectArray(ROWS, COLS);
const arrays = generateRowArrays(ROWS, COLS);

// -- Tests --

suite("csvParseStream", () => {
	bench(`${ROWS} rows, ${COLS} cols, simple`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(csvSimple), csvParseStream()];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});

	bench(`${ROWS} rows, ${COLS} cols, quoted`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(csvQuoted), csvParseStream()];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("csvFormatStream", () => {
	const header = Array.from({ length: COLS }, (_, i) => `col${i}`);

	bench(`${ROWS} rows, ${COLS} cols, from objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				csvObjectToArrayStream({ headers: header }),
				csvInjectHeaderStream({ header }),
				csvFormatStream(),
			];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	bench(`${ROWS} rows, ${COLS} cols, from arrays`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(arrays),
				csvInjectHeaderStream({ header }),
				csvFormatStream(),
			];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("csvDetectDelimitersStream", () => {
	bench(`${ROWS} rows, ${COLS} cols`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const detect = csvDetectDelimitersStream();
			const streams = [createReadableStream(csvSimple), detect];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("csvDetectHeaderStream", () => {
	bench(`${ROWS} rows, ${COLS} cols`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const hdr = csvDetectHeaderStream();
			const streams = [createReadableStream(csvSimple), hdr];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("csvRemoveMalformedRowsStream", () => {
	bench(`${ROWS} rows, all valid`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const filter = csvRemoveMalformedRowsStream();
			const streams = [createReadableStream(arrays), filter];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	const mixedArrays = arrays.map((row, i) =>
		i % 100 === 0 ? row.slice(0, -1) : row,
	);
	bench(`${ROWS} rows, 1% malformed`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const filter = csvRemoveMalformedRowsStream();
			const streams = [createReadableStream(mixedArrays), filter];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("csvRemoveEmptyRowsStream", () => {
	bench(`${ROWS} rows, all valid`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const filter = csvRemoveEmptyRowsStream();
			const streams = [createReadableStream(arrays), filter];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	const mixedArrays = arrays.map((row, i) =>
		i % 100 === 0 ? Array(COLS).fill("") : row,
	);
	bench(`${ROWS} rows, 1% empty`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const filter = csvRemoveEmptyRowsStream();
			const streams = [createReadableStream(mixedArrays), filter];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("csvCoerceValuesStream", () => {
	bench(`${ROWS} rows, auto-coerce`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const coerce = csvCoerceValuesStream();
			const data = objects.map((obj) => ({ ...obj, num: "42", bool: "true" }));
			const streams = [createReadableStream(data), coerce];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	bench(`${ROWS} rows, explicit types`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const coerce = csvCoerceValuesStream({
				columns: { col0: "string", col1: "number" },
			});
			const data = objects.map((obj) => ({ ...obj, col1: "42" }));
			const streams = [createReadableStream(data), coerce];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("full pipeline", () => {
	bench(`${ROWS} rows, ${COLS} cols`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
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
		}
		b.end(OPS);
	});
});
