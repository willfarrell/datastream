import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
// import sinon from 'sinon'
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

import {
	stringLengthStream,
	stringReadableStream,
	stringSkipConsecutiveDuplicates,
	stringSplitStream,
} from "@datastream/string";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** stringReadableStream *** //
test(`${variant}: stringReadableStream should read in initial chunks`, async (_t) => {
	const input = "abc";
	const streams = [stringReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [input]);
});

// *** stringLengthStream *** //
test(`${variant}: stringLengthStream should count length of chunks`, async (_t) => {
	const input = ["1", "2", "3"];
	const streams = [createReadableStream(input), stringLengthStream()];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "length");
	strictEqual(result.length, 3);
	strictEqual(value, 3);
});

test(`${variant}: stringSizeStream should count length of chunks with custom key`, async (_t) => {
	const input = ["1", "2", "3"];
	const streams = [
		createReadableStream(input),
		stringLengthStream({ resultKey: "string" }),
	];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "string");
	strictEqual(result.string, 3);
	strictEqual(value, 3);
});

// *** stringSkipConsecutiveDuplicates *** //
test(`${variant}: stringSkipConsecutiveDuplicates should skip consecutive duplicates`, async (_t) => {
	const input = ["1", "2", "2", "3"];
	const streams = [
		createReadableStream(input),
		stringSkipConsecutiveDuplicates(),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["1", "2", "3"]);
});

// *** stringSplitStream *** //
test(`${variant}: stringSplitStream should split into empty strings`, async (_t) => {
	const input = [",,", ",,"];
	const streams = [
		createReadableStream(input),
		stringSplitStream({ separator: "," }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["", "", "", "", ""]);
});

test(`${variant}: stringSplitStream should split into empty strings`, async (_t) => {
	const input = ["a,b", "c,d"];
	const streams = [
		createReadableStream(input),
		stringSplitStream({ separator: "," }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["a", "bc", "d"]);
});
