import { deepEqual, equal } from "node:assert";
import test from "node:test";
// import sinon from 'sinon'
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

import {
	objectBatchStream,
	objectCountStream,
	objectKeyValueStream,
	objectKeyValuesStream,
	objectPivotLongToWideStream,
	objectPivotWideToLongStream,
	objectReadableStream,
	objectSkipConsecutiveDuplicatesStream,
} from "@datastream/object";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** objectReadableStream *** //
test(`${variant}: objectReadableStream should read in initial chunks`, async (_t) => {
	const input = [{ a: "1" }, { b: "2" }, { c: "3" }];
	const streams = [objectReadableStream(input)];
	const stream = streams[0];
	const output = await streamToArray(stream);

	deepEqual(output, input);
});

// *** objectCountStream *** //
test(`${variant}: objectCountStream should count length of chunks`, async (_t) => {
	const input = ["1", "2", "3"];
	const streams = [createReadableStream(input), objectCountStream()];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	equal(key, "count");
	equal(result.count, 3);
	equal(value, 3);
});

test(`${variant}: objectCountStream should count length of chunks with custom key`, async (_t) => {
	const input = ["1", "2", "3"];
	const streams = [
		createReadableStream(input),
		objectCountStream({ resultKey: "object" }),
	];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	equal(key, "object");
	equal(result.object, 3);
	equal(value, 3);
});

// *** objectBatchStream *** //
test(`${variant}: objectBatchStream should batch chunks by key`, async (_t) => {
	const input = [
		{ a: "1", b: "2" },
		{ a: "1", b: "2" },
		{ a: "2", b: "3" },
		{ a: "3", b: "4" },
		{ a: "3", b: "5" },
	];
	const streams = [
		createReadableStream(input),
		objectBatchStream({ keys: ["a"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepEqual(output, [
		[
			{ a: "1", b: "2" },
			{ a: "1", b: "2" },
		],
		[{ a: "2", b: "3" }],
		[
			{ a: "3", b: "4" },
			{ a: "3", b: "5" },
		],
	]);
});

test(`${variant}: objectBatchStream should batch chunks by index`, async (_t) => {
	const input = [
		["1", "1"],
		["1", "2"],
		["2", "3"],
		["3", "4"],
		["3", "5"],
	];
	const streams = [
		createReadableStream(input),
		objectBatchStream({ keys: [0] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepEqual(output, [
		[
			["1", "1"],
			["1", "2"],
		],
		[["2", "3"]],
		[
			["3", "4"],
			["3", "5"],
		],
	]);
});

// *** objectPivotLongToWideStream *** //
test(`${variant}: objectPivotLongToWideStream should pivot chunks to wide`, async (_t) => {
	const input = [
		{ a: "1", b: "l", v: 1, u: "m" },
		{ a: "1", b: "w", v: 2, u: "m" },
		{ a: "2", b: "w", v: 3, u: "m" },
		{ a: "3", b: "l", v: 4, u: "m" },
		{ a: "3", b: "w", v: 5, u: "m" },
	];
	const streams = [
		createReadableStream(input),
		objectBatchStream({ keys: ["a"] }),
		objectPivotLongToWideStream({ keys: ["b", "u"], valueParam: "v" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepEqual(output, [
		{ a: "1", "l m": 1, "w m": 2 },
		{ a: "2", "w m": 3 },
		{ a: "3", "l m": 4, "w m": 5 },
	]);
});

test(`${variant}: objectPivotLongToWideStream should catch invalid chunk type`, async (_t) => {
	const input = [
		{ a: "1", b: "l", v: 1, u: "m" },
		{ a: "1", b: "w", v: 2, u: "m" },
		{ a: "2", b: "w", v: 3, u: "m" },
		{ a: "3", b: "l", v: 4, u: "m" },
		{ a: "3", b: "w", v: 5, u: "m" },
	];

	const streams = [
		createReadableStream(input),
		objectBatchStream({ keys: ["b", "u"] }),
		objectPivotLongToWideStream({ keys: ["b", "u"], valueParam: "v" }),
	];
	try {
		const stream = pipejoin(streams);
		await streamToArray(stream);
	} catch (e) {
		deepEqual(
			e.message,
			"Expected chunk to be array, use with objectBatchStream",
		);
	}
});

// *** objectPivotWideToLongStream *** //
test(`${variant}: objectPivotWideToLongStream should pivot chunks to wide`, async (_t) => {
	const input = [
		{ a: "1", "l m": 1, "w m": 2 },
		{ a: "2", "w m": 3 },
		{ a: "3", "l m": 4, "w m": 5 },
	];
	const streams = [
		createReadableStream(input),
		objectPivotWideToLongStream({
			keys: ["l m", "w m", "a m"],
			keyParam: "b u",
			valueParam: "v",
		}),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepEqual(output, [
		{ a: "1", "b u": "l m", v: 1 },
		{ a: "1", "b u": "w m", v: 2 },
		{ a: "2", "b u": "w m", v: 3 },
		{ a: "3", "b u": "l m", v: 4 },
		{ a: "3", "b u": "w m", v: 5 },
	]);
});

// *** objectKeyValueStream *** //
test(`${variant}: objectKeyValueStream should transform to {chunk[key]:chunk[value]}`, async (_t) => {
	const input = [{ a: "1", b: "2", c: "3" }];
	const streams = [
		createReadableStream(input),
		objectKeyValueStream({ key: "a", value: "b" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepEqual(output, [{ 1: "2" }]);
});

// *** objectKeyValuesStream *** //
test(`${variant}: objectKeyValuesStream should transform to {chunk[key]:chunk}`, async (_t) => {
	const input = [{ a: "1", b: "2", c: "3" }];
	const streams = [
		createReadableStream(input),
		objectKeyValuesStream({ key: "a" }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepEqual(output, [{ 1: { a: "1", b: "2", c: "3" } }]);
});

test(`${variant}: objectKeyValuesStream should transform to {chunk[key]:chunk[values]}`, async (_t) => {
	const input = [{ a: "1", b: "2", c: "3" }];
	const streams = [
		createReadableStream(input),
		objectKeyValuesStream({ key: "a", values: ["b"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepEqual(output, [{ 1: { b: "2" } }]);
});

// *** objectSkipConsecutiveDuplicates *** //
test(`${variant}: objectSkipConsecutiveDuplicatesStream should skip consecutive duplicates`, async (_t) => {
	const input = [{ a: 1 }, { b: 2 }, { b: 2 }, { c: 3 }];
	const streams = [
		createReadableStream(input),
		objectSkipConsecutiveDuplicatesStream(),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepEqual(output, [{ a: 1 }, { b: 2 }, { c: 3 }]);
});
