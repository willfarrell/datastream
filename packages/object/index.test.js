import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

import {
	objectBatchStream,
	objectCountStream,
	objectFromEntriesStream,
	objectKeyJoinStream,
	objectKeyMapStream,
	objectKeyValueStream,
	objectKeyValuesStream,
	objectOmitStream,
	objectPickStream,
	objectPivotLongToWideStream,
	objectPivotWideToLongStream,
	objectReadableStream,
	objectSkipConsecutiveDuplicatesStream,
	objectValueMapStream,
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

	deepStrictEqual(output, input);
});

// *** objectCountStream *** //
test(`${variant}: objectCountStream should count length of chunks`, async (_t) => {
	const input = ["1", "2", "3"];
	const streams = [createReadableStream(input), objectCountStream()];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "count");
	strictEqual(result.count, 3);
	strictEqual(value, 3);
});

test(`${variant}: objectCountStream should count length of chunks with custom key`, async (_t) => {
	const input = ["1", "2", "3"];
	const streams = [
		createReadableStream(input),
		objectCountStream({ resultKey: "object" }),
	];

	const result = await pipeline(streams);
	const { key, value } = streams[1].result();

	strictEqual(key, "object");
	strictEqual(result.object, 3);
	strictEqual(value, 3);
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

	deepStrictEqual(output, [
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

	deepStrictEqual(output, [
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

	deepStrictEqual(output, [
		{ a: "1", "l m": 1, "w m": 2 },
		{ a: "2", "w m": 3 },
		{ a: "3", "l m": 4, "w m": 5 },
	]);
});

test(`${variant}: objectPivotLongToWideStream should catch invalid chunk type`, async (_t) => {
	const input = [{ a: "1", b: "l", v: 1, u: "m" }];

	const streams = [
		createReadableStream(input),
		objectPivotLongToWideStream({ keys: ["b", "u"], valueParam: "v" }),
	];
	try {
		await pipeline(streams);
		throw new Error("Expected error was not thrown");
	} catch (e) {
		deepStrictEqual(
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

	deepStrictEqual(output, [
		{ a: "1", "b u": "l m", v: 1 },
		{ a: "1", "b u": "w m", v: 2 },
		{ a: "2", "b u": "w m", v: 3 },
		{ a: "3", "b u": "l m", v: 4 },
		{ a: "3", "b u": "w m", v: 5 },
	]);
});

test(`${variant}: objectPivotWideToLongStream should use default keyParam and valueParam`, async (_t) => {
	const input = [{ id: 1, a: 10, b: 20 }];
	const streams = [
		createReadableStream(input),
		objectPivotWideToLongStream({ keys: ["a", "b"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		{ id: 1, keyParam: "a", valueParam: 10 },
		{ id: 1, keyParam: "b", valueParam: 20 },
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

	deepStrictEqual(output, [{ 1: "2" }]);
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

	deepStrictEqual(output, [{ 1: { a: "1", b: "2", c: "3" } }]);
});

test(`${variant}: objectKeyValuesStream should transform to {chunk[key]:chunk[values]}`, async (_t) => {
	const input = [{ a: "1", b: "2", c: "3" }];
	const streams = [
		createReadableStream(input),
		objectKeyValuesStream({ key: "a", values: ["b"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ 1: { b: "2" } }]);
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

	deepStrictEqual(output, [{ a: 1 }, { b: 2 }, { c: 3 }]);
});

// *** objectPivotLongToWideStream with custom delimiter *** //
test(`${variant}: objectPivotLongToWideStream should use custom delimiter`, async (_t) => {
	const input = [
		{ a: "1", b: "l", u: "m", v: 1 },
		{ a: "1", b: "w", u: "m", v: 2 },
	];
	const streams = [
		createReadableStream(input),
		objectBatchStream({ keys: ["a"] }),
		objectPivotLongToWideStream({
			keys: ["b", "u"],
			valueParam: "v",
			delimiter: "-",
		}),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: "1", "l-m": 1, "w-m": 2 }]);
});

// *** objectKeyJoinStream *** //
test(`${variant}: objectKeyJoinStream should join keys into new key`, async (_t) => {
	const input = [{ firstName: "John", lastName: "Doe", age: 30 }];
	const streams = [
		createReadableStream(input),
		objectKeyJoinStream({
			keys: { fullName: ["firstName", "lastName"] },
			separator: " ",
		}),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ age: 30, fullName: "John Doe" }]);
});

// *** objectKeyMapStream *** //
test(`${variant}: objectKeyMapStream should map keys to new names`, async (_t) => {
	const input = [{ a: 1, b: 2, c: 3 }];
	const streams = [
		createReadableStream(input),
		objectKeyMapStream({ keys: { a: "x", b: "y" } }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ x: 1, y: 2, c: 3 }]);
});

// *** objectValueMapStream *** //
test(`${variant}: objectValueMapStream should map values using lookup`, async (_t) => {
	const input = [{ status: "active" }, { status: "inactive" }];
	const streams = [
		createReadableStream(input),
		objectValueMapStream({ key: "status", values: { active: 1, inactive: 0 } }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ status: 1 }, { status: 0 }]);
});

// *** objectPickStream *** //
test(`${variant}: objectPickStream should pick specified keys`, async (_t) => {
	const input = [{ a: 1, b: 2, c: 3 }];
	const streams = [
		createReadableStream(input),
		objectPickStream({ keys: ["a", "c"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1, c: 3 }]);
});

// *** objectFromEntriesStream *** //
test(`${variant}: objectFromEntriesStream should transform array to object with keys`, async (_t) => {
	const input = [[1, 2, 3]];
	const streams = [
		createReadableStream(input),
		objectFromEntriesStream({ keys: ["a", "b", "c"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1, b: 2, c: 3 }]);
});

test(`${variant}: objectFromEntriesStream should support lazy keys function`, async (_t) => {
	const input = [[1, 2, 3]];
	const streams = [
		createReadableStream(input),
		objectFromEntriesStream({ keys: () => ["a", "b", "c"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1, b: 2, c: 3 }]);
});

test(`${variant}: objectFromEntriesStream should transform multiple arrays`, async (_t) => {
	const input = [
		[1, 2, 3],
		[4, 5, 6],
	];
	const streams = [
		createReadableStream(input),
		objectFromEntriesStream({ keys: ["a", "b", "c"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		{ a: 1, b: 2, c: 3 },
		{ a: 4, b: 5, c: 6 },
	]);
});

// *** objectOmitStream *** //
test(`${variant}: objectOmitStream should omit specified keys`, async (_t) => {
	const input = [{ a: 1, b: 2, c: 3 }];
	const streams = [
		createReadableStream(input),
		objectOmitStream({ keys: ["b"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1, c: 3 }]);
});
