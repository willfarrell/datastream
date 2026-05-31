import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

import objectDefault, {
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
	objectToEntriesStream,
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

// *** objectToEntriesStream *** //
test(`${variant}: objectToEntriesStream should transform object to array with keys`, async (_t) => {
	const input = [{ a: 1, b: 2, c: 3 }];
	const streams = [
		createReadableStream(input),
		objectToEntriesStream({ keys: ["a", "b", "c"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [[1, 2, 3]]);
});

test(`${variant}: objectToEntriesStream should support lazy keys function`, async (_t) => {
	const input = [{ a: 1, b: 2, c: 3 }];
	const streams = [
		createReadableStream(input),
		objectToEntriesStream({ keys: () => ["a", "b", "c"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [[1, 2, 3]]);
});

test(`${variant}: objectToEntriesStream should transform multiple objects`, async (_t) => {
	const input = [
		{ a: 1, b: 2, c: 3 },
		{ a: 4, b: 5, c: 6 },
	];
	const streams = [
		createReadableStream(input),
		objectToEntriesStream({ keys: ["a", "b", "c"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		[1, 2, 3],
		[4, 5, 6],
	]);
});

test(`${variant}: objectToEntriesStream should return undefined for missing keys`, async (_t) => {
	const input = [{ a: 1, c: 3 }];
	const streams = [
		createReadableStream(input),
		objectToEntriesStream({ keys: ["a", "b", "c"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [[1, undefined, 3]]);
});

// *** objectSkipConsecutiveDuplicatesStream shallow equality regression *** //
test(`${variant}: objectSkipConsecutiveDuplicatesStream should use shallow equality by default`, async (_t) => {
	const input = [{ a: 1 }, { a: 1 }, { a: 2 }, { a: 2 }, { a: 1 }];
	const streams = [
		createReadableStream(input),
		objectSkipConsecutiveDuplicatesStream(),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [{ a: 1 }, { a: 2 }, { a: 1 }]);
});

test(`${variant}: objectSkipConsecutiveDuplicatesStream isNestedObject should use deep comparison`, async (_t) => {
	const input = [
		{ a: 1, b: { c: 2 } },
		{ a: 1, b: { c: 2 } },
		{ a: 1, b: { c: 3 } },
	];
	const streams = [
		createReadableStream(input),
		objectSkipConsecutiveDuplicatesStream({ isNestedObject: true }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [
		{ a: 1, b: { c: 2 } },
		{ a: 1, b: { c: 3 } },
	]);
});

// *** objectPivotWideToLongStream shallow copy regression *** //
test(`${variant}: objectPivotWideToLongStream should not mutate input chunks`, async (_t) => {
	const input = [{ id: 1, x: 10, y: 20 }];
	const original = { ...input[0] };
	const streams = [
		createReadableStream(input),
		objectPivotWideToLongStream({
			keys: ["x", "y"],
			keyParam: "axis",
			valueParam: "val",
		}),
	];
	const stream = pipejoin(streams);
	await streamToArray(stream);
	deepStrictEqual(input[0], original);
});

// *** objectKeyJoinStream shallow copy regression *** //
test(`${variant}: objectKeyJoinStream should not mutate input chunks`, async (_t) => {
	const input = [{ first: "a", last: "b", other: "c" }];
	const original = { ...input[0] };
	const streams = [
		createReadableStream(input),
		objectKeyJoinStream({
			keys: { full: ["first", "last"] },
			separator: " ",
		}),
	];
	const stream = pipejoin(streams);
	await streamToArray(stream);
	deepStrictEqual(input[0], original);
});

// *** objectBatchStream maxBatchSize *** //
test(`${variant}: objectBatchStream should enforce maxBatchSize`, async (_t) => {
	const { ok } = await import("node:assert");
	const input = Array.from({ length: 10 }, (_, i) => ({ a: "same", b: i }));
	const streams = [
		createReadableStream(input),
		objectBatchStream({ keys: ["a"], maxBatchSize: 3 }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// All items share key "same", but batches should be split at size 3
	for (const batch of output) {
		ok(batch.length <= 3);
	}
	strictEqual(
		output.reduce((sum, b) => sum + b.length, 0),
		10,
	);
});

// *** objectBatchStream collision-proof grouping key *** //
test(`${variant}: objectBatchStream should keep distinct key tuples in separate batches when values contain spaces`, async (_t) => {
	// ["a b","c"] and ["a","b c"] both join to "a b c" with space-delimiter —
	// they must produce two separate batches, not one merged batch.
	const input = [
		{ x: "a b", y: "c" },
		{ x: "a", y: "b c" },
	];
	const streams = [
		createReadableStream(input),
		objectBatchStream({ keys: ["x", "y"] }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [[{ x: "a b", y: "c" }], [{ x: "a", y: "b c" }]]);
});

// *** objectValueMapStream preserves unmapped keys *** //
test(`${variant}: objectValueMapStream should preserve the original value for keys not present in the map`, async (_t) => {
	const input = [{ status: "active" }, { status: "unknown" }];
	const streams = [
		createReadableStream(input),
		objectValueMapStream({ key: "status", values: { active: 1, inactive: 0 } }),
	];

	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	// "unknown" is not in the map — original value must be preserved
	deepStrictEqual(output, [{ status: 1 }, { status: "unknown" }]);
});

// *** objectPivotLongToWideStream should not mutate input *** //
test(`${variant}: objectPivotLongToWideStream should not mutate input chunks`, async (_t) => {
	const input = [
		[
			{ region: "US", metric: "sales", value: 100 },
			{ region: "US", metric: "cost", value: 50 },
		],
	];
	const originalFirst = { ...input[0][0] };
	const streams = [
		createReadableStream(input),
		objectPivotLongToWideStream({
			keys: ["metric"],
			valueParam: "value",
		}),
	];
	const stream = pipejoin(streams);
	await streamToArray(stream);

	// Original input[0][0] should be unchanged
	deepStrictEqual(input[0][0], originalFirst);
});

// *** objectReadableStream default empty input *** //
test(`${variant}: objectReadableStream should produce empty output when called with no args`, async (_t) => {
	const streams = [objectReadableStream()];
	const output = await streamToArray(streams[0]);
	deepStrictEqual(output, []);
});

// *** objectBatchStream should produce no output on empty input *** //
test(`${variant}: objectBatchStream should produce no output when input is empty`, async (_t) => {
	const streams = [
		createReadableStream([]),
		objectBatchStream({ keys: ["a"] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, []);
});

// *** objectToEntriesStream should produce empty arrays when keys is empty *** //
test(`${variant}: objectToEntriesStream should produce empty array when keys is empty`, async (_t) => {
	const input = [{ a: 1, b: 2 }];
	const streams = [
		createReadableStream(input),
		objectToEntriesStream({ keys: [] }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, [[]]);
});

// *** default export contains all stream factories *** //
test(`${variant}: default export should contain all stream factories`, async (_t) => {
	strictEqual(typeof objectDefault.readableStream, "function");
	strictEqual(typeof objectDefault.countStream, "function");
	strictEqual(typeof objectDefault.pickStream, "function");
	strictEqual(typeof objectDefault.omitStream, "function");
	strictEqual(typeof objectDefault.batchStream, "function");
	strictEqual(typeof objectDefault.pivotLongToWideStream, "function");
	strictEqual(typeof objectDefault.pivotWideToLongStream, "function");
	strictEqual(typeof objectDefault.keyValueStream, "function");
	strictEqual(typeof objectDefault.keyValuesStream, "function");
	strictEqual(typeof objectDefault.keyJoinStream, "function");
	strictEqual(typeof objectDefault.keyMapStream, "function");
	strictEqual(typeof objectDefault.valueMapStream, "function");
	strictEqual(typeof objectDefault.fromEntriesStream, "function");
	strictEqual(typeof objectDefault.toEntriesStream, "function");
	strictEqual(typeof objectDefault.skipConsecutiveDuplicatesStream, "function");
});
