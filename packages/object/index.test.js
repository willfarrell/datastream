import { deepStrictEqual, strictEqual, throws } from "node:assert";
import test, { describe } from "node:test";
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
	objectSkipConsecutiveDuplicatesStream,
	objectToEntriesStream,
	objectValueMapStream,
} from "@datastream/object";
import { deepEqual } from "#deepEqual";
import { variant } from "../variant.js";
import {
	deepClone,
	shallowClone,
	shallowEqual,
	deepEqual as structuralDeepEqual,
} from "./helpers.js";

describe(`@datastream/object (${variant})`, () => {
	// *** Major-version API *** //
	test(`exports no default and no objectReadableStream`, async (_t) => {
		const mod = await import("@datastream/object");
		strictEqual(mod.default, undefined);
		strictEqual(mod.objectReadableStream, undefined);
	});

	// *** objectCountStream *** //
	test(`objectCountStream should count length of chunks`, async (_t) => {
		const input = ["1", "2", "3"];
		const streams = [createReadableStream(input), objectCountStream()];

		const result = await pipeline(streams);
		const { key, value } = streams[1].result();

		strictEqual(key, "objectCount");
		strictEqual(result.objectCount, 3);
		strictEqual(value, 3);
	});

	test(`objectCountStream should count length of chunks with custom key`, async (_t) => {
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
	test(`objectBatchStream should batch chunks by key`, async (_t) => {
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

	test(`objectBatchStream should batch chunks by index`, async (_t) => {
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
	test(`objectPivotLongToWideStream should pivot chunks to wide`, async (_t) => {
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

	test(`objectPivotLongToWideStream should catch invalid chunk type`, async (_t) => {
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
	test(`objectPivotWideToLongStream should pivot chunks to wide`, async (_t) => {
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

	test(`objectPivotWideToLongStream should use default keyParam and valueParam`, async (_t) => {
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
	test(`objectKeyValueStream should transform to {chunk[key]:chunk[value]}`, async (_t) => {
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
	test(`objectKeyValuesStream should transform to {chunk[key]:chunk}`, async (_t) => {
		const input = [{ a: "1", b: "2", c: "3" }];
		const streams = [
			createReadableStream(input),
			objectKeyValuesStream({ key: "a" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ 1: { a: "1", b: "2", c: "3" } }]);
	});

	test(`objectKeyValuesStream should transform to {chunk[key]:chunk[values]}`, async (_t) => {
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
	test(`objectSkipConsecutiveDuplicatesStream should skip consecutive duplicates`, async (_t) => {
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
	test(`objectPivotLongToWideStream should use custom delimiter`, async (_t) => {
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
	test(`objectKeyJoinStream should join keys into new key`, async (_t) => {
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
	test(`objectKeyMapStream should map keys to new names`, async (_t) => {
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
	test(`objectValueMapStream should map values using lookup`, async (_t) => {
		const input = [{ status: "active" }, { status: "inactive" }];
		const streams = [
			createReadableStream(input),
			objectValueMapStream({
				key: "status",
				values: { active: 1, inactive: 0 },
			}),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ status: 1 }, { status: 0 }]);
	});

	// *** objectPickStream *** //
	test(`objectPickStream should pick specified keys`, async (_t) => {
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
	test(`objectFromEntriesStream should transform array to object with keys`, async (_t) => {
		const input = [[1, 2, 3]];
		const streams = [
			createReadableStream(input),
			objectFromEntriesStream({ keys: ["a", "b", "c"] }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1, b: 2, c: 3 }]);
	});

	test(`objectFromEntriesStream should support lazy keys function`, async (_t) => {
		const input = [[1, 2, 3]];
		const streams = [
			createReadableStream(input),
			objectFromEntriesStream({ keys: () => ["a", "b", "c"] }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1, b: 2, c: 3 }]);
	});

	test(`objectFromEntriesStream should transform multiple arrays`, async (_t) => {
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
	test(`objectOmitStream should omit specified keys`, async (_t) => {
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
	test(`objectToEntriesStream should transform object to array with keys`, async (_t) => {
		const input = [{ a: 1, b: 2, c: 3 }];
		const streams = [
			createReadableStream(input),
			objectToEntriesStream({ keys: ["a", "b", "c"] }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [[1, 2, 3]]);
	});

	test(`objectToEntriesStream should support lazy keys function`, async (_t) => {
		const input = [{ a: 1, b: 2, c: 3 }];
		const streams = [
			createReadableStream(input),
			objectToEntriesStream({ keys: () => ["a", "b", "c"] }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [[1, 2, 3]]);
	});

	test(`objectToEntriesStream should transform multiple objects`, async (_t) => {
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

	test(`objectToEntriesStream should return undefined for missing keys`, async (_t) => {
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
	test(`objectSkipConsecutiveDuplicatesStream should use shallow equality by default`, async (_t) => {
		const input = [{ a: 1 }, { a: 1 }, { a: 2 }, { a: 2 }, { a: 1 }];
		const streams = [
			createReadableStream(input),
			objectSkipConsecutiveDuplicatesStream(),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [{ a: 1 }, { a: 2 }, { a: 1 }]);
	});

	test(`objectSkipConsecutiveDuplicatesStream isNestedObject should use deep comparison`, async (_t) => {
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

	// deepEqual used to be JSON.stringify(a) === JSON.stringify(b), so records that
	// were structurally identical but had a different key order (routine after
	// JSON.parse or objectPickStream) were not recognised as duplicates.
	test(`objectSkipConsecutiveDuplicatesStream isNestedObject ignores key order`, async (_t) => {
		const input = [
			{ a: 1, b: { c: 2, d: 3 } },
			{ b: { d: 3, c: 2 }, a: 1 },
			{ a: 1, b: { c: 2, d: 4 } },
		];
		const streams = [
			createReadableStream(input),
			objectSkipConsecutiveDuplicatesStream({ isNestedObject: true }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [
			{ a: 1, b: { c: 2, d: 3 } },
			{ a: 1, b: { c: 2, d: 4 } },
		]);
	});

	// *** objectPivotWideToLongStream shallow copy regression *** //
	test(`objectPivotWideToLongStream should not mutate input chunks`, async (_t) => {
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

	// *** objectPivotWideToLongStream isNestedObject deep clone *** //
	test(`objectPivotWideToLongStream should deep clone when isNestedObject is true`, async (_t) => {
		const input = [{ id: 1, a: { x: 10 }, b: { x: 20 } }];
		const streams = [
			createReadableStream(input),
			objectPivotWideToLongStream({
				keys: ["a", "b"],
				keyParam: "axis",
				valueParam: "val",
				isNestedObject: true,
			}),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			{ id: 1, axis: "a", val: { x: 10 } },
			{ id: 1, axis: "b", val: { x: 20 } },
		]);
	});

	// *** objectKeyJoinStream isNestedObject deep clone *** //
	test(`objectKeyJoinStream should deep clone when isNestedObject is true`, async (_t) => {
		const input = [{ firstName: "John", lastName: "Doe", nested: { v: 1 } }];
		const streams = [
			createReadableStream(input),
			objectKeyJoinStream({
				keys: { fullName: ["firstName", "lastName"] },
				separator: " ",
				isNestedObject: true,
			}),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ nested: { v: 1 }, fullName: "John Doe" }]);
	});

	// *** objectKeyJoinStream shallow copy regression *** //
	test(`objectKeyJoinStream should not mutate input chunks`, async (_t) => {
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
	test(`objectBatchStream should enforce maxBatchSize`, async (_t) => {
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
	test(`objectBatchStream should keep distinct key tuples in separate batches when values contain spaces`, async (_t) => {
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
	test(`objectValueMapStream should preserve the original value for keys not present in the map`, async (_t) => {
		const input = [{ status: "active" }, { status: "unknown" }];
		const streams = [
			createReadableStream(input),
			objectValueMapStream({
				key: "status",
				values: { active: 1, inactive: 0 },
			}),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		// "unknown" is not in the map — original value must be preserved
		deepStrictEqual(output, [{ status: 1 }, { status: "unknown" }]);
	});

	// *** objectPivotLongToWideStream should not mutate input *** //
	test(`objectPivotLongToWideStream should not mutate input chunks`, async (_t) => {
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

	// *** objectBatchStream should produce no output on empty input *** //
	test(`objectBatchStream should produce no output when input is empty`, async (_t) => {
		const streams = [
			createReadableStream([]),
			objectBatchStream({ keys: ["a"] }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, []);
	});

	// *** objectToEntriesStream should produce empty arrays when keys is empty *** //
	test(`objectToEntriesStream should produce empty array when keys is empty`, async (_t) => {
		const input = [{ a: 1, b: 2 }];
		const streams = [
			createReadableStream(input),
			objectToEntriesStream({ keys: [] }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [[]]);
	});

	// *** objectPickStream inherited keys *** //
	test(`objectPickStream should not pick keys that only exist on Object.prototype`, async (_t) => {
		const input = [
			{ a: 1, constructor: 2, toString: 3 },
			JSON.parse('{"a":1,"__proto__":{"isAdmin":true}}'),
		];
		const streams = [
			createReadableStream(input),
			objectPickStream({ keys: ["a"] }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1 }, { a: 1 }]);
		strictEqual(Object.getPrototypeOf(output[1]), Object.prototype);
		strictEqual(output[1].isAdmin, undefined);
	});

	// *** objectOmitStream inherited keys *** //
	test(`objectOmitStream should keep own keys whose names exist on Object.prototype`, async (_t) => {
		const input = [{ a: 1, b: 2, toString: 3, valueOf: 4 }];
		const streams = [
			createReadableStream(input),
			objectOmitStream({ keys: ["b"] }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ a: 1, toString: 3, valueOf: 4 }]);
	});

	// *** objectBatchStream maxBatchSize empty batch *** //
	test(`objectBatchStream should not emit empty batches after a maxBatchSize flush`, async (_t) => {
		const input = [{ k: 1 }, { k: 1 }, { k: 2 }, { k: 2 }];
		const streams = [
			createReadableStream(input),
			objectBatchStream({ keys: ["k"], maxBatchSize: 2 }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			[{ k: 1 }, { k: 1 }],
			[{ k: 2 }, { k: 2 }],
		]);
	});

	// *** objectKeyMapStream inherited keys *** //
	test(`objectKeyMapStream should not rename keys that only exist on Object.prototype`, async (_t) => {
		const input = [
			{ a: 1, constructor: 2, toString: 3 },
			JSON.parse('{"a":1,"__proto__":{"isAdmin":true}}'),
		];
		const streams = [
			createReadableStream(input),
			objectKeyMapStream({ keys: { a: "A" } }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			{ A: 1, constructor: 2, toString: 3 },
			JSON.parse('{"A":1,"__proto__":{"isAdmin":true}}'),
		]);
		strictEqual(Object.getPrototypeOf(output[1]), Object.prototype);
		strictEqual(output[1].isAdmin, undefined);
	});

	// *** objectPivotLongToWideStream reserved pivot value *** //
	test(`objectPivotLongToWideStream should keep a "__proto__" pivot value as an own column`, async (_t) => {
		const input = [
			{ id: 1, k: "__proto__", v: { polluted: true } },
			{ id: 1, k: "x", v: 2 },
		];
		const streams = [
			createReadableStream(input),
			objectBatchStream({ keys: ["id"] }),
			objectPivotLongToWideStream({ keys: ["k"], valueParam: "v" }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			JSON.parse('{"id":1,"__proto__":{"polluted":true},"x":2}'),
		]);
		strictEqual(output[0].polluted, undefined);
		deepStrictEqual(Object.getOwnPropertyDescriptor(output[0], "x"), {
			value: 2,
			writable: true,
			enumerable: true,
			configurable: true,
		});
	});

	// *** objectFromEntriesStream reserved key *** //
	test(`objectFromEntriesStream should keep a "__proto__" key as an own property`, async (_t) => {
		const input = [[{ polluted: true }, 2]];
		const streams = [
			createReadableStream(input),
			objectFromEntriesStream({ keys: ["__proto__", "x"] }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			JSON.parse('{"__proto__":{"polluted":true},"x":2}'),
		]);
		strictEqual(output[0].polluted, undefined);
	});

	// *** objectPickStream reserved key *** //
	test(`objectPickStream should pick a "__proto__" key as an own property`, async (_t) => {
		const input = [JSON.parse('{"a":1,"__proto__":{"isAdmin":true}}')];
		const streams = [
			createReadableStream(input),
			objectPickStream({ keys: ["__proto__"] }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [JSON.parse('{"__proto__":{"isAdmin":true}}')]);
		strictEqual(output[0].isAdmin, undefined);
	});

	// *** objectOmitStream reserved key *** //
	test(`objectOmitStream should keep a "__proto__" key as an own property`, async (_t) => {
		const input = [JSON.parse('{"a":1,"__proto__":{"isAdmin":true}}')];
		const streams = [
			createReadableStream(input),
			objectOmitStream({ keys: ["a"] }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [JSON.parse('{"__proto__":{"isAdmin":true}}')]);
		strictEqual(output[0].isAdmin, undefined);
	});

	// *** objectKeyValuesStream reserved key *** //
	test(`objectKeyValuesStream should keep a "__proto__" value key as an own property`, async (_t) => {
		const input = [JSON.parse('{"id":"k","__proto__":{"isAdmin":true}}')];
		const streams = [
			createReadableStream(input),
			objectKeyValuesStream({ key: "id", values: ["__proto__"] }),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			{ k: JSON.parse('{"__proto__":{"isAdmin":true}}') },
		]);
		strictEqual(output[0].k.isAdmin, undefined);
	});

	// *** objectKeyJoinStream reserved key *** //
	test(`objectKeyJoinStream should keep a "__proto__" joined key as an own property`, async (_t) => {
		const input = [{ a: "1", b: "2", c: 3 }];
		const streams = [
			createReadableStream(input),
			objectKeyJoinStream({
				keys: JSON.parse('{"__proto__":["a","b"]}'),
				separator: "-",
			}),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [JSON.parse('{"c":3,"__proto__":"1-2"}')]);
	});

	// *** objectSkipConsecutiveDuplicatesStream differing undefined keys *** //
	test(`objectSkipConsecutiveDuplicatesStream should keep rows whose keys differ but read undefined`, async (_t) => {
		const input = [{ x: undefined }, { y: undefined }];
		const streams = [
			createReadableStream(input),
			objectSkipConsecutiveDuplicatesStream(),
		];

		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [{ x: undefined }, { y: undefined }]);
		strictEqual(shallowEqual({ x: undefined }, { y: undefined }), false);
	});

	// *** objectBatchStream maxBatchSize: undefined and null are unlimited *** //
	test(`objectBatchStream maxBatchSize null keeps a whole group in one batch`, async (_t) => {
		const input = Array.from({ length: 5 }, (_, i) => ({ a: "same", b: i }));
		for (const maxBatchSize of [null, undefined]) {
			const output = await streamToArray(
				pipejoin([
					createReadableStream(input),
					objectBatchStream({ keys: ["a"], maxBatchSize }),
				]),
			);
			deepStrictEqual(output, [input]);
		}
	});

	// *** Clone / equality helpers (moved here from @datastream/core) *** //
	test(`shallowClone copies own enumerable properties`, () => {
		const src = { a: 1, b: { c: 2 } };
		const copy = shallowClone(src);
		deepStrictEqual(copy, src);
		strictEqual(copy === src, false);
		strictEqual(copy.b === src.b, true);
	});

	test(`deepClone deep-copies via structuredClone`, () => {
		const src = { a: 1, b: { c: 2 } };
		const copy = deepClone(src);
		deepStrictEqual(copy, src);
		strictEqual(copy.b === src.b, false);
	});

	test(`deepClone throws for non-cloneable values with the original error as cause`, () => {
		throws(
			() => deepClone({ fn: () => {} }),
			(e) => {
				strictEqual(
					e.message,
					"Failed to clone chunk, possibly circular reference",
				);
				strictEqual(e.cause.name, "DataCloneError");
				return true;
			},
		);
	});

	test(`shallowEqual compares own keys`, () => {
		strictEqual(shallowEqual({ a: 1 }, { a: 1 }), true);
		strictEqual(shallowEqual({ a: 1 }, { a: 2 }), false);
		strictEqual(shallowEqual({ a: 1 }, { a: 1, b: 2 }), false);
		strictEqual(shallowEqual(null, null), true);
		strictEqual(shallowEqual(undefined, undefined), true);
		strictEqual(shallowEqual(null, { a: 1 }), false);
		strictEqual(shallowEqual(undefined, { a: 1 }), false);
		strictEqual(shallowEqual({ a: 1 }, null), false);
		strictEqual(shallowEqual({ a: 1 }, undefined), false);
	});

	test(`#deepEqual resolves to node:util on node and the structural fallback in the browser`, () => {
		strictEqual(deepEqual === structuralDeepEqual, variant === "browser");
	});

	// The platform deepEqual (node:util isDeepStrictEqual on node) and the
	// structural fallback the browser build uses must agree on these cases.
	for (const [name, equal] of [
		["platform", deepEqual],
		["structural", structuralDeepEqual],
	]) {
		test(`deepEqual (${name}) compares structurally`, () => {
			strictEqual(equal({ a: { b: 1 } }, { a: { b: 1 } }), true);
			strictEqual(equal({ a: { b: 1 } }, { a: { b: 2 } }), false);
			// The cases a JSON.stringify comparison gets wrong.
			strictEqual(equal({ a: 1, b: 2 }, { b: 2, a: 1 }), true);
			strictEqual(equal({ a: undefined }, {}), false);
			strictEqual(equal(Number.NaN, Number.NaN), true);
			strictEqual(equal(new Date(0), new Date(0)), true);
			strictEqual(equal(new Date(0), new Date(1)), false);
			strictEqual(equal(new Set([1]), new Set([1])), true);
			strictEqual(equal(new Set([1]), new Set([2])), false);
			strictEqual(equal(new Set([1]), new Set([1, 2])), false);
			strictEqual(equal(new Set([1, 2]), new Set([1])), false);
			strictEqual(
				equal(new Map([["a", { b: 1 }]]), new Map([["a", { b: 1 }]])),
				true,
			);
			strictEqual(equal(new Map([["a", 1]]), new Map([["b", 1]])), false);
			strictEqual(equal(new Map([["a", 1]]), new Map([["a", 2]])), false);
			strictEqual(equal(new Map([["a", 1]]), new Map()), false);
			strictEqual(equal(new Map(), new Map([["a", 1]])), false);
			strictEqual(equal(/a/g, /a/g), true);
			strictEqual(equal(/a/g, /a/i), false);
			strictEqual(equal(/a/, /b/), false);
			strictEqual(equal(Uint8Array.of(1, 2), Uint8Array.of(1, 2)), true);
			strictEqual(equal(Uint8Array.of(1, 2), Uint8Array.of(1, 3)), false);
			strictEqual(equal(Uint8Array.of(1), Uint8Array.of(1, 2)), false);
			strictEqual(equal(Uint8Array.of(1), Int8Array.of(1)), false);
			strictEqual(equal([1, 2], [1, 3]), false);
			strictEqual(equal({}, []), false);
			strictEqual(equal({ a: 1 }, { b: 1 }), false);
			strictEqual(equal({ a: 1 }, null), false);
			strictEqual(equal(null, {}), false);
			strictEqual(equal({}, 1), false);
			strictEqual(equal(1, {}), false);
			strictEqual(equal(1, "1"), false);
			strictEqual(equal(1, Object(1)), false);
			strictEqual(equal(Object(1), 1), false);
		});

		test(`deepEqual (${name}) handles circular references`, () => {
			const a = {};
			a.self = a;
			const b = {};
			b.self = b;
			strictEqual(equal(a, a), true);
			strictEqual(equal(a, b), true);
			b.other = 1;
			strictEqual(equal(a, b), false);
		});
	}
});
