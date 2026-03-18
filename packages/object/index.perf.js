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
	objectOmitStream,
	objectPickStream,
	objectPivotWideToLongStream,
	objectSkipConsecutiveDuplicatesStream,
	objectValueMapStream,
} from "@datastream/object";
import { Bench } from "tinybench";

// -- Data generators --

const ITEMS = 100_000;
const COLS = 10;
const time = Number(process.env.BENCH_TIME ?? 5_000);

const generateObjects = (rows, cols) =>
	Array.from({ length: rows }, (_, r) => {
		const obj = {};
		for (let c = 0; c < cols; c++) {
			obj[`col${c}`] = `val_${r}_${c}`;
		}
		return obj;
	});

const objects = generateObjects(ITEMS, COLS);

// -- Tests --

test("perf: objectCountStream", async () => {
	const bench = new Bench({ name: "objectCountStream", time });

	bench.add(`${ITEMS} objects`, async () => {
		const count = objectCountStream();
		const streams = [createReadableStream(objects), count];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: objectPickStream", async () => {
	const bench = new Bench({ name: "objectPickStream", time });

	bench.add(`${ITEMS} objects, pick 3 of ${COLS} keys`, async () => {
		const streams = [
			createReadableStream(objects),
			objectPickStream({ keys: ["col0", "col1", "col2"] }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: objectOmitStream", async () => {
	const bench = new Bench({ name: "objectOmitStream", time });

	bench.add(`${ITEMS} objects, omit 3 of ${COLS} keys`, async () => {
		const streams = [
			createReadableStream(objects),
			objectOmitStream({ keys: ["col0", "col1", "col2"] }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: objectKeyMapStream", async () => {
	const bench = new Bench({ name: "objectKeyMapStream", time });

	bench.add(`${ITEMS} objects, rename 3 keys`, async () => {
		const streams = [
			createReadableStream(objects),
			objectKeyMapStream({
				keys: { col0: "renamed0", col1: "renamed1", col2: "renamed2" },
			}),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: objectKeyValueStream", async () => {
	const bench = new Bench({ name: "objectKeyValueStream", time });

	bench.add(`${ITEMS} objects`, async () => {
		const streams = [
			createReadableStream(objects),
			objectKeyValueStream({ key: "col0", value: "col1" }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: objectValueMapStream", async () => {
	const bench = new Bench({ name: "objectValueMapStream", time });

	const valueMap = {};
	for (let i = 0; i < ITEMS; i++) {
		valueMap[`val_${i}_0`] = `mapped_${i}`;
	}

	bench.add(`${ITEMS} objects`, async () => {
		const streams = [
			createReadableStream(objects),
			objectValueMapStream({ key: "col0", values: valueMap }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: objectKeyJoinStream", async () => {
	const bench = new Bench({ name: "objectKeyJoinStream", time });

	bench.add(`${ITEMS} objects, join 3 keys`, async () => {
		const streams = [
			createReadableStream(objects),
			objectKeyJoinStream({
				keys: { combined: ["col0", "col1", "col2"] },
				separator: "-",
			}),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: objectFromEntriesStream", async () => {
	const bench = new Bench({ name: "objectFromEntriesStream", time });

	const arrays = Array.from({ length: ITEMS }, (_, r) =>
		Array.from({ length: COLS }, (_, c) => `val_${r}_${c}`),
	);
	const keys = Array.from({ length: COLS }, (_, i) => `col${i}`);

	bench.add(`${ITEMS} arrays → objects`, async () => {
		const streams = [
			createReadableStream(arrays),
			objectFromEntriesStream({ keys }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: objectBatchStream", async () => {
	const bench = new Bench({ name: "objectBatchStream", time });

	// Add a group key so batching has ~100 groups
	const groupedObjects = objects.map((obj, i) => ({
		...obj,
		group: `group_${Math.floor(i / (ITEMS / 100))}`,
	}));

	bench.add(`${ITEMS} objects, ~100 batches`, async () => {
		const streams = [
			createReadableStream(groupedObjects),
			objectBatchStream({ keys: ["group"] }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: objectPivotWideToLongStream", async () => {
	const bench = new Bench({ name: "objectPivotWideToLongStream", time });

	bench.add(`${ITEMS} objects, pivot 3 keys`, async () => {
		const streams = [
			createReadableStream(objects),
			objectPivotWideToLongStream({ keys: ["col0", "col1", "col2"] }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: objectSkipConsecutiveDuplicatesStream", async () => {
	const bench = new Bench({
		name: "objectSkipConsecutiveDuplicatesStream",
		time,
	});

	bench.add(`${ITEMS} objects, all unique`, async () => {
		const streams = [
			createReadableStream(objects),
			objectSkipConsecutiveDuplicatesStream(),
		];
		await streamToArray(pipejoin(streams));
	});

	const duplicatedObjects = objects
		.flatMap((obj) => [obj, obj])
		.slice(0, ITEMS);
	bench.add(`${ITEMS} objects, 50% duplicates`, async () => {
		const streams = [
			createReadableStream(duplicatedObjects),
			objectSkipConsecutiveDuplicatesStream(),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
