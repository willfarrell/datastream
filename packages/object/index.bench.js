import { bench, suite } from "node:bench";
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

// -- Data generators --

const ITEMS = 100_000;
const COLS = 10;
const OPS = 1;
const options = { warmup: 1, samples: 30 };

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

suite("objectCountStream", () => {
	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const count = objectCountStream();
			const streams = [createReadableStream(objects), count];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("objectPickStream", () => {
	bench(`${ITEMS} objects, pick 3 of ${COLS} keys`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				objectPickStream({ keys: ["col0", "col1", "col2"] }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("objectOmitStream", () => {
	bench(`${ITEMS} objects, omit 3 of ${COLS} keys`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				objectOmitStream({ keys: ["col0", "col1", "col2"] }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("objectKeyMapStream", () => {
	bench(`${ITEMS} objects, rename 3 keys`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				objectKeyMapStream({
					keys: { col0: "renamed0", col1: "renamed1", col2: "renamed2" },
				}),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("objectKeyValueStream", () => {
	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				objectKeyValueStream({ key: "col0", value: "col1" }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("objectValueMapStream", () => {
	const valueMap = {};
	for (let i = 0; i < ITEMS; i++) {
		valueMap[`val_${i}_0`] = `mapped_${i}`;
	}

	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				objectValueMapStream({ key: "col0", values: valueMap }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("objectKeyJoinStream", () => {
	bench(`${ITEMS} objects, join 3 keys`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				objectKeyJoinStream({
					keys: { combined: ["col0", "col1", "col2"] },
					separator: "-",
				}),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("objectFromEntriesStream", () => {
	const arrays = Array.from({ length: ITEMS }, (_, r) =>
		Array.from({ length: COLS }, (_, c) => `val_${r}_${c}`),
	);
	const keys = Array.from({ length: COLS }, (_, i) => `col${i}`);

	bench(`${ITEMS} arrays → objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(arrays),
				objectFromEntriesStream({ keys }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("objectBatchStream", () => {
	// Add a group key so batching has ~100 groups
	const groupedObjects = objects.map((obj, i) => ({
		...obj,
		group: `group_${Math.floor(i / (ITEMS / 100))}`,
	}));

	bench(`${ITEMS} objects, ~100 batches`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(groupedObjects),
				objectBatchStream({ keys: ["group"] }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("objectPivotWideToLongStream", () => {
	bench(`${ITEMS} objects, pivot 3 keys`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				objectPivotWideToLongStream({ keys: ["col0", "col1", "col2"] }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("objectSkipConsecutiveDuplicatesStream", () => {
	bench(`${ITEMS} objects, all unique`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				objectSkipConsecutiveDuplicatesStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});

	const duplicatedObjects = objects
		.flatMap((obj) => [obj, obj])
		.slice(0, ITEMS);
	bench(`${ITEMS} objects, 50% duplicates`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(duplicatedObjects),
				objectSkipConsecutiveDuplicatesStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});
