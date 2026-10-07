import { bench, suite } from "node:bench";
import {
	arrowBatchFromObjectStream,
	arrowDetectSchemaStream,
	arrowToArrayStream,
	arrowToObjectStream,
} from "@datastream/arrow";
import {
	createReadableStream,
	pipejoin,
	streamToArray,
} from "@datastream/core";
import { Field, Int32, Schema, Utf8 } from "apache-arrow";

// -- Data generators --

const ITEMS = 10_000;
const OPS = 10;
const options = { warmup: 2, samples: 30 };

const usersSchema = new Schema([
	new Field("id", new Int32(), true),
	new Field("name", new Utf8(), true),
]);

const objects = Array.from({ length: ITEMS }, (_, i) => ({
	id: i,
	name: `user_${i}`,
}));

// -- Tests --

suite("arrowDetectSchemaStream", () => {
	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const detect = arrowDetectSchemaStream({ sampleSize: 100 });
			const streams = [createReadableStream(objects), detect];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("arrowBatchFromObjectStream", () => {
	bench(`${ITEMS} objects, batchSize 1000`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 1_000 }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});

	bench(`${ITEMS} objects, batchSize 100`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 100 }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("arrowToObjectStream", () => {
	bench(`${ITEMS} objects, batchSize 1000`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 1_000 }),
				arrowToObjectStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("arrowToArrayStream", () => {
	bench(`${ITEMS} objects, batchSize 1000`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 1_000 }),
				arrowToArrayStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("object roundtrip", () => {
	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const detect = arrowDetectSchemaStream({ sampleSize: 100 });
			const streams = [
				createReadableStream(objects),
				detect,
				arrowBatchFromObjectStream({
					schema: () => detect.result().value.schema,
					batchSize: 1_000,
				}),
				arrowToObjectStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});
