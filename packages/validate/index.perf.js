import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { transpileSchema, validateStream } from "@datastream/validate";
import { Bench } from "tinybench";

// -- Data generators --

const ITEMS = 100_000;
const time = Number(process.env.BENCH_TIME ?? 5_000);

const schema = {
	type: "object",
	properties: {
		id: { type: "integer" },
		name: { type: "string" },
		email: { type: "string" },
		age: { type: "integer", minimum: 0, maximum: 150 },
		active: { type: "boolean" },
	},
	required: ["id", "name", "email"],
	additionalProperties: false,
};

const validObjects = Array.from({ length: ITEMS }, (_, i) => ({
	id: i,
	name: `user_${i}`,
	email: `user_${i}@example.com`,
	age: (i % 100) + 18,
	active: i % 2 === 0,
}));

const mixedObjects = validObjects.map((obj, i) =>
	i % 100 === 0
		? { id: `not_a_number_${i}`, name: obj.name, email: obj.email, age: -1 }
		: obj,
);

// -- Tests --

test("perf: transpileSchema", async () => {
	const bench = new Bench({ name: "transpileSchema", time });

	bench.add("compile schema", () => {
		transpileSchema(schema);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: validateStream (all valid)", async () => {
	const bench = new Bench({ name: "validateStream (all valid)", time });
	const compiledSchema = transpileSchema(schema);

	bench.add(`${ITEMS} objects, all valid`, async () => {
		const streams = [
			createReadableStream(validObjects),
			validateStream({ schema: compiledSchema }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: validateStream (1% invalid)", async () => {
	const bench = new Bench({ name: "validateStream (1% invalid)", time });
	const compiledSchema = transpileSchema(schema);

	bench.add(`${ITEMS} objects, 1% invalid`, async () => {
		const validate = validateStream({ schema: compiledSchema });
		const streams = [createReadableStream(mixedObjects), validate];
		await pipeline(streams);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: validateStream (precompiled vs inline schema)", async () => {
	const bench = new Bench({
		name: "validateStream precompiled vs inline",
		time,
	});
	const compiledSchema = transpileSchema(schema);

	bench.add(`${ITEMS} objects, precompiled`, async () => {
		const streams = [
			createReadableStream(validObjects),
			validateStream({ schema: compiledSchema }),
		];
		await streamToArray(pipejoin(streams));
	});

	bench.add(`${ITEMS} objects, inline schema`, async () => {
		const streams = [
			createReadableStream(validObjects),
			validateStream({ schema }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
