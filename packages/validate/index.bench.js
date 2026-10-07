import { bench, suite } from "node:bench";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { transpileSchema, validateStream } from "@datastream/validate";

// -- Data generators --

const ITEMS = 100_000;
const OPS = 1;
const options = { warmup: 1, samples: 30 };

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

suite("transpileSchema", () => {
	bench("compile schema", options, async (b) => {
		b.start();
		for (let op = 0; op < 1_000; op++) {
			transpileSchema(schema);
		}
		b.end(1_000);
	});
});

suite("validateStream (all valid)", () => {
	const compiledSchema = transpileSchema(schema);

	bench(`${ITEMS} objects, all valid`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(validObjects),
				validateStream({ schema: compiledSchema }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("validateStream (1% invalid)", () => {
	const compiledSchema = transpileSchema(schema);

	bench(`${ITEMS} objects, 1% invalid`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const validate = validateStream({ schema: compiledSchema });
			const streams = [createReadableStream(mixedObjects), validate];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("validateStream precompiled vs inline", () => {
	const compiledSchema = transpileSchema(schema);

	bench(`${ITEMS} objects, precompiled`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(validObjects),
				validateStream({ schema: compiledSchema }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});

	bench(`${ITEMS} objects, inline schema`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(validObjects),
				validateStream({ schema }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});
