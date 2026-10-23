// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
import { avroDecodeStream, avroEncodeStream } from "@datastream/avro";
import {
	createReadableStream,
	pipejoin,
	streamToArray,
} from "@datastream/core";

const OPS = 10;
const options = { warmup: 2, samples: 30 };
const ITEMS = 10_000;

const schema = {
	type: "record",
	name: "Msg",
	fields: [
		{ name: "id", type: "int" },
		{ name: "name", type: "string" },
		{ name: "value", type: "double" },
	],
};

const objects = Array.from({ length: ITEMS }, (_, i) => ({
	id: i,
	name: `item_${i}`,
	value: i * 1.5,
}));

const encoded = await streamToArray(
	pipejoin([createReadableStream(objects), avroEncodeStream({ schema })]),
);

suite("avroEncodeStream", () => {
	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await streamToArray(
				pipejoin([createReadableStream(objects), avroEncodeStream({ schema })]),
			);
		}
		b.end(OPS);
	});
});

suite("avroDecodeStream", () => {
	bench(`${ITEMS} records`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await streamToArray(
				pipejoin([createReadableStream(encoded), avroDecodeStream({ schema })]),
			);
		}
		b.end(OPS);
	});
});
