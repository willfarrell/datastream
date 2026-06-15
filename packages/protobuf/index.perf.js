// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	streamToArray,
} from "@datastream/core";
import {
	protobufDecodeStream,
	protobufEncodeStream,
	protobufLengthPrefixFrameStream,
	protobufLengthPrefixUnframeStream,
} from "@datastream/protobuf";
import protobuf from "protobufjs";
import { Bench } from "tinybench";

// -- Data generators --

const time = Number(process.env.BENCH_TIME ?? 5_000);
const ITEMS = 10_000;

const Type = new protobuf.Type("Msg")
	.add(new protobuf.Field("id", 1, "int32"))
	.add(new protobuf.Field("name", 2, "string"))
	.add(new protobuf.Field("value", 3, "double"));

const objects = Array.from({ length: ITEMS }, (_, i) => ({
	id: i,
	name: `item_${i}`,
	value: i * 1.5,
}));

// Pre-encoded buffers for the decode/unframe benchmarks.
const encoded = await streamToArray(
	pipejoin([createReadableStream(objects), protobufEncodeStream({ Type })]),
);
const framed = await streamToArray(
	pipejoin([createReadableStream(encoded), protobufLengthPrefixFrameStream()]),
);

// -- Tests --

test("perf: protobufEncodeStream", async () => {
	const bench = new Bench({ name: "protobufEncodeStream", time });

	bench.add(`${ITEMS} objects`, async () => {
		const streams = [
			createReadableStream(objects),
			protobufEncodeStream({ Type }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: protobufDecodeStream", async () => {
	const bench = new Bench({ name: "protobufDecodeStream", time });

	bench.add(`${ITEMS} messages`, async () => {
		const streams = [
			createReadableStream(encoded),
			protobufDecodeStream({ Type }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: protobuf roundtrip (encode → decode)", async () => {
	const bench = new Bench({ name: "protobuf roundtrip", time });

	bench.add(`${ITEMS} objects`, async () => {
		const streams = [
			createReadableStream(objects),
			protobufEncodeStream({ Type }),
			protobufDecodeStream({ Type }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: protobuf length-prefix framing roundtrip", async () => {
	const bench = new Bench({ name: "protobuf framing roundtrip", time });

	bench.add(`${ITEMS} messages`, async () => {
		const streams = [
			createReadableStream(encoded),
			protobufLengthPrefixFrameStream(),
			protobufLengthPrefixUnframeStream(),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: protobufLengthPrefixUnframeStream", async () => {
	const bench = new Bench({ name: "protobufLengthPrefixUnframeStream", time });

	bench.add(`${ITEMS} framed messages`, async () => {
		const streams = [
			createReadableStream(framed),
			protobufLengthPrefixUnframeStream(),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
