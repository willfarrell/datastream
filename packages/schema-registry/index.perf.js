// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	streamToArray,
} from "@datastream/core";
import {
	confluentFrameStream,
	confluentUnframeStream,
	glueFrameStream,
	glueUnframeStream,
} from "@datastream/schema-registry";
import { Bench } from "tinybench";

// -- Data generators --

const time = Number(process.env.BENCH_TIME ?? 5_000);

const schemaId = 42;
const schemaVersionId = "12345678-1234-1234-1234-1234567890ab";
const count = 10_000;

// Generate N Uint8Array payloads of 64 bytes each
const messages = Array.from({ length: count }, (_, i) => {
	const buf = new Uint8Array(64);
	for (let j = 0; j < 64; j++) buf[j] = (i + j) % 256;
	return buf;
});

// Pre-frame messages for unframe benchmarks
const confluentFramed = await streamToArray(
	pipejoin([
		createReadableStream(messages),
		confluentFrameStream({ schemaId }),
	]),
);

const glueFramed = await streamToArray(
	pipejoin([
		createReadableStream(messages),
		glueFrameStream({ schemaVersionId }),
	]),
);

// -- Tests --

test("perf: confluentFrameStream", async () => {
	const bench = new Bench({ name: "confluentFrameStream", time });

	bench.add(`${count} messages`, async () => {
		const streams = [
			createReadableStream(messages),
			confluentFrameStream({ schemaId }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: confluentUnframeStream", async () => {
	const bench = new Bench({ name: "confluentUnframeStream", time });

	bench.add(`${count} messages`, async () => {
		const streams = [
			createReadableStream(confluentFramed),
			confluentUnframeStream(),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: confluent frame -> unframe roundtrip", async () => {
	const bench = new Bench({ name: "confluent roundtrip", time });

	bench.add(`${count} messages`, async () => {
		const streams = [
			createReadableStream(messages),
			confluentFrameStream({ schemaId }),
			confluentUnframeStream(),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: glueFrameStream", async () => {
	const bench = new Bench({ name: "glueFrameStream", time });

	bench.add(`${count} messages`, async () => {
		const streams = [
			createReadableStream(messages),
			glueFrameStream({ schemaVersionId }),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: glueUnframeStream", async () => {
	const bench = new Bench({ name: "glueUnframeStream", time });

	bench.add(`${count} messages`, async () => {
		const streams = [createReadableStream(glueFramed), glueUnframeStream()];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: glue frame -> unframe roundtrip", async () => {
	const bench = new Bench({ name: "glue roundtrip", time });

	bench.add(`${count} messages`, async () => {
		const streams = [
			createReadableStream(messages),
			glueFrameStream({ schemaVersionId }),
			glueUnframeStream(),
		];
		await streamToArray(pipejoin(streams));
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
