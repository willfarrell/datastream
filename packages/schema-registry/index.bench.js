// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
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

// -- Data generators --

const OPS = 10;
const options = { warmup: 2, samples: 30 };

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

suite("confluentFrameStream", () => {
	bench(`${count} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(messages),
				confluentFrameStream({ schemaId }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("confluentUnframeStream", () => {
	bench(`${count} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(confluentFramed),
				confluentUnframeStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("confluent roundtrip", () => {
	bench(`${count} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(messages),
				confluentFrameStream({ schemaId }),
				confluentUnframeStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("glueFrameStream", () => {
	bench(`${count} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(messages),
				glueFrameStream({ schemaVersionId }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("glueUnframeStream", () => {
	bench(`${count} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [createReadableStream(glueFramed), glueUnframeStream()];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("glue roundtrip", () => {
	bench(`${count} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(messages),
				glueFrameStream({ schemaVersionId }),
				glueUnframeStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});
