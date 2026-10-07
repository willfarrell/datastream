// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
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

// -- Data generators --

const OPS = 10;
const options = { warmup: 2, samples: 30 };
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

suite("protobufEncodeStream", () => {
	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				protobufEncodeStream({ Type }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("protobufDecodeStream", () => {
	bench(`${ITEMS} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(encoded),
				protobufDecodeStream({ Type }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("protobuf roundtrip", () => {
	bench(`${ITEMS} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(objects),
				protobufEncodeStream({ Type }),
				protobufDecodeStream({ Type }),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("protobuf framing roundtrip", () => {
	bench(`${ITEMS} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(encoded),
				protobufLengthPrefixFrameStream(),
				protobufLengthPrefixUnframeStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});

suite("protobufLengthPrefixUnframeStream", () => {
	bench(`${ITEMS} framed messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const streams = [
				createReadableStream(framed),
				protobufLengthPrefixUnframeStream(),
			];
			await streamToArray(pipejoin(streams));
		}
		b.end(OPS);
	});
});
