// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
import { EventEmitter } from "node:events";
import { createReadableStream, pipeline } from "@datastream/core";
import {
	gcpPubSubAckStream,
	gcpPubSubPublishStream,
	gcpPubSubSubscribeStream,
} from "@datastream/gcp/pubsub";

const OPS = 5;
const options = { warmup: 1, samples: 30 };
const COUNT = 10_000;
const messages = Array.from({ length: COUNT }, (_, i) => ({
	data: Buffer.from(`message-${i}`),
	attributes: { i: `${i}` },
}));

const topic = {
	publishMessage: async () => "id",
	flush: async () => {},
};
class Subscription extends EventEmitter {
	close = async () => {};
}
const received = messages.map((message) => ({
	...message,
	ack: () => {},
	nack: () => {},
}));

suite("gcpPubSub", () => {
	bench(`publish ${COUNT} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await pipeline([
				createReadableStream(messages),
				gcpPubSubPublishStream({ topic }),
			]);
		}
		b.end(OPS);
	});

	bench(`subscribe + ack ${COUNT} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const subscription = new Subscription();
			const stream = gcpPubSubSubscribeStream(
				{ subscription },
				{ highWaterMark: COUNT },
			);
			const done = pipeline([stream, gcpPubSubAckStream({ subscription })]);
			for (const message of received) subscription.emit("message", message);
			await stream.stop();
			await done;
		}
		b.end(OPS);
	});
});
