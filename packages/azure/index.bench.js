// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
import {
	azureCosmosUpsertItemStream,
	azureServiceBusReceiveMessagesStream,
	azureServiceBusSendMessagesStream,
} from "@datastream/azure";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";

const OPS = 5;
const options = { warmup: 1, samples: 30 };
const COUNT = 10_000;
const messages = Array.from({ length: COUNT }, (_, i) => ({
	body: `message-${i}`,
}));

// Fake SDK clients: no I/O, so the numbers are the stream overhead only.
const sender = {
	createMessageBatch: async () => {
		let count = 0;
		return {
			get count() {
				return count;
			},
			tryAddMessage: () => count < 100 && !!++count,
		};
	},
	sendMessages: async () => {},
};
const container = {
	items: {
		executeBulkOperations: async (operations) =>
			operations.map(() => ({ response: { statusCode: 200 } })),
	},
};

suite("azureServiceBus", () => {
	bench(`send ${COUNT} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await pipeline([
				createReadableStream(messages),
				azureServiceBusSendMessagesStream({ client: sender }),
			]);
		}
		b.end(OPS);
	});

	bench(`receive ${COUNT} messages`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			let offset = 0;
			const client = {
				receiveMessages: async (max) => {
					const page = messages.slice(offset, offset + max);
					offset += max;
					return page;
				},
			};
			await streamToArray(
				await azureServiceBusReceiveMessagesStream({
					client,
					maxMessageCount: 100,
				}),
			);
		}
		b.end(OPS);
	});
});

suite("azureCosmos", () => {
	bench(`upsert ${COUNT} items`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await pipeline([
				createReadableStream(messages),
				azureCosmosUpsertItemStream({ client: container }),
			]);
		}
		b.end(OPS);
	});
});
