// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream, timeout } from "@datastream/core";
import { azureBatchStream } from "./client.js";

// client = ServiceBusReceiver (peekLock or receiveAndDelete)
export const azureServiceBusReceiveMessagesStream = async (
	options,
	streamOptions = {},
) => {
	const {
		client,
		maxMessageCount = 10,
		pollingActive,
		pollingDelay = 1000,
		...receiveOptions
	} = options;
	async function* command() {
		let expectMore = true;
		while (expectMore) {
			const messages = await client.receiveMessages(maxMessageCount, {
				...receiveOptions,
				abortSignal: streamOptions.signal,
			});
			for (const item of messages) {
				yield item;
			}
			expectMore = pollingActive || messages.length > 0;
			if (pollingActive && messages.length === 0 && pollingDelay > 0) {
				await timeout(pollingDelay, { signal: streamOptions.signal });
			}
		}
	}
	return command();
};

// client = ServiceBusSender
export const azureServiceBusSendMessagesStream = (
	options,
	streamOptions = {},
) => {
	const { client, ...batchOptions } = options;
	return azureBatchStream(
		{
			createBatch: () =>
				client.createMessageBatch({
					...batchOptions,
					abortSignal: streamOptions.signal,
				}),
			sendBatch: (batch) =>
				client.sendMessages(batch, { abortSignal: streamOptions.signal }),
			add: (batch, chunk) => batch.tryAddMessage(chunk),
			oversizeMessage:
				"azureServiceBusSendMessages message exceeds batch limit",
		},
		streamOptions,
	);
};

// client = ServiceBusReceiver (peekLock); chunk = a received message
export const azureServiceBusCompleteMessageStream = (
	options,
	streamOptions = {},
) => {
	const { client } = options;
	const write = (chunk) => client.completeMessage(chunk);
	return createWritableStream(write, streamOptions);
};
