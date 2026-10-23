// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream, timeout } from "@datastream/core";
import { azureBatchStream, azureReadableStream } from "./client.js";

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
	async function* command(signal) {
		let expectMore = true;
		while (expectMore) {
			const messages = await client.receiveMessages(maxMessageCount, {
				...receiveOptions,
				abortSignal: signal,
			});
			for (const item of messages) {
				yield item;
			}
			expectMore = pollingActive || messages.length > 0;
			if (pollingActive && messages.length === 0 && pollingDelay > 0) {
				await timeout(pollingDelay, { signal });
			}
		}
	}
	return azureReadableStream(command, streamOptions);
};

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
			destroyMessage:
				"azureServiceBusSendMessages destroyed with a buffered batch",
		},
		streamOptions,
	);
};

export const azureServiceBusCompleteMessageStream = (
	options,
	streamOptions = {},
) => {
	const { client } = options;
	const write = (chunk) => client.completeMessage(chunk);
	return createWritableStream(write, streamOptions);
};

export const azureServiceBusAbandonMessageStream = (
	options,
	streamOptions = {},
) => {
	const { client, propertiesToModify } = options;
	const write = (chunk) => client.abandonMessage(chunk, propertiesToModify);
	return createWritableStream(write, streamOptions);
};

export const azureServiceBusDeadLetterMessageStream = (
	options,
	streamOptions = {},
) => {
	const { client, ...deadLetterOptions } = options;
	const write = (chunk) => client.deadLetterMessage(chunk, deadLetterOptions);
	return createWritableStream(write, streamOptions);
};
