// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createWritableStream, timeout } from "@datastream/core";

export const azureQueueReceiveMessagesStream = async (
	options,
	streamOptions = {},
) => {
	const {
		client,
		pollingActive,
		pollingDelay = 1000,
		...receiveOptions
	} = options;
	async function* command() {
		let expectMore = true;
		while (expectMore) {
			const response = await client.receiveMessages({
				...receiveOptions,
				abortSignal: streamOptions.signal,
			});
			const messages = response.receivedMessageItems ?? [];
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

// Queue Storage has no batch API: one request per message.
export const azureQueueSendMessageStream = (options, streamOptions = {}) => {
	const { client, ...sendOptions } = options;
	const write = async (chunk) => {
		await client.sendMessage(chunk, {
			...sendOptions,
			abortSignal: streamOptions.signal,
		});
	};
	return createWritableStream(write, streamOptions);
};

// chunk = { messageId, popReceipt } (a received message item works as-is).
export const azureQueueDeleteMessageStream = (options, streamOptions = {}) => {
	const { client } = options;
	const write = async (chunk) => {
		await client.deleteMessage(chunk.messageId, chunk.popReceipt, {
			abortSignal: streamOptions.signal,
		});
	};
	return createWritableStream(write, streamOptions);
};
