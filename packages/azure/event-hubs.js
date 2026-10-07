// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { azureBatchStream } from "./client.js";

// client = EventHubConsumerClient. Adapts subscribe() callbacks to a pull-based
// generator: processEvents waits until the consumer has drained the page, so
// the SDK does not read ahead of a slow pipeline. Without pollingActive the
// stream ends on the first empty page (the SDK delivers one after
// maxWaitTimeInSeconds with no events).
export const azureEventHubsReceiveEventsStream = async (
	options,
	streamOptions = {},
) => {
	const { client, partitionId, pollingActive, ...subscribeOptions } = options;
	const { signal } = streamOptions;
	async function* command() {
		const queue = [];
		let error;
		let ended = false;
		let wake;
		let drained = () => {};
		const handlers = {
			processEvents: async (events) => {
				queue.push(...events);
				if (!events.length && !pollingActive) {
					ended = true;
				}
				wake?.();
				if (queue.length) {
					await new Promise((resolve) => {
						drained = resolve;
					});
				}
			},
			processError: async (e) => {
				error ??= e;
				wake?.();
			},
		};
		const onAbort = () => handlers.processError(signal.reason);
		signal?.addEventListener("abort", onAbort, { once: true });
		const subscription =
			partitionId === undefined
				? client.subscribe(handlers, subscribeOptions)
				: client.subscribe(partitionId, handlers, subscribeOptions);
		try {
			if (signal?.aborted) {
				throw signal.reason;
			}
			while (true) {
				if (queue.length) {
					yield queue.shift();
					if (!queue.length) {
						drained();
					}
				} else if (error) {
					throw error;
				} else if (ended) {
					return;
				} else {
					await new Promise((resolve) => {
						wake = resolve;
					});
				}
			}
		} finally {
			signal?.removeEventListener("abort", onAbort);
			drained();
			await subscription.close();
		}
	}
	return command();
};

// client = EventHubProducerClient; partitionKey/partitionId go to createBatch.
export const azureEventHubsSendEventsStream = (options, streamOptions = {}) => {
	const { client, ...batchOptions } = options;
	return azureBatchStream(
		{
			createBatch: () =>
				client.createBatch({
					...batchOptions,
					abortSignal: streamOptions.signal,
				}),
			sendBatch: (batch) =>
				client.sendBatch(batch, { abortSignal: streamOptions.signal }),
			add: (batch, chunk) => batch.tryAdd(chunk),
			oversizeMessage: "azureEventHubsSendEvents event exceeds batch limit",
		},
		streamOptions,
	);
};
