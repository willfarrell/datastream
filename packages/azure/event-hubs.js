// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { azureBatchStream, azureReadableStream } from "./client.js";

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
	async function* command(signal) {
		const queue = [];
		let error;
		let ended = false;
		let wake;
		// subscribe() without a partitionId runs one pump per partition, so several
		// processEvents calls can be waiting at once: resolve them all on drain.
		const waiters = [];
		const drained = () => {
			for (const resolve of waiters.splice(0)) resolve();
		};
		const handlers = {
			processEvents: async (events) => {
				queue.push(...events);
				if (!events.length && !pollingActive) {
					ended = true;
				}
				wake?.();
				if (queue.length) {
					await new Promise((resolve) => {
						waiters.push(resolve);
					});
				}
			},
			processError: async (e) => {
				// The SDK also reports errors it is retrying itself (MessagingError
				// retryable: true); only the rest end the stream.
				if (e?.retryable) {
					return;
				}
				error ??= e;
				wake?.();
			},
		};
		const onAbort = () => handlers.processError(signal.reason);
		// No `once`: abort fires at most once and the finally below removes it.
		signal.addEventListener("abort", onAbort);
		const subscription =
			partitionId === undefined
				? client.subscribe(handlers, subscribeOptions)
				: client.subscribe(partitionId, handlers, subscribeOptions);
		// No already-aborted check: the generator only starts on the first read,
		// and an aborted streamOptions.signal has destroyed the stream by then.
		try {
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
			signal.removeEventListener("abort", onAbort);
			drained();
			await subscription.close();
		}
	}
	return azureReadableStream(command, streamOptions);
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
			destroyMessage:
				"azureEventHubsSendEvents destroyed with a buffered batch",
		},
		streamOptions,
	);
};
