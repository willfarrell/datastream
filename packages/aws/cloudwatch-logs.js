// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	CloudWatchLogsClient,
	FilterLogEventsCommand,
	GetLogEventsCommand,
	PutLogEventsCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import { createWritableStream, timeout } from "@datastream/core";
import {
	awsClientDefaults,
	awsReadableStream,
	awsUnsentOnDestroy,
} from "./client.js";

let defaultClient;
const getDefaultClient = () =>
	(defaultClient ??= new CloudWatchLogsClient(awsClientDefaults));
export const awsCloudWatchLogsSetClient = (cwlClient) => {
	defaultClient = cwlClient;
};

export const awsCloudWatchLogsGetLogEventsStream = async (
	options,
	streamOptions = {},
) => {
	const { pollingActive, pollingDelay = 1000, client, ...cwlOptions } = options;
	cwlOptions.startFromHead ??= true;
	async function* command(opts, signal) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new GetLogEventsCommand(opts),
				{ abortSignal: signal },
			);
			const events = response.events ?? [];
			for (const item of events) {
				yield item;
			}
			// CloudWatch echoes the token you sent (opts.nextToken) back as
			// nextForwardToken once there are no further events, so an unchanged
			// token is the end-of-page / caught-up signal.
			const tokenUnchanged = response.nextForwardToken === opts.nextToken;
			opts.nextToken = response.nextForwardToken;

			if (tokenUnchanged) {
				if (pollingActive) {
					if (pollingDelay > 0) {
						await timeout(pollingDelay, { signal });
					}
				} else {
					expectMore = false;
				}
			}
		}
	}
	return awsReadableStream(
		(signal) => command({ ...cwlOptions }, signal),
		streamOptions,
	);
};

export const awsCloudWatchLogsFilterLogEventsStream = async (
	options,
	streamOptions = {},
) => {
	const { client, ...filterOptions } = options;
	async function* command(opts, signal) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new FilterLogEventsCommand(opts),
				{ abortSignal: signal },
			);
			for (const item of response.events ?? []) {
				yield item;
			}
			opts.nextToken = response.nextToken;
			expectMore = !!response.nextToken;
		}
	}
	return awsReadableStream(
		(signal) => command(filterOptions, signal),
		streamOptions,
	);
};

// PutLogEvents batch limits: <=10,000 events, <=1,048,576 bytes (each event
// counts its UTF-8 message bytes plus 26), and a time span of <=24 hours.
const PUT_LOG_EVENTS_MAX_EVENTS = 10_000;
const PUT_LOG_EVENTS_MAX_BYTES = 1_048_576;
const PUT_LOG_EVENTS_EVENT_OVERHEAD_BYTES = 26;
const PUT_LOG_EVENTS_MAX_SPAN_MS = 24 * 60 * 60 * 1000;

// No sequenceToken: PutLogEvents has ignored it since 2023.
export const awsCloudWatchLogsPutLogEventsStream = (
	options,
	streamOptions = {},
) => {
	const { client, ...putOptions } = options;
	let batch = [];
	let batchBytes = 0;
	let minTimestamp = Number.POSITIVE_INFINITY;
	let maxTimestamp = Number.NEGATIVE_INFINITY;
	const send = async () => {
		if (!batch.length) {
			return;
		}
		// The service rejects a batch whose events are not in chronological order.
		const logEvents = batch.sort((a, b) => a.timestamp - b.timestamp);
		batch = [];
		batchBytes = 0;
		minTimestamp = Number.POSITIVE_INFINITY;
		maxTimestamp = Number.NEGATIVE_INFINITY;
		const response = await (client ?? getDefaultClient()).send(
			new PutLogEventsCommand({ ...putOptions, logEvents }),
			{ abortSignal: streamOptions.signal },
		);
		// Too old / too new / expired events are dropped while the rest of the
		// batch IS accepted; surface that instead of losing them silently. The
		// indexes point into the sorted batch the caller never sees, so attach the
		// rejected events themselves: tooNew starts at its (inclusive) index,
		// tooOld and expired end before their (exclusive) index.
		const { rejectedLogEventsInfo } = response;
		if (rejectedLogEventsInfo) {
			const {
				tooNewLogEventStartIndex,
				tooOldLogEventEndIndex,
				expiredLogEventEndIndex,
			} = rejectedLogEventsInfo;
			throw new Error("awsCloudWatchLogsPutLogEvents has rejected events", {
				cause: {
					rejectedLogEventsInfo,
					rejected: {
						tooNew: logEvents.slice(
							tooNewLogEventStartIndex ?? logEvents.length,
						),
						tooOld: logEvents.slice(0, tooOldLogEventEndIndex ?? 0),
						expired: logEvents.slice(0, expiredLogEventEndIndex ?? 0),
					},
				},
			});
		}
	};
	const write = async (chunk) => {
		const { timestamp, message } = chunk;
		if (!Number.isFinite(timestamp)) {
			throw new TypeError(
				"awsCloudWatchLogsPutLogEvents event timestamp must be a finite number",
			);
		}
		if (typeof message !== "string") {
			throw new TypeError(
				"awsCloudWatchLogsPutLogEvents event message must be a string",
			);
		}
		const chunkBytes =
			Buffer.byteLength(message) + PUT_LOG_EVENTS_EVENT_OVERHEAD_BYTES;
		if (chunkBytes > PUT_LOG_EVENTS_MAX_BYTES) {
			throw new RangeError(
				"awsCloudWatchLogsPutLogEvents event exceeds 1MiB limit",
				{ cause: { bytes: chunkBytes, limit: PUT_LOG_EVENTS_MAX_BYTES } },
			);
		}
		if (
			batch.length === PUT_LOG_EVENTS_MAX_EVENTS ||
			batchBytes + chunkBytes > PUT_LOG_EVENTS_MAX_BYTES ||
			Math.max(maxTimestamp, timestamp) - Math.min(minTimestamp, timestamp) >
				PUT_LOG_EVENTS_MAX_SPAN_MS
		) {
			await send();
		}
		batch.push(chunk);
		batchBytes += chunkBytes;
		minTimestamp = Math.min(minTimestamp, timestamp);
		maxTimestamp = Math.max(maxTimestamp, timestamp);
	};
	const final = () => send();
	return awsUnsentOnDestroy(
		createWritableStream(write, final, streamOptions),
		() => batch,
		"awsCloudWatchLogsPutLogEvents destroyed with buffered events",
	);
};
