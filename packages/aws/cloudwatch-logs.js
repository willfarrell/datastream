// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	CloudWatchLogsClient,
	FilterLogEventsCommand,
	GetLogEventsCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import { timeout } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

let client = new CloudWatchLogsClient(awsClientDefaults);
export const awsCloudWatchLogsSetClient = (cwlClient) => {
	client = cwlClient;
};

export const awsCloudWatchLogsGetLogEventsStream = async (
	options,
	streamOptions = {},
) => {
	const { pollingActive, pollingDelay = 1000, ...cwlOptions } = options;
	cwlOptions.startFromHead ??= true;
	async function* command(opts) {
		let expectMore = true;
		while (expectMore) {
			const response = await client.send(new GetLogEventsCommand(opts), {
				abortSignal: streamOptions.signal,
			});
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
						// Abortable idle wait: rejects immediately and clears the
						// timer when streamOptions.signal aborts mid-delay.
						await timeout(pollingDelay, { signal: streamOptions.signal });
					}
				} else {
					expectMore = false;
				}
			}
		}
	}
	return command({ ...cwlOptions });
};

export const awsCloudWatchLogsFilterLogEventsStream = async (
	options,
	streamOptions = {},
) => {
	async function* command(opts) {
		let expectMore = true;
		while (expectMore) {
			const response = await client.send(new FilterLogEventsCommand(opts), {
				abortSignal: streamOptions.signal,
			});
			for (const item of response.events ?? []) {
				yield item;
			}
			opts.nextToken = response.nextToken;
			expectMore = !!response.nextToken;
		}
	}
	return command({ ...options });
};

export default {
	setClient: awsCloudWatchLogsSetClient,
	getLogEventsStream: awsCloudWatchLogsGetLogEventsStream,
	filterLogEventsStream: awsCloudWatchLogsFilterLogEventsStream,
};
