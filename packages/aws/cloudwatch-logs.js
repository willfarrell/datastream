// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	CloudWatchLogsClient,
	FilterLogEventsCommand,
	GetLogEventsCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import { awsClientDefaults } from "./client.js";

let client = new CloudWatchLogsClient(awsClientDefaults);
export const awsCloudWatchLogsSetClient = (cwlClient) => {
	client = cwlClient;
};

export const awsCloudWatchLogsGetLogEventsStream = async (
	options,
	_streamOptions = {},
) => {
	const { pollingActive, pollingDelay = 1000, ...cwlOptions } = options;
	cwlOptions.startFromHead ??= true;
	async function* command(options) {
		let previousToken;
		let expectMore = true;
		while (expectMore) {
			const response = await client.send(new GetLogEventsCommand(options));
			const events = response.events ?? [];
			for (const item of events) {
				yield item;
			}
			const tokenUnchanged =
				response.nextForwardToken === previousToken ||
				response.nextForwardToken === options.nextToken;
			previousToken = response.nextForwardToken;
			options.nextToken = response.nextForwardToken;

			if (tokenUnchanged) {
				if (pollingActive) {
					if (pollingDelay > 0) {
						await new Promise((resolve) => setTimeout(resolve, pollingDelay));
					}
				} else {
					expectMore = false;
				}
			}
		}
	}
	return command(cwlOptions);
};

export const awsCloudWatchLogsFilterLogEventsStream = async (
	options,
	_streamOptions = {},
) => {
	async function* command(options) {
		let expectMore = true;
		while (expectMore) {
			const response = await client.send(new FilterLogEventsCommand(options));
			for (const item of response.events ?? []) {
				yield item;
			}
			options.nextToken = response.nextToken;
			expectMore = !!response.nextToken;
		}
	}
	return command(options);
};

export default {
	setClient: awsCloudWatchLogsSetClient,
	getLogEventsStream: awsCloudWatchLogsGetLogEventsStream,
	filterLogEventsStream: awsCloudWatchLogsFilterLogEventsStream,
};
