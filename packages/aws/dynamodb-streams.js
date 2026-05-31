// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	DynamoDBStreamsClient,
	GetRecordsCommand,
} from "@aws-sdk/client-dynamodb-streams";
import { timeout } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

let client = new DynamoDBStreamsClient(awsClientDefaults);
export const awsDynamoDBStreamsSetClient = (dynamoDBStreamsClient) => {
	client = dynamoDBStreamsClient;
};

export const awsDynamoDBStreamsGetRecordsStream = async (
	options,
	streamOptions = {},
) => {
	const { pollingActive, pollingDelay = 1000, ...streamsOptions } = options;
	async function* command(opts) {
		let expectMore = true;
		while (expectMore) {
			const response = await client.send(new GetRecordsCommand(opts), {
				abortSignal: streamOptions.signal,
			});
			const records = response.Records ?? [];
			for (const item of records) {
				yield item;
			}
			opts.ShardIterator = response.NextShardIterator;
			expectMore =
				opts.ShardIterator !== null && (pollingActive || records.length > 0);
			if (pollingActive && records.length === 0 && pollingDelay > 0) {
				// Abortable idle wait: rejects immediately and clears the timer
				// when streamOptions.signal aborts mid-delay.
				await timeout(pollingDelay, { signal: streamOptions.signal });
			}
		}
	}
	return command({ ...streamsOptions });
};

export default {
	setClient: awsDynamoDBStreamsSetClient,
	getRecordsStream: awsDynamoDBStreamsGetRecordsStream,
};
