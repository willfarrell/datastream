// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	DynamoDBStreamsClient,
	GetRecordsCommand,
} from "@aws-sdk/client-dynamodb-streams";
import { timeout } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

// Created on first use, so importing the module (or always passing a per-call
// client) never constructs an unused SDK client.
let defaultClient;
const getDefaultClient = () =>
	(defaultClient ??= new DynamoDBStreamsClient(awsClientDefaults));
export const awsDynamoDBStreamsSetClient = (dynamoDBStreamsClient) => {
	defaultClient = dynamoDBStreamsClient;
};

export const awsDynamoDBStreamsGetRecordsStream = async (
	options,
	streamOptions = {},
) => {
	const {
		pollingActive,
		pollingDelay = 1000,
		client,
		...streamsOptions
	} = options;
	async function* command(opts) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new GetRecordsCommand(opts),
				{ abortSignal: streamOptions.signal },
			);
			const records = response.Records ?? [];
			for (const item of records) {
				yield item;
			}
			// SDK v3 returns NextShardIterator as undefined (not null) for a closed
			// shard; normalise so either value ends the loop.
			opts.ShardIterator = response.NextShardIterator ?? null;
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
