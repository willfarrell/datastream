// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { PublishBatchCommand, SNSClient } from "@aws-sdk/client-sns";
import { awsBatchEntriesStream, awsClientDefaults } from "./client.js";

// SNS PublishBatch aggregate payload limit (256KiB).
const SNS_MAX_BATCH_BYTES = 262_144;

// Created on first use, so importing the module (or always passing a per-call
// client) never constructs an unused SDK client.
let defaultClient;
const getDefaultClient = () =>
	(defaultClient ??= new SNSClient(awsClientDefaults));
export const awsSNSSetClient = (snsClient) => {
	defaultClient = snsClient;
};

export const awsSNSPublishMessageStream = (options, streamOptions = {}) => {
	const { retryMaxCount, client, ...sendOptions } = options;
	return awsBatchEntriesStream(
		(entries) =>
			(client ?? getDefaultClient()).send(
				new PublishBatchCommand({
					...sendOptions,
					PublishBatchRequestEntries: entries,
				}),
				{ abortSignal: streamOptions.signal },
			),
		{
			retryMaxCount,
			maxBatchBytes: SNS_MAX_BATCH_BYTES,
			errorMessage: "awsSNSPublishBatch has failed entries",
			oversizeMessage: "awsSNSPublishBatch entry exceeds 256KiB limit",
			destroyMessage: "awsSNSPublishBatch destroyed with buffered entries",
		},
		streamOptions,
	);
};
