// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { PublishBatchCommand, SNSClient } from "@aws-sdk/client-sns";
import { createWritableStream } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

let client = new SNSClient(awsClientDefaults);
export const awsSNSSetClient = (snsClient) => {
	client = snsClient;
};

export const awsSNSPublishMessageStream = (options, streamOptions = {}) => {
	let batch = [];
	const send = () => {
		options.PublishBatchRequestEntries = batch;
		batch = [];
		return client.send(new PublishBatchCommand(options));
	};
	const write = async (chunk) => {
		if (batch.length === 10) {
			await send();
		}
		batch.push(chunk);
	};
	const final = () => (batch.length ? send() : undefined);
	return createWritableStream(write, final, streamOptions);
};

export default {
	setClient: awsSNSSetClient,
	publishMessageStream: awsSNSPublishMessageStream,
};
