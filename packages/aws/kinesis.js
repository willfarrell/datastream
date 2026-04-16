// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	GetRecordsCommand,
	KinesisClient,
	PutRecordsCommand,
} from "@aws-sdk/client-kinesis";
import { createWritableStream } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

let client = new KinesisClient(awsClientDefaults);
export const awsKinesisSetClient = (kinesisClient) => {
	client = kinesisClient;
};

export const awsKinesisGetRecordsStream = async (
	options,
	streamOptions = {},
) => {
	const { pollingActive, pollingDelay = 1000, ...kinesisOptions } = options;
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
			expectMore = pollingActive || records.length > 0;
			if (pollingActive && records.length === 0 && pollingDelay > 0) {
				await new Promise((resolve) => setTimeout(resolve, pollingDelay));
			}
		}
	}
	return command({ ...kinesisOptions });
};

export const awsKinesisPutRecordsStream = (options, streamOptions = {}) => {
	let batch = [];
	const send = () => {
		options.Records = batch;
		batch = [];
		return client.send(new PutRecordsCommand(options));
	};
	const write = async (chunk) => {
		if (batch.length === 500) {
			await send();
		}
		batch.push(chunk);
	};
	const final = () => (batch.length ? send() : undefined);
	return createWritableStream(write, final, streamOptions);
};

export default {
	setClient: awsKinesisSetClient,
	getRecordsStream: awsKinesisGetRecordsStream,
	putRecordsStream: awsKinesisPutRecordsStream,
};
