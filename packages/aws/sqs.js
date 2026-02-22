// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	DeleteMessageBatchCommand,
	ReceiveMessageCommand,
	SendMessageBatchCommand,
	SQSClient,
} from "@aws-sdk/client-sqs";
import { createWritableStream } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

let client = new SQSClient(awsClientDefaults);
export const awsSQSSetClient = (sqsClient) => {
	client = sqsClient;
};

export const awsSQSReceiveMessageStream = async (
	options,
	_streamOptions = {},
) => {
	// TODO needs option to keep polling or not
	async function* command(options) {
		let expectMore = true;
		while (expectMore) {
			const response = await client.send(new ReceiveMessageCommand(options));
			const messages = response.Messages ?? [];
			for (const item of messages) {
				yield item;
			}
			expectMore = messages.length;
		}
	}
	return command(options);
};

export const awsSQSDeleteMessageStream = (options, streamOptions) => {
	let batch = [];
	const send = () => {
		options.Entries = batch;
		batch = [];
		return client.send(new DeleteMessageBatchCommand(options));
	};
	const write = async (chunk) => {
		if (batch.length === 10) {
			await send();
		}
		batch.push(chunk);
	};
	const final = send;
	return createWritableStream(write, final, streamOptions);
};

export const awsSQSSendMessageStream = (options, streamOptions) => {
	let batch = [];
	const send = () => {
		options.Entries = batch;
		batch = [];
		return client.send(new SendMessageBatchCommand(options));
	};
	const write = async (chunk) => {
		if (batch.length === 10) {
			await send();
		}
		batch.push(chunk);
	};
	const final = send;
	return createWritableStream(write, final, streamOptions);
};

export default {
	setClient: awsSQSSetClient,
	sendMessageStream: awsSQSSendMessageStream,
	receiveMessageStream: awsSQSReceiveMessageStream,
	deleteMessageStream: awsSQSDeleteMessageStream,
};
