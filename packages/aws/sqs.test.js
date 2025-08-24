import { deepEqual } from "node:assert";
import test from "node:test";
import {
	DeleteMessageBatchCommand,
	ReceiveMessageCommand,
	SendMessageCommand,
	SQSClient,
} from "@aws-sdk/client-sqs";
import {
	awsSQSDeleteMessageStream,
	awsSQSReceiveMessageStream,
	awsSQSSendMessageStream,
	awsSQSSetClient,
} from "@datastream/aws";

import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
// import sinon from 'sinon'
import { mockClient } from "aws-sdk-client-mock";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

test(`${variant}: awsSQSReceiveMessageStream should get chunk`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	client
		.on(ReceiveMessageCommand)
		.resolvesOnce({
			Messages: [{ id: "a" }],
		})
		.resolvesOnce({
			Messages: [],
		});

	const options = {};
	const stream = await awsSQSReceiveMessageStream(options);
	const output = await streamToArray(stream);

	deepEqual(output, [{ id: "a" }]);
});

test(`${variant}: awsSQSDeleteMessageStream should delete chunk`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	const input = "abcdefghijk".split("").map((Id) => ({ Id }));
	const options = {
		// TODO
	};

	client
		.on(DeleteMessageBatchCommand, {
			Entries: "abcdefghij".split("").map((Id) => ({ Id })),
		})
		.resolves({})
		.on(DeleteMessageBatchCommand, {
			Entries: "k".split("").map((Id) => ({ Id })),
		})
		.resolves({});

	const stream = [
		createReadableStream(input),
		awsSQSDeleteMessageStream(options),
	];
	const result = await pipeline(stream);

	deepEqual(result, {});
});

test(`${variant}: awsSQSSendMessageStream should put chunk`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	const input = "abcdefghijk".split("").map((id) => ({ id }));
	const options = {
		// TODO
	};

	client
		.on(SendMessageCommand, {
			Entries: "abcdefghij".split("").map((id) => ({ id })),
		})
		.resolves({})
		.on(SendMessageCommand, {
			Entries: "k".split("").map((id) => ({ id })),
		})
		.resolves({});

	const stream = [
		createReadableStream(input),
		awsSQSSendMessageStream(options),
	];
	const result = await pipeline(stream);

	deepEqual(result, {});
});
