import { deepStrictEqual } from "node:assert";
import test from "node:test";
import {
	DeleteMessageBatchCommand,
	ReceiveMessageCommand,
	SendMessageBatchCommand,
	SQSClient,
} from "@aws-sdk/client-sqs";
import sqsDefault, {
	awsSQSDeleteMessageStream,
	awsSQSReceiveMessageStream,
	awsSQSSendMessageStream,
	awsSQSSetClient,
} from "@datastream/aws/sqs";

import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
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

	deepStrictEqual(output, [{ id: "a" }]);
});

test(`${variant}: awsSQSReceiveMessageStream should handle empty queue (Messages: undefined)`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	client.on(ReceiveMessageCommand).resolves({});

	const options = {};
	const stream = await awsSQSReceiveMessageStream(options);
	const output = await streamToArray(stream);

	deepStrictEqual(output, []);
});

test(`${variant}: awsSQSReceiveMessageStream should keep polling with pollingActive option`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	client
		.on(ReceiveMessageCommand)
		.resolvesOnce({ Messages: [{ id: "a" }] })
		.resolvesOnce({ Messages: [] })
		.resolvesOnce({ Messages: [{ id: "b" }] })
		.resolves({ Messages: [] });

	const options = { pollingActive: true, pollingDelay: 0 };
	const stream = await awsSQSReceiveMessageStream(options);

	// Collect first 2 items manually since pollingActive means infinite generator
	const output = [];
	for await (const item of stream) {
		output.push(item);
		if (output.length >= 2) break;
	}

	deepStrictEqual(output, [{ id: "a" }, { id: "b" }]);
});

test(`${variant}: awsSQSReceiveMessageStream should delay polling when pollingActive and queue is empty`, async (t) => {
	t.mock.timers.enable({ apis: ["setTimeout"] });

	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	client
		.on(ReceiveMessageCommand)
		.resolvesOnce({ Messages: [] })
		.resolvesOnce({ Messages: [{ id: "a" }] })
		.resolves({ Messages: [] });

	const options = { pollingActive: true, pollingDelay: 1000 };
	const stream = await awsSQSReceiveMessageStream(options);

	const output = [];
	const consuming = (async () => {
		for await (const item of stream) {
			output.push(item);
			if (output.length >= 1) break;
		}
	})();

	// Allow microtasks to settle so generator reaches setTimeout
	await new Promise((resolve) => setImmediate(resolve));
	// Advance mock timer past pollingDelay
	t.mock.timers.tick(1000);

	await consuming;

	deepStrictEqual(output, [{ id: "a" }]);
});

test(`${variant}: awsSQSReceiveMessageStream should not delay polling when messages are returned`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	client
		.on(ReceiveMessageCommand)
		.resolvesOnce({ Messages: [{ id: "a" }] })
		.resolvesOnce({ Messages: [{ id: "b" }] })
		.resolves({ Messages: [] });

	const options = { pollingActive: true, pollingDelay: 1000 };
	const stream = await awsSQSReceiveMessageStream(options);

	const output = [];
	for await (const item of stream) {
		output.push(item);
		if (output.length >= 2) break;
	}

	deepStrictEqual(output, [{ id: "a" }, { id: "b" }]);
});

test(`${variant}: awsSQSDeleteMessageStream should delete chunk`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	const input = "abcdefghijk".split("").map((Id) => ({ Id }));
	const options = {
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
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

	deepStrictEqual(result, {});
});

test(`${variant}: awsSQSSendMessageStream should put chunk`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	const input = "abcdefghijk".split("").map((id) => ({ id }));
	const options = {
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
	};

	client
		.on(SendMessageBatchCommand, {
			Entries: "abcdefghij".split("").map((id) => ({ id })),
		})
		.resolves({})
		.on(SendMessageBatchCommand, {
			Entries: "k".split("").map((id) => ({ id })),
		})
		.resolves({});

	const stream = [
		createReadableStream(input),
		awsSQSSendMessageStream(options),
	];
	const result = await pipeline(stream);

	deepStrictEqual(result, {});
});

test(`${variant}: awsSQSDeleteMessageStream should handle empty input`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	const input = [];
	const options = {
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
	};

	const stream = [
		createReadableStream(input),
		awsSQSDeleteMessageStream(options),
	];
	const result = await pipeline(stream);

	deepStrictEqual(result, {});
});

test(`${variant}: awsSQSSendMessageStream should handle empty input`, async (_t) => {
	const client = mockClient(SQSClient);
	awsSQSSetClient(client);

	const input = [];
	const options = {
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/000000000000/test",
	};

	const stream = [
		createReadableStream(input),
		awsSQSSendMessageStream(options),
	];
	const result = await pipeline(stream);

	deepStrictEqual(result, {});
});

test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(sqsDefault).sort(), [
		"deleteMessageStream",
		"receiveMessageStream",
		"sendMessageStream",
		"setClient",
	]);
});
