import { deepEqual, equal } from "node:assert";
import test from "node:test";
import {
	InvokeWithResponseStreamCommand,
	LambdaClient,
} from "@aws-sdk/client-lambda";
import { awsLambdaReadableStream, awsLambdaSetClient } from "@datastream/aws";

import { createReadableStream, pipeline } from "@datastream/core";
// import sinon from 'sinon'
import { mockClient } from "aws-sdk-client-mock";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

if (variant === "node") {
	test(`${variant}: awsLambdaReadableStream should return chunk`, async (_t) => {
		const client = mockClient(LambdaClient);
		awsLambdaSetClient(client);

		const encoder = new TextEncoder();
		const decoder = new TextDecoder();

		client.on(InvokeWithResponseStreamCommand, {}).resolves({
			EventStream: createReadableStream([
				{
					PayloadChunk: {
						Payload: encoder.encode("1"),
					},
				},
				{
					PayloadChunk: {
						Payload: encoder.encode("2"),
					},
				},
			]),
		});

		let result = "";
		for await (const chunk of await awsLambdaReadableStream({})) {
			result += decoder.decode(chunk);
		}

		deepEqual(result, "12");
	});

	test(`${variant}: awsLambdaReadableStream should throw error`, async (_t) => {
		const client = mockClient(LambdaClient);
		awsLambdaSetClient(client);

		client.on(InvokeWithResponseStreamCommand, {}).resolves({
			EventStream: createReadableStream([
				{
					InvokeComplete: {
						ErrorCode: "ErrorCode",
						ErrorDetails: "ErrorDetails",
					},
				},
			]),
		});

		try {
			await pipeline([awsLambdaReadableStream({})]);

			equal(true, false);
		} catch (e) {
			equal(e.message, "ErrorCode");
			equal(e.cause, "ErrorDetails");
		}
	});
}
