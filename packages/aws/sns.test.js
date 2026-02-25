import { deepStrictEqual } from "node:assert";
import test from "node:test";
import { PublishBatchCommand, SNSClient } from "@aws-sdk/client-sns";
import { awsSNSPublishMessageStream, awsSNSSetClient } from "@datastream/aws";

import { createReadableStream, pipeline } from "@datastream/core";
import { mockClient } from "aws-sdk-client-mock";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

test(`${variant}: awsSNSPublishMessageStream should put chunk`, async (_t) => {
	const client = mockClient(SNSClient);
	awsSNSSetClient(client);

	const input = "abcdefghijk".split("").map((id) => ({ id }));
	const options = {
		// TODO
	};

	client
		.on(PublishBatchCommand, {
			PublishBatchRequestEntries: "abcdefghij".split("").map((id) => ({ id })),
		})
		.resolves({})
		.on(PublishBatchCommand, {
			PublishBatchRequestEntries: "k".split("").map((id) => ({ id })),
		})
		.resolves({});

	const stream = [
		createReadableStream(input),
		awsSNSPublishMessageStream(options),
	];
	const result = await pipeline(stream);

	deepStrictEqual(result, {});
});
