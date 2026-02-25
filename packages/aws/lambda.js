// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	InvokeWithResponseStreamCommand,
	LambdaClient,
} from "@aws-sdk/client-lambda";
import { createReadableStream } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

let defaultClient = new LambdaClient(awsClientDefaults);
export const awsLambdaSetClient = (lambdaClient) => {
	defaultClient = lambdaClient;
};

export const awsLambdaReadableStream = (lambdaOptions, streamOptions = {}) => {
	return createReadableStream(awsLambdaGenerator(lambdaOptions), streamOptions);
};
export const awsLambdaResponseStream = awsLambdaReadableStream;

async function* awsLambdaGenerator(lambdaOptions, _streamOptions = {}) {
	if (!Array.isArray(lambdaOptions)) {
		lambdaOptions = [lambdaOptions];
	}
	for (const options of lambdaOptions) {
		const response = await defaultClient.send(
			new InvokeWithResponseStreamCommand(options),
		);
		for await (const chunk of response.EventStream) {
			if (chunk?.PayloadChunk?.Payload) {
				yield chunk.PayloadChunk.Payload;
			} else if (chunk?.InvokeComplete?.ErrorCode) {
				throw new Error(chunk.InvokeComplete.ErrorCode, {
					cause: chunk.InvokeComplete.ErrorDetails,
				});
			}
		}
	}
}

export default {
	setClient: awsLambdaSetClient,
	readableStream: awsLambdaReadableStream,
	responseStream: awsLambdaReadableStream,
};
