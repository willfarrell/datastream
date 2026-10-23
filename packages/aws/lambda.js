// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	InvokeWithResponseStreamCommand,
	LambdaClient,
} from "@aws-sdk/client-lambda";
import { awsClientDefaults, awsReadableStream } from "./client.js";

let defaultClient;
const getDefaultClient = () =>
	(defaultClient ??= new LambdaClient(awsClientDefaults));
export const awsLambdaSetClient = (lambdaClient) => {
	defaultClient = lambdaClient;
};

export const awsLambdaReadableStream = (lambdaOptions, streamOptions = {}) =>
	awsReadableStream(
		(signal) => awsLambdaGenerator(lambdaOptions, signal),
		streamOptions,
	);
export const awsLambdaResponseStream = awsLambdaReadableStream;

// Fail-fast across the array form: a send() rejection or an InvokeComplete
// ErrorCode terminates the generator and later invocations do not run. The
// thrown Error's cause carries the offending FunctionName/index so consumers
// can identify which invocation failed.
async function* awsLambdaGenerator(lambdaOptions, signal) {
	if (!Array.isArray(lambdaOptions)) {
		lambdaOptions = [lambdaOptions];
	}
	for (let index = 0; index < lambdaOptions.length; index++) {
		const { client, ...invokeOptions } = lambdaOptions[index];
		const response = await (client ?? getDefaultClient()).send(
			new InvokeWithResponseStreamCommand(invokeOptions),
			{ abortSignal: signal },
		);
		for await (const chunk of response.EventStream) {
			if (chunk?.PayloadChunk?.Payload) {
				yield chunk.PayloadChunk.Payload;
			} else if (chunk?.InvokeComplete?.ErrorCode) {
				throw new Error(chunk.InvokeComplete.ErrorCode, {
					cause: {
						FunctionName: invokeOptions.FunctionName,
						index,
						ErrorDetails: chunk.InvokeComplete.ErrorDetails,
					},
				});
			}
		}
	}
}
