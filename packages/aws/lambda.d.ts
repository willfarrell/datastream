// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

export function awsLambdaSetClient(lambdaClient: unknown): void;

export function awsLambdaReadableStream(
	lambdaOptions: Record<string, unknown> | Record<string, unknown>[],
	streamOptions?: StreamOptions,
): unknown;
export { awsLambdaReadableStream as awsLambdaResponseStream };
