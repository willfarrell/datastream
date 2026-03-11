// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions, StreamResult } from "@datastream/core";

export function ipfsGetStream(
	options: {
		node: unknown;
		cid: string;
	},
	streamOptions?: StreamOptions,
): Promise<unknown>;

export function ipfsAddStream(
	options?: {
		node?: unknown;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): Promise<
	unknown & {
		result: () => StreamResult<string>;
	}
>;

export default ipfsGetStream;
