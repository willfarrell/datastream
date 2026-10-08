// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export interface IpfsNode {
	get(cid: string): unknown;
	add(data: AsyncIterable<unknown>): Promise<{ cid: string }> | { cid: string };
}

export function ipfsGetStream(
	options: {
		node: IpfsNode;
		cid: string;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function ipfsAddStream(
	options: {
		node: IpfsNode;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): Promise<
	DatastreamWritable & {
		result: () => StreamResult<string>;
	}
>;
