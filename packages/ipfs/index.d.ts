// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamPassThrough,
	DatastreamReadable,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

// streamOptions.signal is forwarded as AbortOptions (js-ipfs, kubo-rpc-client, helia).
export interface IpfsAbortOptions {
	signal?: AbortSignal;
}

export interface IpfsNode {
	get(cid: string, options?: IpfsAbortOptions): unknown;
	add(
		data: AsyncIterable<unknown>,
		options?: IpfsAbortOptions,
	): Promise<{ cid: string }> | { cid: string };
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
	DatastreamPassThrough & {
		result: () => StreamResult<string>;
	}
>;
