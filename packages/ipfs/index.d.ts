// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions, StreamResult } from "@datastream/core";

export interface IpfsNode {
	get(cid: string): unknown;
	add(data: unknown[]): Promise<{ cid: string }>;
}

export function ipfsGetStream(
	options: {
		node: IpfsNode;
		cid: string;
	},
	streamOptions?: StreamOptions,
): Promise<unknown>;

export function ipfsAddStream(
	options?: {
		node?: IpfsNode;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): Promise<
	unknown & {
		result: () => StreamResult<string>;
	}
>;

declare const _default: {
	getStream: typeof ipfsGetStream;
	addStream: typeof ipfsAddStream;
};
export default _default;
