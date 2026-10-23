// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export interface FetchOptions {
	url?: string;
	method?: string;
	headers?: Record<string, string>;
	body?: unknown;
	mode?: RequestMode;
	credentials?: RequestCredentials;
	cache?: RequestCache;
	redirect?: RequestRedirect;
	referrer?: string;
	referrerPolicy?: ReferrerPolicy;
	integrity?: string;
	keepalive?: boolean;
	duplex?: string;
	rateLimit?: number;
	dataPath?: string | string[];
	nextPath?: string | string[];
	qs?: Record<string, string | number>;
	offsetParam?: string;
	offsetAmount?: number;
	rateLimitTimestamp?: number;
	concurrency?: number;
	retryMaxCount?: number | null;
	retryAfterMax?: number | null;
	timeout?: number | null;
	maxPages?: number | null;
	maxBodySize?: number | null;
}

export interface FetchWritableOptions extends FetchOptions {
	resultKey?: string;
}

export function fetchSetDefaults(options: Partial<FetchOptions>): void;

export function fetchWritableStream(
	options: FetchWritableOptions,
	streamOptions?: StreamOptions,
): Promise<
	DatastreamWritable & {
		result: () => StreamResult<Response>;
	}
>;
export { fetchWritableStream as fetchRequestStream };

export function fetchReadableStream(
	fetchOptions: FetchOptions | FetchOptions[],
	streamOptions?: StreamOptions,
): DatastreamReadable;
export { fetchReadableStream as fetchResponseStream };

export function fetchRateLimit(
	options: FetchOptions,
	streamOptions?: StreamOptions,
): Promise<Response>;
