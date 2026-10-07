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
	// Max array items fetched concurrently (default 1 = sequential). Read from
	// the first item of an array; items are yielded in array order and request
	// starts are spaced by rateLimit.
	concurrency?: number;
	// Limits: undefined = default, null = unlimited. Exceeding one throws a
	// RangeError.
	// Max attempts on 429 before throwing (default 10); resets after a success.
	retryMaxCount?: number | null;
	// Upper bound in ms for a 429 Retry-After wait (default 60_000); null = no
	// cap (still bounded by 2^31-1, the largest delay setTimeout supports).
	retryAfterMax?: number | null;
	// Max JSON pages fetched per request config before throwing (default 10_000).
	maxPages?: number | null;
	// Max bytes of a JSON response body (default 16_777_216).
	maxBodySize?: number | null;
}

export interface FetchWritableOptions extends FetchOptions {
	// Key of the response in pipeline results (default "output").
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
