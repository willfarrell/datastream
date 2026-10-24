// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export function azureCosmosQueryStream(
	options: {
		client: unknown;
		query: string | { query: string; parameters?: unknown[] };
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function azureCosmosUpsertItemStream(
	options: {
		client: unknown;
		// Max retries of throttled (429) operations (default 10); null = unlimited.
		retryMaxCount?: number | null;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;

export function azureCosmosDeleteItemStream(
	options: {
		client: unknown;
		// Max retries of throttled (429) operations (default 10); null = unlimited.
		retryMaxCount?: number | null;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable;
