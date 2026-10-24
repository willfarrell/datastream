// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamPassThrough,
	DatastreamReadable,
	StreamOptions,
} from "@datastream/core";

export function azureBlobDownloadStream(
	options: {
		client: unknown;
		offset?: number;
		count?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function azureBlobListStream(
	options: {
		client: unknown;
		prefix?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function azureBlobUploadStream(
	options: {
		client: unknown;
		bufferSize?: number;
		maxConcurrency?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamPassThrough & {
	// Resolves once the upload completes; contributes no key to pipeline().
	result: () => Promise<Record<string, never>>;
};
