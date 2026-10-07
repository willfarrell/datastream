// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export function azureBlobDownloadStream(
	options: {
		// BlobClient
		client: unknown;
		offset?: number;
		count?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function azureBlobUploadStream(
	options: {
		// BlockBlobClient
		client: unknown;
		bufferSize?: number;
		maxConcurrency?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable & {
	result: () => Promise<StreamResult<unknown>>;
};
