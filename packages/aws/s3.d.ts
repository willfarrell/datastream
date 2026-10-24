// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamPassThrough,
	DatastreamReadable,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export function awsS3SetClient(s3Client: unknown): void;

export function awsS3GetObjectStream(
	options: {
		client?: unknown;
		Bucket?: string;
		Key?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

// Yields ListObjectsV2 `Contents` entries across all pages.
export function awsS3ListObjectsStream(
	options: {
		client?: unknown;
		Bucket: string;
		Prefix?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function awsS3PutObjectStream(
	options: {
		client?: unknown;
		onProgress?: (progress: unknown) => void;
		// @aws-sdk/lib-storage Upload tags (Tag[])
		tags?: { Key: string; Value: string }[];
		partSize?: number;
		queueSize?: number;
		Bucket?: string;
		Key?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamPassThrough & {
	// Resolves once the upload completes; contributes no key to pipeline().
	result: () => Promise<Record<string, never>>;
};

export function awsS3ChecksumStream(
	options?: {
		ChecksumAlgorithm?: string;
		partSize?: number;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamPassThrough & {
	result: () => StreamResult<{
		checksum: string;
		checksums: string[];
		partSize: number;
	}>;
};
