// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions, StreamResult } from "@datastream/core";

export function awsS3SetClient(s3Client: unknown): void;

export function awsS3GetObjectStream(
	options: {
		client?: unknown;
		Bucket?: string;
		Key?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): Promise<unknown>;

export function awsS3PutObjectStream(
	options: {
		client?: unknown;
		onProgress?: (progress: unknown) => void;
		tags?: Record<string, string>;
		Bucket?: string;
		Key?: string;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => Promise<StreamResult<unknown>>;
};

export function awsS3ChecksumStream(
	options?: {
		ChecksumAlgorithm?: string;
		partSize?: number;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<{
		checksum: string;
		checksums: string[];
		partSize: number;
	}>;
};
