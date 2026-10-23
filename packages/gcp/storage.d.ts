// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { Readable, Writable } from "node:stream";
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

/** The parts of an @google-cloud/storage `File` the streams use. */
export interface GcpStorageFile {
	createReadStream(options?: object): Readable;
	createWriteStream(options?: object): Writable;
}

/** The parts of an @google-cloud/storage `Bucket` the streams use. */
export interface GcpStorageBucket {
	file(name: string): GcpStorageFile;
	// Resolves [files, nextQuery, apiResponse]; nextQuery is null on the last page.
	getFiles(query: object): Promise<[unknown[], ...unknown[]]>;
}

/** A Cloud Storage object resource (File.metadata), as listed. */
export interface GcpStorageObjectMetadata {
	name?: string;
	[key: string]: unknown;
}

export function gcpStorageReadStream(
	options: {
		bucket: Pick<GcpStorageBucket, "file">;
		name: string;
		start?: number;
		end?: number;
		decompress?: boolean;
		validation?: "crc32c" | "md5" | boolean;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamReadable<Uint8Array>;

export function gcpStorageWriteStream(
	options: {
		bucket: Pick<GcpStorageBucket, "file">;
		name: string;
		resumable?: boolean;
		contentType?: string;
		metadata?: Record<string, unknown>;
		validation?: "crc32c" | "md5" | boolean;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable<Uint8Array | string>;

export function gcpStorageListStream(
	options: {
		bucket: Pick<GcpStorageBucket, "getFiles">;
		prefix?: string;
		maxResults?: number;
		[key: string]: unknown;
	},
	streamOptions?: StreamOptions,
): DatastreamReadable<GcpStorageObjectMetadata>;
