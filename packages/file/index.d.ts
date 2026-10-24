// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export interface FilePickerTypes {
	description?: string;
	accept?: Record<string, string[]>;
}

// Both builds return the stream as a Promise.
// node: `path` is required; `basePath` confines `path` to that directory (no
// symlinks, no traversal).
// browser: File System Access API picker; `path` is the suggested save name and
// `basePath` is ignored.
export interface FileStreamOptions {
	path?: string;
	basePath?: string;
	types?: FilePickerTypes[];
}

export function fileReadStream(
	options: FileStreamOptions,
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function fileWriteStream(
	options: FileStreamOptions,
	streamOptions?: StreamOptions,
): Promise<DatastreamWritable>;
