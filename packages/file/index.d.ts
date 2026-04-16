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

export function fileReadStream(
	options: {
		types?: FilePickerTypes[];
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamReadable>;

export function fileWriteStream(
	options: {
		path?: string;
		types?: FilePickerTypes[];
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamWritable>;

declare const _default: {
	readStream: typeof fileReadStream;
	writeStream: typeof fileWriteStream;
};
export default _default;
