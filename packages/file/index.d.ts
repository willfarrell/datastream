// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

export interface FilePickerTypes {
	description?: string;
	accept?: Record<string, string[]>;
}

export function fileReadStream(
	options: {
		types?: FilePickerTypes[];
	},
	streamOptions?: StreamOptions,
): Promise<unknown>;

export function fileWriteStream(
	options: {
		path?: string;
		types?: FilePickerTypes[];
	},
	streamOptions?: StreamOptions,
): Promise<unknown>;
