// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

export { openDB as indexedDBConnect } from "idb";

export function indexedDBReadStream(
	options: {
		db: unknown;
		store: string;
		index?: string;
		key?: IDBKeyRange | IDBValidKey;
	},
	streamOptions?: StreamOptions,
): Promise<unknown>;

export function indexedDBWriteStream(
	options: {
		db: unknown;
		store: string;
	},
	streamOptions?: StreamOptions,
): Promise<unknown>;
