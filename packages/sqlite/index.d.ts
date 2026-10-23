// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatabaseSync, SQLInputValue, SQLOutputValue } from "node:sqlite";
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export function sqliteQueryStream(
	options: {
		db: DatabaseSync;
		sql: string;
		/** Array binds positional `?` params; object binds named `$name` params. `null` means none. */
		params?: SQLInputValue[] | Record<string, SQLInputValue> | null;
	},
	streamOptions?: StreamOptions,
): DatastreamReadable<Record<string, SQLOutputValue>>;

export function sqliteInsertStream(
	options: {
		db: DatabaseSync;
		table: string;
		/** Defaults to the first row's keys (later rows may not add keys); required for array rows. */
		columns?: string[];
	},
	streamOptions?: StreamOptions,
): DatastreamWritable<
	SQLInputValue[] | Record<string, SQLInputValue | undefined>
>;
