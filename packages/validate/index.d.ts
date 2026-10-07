// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamTransform,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export interface ValidateError {
	id: string;
	keys: string[];
	message: string;
	idx: number[];
}

export function transpileSchema(
	schema: Record<string, unknown>,
	ajvOptions?: Record<string, unknown>,
): (data: unknown) => boolean;

export function validateStream(
	options: {
		schema: Record<string, unknown> | ((data: unknown) => boolean);
		idxStart?: number;
		onErrorEnqueue?: boolean;
		allowCoerceTypes?: boolean;
		resultKey?: string;
		/** idx entries kept per error id. Default 1000; null = unlimited. */
		maxErrorRows?: number | null;
		/** Distinct error ids kept. Default 1000; null = unlimited. */
		maxErrorKeys?: number | null;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform & {
	result: () => StreamResult<Record<string, ValidateError>>;
};
