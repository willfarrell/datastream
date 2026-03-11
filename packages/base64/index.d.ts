// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

export function base64EncodeStream(
	options?: Record<string, unknown>,
	streamOptions?: StreamOptions,
): unknown;
export function base64DecodeStream(
	options?: Record<string, unknown>,
	streamOptions?: StreamOptions,
): unknown;

declare const _default: {
	encodeStream: typeof base64EncodeStream;
	decodeStream: typeof base64DecodeStream;
};
export default _default;
