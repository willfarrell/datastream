// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

export function base64EncodeStream(
	options?: {},
	streamOptions?: StreamOptions,
): DatastreamTransform;
export function base64DecodeStream(
	options?: {},
	streamOptions?: StreamOptions,
): DatastreamTransform;

declare const _default: {
	encodeStream: typeof base64EncodeStream;
	decodeStream: typeof base64DecodeStream;
};
export default _default;
