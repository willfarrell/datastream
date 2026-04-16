// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { DatastreamTransform, StreamOptions } from "@datastream/core";

export function charsetEncodeStream(
	options?: { charset?: string },
	streamOptions?: StreamOptions,
): DatastreamTransform;
