// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions } from "@datastream/core";

export interface ProtobufType {
	encode(message: unknown): { finish(): Uint8Array };
	decode(buffer: Uint8Array): unknown;
	toObject(message: unknown): Record<string, unknown>;
}

export function protobufSerializeStream(
	options?: { Type?: ProtobufType },
	streamOptions?: StreamOptions,
): unknown;
export function protobufDeserializeStream(
	options?: { Type?: ProtobufType },
	streamOptions?: StreamOptions,
): unknown;
