// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamTransform,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export type EncryptAlgorithm =
	| "AES-128-GCM"
	| "AES-256-GCM"
	| "AES-128-CTR"
	| "AES-256-CTR"
	| "CHACHA20-POLY1305";

type EncryptStreamResult = DatastreamTransform & {
	result: () => StreamResult<{
		algorithm: EncryptAlgorithm;
		iv: Uint8Array;
		authTag?: Uint8Array;
	}>;
};

export function encryptStream(
	options: {
		algorithm?: EncryptAlgorithm;
		key: Uint8Array | Buffer;
		iv?: Uint8Array | Buffer;
		aad?: Uint8Array | Buffer | null;
		maxInputSize?: number | null;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): Promise<EncryptStreamResult>;

export function decryptStream(
	options: {
		algorithm?: EncryptAlgorithm;
		key: Uint8Array | Buffer;
		iv: Uint8Array | Buffer;
		authTag?: Uint8Array | Buffer;
		aad?: Uint8Array | Buffer | null;
		maxInputSize?: number | null;
		maxOutputSize?: number | null;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamTransform>;

export function generateEncryptionKey(options?: {
	bits?: 128 | 256;
}): Uint8Array;
